// Command da_control runs the Controller of the distributed migration tool: it
// scans the source tree, diffs against the destination, ingests the resulting
// file-level tasks into a persistent store, and serves the gRPC control plane
// that Workers use to claim and report tasks. It also serves the embedded web
// console (http://127.0.0.1:8080 by default) for run control and monitoring.
//
// Usage:
//
//	da_control --src /mnt/src --dst /mnt/dst --listen :50051 --state /var/lib/migrate
package main

import (
	"context"
	"crypto/subtle"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"datants/internal/clilog"
	"datants/internal/control"
	"datants/internal/history"
	"datants/internal/sshrun"
	"datants/internal/taskstore"
	"datants/internal/webapi"
	"datants/internal/webui"
	"datants/internal/workerctl"
)

var _ = syscall.SIGTERM

func main() {
	fs := flag.NewFlagSet("da_control", flag.ExitOnError)
	src := fs.String("src", "", "source root directory (shared storage)")
	dst := fs.String("dst", "", "destination root directory (shared storage)")
	listen := fs.String("listen", ":50051", "gRPC listen address")
	state := fs.String("state", "", "persistent state directory for the task store (required)")
	workers := fs.Int("workers", 0, "scan workers (default GOMAXPROCS)")
	includeDirs := fs.Bool("include-empty-dirs", false, "also migrate empty directories")
	syncMeta := fs.Bool("sync-meta", false, "apply mode/owner changes to already-copied files (no content recopy); enables owner preservation on copy")
	deleteStale := fs.Bool("delete-stale", false, "delete destination entries that have no source counterpart (with --include-empty-dirs also prunes stale directories)")
	claimTimeout := fs.Duration("claim-timeout", 0, "reclaim timeout for in-progress tasks (default 5m)")
	reclaimInterval := fs.Duration("reclaim-interval", 0, "stale-claim scan interval (default 30s)")
	batch := fs.Int("batch", 0, "default tasks per claim (default 64)")
	var ignore multiFlag
	fs.Var(&ignore, "ignore", "skip any path containing this fragment (repeatable)")
	logDest := fs.String("log", "", "log file (default /var/log/da_control.log; \"-\" for stderr only)")
	quiet := fs.Bool("quiet", false, "only print errors")
	httpAddr := fs.String("http", "127.0.0.1:8080", "web console listen address (\"\" disables the console)")
	webAuth := fs.String("web-auth", "", "optional HTTP basic auth for the console as user:pass")
	workerBin := fs.String("worker-bin", "", "path to da_worker so the console can launch local worker processes (\"\" disables the feature)")
	sshUser := fs.String("ssh-user", "", "SSH account used to launch pool workers on remote nodes (\"\" disables remote launch)")
	sshPort := fs.Int("ssh-port", 22, "SSH port used for remote pool workers")
	sshKey := fs.String("ssh-key", "", "SSH private key for remote pool workers (default ~/.ssh/id_rsa or ~/.ssh/id_ed25519)")
	sshPassword := fs.String("ssh-password", "", "SSH password for remote pool workers (fallback when no key); prefer --ssh-key")
	sshRemoteDir := fs.String("ssh-remote-dir", "/usr/local/datantsadv", "remote directory where da_worker is uploaded and run")
	sshCtrlAddr := fs.String("ssh-ctrl-addr", "", "controller address remote pool workers dial to register (default: derived from --listen, using the first non-loopback IP)")
	fs.Parse(os.Args[1:])

	if *state == "" {
		fmt.Fprintln(os.Stderr, "error: --state is required")
		fs.Usage()
		os.Exit(2)
	}
	if (*src == "") != (*dst == "") {
		fmt.Fprintln(os.Stderr, "error: --src and --dst must be provided together; omit both to start in web-only standby mode")
		fs.Usage()
		os.Exit(2)
	}
	// Legacy mode: with --src/--dst the first run starts automatically at boot
	// (systemd/script friendly). Without them da_control stands by and runs are
	// started from the web console / POST /api/start.
	autoStart := *src != "" && *dst != ""
	if *workers <= 0 {
		*workers = runtime.GOMAXPROCS(0)
	}

	logger, closeLog := clilog.New("control", *logDest)
	defer closeLog()

	srcAbs, err := filepath.Abs(*src)
	if err != nil {
		srcAbs = *src
	}
	dstAbs, err := filepath.Abs(*dst)
	if err != nil {
		dstAbs = *dst
	}

	store, err := taskstore.New(*state)
	if err != nil {
		logger.Fatalf("taskstore: %v", err)
	}

	hist, err := history.New(filepath.Join(*state, "history"))
	if err != nil {
		logger.Fatalf("history: %v", err)
	}

	ctrl := control.NewController(store, control.Config{
		ClaimTimeout:    *claimTimeout,
		ReclaimInterval: *reclaimInterval,
		DefaultBatch:    *batch,
		Logger:          logger,
		History:         hist,
		WorkerPoolFile:  filepath.Join(*state, "workerpool.json"),
	})

	if autoStart {
		// scan + ingest (incremental: only changed entries become tasks)
		if !*quiet {
			logger.Printf("starting run: %s -> %s", srcAbs, dstAbs)
		}
		runID, err := ctrl.StartRun(srcAbs, dstAbs, control.RunConfig{
			Workers:     *workers,
			IncludeDirs: *includeDirs,
			Ignore:      ignore,
			SyncMeta:    *syncMeta,
			DeleteStale: *deleteStale,
		})
		if err != nil {
			store.Done()
			logger.Fatalf("start run: %v", err)
		}
		if !*quiet {
			logger.Printf("run %s started", runID)
		}
	} else {
		// Standby: hold claims so workers cannot pull leftover tasks from an
		// earlier (stopped) run's store out of band. A console/API run start
		// flips the state back to running.
		ctrl.SetIdle()
		if !*quiet {
			logger.Printf("standby mode: no --src/--dst, awaiting run start from web console or API")
		}
	}

	ctrl.Start()
	defer ctrl.Stop()

	webUser, webPass := "", ""
	if *webAuth != "" {
		webUser, webPass, _ = strings.Cut(*webAuth, ":")
		if webUser == "" || webPass == "" {
			logger.Fatalf("--web-auth must be user:pass")
		}
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Fatalf("listen %s: %v", *listen, err)
	}
	if !*quiet {
		logger.Printf("control listening on %s", *listen)
	}

	// Optional management of local worker processes from the web console.
	var spawner *workerctl.Manager
	var spawnCfg webapi.SpawnConfig
	if *workerBin != "" {
		if fi, err := os.Stat(*workerBin); err != nil || fi.IsDir() {
			logger.Fatalf("--worker-bin %s: not a valid executable", *workerBin)
		}
		logDir := filepath.Join(*state, "workers")
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			logger.Fatalf("worker log dir: %v", err)
		}
		spawner = workerctl.NewManager(nil)
		spawnCfg = webapi.SpawnConfig{
			WorkerBin:      *workerBin,
			ControllerAddr: localConnectAddr(lis.Addr().String()),
			LogDir:         logDir,
			// console-launched workers stay resident (idle-poll) so operators
			// can bring them up before starting a run
			IdlePoll: 3 * time.Second,
		}
		if !*quiet {
			logger.Printf("console may launch local workers from bin=%s (thread to %s)", *workerBin, spawnCfg.ControllerAddr)
		}
	}

	// Optional remote launch of pool workers over SSH (uploads da_worker to
	// each node, then runs it there). Needs --worker-bin and --ssh-user.
	var remote *sshrun.Manager
	if *workerBin != "" && *sshUser != "" {
		ctrlAddr := *sshCtrlAddr
		if ctrlAddr == "" {
			ctrlAddr = externalConnectAddr(lis.Addr().String())
		}
		remote = sshrun.NewManager(sshrun.Config{
			User:        *sshUser,
			Port:        *sshPort,
			KeyFile:     *sshKey,
			Password:    *sshPassword,
			WorkerBin:   *workerBin,
			CtrlAddr:    ctrlAddr,
			RemoteDir:   *sshRemoteDir,
			IdlePoll:    3 * time.Second,
			DialTimeout: 10 * time.Second,
			Logger:      logger,
		})
		if !*quiet {
			logger.Printf("console may launch pool workers over SSH (%s), nodes dial %s", *sshUser, ctrlAddr)
		}
	}

	webHandler := buildWebHandler(ctrl, webUser, webPass, spawner, spawnCfg, remote)
	var httpSrv *http.Server
	if *httpAddr != "" {
		httpSrv = &http.Server{
			Addr:              *httpAddr,
			Handler:           webHandler,
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatalf("http serve %s: %v", *httpAddr, err)
			}
		}()
		if !*quiet {
			logger.Printf("web console on http://%s", *httpAddr)
		}
	}

	grpcServer := grpc.NewServer()
	ctrl.RegisterServer(grpcServer)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		if err := ctrl.StopRun(); err != nil && !*quiet {
			logger.Printf("stop run: %v", err)
		}
		if spawner != nil {
			if err := spawner.StopAll(); err != nil && !*quiet {
				logger.Printf("stop spawned workers: %v", err)
			}
		}
		if remote != nil {
			if err := remote.StopAll(); err != nil && !*quiet {
				logger.Printf("stop remote workers: %v", err)
			}
		}
		if httpSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			httpSrv.Shutdown(ctx)
			cancel()
		}
		grpcServer.GracefulStop()
	}()

	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("grpc serve: %v", err)
	}
}

type multiFlag []string

func (m *multiFlag) String() string { return fmt.Sprintf("%v", []string(*m)) }
func (m *multiFlag) Set(v string) error {
	(*m) = append(*m, v)
	return nil
}

// buildWebHandler assembles the web console: the webapi REST/SSE handlers
// mounted behind the embedded SPA, wrapped in optional HTTP basic auth.
func buildWebHandler(ctl *control.Controller, user, pass string, spawner *workerctl.Manager, spawnCfg webapi.SpawnConfig, remote *sshrun.Manager) http.Handler {
	opts := []webapi.Option{webapi.WithWorkerSpawning(spawner, spawnCfg)}
	if remote != nil {
		opts = append(opts, webapi.WithRemoteSpawning(remote))
	}
	web := webapi.NewServer(ctl, opts...)
	h := webui.Handler(web.Handler())
	if user != "" {
		h = basicAuth(user, pass, h)
	}
	return h
}

// localConnectAddr turns a bound listener address into the address a worker on
// the same host should dial: wildcard hosts (0.0.0.0/[::]/empty) become
// 127.0.0.1 while a concrete host is kept as-is.
func localConnectAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// externalConnectAddr is like localConnectAddr but for machines that must
// reach the controller over the network: a wildcard listen host resolves to the
// first non-loopback IPv4 of this host.
func externalConnectAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	switch host {
	case "", "0.0.0.0", "::":
		if ip := firstIPv4(); ip != "" {
			host = ip
		}
	case "*":
		if ip := firstIPv4(); ip != "" {
			host = ip
		}
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func firstIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok {
			ip := ipn.IP.To4()
			if ip != nil && !ip.IsLoopback() {
				return ip.String()
			}
		}
	}
	return ""
}

// basicAuth guards h with HTTP Basic authentication against user:pass.
func basicAuth(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(u), []byte(user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="datants"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
