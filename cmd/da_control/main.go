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
	"datants/internal/taskstore"
	"datants/internal/webapi"
	"datants/internal/webui"
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
	fs.Parse(os.Args[1:])

	if *src == "" || *dst == "" || *state == "" {
		fmt.Fprintln(os.Stderr, "error: --src, --dst and --state are required")
		fs.Usage()
		os.Exit(2)
	}
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
	})

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

	ctrl.Start()
	defer ctrl.Stop()

	webUser, webPass := "", ""
	if *webAuth != "" {
		webUser, webPass, _ = strings.Cut(*webAuth, ":")
		if webUser == "" || webPass == "" {
			logger.Fatalf("--web-auth must be user:pass")
		}
	}

	webHandler := buildWebHandler(ctrl, webUser, webPass)
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

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Fatalf("listen %s: %v", *listen, err)
	}
	grpcServer := grpc.NewServer()
	ctrl.RegisterServer(grpcServer)

	if !*quiet {
		logger.Printf("control listening on %s", *listen)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		if err := ctrl.StopRun(); err != nil && !*quiet {
			logger.Printf("stop run: %v", err)
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
func buildWebHandler(ctl *control.Controller, user, pass string) http.Handler {
	h := webui.Handler(webapi.NewServer(ctl).Handler())
	if user != "" {
		h = basicAuth(user, pass, h)
	}
	return h
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
