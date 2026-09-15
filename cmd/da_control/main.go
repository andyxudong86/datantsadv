// Command da_control runs the Controller of the distributed migration tool: it
// scans the source tree, diffs against the destination, ingests the resulting
// file-level tasks into a persistent store, and serves the gRPC control plane
// that Workers use to claim and report tasks.
//
// Usage:
//
//	da_control --src /mnt/src --dst /mnt/dst --listen :50051 --state /var/lib/migrate
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"google.golang.org/grpc"

	"datants/internal/clilog"
	"datants/internal/control"
	"datants/internal/taskstore"
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

	ctrl := control.NewController(store, control.Config{
		ClaimTimeout:    *claimTimeout,
		ReclaimInterval: *reclaimInterval,
		DefaultBatch:    *batch,
		Logger:          logger,
	})

	// scan + ingest (incremental: only changed entries become tasks)
	if !*quiet {
		logger.Printf("scanning %s -> %s", srcAbs, dstAbs)
	}
	n, err := ctrl.ScanAndIngest(srcAbs, dstAbs, control.ScanConfig{
		Workers:     *workers,
		IncludeDirs: *includeDirs,
		Ignore:      ignore,
		SyncMeta:    *syncMeta,
		DeleteStale: *deleteStale,
	})
	if err != nil {
		store.Done()
		logger.Fatalf("scan/ingest: %v", err)
	}
	if !*quiet {
		st := ctrl.Stats()
		logger.Printf("ingested %d new tasks (pending=%d)", n, st.Pending)
	}

	ctrl.Start()
	defer ctrl.Stop()

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
