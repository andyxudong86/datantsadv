// Command da_worker runs a Worker of the distributed migration tool. It connects
// to a Controller over gRPC, claims batches of file-level tasks, copies file
// content directly between the shared source and destination storage, and
// reports results. Run one instance per machine in the migration fleet.
//
// Usage:
//
//	da_worker --controller host:50051 [flags]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"datants/internal/clilog"
	"datants/internal/worker"
)

func main() {
	fs := flag.NewFlagSet("da_worker", flag.ExitOnError)
	controller := fs.String("controller", "", "controller gRPC address host:port (required)")
	id := fs.String("id", "", "worker id (empty: controller assigns)")
	addr := fs.String("address", "", "human-readable address for logs")
	capacity := fs.Int("capacity", 0, "concurrent local copy workers (default GOMAXPROCS)")
	batchSize := fs.Int("batch", 0, "tasks claimed per control-plane round-trip (default 64)")
	bufferSize := fs.Int("buffer-size", 1<<20, "IO buffer size in bytes")
	rate := fs.Int64("rate", 0, "max bytes/second this worker writes (0 = unlimited)")
	heartbeat := fs.Duration("heartbeat", 0, "heartbeat interval (default 10s)")
	syncMeta := fs.Bool("sync-meta", false, "preserve file owner (uid/gid) when copying new files (requires privileges); note: --delete-stale and delete/meta task generation are controller-side --sync-meta/--delete-stale flags")
	logDest := fs.String("log", "", "log file (default /var/log/da_worker.log; \"-\" for stderr only)")
	quiet := fs.Bool("quiet", false, "only print errors")
	fs.Parse(os.Args[1:])

	if *controller == "" {
		fmt.Fprintln(os.Stderr, "error: --controller is required")
		fs.Usage()
		os.Exit(2)
	}
	if *capacity <= 0 {
		*capacity = runtime.GOMAXPROCS(0)
	}

	logger, closeLog := clilog.New("worker", *logDest)
	defer closeLog()

	conn, client, err := worker.DialControl(*controller, true)
	if err != nil {
		logger.Fatalf("dial %s: %v", *controller, err)
	}
	defer conn.Close()

	w := worker.New(client, worker.Config{
		WorkerID:          *id,
		Address:           *addr,
		BatchSize:         *batchSize,
		Capacity:          *capacity,
		BufferSize:        *bufferSize,
		Rate:              *rate,
		HeartbeatInterval: *heartbeat,
		PreserveOwner:     *syncMeta,
	})

	if !*quiet {
		logger.Printf("worker connecting to %s", *controller)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		cancel()
	}()

	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Fatalf("worker: %v", err)
	}
	if !*quiet {
		logger.Printf("worker %s finished", w.ID())
	}
}
