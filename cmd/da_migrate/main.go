// Command da_migrate is an MVP enterprise data-migration tool optimized for
// migrating very large numbers of small files between two directory trees
// (for example two NFS-mounted exports). It performs an incremental, parallel,
// buffered copy with bandwidth throttling and crash-resume semantics.
//
// Usage:
//
//	da_migrate --src /mnt/src --dst /mnt/dst [flags]
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"

	"datants/internal/clilog"
	"datants/internal/manifest"
	"datants/internal/plan"
	"datants/internal/scanner"
	"datants/internal/transfer"
)

func main() {
	fs := flag.NewFlagSet("da_migrate", flag.ExitOnError)
	src := fs.String("src", "", "source root directory (may be an NFS mount)")
	dst := fs.String("dst", "", "destination root directory (may be an NFS mount)")
	workers := fs.Int("workers", 0, "number of concurrent copiers (default: GOMAXPROCS)")
	bufferSize := fs.Int("buffer-size", 1<<20, "IO buffer size in bytes")
	rate := fs.Int64("rate", 0, "max bytes/second to write (0 = unlimited)")
	retries := fs.Int("retries", 3, "retries per failed file copy")
	verify := fs.Bool("verify", false, "hash-verify")
	force := fs.Bool("force", false, "copy every file regardless of destination state")
	includeDirs := fs.Bool("include-empty-dirs", false, "also create empty directories")
	syncMeta := fs.Bool("sync-meta", false, "apply mode/owner changes to already-copied files (no content recopy); enables owner preservation on copy")
	deleteStale := fs.Bool("delete-stale", false, "delete destination entries that have no source counterpart (with --include-empty-dirs also prunes stale directories)")
	var ignore listFlag
	fs.Var(&ignore, "ignore", "skip any path containing this fragment (repeatable)")
	logDest := fs.String("log", "", "log file (default /var/log/da_migrate.log; \"-\" for stderr only)")
	quiet := fs.Bool("quiet", false, "only print errors")
	fs.Parse(os.Args[1:])

	if *src == "" || *dst == "" {
		fmt.Fprintln(os.Stderr, "error: --src and --dst are required")
		fs.Usage()
		os.Exit(2)
	}
	if *workers <= 0 {
		*workers = runtime.GOMAXPROCS(0)
	}
	if *verify {
		*force = true
	}

	logger, closeLog := clilog.New("migrate", *logDest)
	defer closeLog()

	var transferred atomic.Int64
	pool := transfer.NewPool(transfer.Config{
		Workers:       *workers,
		BufferSize:    *bufferSize,
		Rate:          *rate,
		Retries:       *retries,
		PreserveOwner: *syncMeta,
		OnResult: func(r transfer.TaskResult) {
			if r.Err != nil {
				if !*quiet {
					logger.Printf("FAIL %s: %v (retries=%d)", r.Task.DstPath, r.Err, r.Retries)
				}
				return
			}
			if r.Task.Kind == plan.CopyTask {
				transferred.Add(r.Bytes)
			}
		},
	})

	srcAbs, err := filepath.Abs(*src)
	if err != nil {
		srcAbs = *src
	}
	dstAbs, err := filepath.Abs(*dst)
	if err != nil {
		dstAbs = *dst
	}

	planner := &plan.Planner{
		Source: plan.Source{
			SrcRoot:      srcAbs,
			DstRoot:      dstAbs,
			Verify:       *verify,
			Force:        *force,
			SyncMeta:     *syncMeta,
			DeleteStale:  *deleteStale,
			DirsIncluded: *includeDirs,
		},
		Emit: func(t plan.Task) error {
			pool.Submit(t)
			return nil
		},
	}

	if !*quiet {
		logger.Printf("migrating %s -> %s with %d workers", srcAbs, dstAbs, *workers)
	}

	stopScan := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	go func() { <-interrupt; close(stopScan) }()

	scanErr := scanner.Scan(srcAbs, scanner.Options{
		Workers:     *workers,
		IncludeDirs: *includeDirs,
		Ignore:      ignore,
	}, func(dir string, recs []manifest.Record) error {
		return planner.CompareDir(dir, recs)
	})

	_ = stopScan
	pool.Close()

	files, failed := pool.Counters()

	if !*quiet {
		logger.Printf("done: %d files, %d bytes, %d failures", files, transferred.Load(), failed)
	}
	if scanErr != nil {
		logger.Printf("scan error: %v", scanErr)
	}
	if failed > 0 {
		os.Exit(1)
	}
}

type listFlag []string

func (l *listFlag) String() string { return fmt.Sprintf("%v", []string(*l)) }
func (l *listFlag) Set(v string) error {
	(*l) = append(*l, v)
	return nil
}
