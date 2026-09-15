// Package scanner performs a parallel, breadth-first walk of a directory tree,
// grouping the discovered file records by their parent directory. Each directory
// is handed to a callback as soon as it has been read, so memory usage scales
// with the size of a single directory rather than the whole tree — enabling
// scans of tens of millions of files without exhausting RAM.
package scanner

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"datants/internal/fsinfo"
	"datants/internal/manifest"
)

// OnDir is invoked for every directory that is fully enumerated. The records
// are the immediate children of dir (files, and optionally subdirectories of
// dir if IncludeDirs is set). The implementation may run concurrently.
type OnDir func(dir string, recs []manifest.Record) error

// Options controls the scan behaviour.
type Options struct {
	// Workers is the number of concurrent directory readers. Defaults to GOMAXPROCS.
	Workers int
	// IncludeDirs also yields directory records (in addition to descending into
	// them). Off by default.
	IncludeDirs bool
	// Filter, if set, decides whether a discovered entry is included in output.
	// It is not called for the root itself.
	Filter func(path string, entry fs.DirEntry) (bool, error)
	// SkipErrors suppresses permission / IO errors instead of aborting the scan.
	SkipErrors bool
	// Ignore is an optional set of path fragments; any path containing one of
	// these fragments is skipped entirely (files and subtrees).
	Ignore []string
}

// scanner walks a tree with a pool of workers consuming directories from an
// unbounded FIFO. A tracked pending-count ensures the queue is only closed once
// no further enqueues are possible, avoiding both premature close and deadlock.
// The queue is unbounded (mutex + cond) so that a single worker never deadlocks
// while enqueueing child directories it just discovered.
type scanner struct {
	opts    Options
	mu      sync.Mutex
	cond    *sync.Cond
	queue   []string // FIFO of directories awaiting processing
	closed  bool     // set when no further enqueues are possible
	pending int      // directories enqueued but not yet processed
	root    string
	onDir   OnDir
	skipFn  func(string) bool
}

func newScanner(opts Options, onDir OnDir) *scanner {
	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	s := &scanner{
		opts:    opts,
		queue:   nil,
		pending: 0,
		onDir:   onDir,
	}
	s.cond = sync.NewCond(&s.mu)
	s.skipFn = func(p string) bool {
		for _, frag := range opts.Ignore {
			if strings.Contains(p, frag) {
				return true
			}
		}
		return false
	}
	return s
}

// enqueue adds a directory, marking it as pending before pushing it onto the
// FIFO. It never blocks: the queue is unbounded, and callers run inside worker
// goroutines, so a blocking send here could deadlock a single-worker scan.
func (s *scanner) enqueue(dir string) {
	s.mu.Lock()
	s.pending++
	s.queue = append(s.queue, dir)
	s.cond.Broadcast()
	s.mu.Unlock()
}

// pop removes the next directory to process, blocking until one is available or
// the queue has been closed. Returns ok=false once closed and drained.
func (s *scanner) pop() (string, bool) {
	s.mu.Lock()
	for len(s.queue) == 0 && !s.closed {
		s.cond.Wait()
	}
	if len(s.queue) == 0 {
		s.mu.Unlock()
		return "", false
	}
	dir := s.queue[0]
	s.queue = s.queue[1:]
	if len(s.queue) == 0 {
		s.queue = nil
	}
	s.mu.Unlock()
	return dir, true
}

// finished decrements pending and wakes any supervisor waiting for zero.
func (s *scanner) finished() {
	s.mu.Lock()
	s.pending--
	zero := s.pending == 0
	s.mu.Unlock()
	if zero {
		s.cond.Broadcast()
	}
}

// supervisor closes the queue once all directories have been processed.
func (s *scanner) supervisor() {
	s.mu.Lock()
	for s.pending > 0 {
		s.cond.Wait()
	}
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *scanner) processDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		// even on failure we must mark this dir finished so the supervisor can
		// eventually close the queue; children were never enqueued.
		s.finished()
		return err
	}
	var recs []manifest.Record
	var subdirs []string
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())
		if s.skipFn(path) {
			continue
		}
		if s.opts.Filter != nil {
			ok, ferr := s.opts.Filter(path, e)
			if ferr != nil {
				s.finished()
				return ferr
			}
			if !ok {
				continue
			}
		}
		info, ierr := e.Info()
		if ierr != nil {
			s.finished()
			return ierr
		}
		if e.IsDir() {
			subdirs = append(subdirs, path)
			if s.opts.IncludeDirs {
				recs = append(recs, toRecord(path, info, true))
			}
			continue
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			if s.opts.IncludeDirs {
				recs = append(recs, toRecord(path, info, false))
			}
			continue
		}
		recs = append(recs, toRecord(path, info, false))
	}
	manifest.SortRecords(dir, recs)
	// deliver current directory's records before enqueuing children
	var cbErr error
	if s.onDir != nil {
		cbErr = s.onDir(dir, recs)
	}
	for _, sd := range subdirs {
		s.enqueue(sd)
	}
	s.finished()
	return cbErr
}

// Scan walks root and delivers each directory's records to onDir. The callback
// may be invoked concurrently from multiple workers.
func Scan(root string, opts Options, onDir OnDir) error {
	s := newScanner(opts, onDir)
	s.root = filepath.Clean(root)

	go s.supervisor()
	s.enqueue(s.root)

	var firstErr error
	var errMu sync.Mutex
	recordErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	var workers sync.WaitGroup
	for i := 0; i < s.opts.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				dir, ok := s.pop()
				if !ok {
					return
				}
				if err := s.processDir(dir); err != nil {
					if !opts.SkipErrors {
						recordErr(err)
					}
				}
			}
		}()
	}
	workers.Wait()

	errMu.Lock()
	defer errMu.Unlock()
	return firstErr
}

func toRecord(path string, info fs.FileInfo, isDir bool) manifest.Record {
	uid, gid := fsinfo.Owner(info)
	return manifest.Record{
		Path:    filepath.Clean(path),
		Size:    info.Size(),
		ModTime: info.ModTime().Unix(),
		Mode:    uint32(info.Mode()),
		Uid:     uid,
		Gid:     gid,
		IsDir:   isDir,
	}
}

// IsNotExist reports whether err is os.ErrNotExist from a Scan.
func IsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
