// Package transfer executes the copy work items produced by the planner using a
// bounded pool of concurrent workers. It targets the "massive number of small
// files" problem by:
//
//   - running many workers in parallel to hide per-file latency;
//   - reusing a pool of large IO buffers so small files are written with a
//     single buffered pass rather than many small syscalls;
//   - MkdirAll-ing destination directories so nested trees are built on demand;
//   - preserving mtime and mode after copy, so later incremental runs skip the
//     file.
//
// A dedicated consumer goroutine drains task results continuously, so producers
// may stream an unbounded number of tasks without deadlocking.
package transfer

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"datants/internal/fsinfo"
	"datants/internal/plan"
	"datants/internal/throttle"
)

// Config controls the transfer behaviour.
type Config struct {
	// Workers is the number of concurrent copiers.
	Workers int
	// BufferSize is the size of each IO buffer used per copy. Defaults to 1MiB.
	BufferSize int
	// Rate limits aggregate bytes/second (0 = unlimited).
	Rate int64
	// Retries is how many times a failed copy is retried before giving up.
	Retries int
	// PreserveOwner, when true, applies the task's uid/gid to copied and
	// meta-updated destination files. Off by default; requires privileges on
	// the destination filesystem.
	PreserveOwner bool
	// OnResult, if set, is called for every completed task as results arrive.
	OnResult func(TaskResult)
}

// TaskResult reports the outcome of a single transfer task.
type TaskResult struct {
	Task    plan.Task
	Bytes   int64
	Err     error
	Retries int
}

// Pool is a bounded set of copier workers processing a stream of tasks.
type Pool struct {
	cfg       Config
	limiter   *throttle.Limiter
	work      chan plan.Task
	results   chan TaskResult
	wg        sync.WaitGroup // copier workers
	consumer  sync.WaitGroup // result consumer
	bufPool   sync.Pool
	copied    atomic.Int64
	failed    atomic.Int64
	cbMu      sync.Mutex
	cb        func(TaskResult)
}

// NewPool creates a worker pool configured for the transfer.
func NewPool(cfg Config) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1 << 20
	}
	p := &Pool{
		cfg:     cfg,
		limiter: throttle.New(cfg.Rate),
		work:    make(chan plan.Task, cfg.Workers*2),
		results: make(chan TaskResult, cfg.Workers*4),
		cb:      cfg.OnResult,
	}
	p.bufPool.New = func() interface{} {
		b := make([]byte, cfg.BufferSize)
		return &b
	}
	p.consumer.Add(1)
	go p.consume()
	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *Pool) consume() {
	defer p.consumer.Done()
	for r := range p.results {
		if r.Err != nil {
			p.failed.Add(1)
		} else if r.Task.Kind == plan.CopyTask {
			p.copied.Add(1)
		}
		if cb := p.onResult(); cb != nil {
			cb(r)
		}
	}
}

// SetOnResult attaches (or replaces) the per-result callback. It must be called
// before the first task is submitted to avoid races.
func (p *Pool) SetOnResult(cb func(TaskResult)) {
	p.cbMu.Lock()
	p.cb = cb
	p.cbMu.Unlock()
}

func (p *Pool) onResult() func(TaskResult) {
	p.cbMu.Lock()
	defer p.cbMu.Unlock()
	return p.cb
}

// Submit enqueues a task for processing. It blocks if the pool is saturated.
func (p *Pool) Submit(t plan.Task) {
	p.work <- t
}

// Close signals that no more tasks will be submitted and waits for every task
// to complete and every result to be consumed.
func (p *Pool) Close() {
	close(p.work)
	p.wg.Wait()
	close(p.results)
	p.consumer.Wait()
}

// Counters returns the number of completed/failed copy tasks so far.
func (p *Pool) Counters() (copied, failed int64) { return p.copied.Load(), p.failed.Load() }

func (p *Pool) worker() {
	defer p.wg.Done()
	for t := range p.work {
		p.results <- p.do(t)
	}
}

func (p *Pool) do(t plan.Task) TaskResult {
	if t.Kind == plan.MkdirTask {
		return TaskResult{Task: t, Err: os.MkdirAll(t.DstPath, defaultDirMode)}
	}
	if t.Kind == plan.DeleteTask {
		return TaskResult{Task: t, Err: os.RemoveAll(t.DstPath)}
	}
	if t.Kind == plan.MetaTask {
		return TaskResult{Task: t, Err: p.applyMeta(t)}
	}
	var lastErr error
	retries := 0
	for attempt := 0; attempt <= p.cfg.Retries; attempt++ {
		if attempt > 0 {
			retries = attempt
			time.Sleep(backoff(attempt))
		}
		lastErr = p.copyFile(t)
		if lastErr == nil {
			break
		}
	}
	return TaskResult{Task: t, Bytes: t.Size, Err: lastErr, Retries: retries}
}

// applyMeta updates mode and ownership of an existing destination entry. It is
// the only path that carries a mode/owner change for an otherwise-unchanged
// file, so the chown is NOT gated on PreserveOwner: a META task requesting an
// uid/gid change must apply it, and if the worker lacks permission the task
// fails loudly (reported to the controller) instead of silently succeeding.
func (p *Pool) applyMeta(t plan.Task) error {
	if err := os.Chmod(t.DstPath, os.FileMode(t.Mode).Perm()); err != nil {
		return err
	}
	if err := fsinfo.Chown(t.DstPath, t.Uid, t.Gid); err != nil {
		return err
	}
	return nil
}

func (p *Pool) copyFile(t plan.Task) error {
	if err := os.MkdirAll(filepath.Dir(t.DstPath), defaultDirMode); err != nil {
		return err
	}
	// Write to a temp file in the same directory, then atomically rename, so a
	// crash never leaves a half-written file masquerading as a completed one.
	tmp := t.DstPath + ".mig.tmp"
	src, err := os.Open(t.SrcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, defaultFileMode)
	if err != nil {
		return err
	}
	buf := *p.bufPool.Get().(*[]byte)
	defer p.bufPool.Put(&buf)
	_, copyErr := copyStream(dst, src, buf, p.limiter)
	closeErr := dst.Close()
	if copyErr != nil {
		os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		os.Remove(tmp)
		return closeErr
	}
	// preserve metadata
	_ = os.Chtimes(tmp, time.Unix(t.ModTime, 0), time.Unix(t.ModTime, 0))
	_ = os.Chmod(tmp, os.FileMode(t.Mode))
	if p.cfg.PreserveOwner {
		if err := fsinfo.Chown(tmp, t.Uid, t.Gid); err != nil {
			os.Remove(tmp)
			return err
		}
	}
	if err := os.Rename(tmp, t.DstPath); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func copyStream(dst, src *os.File, buf []byte, l *throttle.Limiter) (int64, error) {
	var written int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			l.Wait(int64(n))
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return written, werr
			}
			written += int64(n)
		}
		if err == io.EOF {
			return written, nil
		}
		if err != nil {
			return written, err
		}
	}
}

func backoff(attempt int) time.Duration {
	d := time.Duration(attempt) * 500 * time.Millisecond
	if d > 10*time.Second {
		d = 10 * time.Second
	}
	return d
}

const (
	defaultDirMode  = 0o755
	defaultFileMode = 0o644
)
