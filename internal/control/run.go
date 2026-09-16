package control

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"datants/internal/history"
)

// RunState is the lifecycle state of the controller's migration activity.
// It governs whether claiming workers are granted new tasks.
type RunState int

const (
	// RunRunning grants new tasks to claiming workers. It is also the default
	// state of a fresh controller, preserving the legacy direct-ingest flow.
	RunRunning RunState = iota
	// RunPaused holds claims; work already claimed and in flight continues.
	RunPaused
	// RunIdle means no run is active: claims are held so leftover tasks are
	// not processed outside of a run.
	RunIdle
)

func (s RunState) String() string {
	switch s {
	case RunRunning:
		return "running"
	case RunPaused:
		return "paused"
	case RunIdle:
		return "idle"
	}
	return "unknown"
}

// RunConfig carries the user-supplied options for a new migration run.
type RunConfig struct {
	Workers     int
	IncludeDirs bool
	SyncMeta    bool
	DeleteStale bool
	Ignore      []string
	// Fresh clears the backing task store first, so every file is re-copied
	// from scratch. Otherwise only changed entries are enqueued, preserving the
	// incremental/resume semantics of the original tool.
	Fresh bool
	// WorkerIPs restricts which workers may pull tasks for this run, by the IP
	// the controller sees them connect from (as shown in /api/workers). Empty
	// or nil means any connected worker may participate.
	WorkerIPs []string
}

// RunTarget is one source→destination directory pair for a migration run.
type RunTarget struct {
	Src string
	Dst string
}

// RunSummary is a cheap snapshot of the active run, for the web layer and tests.
type RunSummary struct {
	ID           string
	Targets      []RunTarget
	StartedAt    time.Time
	FilesDone    int64
	FilesFailed  int64
	BytesDone    int64
	BytesFailed  int64
	ScannedTasks int64
	ScanDone     bool
	WorkerIPs    []string
}

// activeRun holds the mutable bookkeeping of the one run currently in flight.
// Fields are guarded by Controller.mu.
type activeRun struct {
	id          string
	targets     []RunTarget
	cfg         RunConfig
	startedAt   time.Time
	filesDone   int64
	filesFailed int64
	bytesDone   int64
	bytesFailed int64
	scanned     int64
	scanDone    bool
}

// StartRun begins a new migration run src -> dst with the given options. The
// scan+ingest runs in the background so the gRPC/web servers keep serving. The
// run is recorded in the controller's history store when one is configured. At
// most one run may be active at a time.
func (c *Controller) StartRun(src, dst string, cfg RunConfig) (string, error) {
	if src == "" || dst == "" {
		return "", errors.New("control: src and dst are required")
	}
	return c.StartRuns([]RunTarget{{Src: src, Dst: dst}}, cfg)
}

// StartRuns begins a new migration run covering one or more source→destination
// directory pairs. Each pair is scanned independently; all resulting tasks share
// one task store and one run lifecycle.
func (c *Controller) StartRuns(targets []RunTarget, cfg RunConfig) (string, error) {
	if len(targets) == 0 {
		return "", errors.New("control: at least one src→dst target is required")
	}
	for _, t := range targets {
		if t.Src == "" || t.Dst == "" {
			return "", errors.New("control: every target must have both src and dst")
		}
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}

	// Absent an explicit per-run allowlist, restrict claims to the worker pool
	// declared on the Workers page (empty pool = any online worker).
	if len(cfg.WorkerIPs) == 0 {
		cfg.WorkerIPs = c.poolIPs()
	}

	c.mu.Lock()
	if c.active != nil {
		c.mu.Unlock()
		return "", errors.New("control: a run is already active; stop it first")
	}
	if cfg.Fresh {
		if err := c.store.Reset(); err != nil {
			c.mu.Unlock()
			return "", err
		}
	}
	run := &activeRun{startedAt: time.Now(), targets: targets, cfg: cfg}
	if c.cfg.History != nil {
		histTargets := make([]history.Target, len(targets))
		for i, t := range targets {
			histTargets[i] = history.Target{Src: t.Src, Dst: t.Dst}
		}
		h := &history.Run{
			Src:     targets[0].Src,
			Dst:     targets[0].Dst,
			Targets: histTargets,
			Config: history.Config{
				Workers:     cfg.Workers,
				IncludeDirs: cfg.IncludeDirs,
				SyncMeta:    cfg.SyncMeta,
				DeleteStale: cfg.DeleteStale,
				Ignore:      cfg.Ignore,
				WorkerIPs:   cfg.WorkerIPs,
			},
			StartedAt: run.startedAt,
			Status:    history.StatusRunning,
		}
		if err := c.cfg.History.Create(h); err != nil {
			c.mu.Unlock()
			return "", err
		}
		run.id = h.ID
	}
	c.active = run
	c.runState = RunRunning
	c.mu.Unlock()

	go c.runScan(run, run.targets, cfg)
	return run.id, nil
}

// Pause holds task claims until Resume is called. In-flight work continues and
// the run stays active.
func (c *Controller) Pause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil || c.runState != RunRunning {
		return errors.New("control: no running run to pause")
	}
	c.runState = RunPaused
	return nil
}

// Resume re-allows task claims after a Pause.
func (c *Controller) Resume() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil || c.runState != RunPaused {
		return errors.New("control: no paused run to resume")
	}
	c.runState = RunRunning
	// The run may have drained while paused; adopt the completed state now.
	c.finalizeIfDoneLocked()
	return nil
}

// StopRun ends the active run: workers are no longer granted tasks and the run
// is recorded as stopped. Pending tasks are kept in the store so a later
// incremental run can pick them up.
func (c *Controller) StopRun() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		return errors.New("control: no active run to stop")
	}
	c.logf("run %s stopped by operator", c.active.id)
	c.finishRunLocked(c.active, history.StatusStopped)
	c.active = nil
	c.runState = RunIdle
	return nil
}

// RunState returns the current claim-gating state.
func (c *Controller) RunState() RunState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runState
}

// Runs returns the recorded history of past migration runs, most recent first.
func (c *Controller) Runs() []*history.Run {
	if c.cfg.History == nil {
		return nil
	}
	return c.cfg.History.List()
}

// Run returns the recorded run with the given id, if any.
func (c *Controller) Run(id string) (*history.Run, bool) {
	if c.cfg.History == nil {
		return nil, false
	}
	return c.cfg.History.Get(id)
}

// CurrentRun returns a snapshot of the active run, if any.
func (c *Controller) CurrentRun() (RunSummary, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		return RunSummary{}, false
	}
	return RunSummary{
		ID:           c.active.id,
		Targets:      c.active.targets,
		StartedAt:    c.active.startedAt,
		FilesDone:    c.active.filesDone,
		FilesFailed:  c.active.filesFailed,
		BytesDone:    c.active.bytesDone,
		BytesFailed:  c.active.bytesFailed,
		ScannedTasks: c.active.scanned,
		ScanDone:     c.active.scanDone,
	}, true
}

// ReScan re-runs the incremental scan+ingest of the active run, re-enqueueing
// changed files since the first scan. It is a no-op error when no run is
// active, and is skipped while a previous scan is still running.
func (c *Controller) ReScan() error {
	c.mu.Lock()
	if c.active == nil {
		c.mu.Unlock()
		return errors.New("control: no active run to rescan")
	}
	if !c.active.scanDone {
		c.mu.Unlock()
		return errors.New("control: a scan is already in progress")
	}
	run := c.active
	targets, cfg := run.targets, run.cfg
	run.scanDone = false
	c.mu.Unlock()

	c.logf("run %s rescanning %d targets", run.id, len(targets))
	go c.runScan(run, targets, cfg)
	return nil
}

// runScan performs the background scan+ingest for a started run.
func (c *Controller) runScan(run *activeRun, targets []RunTarget, cfg RunConfig) {
	scfg := ScanConfig{
		Workers:     cfg.Workers,
		IncludeDirs: cfg.IncludeDirs,
		Ignore:      cfg.Ignore,
		SyncMeta:    cfg.SyncMeta,
		DeleteStale: cfg.DeleteStale,
	}
	var total int64
	var scanErr error
	for _, tgt := range targets {
		n, err := c.ScanAndIngest(tgt.Src, tgt.Dst, scfg)
		total += n
		if err != nil {
			scanErr = fmt.Errorf("%s -> %s: %w", tgt.Src, tgt.Dst, err)
			break
		}
	}
	c.mu.Lock()
	run.scanned = total
	run.scanDone = true
	c.mu.Unlock()
	if scanErr != nil {
		c.logf("run %s scan failed: %v", run.id, scanErr)
		c.mu.Lock()
		if c.active == run {
			c.active = nil
			c.runState = RunIdle
			c.finishRunLocked(run, history.StatusFailed)
		}
		c.mu.Unlock()
		return
	}
	c.logf("run %s scanned %d tasks across %d targets", run.id, total, len(targets))
	c.finalizeIfDone()
}

// finalizeIfDone completes the active run once scanning finished and the task
// store has nothing pending or in progress left.
func (c *Controller) finalizeIfDone() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finalizeIfDoneLocked()
}

// finalizeIfDoneLocked is finalizeIfDone with c.mu already held.
func (c *Controller) finalizeIfDoneLocked() {
	if c.active == nil || !c.active.scanDone || c.runState != RunRunning {
		return
	}
	st := c.store.Stats()
	if st.Pending > 0 || st.InProgress > 0 {
		return
	}
	run := c.active
	c.active = nil
	c.runState = RunIdle
	if run.id != "" {
		c.logf("run %s complete: done=%d failed=%d", run.id, st.Done, st.Failed)
		c.finishRunLocked(run, history.StatusDone)
	}
}

// persistProgress writes the active run's counters to history. Best-effort:
// history failures are logged, never fatal.
func (c *Controller) persistProgress() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.History == nil || c.active == nil || c.active.id == "" {
		return
	}
	_, err := c.cfg.History.Update(c.active.id, func(h *history.Run) {
		h.Result.FilesDone = c.active.filesDone
		h.Result.FilesFailed = c.active.filesFailed
		h.Result.BytesDone = c.active.bytesDone
		h.Result.BytesFailed = c.active.bytesFailed
	})
	if err != nil {
		c.logf("history: persist run %s: %v", c.active.id, err)
	}
}

// finishRunLocked records the final outcome of a run in history. c.mu must be
// held. Runs without an id (no history configured) are skipped.
func (c *Controller) finishRunLocked(run *activeRun, status history.Status) {
	if c.cfg.History == nil || run.id == "" {
		return
	}
	now := time.Now()
	_, err := c.cfg.History.Update(run.id, func(h *history.Run) {
		h.Status = status
		h.FinishedAt = now
		h.Result.FilesDone = run.filesDone
		h.Result.FilesFailed = run.filesFailed
		h.Result.BytesDone = run.bytesDone
		h.Result.BytesFailed = run.bytesFailed
		if d := now.Sub(run.startedAt); d > 0 {
			h.Result.AvgBytesSec = float64(run.bytesDone) / d.Seconds()
		}
	})
	if err != nil {
		c.logf("history: finalize run %s: %v", run.id, err)
	}
}
