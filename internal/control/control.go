// Package control implements the Controller side of the distributed migration
// system: a single orchestrator that hands out file-level migration tasks to a
// fleet of Workers over gRPC and tracks their progress and liveness.
//
// Task distribution uses a pull/claim model for natural load balancing and
// fault tolerance: Workers call Claim to receive their next batch, do the copy
// on the shared storage, then Report the outcome. In-progress tasks whose
// claim times out are reclaimed by the Controller and re-queued so a crashed or
// stalled Worker never leaves work undone.
package control

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	migratev1 "datants/gen/migrate/v1"
	"datants/internal/history"
	"datants/internal/taskstore"
)

// Config controls the Controller behaviour.
type Config struct {
	// ClaimTimeout is how long a claimed (in-progress) task may be held before
	// the Controller assumes its Worker died and reclaims it. Default 5m.
	ClaimTimeout time.Duration
	// ReclaimInterval is how often the Controller scans for stale claims.
	ReclaimInterval time.Duration
	// DefaultBatch is the number of tasks handed to a Worker per Claim when the
	// worker does not request a specific batch size.
	DefaultBatch int
	// Logger, if non-nil, receives progress and per-report failure details.
	// Leave nil in tests/programmatic use to keep the controller silent.
	Logger *log.Logger
	// ProgressInterval is how often cumulative stats are logged. Default 10s.
	ProgressInterval time.Duration
	// History, if set, records each migration run (status + summary) for the
	// web UI and for post-hoc inspection of past migrations.
	History *history.Store
}

// Controller is the orchestrator. It is safe for concurrent use.
type Controller struct {
	migratev1.UnimplementedControlServer

	store  *taskstore.Store
	cfg    Config
	logger *log.Logger
	mu     sync.Mutex
	nextID int
	// workers tracks liveness: id -> last heartbeat time.
	workers map[string]time.Time
	// runState gates task claims (see RunState). Guarded by mu.
	runState RunState
	// active tracks the single run currently in flight. Guarded by mu.
	active *activeRun
	quit   chan struct{}
	wg     sync.WaitGroup
}

// NewController creates a Controller over the given persistent task store.
func NewController(store *taskstore.Store, cfg Config) *Controller {
	if cfg.ClaimTimeout <= 0 {
		cfg.ClaimTimeout = 5 * time.Minute
	}
	if cfg.ReclaimInterval <= 0 {
		cfg.ReclaimInterval = 30 * time.Second
	}
	if cfg.DefaultBatch <= 0 {
		cfg.DefaultBatch = 64
	}
	if cfg.ProgressInterval <= 0 {
		cfg.ProgressInterval = 10 * time.Second
	}
	return &Controller{
		store:    store,
		cfg:      cfg,
		logger:   cfg.Logger,
		nextID:   1,
		workers:  map[string]time.Time{},
		runState: RunRunning, // legacy direct-ingest flow grants claims by default
		quit:     make(chan struct{}),
	}
}

// IngestTasks adds a batch of tasks to the store. Used by the scan phase.
func (c *Controller) IngestTasks(tasks []taskstore.Task) error {
	return c.store.Ingest(tasks)
}

// Start launches the background stale-claim reclaimer and (when a logger is
// configured) a periodic progress reporter. Call Stop to shut down.
func (c *Controller) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(c.cfg.ReclaimInterval)
		defer t.Stop()
		for {
			select {
			case <-c.quit:
				return
			case <-t.C:
				n, err := c.store.ReclaimStale(c.cfg.ClaimTimeout, time.Now())
				if err == nil && n > 0 {
					// reclaimed tasks wait for another worker to pick them up
				}
				c.finalizeIfDone()
			}
		}
	}()
	if c.logger == nil {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(c.cfg.ProgressInterval)
		defer t.Stop()
		for {
			select {
			case <-c.quit:
				return
			case <-t.C:
				st := c.store.Stats()
				c.logger.Printf("progress: done=%d failed=%d in-progress=%d pending=%d",
					st.Done, st.Failed, st.InProgress, st.Pending)
				c.persistProgress()
				c.finalizeIfDone()
			}
		}
	}()
}

// Stop halts background loops, finalizes any in-flight run as stopped, and
// flushes/compacts the store.
func (c *Controller) Stop() error {
	c.mu.Lock()
	if c.active != nil {
		c.finishRunLocked(c.active, history.StatusStopped)
		c.active = nil
		c.runState = RunIdle
	}
	c.mu.Unlock()
	close(c.quit)
	c.wg.Wait()
	return c.store.Done()
}

// Register implements migratev1.ControlServer.
func (c *Controller) Register(ctx context.Context, req *migratev1.RegisterRequest) (*migratev1.RegisterResponse, error) {
	id := req.GetWorkerId()
	if id == "" {
		c.mu.Lock()
		id = fmt.Sprintf("worker-%d", c.nextID)
		c.nextID++
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.workers[id] = time.Now()
	c.mu.Unlock()
	return &migratev1.RegisterResponse{WorkerId: id}, nil
}

// Claim implements migratev1.ControlServer.
func (c *Controller) Claim(ctx context.Context, req *migratev1.ClaimRequest) (*migratev1.ClaimResponse, error) {
	c.mu.Lock()
	claimsAllowed := c.runState == RunRunning
	c.mu.Unlock()
	if !claimsAllowed {
		// paused, idle or stopped: hold all tasks until the run resumes.
		return &migratev1.ClaimResponse{}, nil
	}
	batch := int(req.GetBatchSize())
	if batch <= 0 {
		batch = c.cfg.DefaultBatch
	}
	tsk, err := c.store.Claim(req.GetWorkerId(), batch)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &migratev1.ClaimResponse{}
	for _, t := range tsk {
		resp.Tasks = append(resp.Tasks, toProto(t))
	}
	return resp, nil
}

// Report implements migratev1.ControlServer.
func (c *Controller) Report(ctx context.Context, req *migratev1.ReportRequest) (*migratev1.ReportResponse, error) {
	var okIDs, failIDs []string
	var firstErrs []string
	for _, r := range req.GetResults() {
		if r.GetOk() {
			okIDs = append(okIDs, r.GetTaskId())
		} else {
			failIDs = append(failIDs, r.GetTaskId())
			if len(firstErrs) < 3 {
				firstErrs = append(firstErrs, fmt.Sprintf("%s: %s", r.GetTaskId(), r.GetError()))
			}
		}
	}
	if c.logger != nil && len(failIDs) > 0 {
		c.logger.Printf("report: worker=%s ok=%d failed=%d (e.g. %q)", req.GetWorkerId(), len(okIDs), len(failIDs), firstErrs)
	}

	// Account the payload into the active run (bytes/files). Fails on the run
	// are double-counted in store.Fail below, but the run summary only cares
	// about this run's outcome totals.
	c.mu.Lock()
	if r := c.active; r != nil {
		r.filesDone += int64(len(okIDs))
		r.filesFailed += int64(len(failIDs))
		for _, res := range req.GetResults() {
			if res.GetOk() {
				r.bytesDone += res.GetBytes()
			} else {
				r.bytesFailed += res.GetBytes()
			}
		}
	}
	c.mu.Unlock()

	if len(okIDs) > 0 {
		if err := c.store.Complete(okIDs); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if len(failIDs) > 0 {
		if err := c.store.Fail(failIDs); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &migratev1.ReportResponse{}, nil
}

// Heartbeat implements migratev1.ControlServer.
func (c *Controller) Heartbeat(ctx context.Context, req *migratev1.HeartbeatRequest) (*migratev1.HeartbeatResponse, error) {
	c.mu.Lock()
	c.workers[req.GetWorkerId()] = time.Now()
	c.mu.Unlock()
	return &migratev1.HeartbeatResponse{Ok: true}, nil
}

// Stats returns the store's current task distribution.
func (c *Controller) Stats() taskstore.Stats { return c.store.Stats() }

// logf logs through the controller's logger when one is configured.
func (c *Controller) logf(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
}

// RegisterServer registers this controller on a gRPC server.
func (c *Controller) RegisterServer(s grpc.ServiceRegistrar) {
	migratev1.RegisterControlServer(s, c)
}

// toProto converts a store Task into its protobuf form.
func toProto(t taskstore.Task) *migratev1.Task {
	kind := migratev1.Task_KIND_COPY
	switch t.Kind {
	case taskstore.KindMkdir:
		kind = migratev1.Task_KIND_MKDIR
	case taskstore.KindMeta:
		kind = migratev1.Task_KIND_META
	case taskstore.KindDelete:
		kind = migratev1.Task_KIND_DELETE
	}
	return &migratev1.Task{
		Id:      t.ID,
		Kind:    kind,
		SrcPath: t.SrcPath,
		DstPath: t.DstPath,
		Size:    t.Size,
		ModTime: t.ModTime,
		Mode:    t.Mode,
		Uid:     t.Uid,
		Gid:     t.Gid,
	}
}

// FromProto converts a protobuf Task into its store form.
func FromProto(t *migratev1.Task) taskstore.Task {
	kind := taskstore.KindCopy
	switch t.GetKind() {
	case migratev1.Task_KIND_MKDIR:
		kind = taskstore.KindMkdir
	case migratev1.Task_KIND_META:
		kind = taskstore.KindMeta
	case migratev1.Task_KIND_DELETE:
		kind = taskstore.KindDelete
	}
	return taskstore.Task{
		ID:      t.GetId(),
		Kind:    kind,
		SrcPath: t.GetSrcPath(),
		DstPath: t.GetDstPath(),
		Size:    t.GetSize(),
		ModTime: t.GetModTime(),
		Mode:    t.GetMode(),
		Uid:     t.GetUid(),
		Gid:     t.GetGid(),
	}
}
