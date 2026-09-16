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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
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
	// WorkerTimeout is how long without a heartbeat a worker is considered
	// disconnected. Default 60s.
	WorkerTimeout time.Duration
	// WorkerPoolFile, if set, is a JSON file the controller loads the declared
	// worker pool from at boot and rewrites whenever SetWorkerPool is called.
	WorkerPoolFile string
}

// workerState is the Controller's per-worker bookkeeping, guarded by mu.
type workerState struct {
	id          string
	ip          string
	address     string
	capacity    int
	joined      time.Time
	lastSeen    time.Time
	filesDone   int64
	filesFailed int64
	bytesDone   int64
}

// WorkerInfo is a public snapshot of a worker's liveness and cumulative
// counters, for the web layer.
type WorkerInfo struct {
	ID          string
	IP          string
	Address     string
	Capacity    int
	Joined      time.Time
	LastSeen    time.Time
	Connected   bool
	FilesDone   int64
	FilesFailed int64
	BytesDone   int64
}

// WorkerPoolEntry describes one worker node the operator declared on the web
// console's Workers page. IP is the controller-side address the da_worker must
// connect from to be allowed to pull tasks for a run. Local entries are
// metadata for console-launched workers (spawn params); remote entries are
// declarations of nodes that run their own da_worker. Entries with an empty IP
// are inert (they never gate runs) and exist only to carry node parameters.
type WorkerPoolEntry struct {
	ID       string `json:"id,omitempty"`
	IP       string `json:"ip,omitempty"`
	Local    bool   `json:"local,omitempty"`
	Count    int    `json:"count,omitempty"`    // local: how many workers to launch
	Capacity int    `json:"capacity,omitempty"` // concurrent copy workers
	Batch    int    `json:"batch,omitempty"`    // tasks per claim
}

// ServerState is a composite snapshot of the controller for the web layer,
// gathered in one lock hold.
type ServerState struct {
	RunState RunState
	Run      *RunSummary
	Stats    taskstore.Stats
	Workers  []WorkerInfo
	Now      time.Time
}

// Controller is the orchestrator. It is safe for concurrent use.
type Controller struct {
	migratev1.UnimplementedControlServer

	store  *taskstore.Store
	cfg    Config
	logger *log.Logger
	mu     sync.Mutex
	nextID int
	// workers tracks per-worker bookkeeping keyed by id.
	workers map[string]*workerState
	// workerPool is the operator-declared node set from the Workers page. Runs
	// without an explicit allowlist fall back to the pool's IPs. Guarded by mu.
	workerPool []WorkerPoolEntry
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
	if cfg.WorkerTimeout <= 0 {
		cfg.WorkerTimeout = 60 * time.Second
	}
	ctl := &Controller{
		store:    store,
		cfg:      cfg,
		logger:   cfg.Logger,
		nextID:   1,
		workers:  map[string]*workerState{},
		runState: RunRunning, // legacy direct-ingest flow grants claims by default
		quit:     make(chan struct{}),
	}
	if cfg.WorkerPoolFile != "" {
		if pool, err := loadWorkerPool(cfg.WorkerPoolFile); err != nil {
			if cfg.Logger != nil {
				cfg.Logger.Printf("control: load worker pool %s: %v", cfg.WorkerPoolFile, err)
			}
		} else {
			ctl.workerPool = pool
		}
	}
	return ctl
}

// loadWorkerPool reads a persisted worker pool; a missing file yields an empty
// pool with nil error.
func loadWorkerPool(path string) ([]WorkerPoolEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var pool []WorkerPoolEntry
	if err := json.Unmarshal(data, &pool); err != nil {
		return nil, err
	}
	for i := range pool {
		pool[i].IP = NormalizeIP(pool[i].IP)
	}
	return pool, nil
}

// IngestTasks adds a batch of tasks to the store. Used by the scan phase.
func (c *Controller) IngestTasks(tasks []taskstore.Task) error {
	return c.store.Ingest(tasks)
}

// SetIdle puts the controller into the no-run state, so claiming workers get
// an empty batch until a run is started. da_control boots web-only standby with
// this: without it leftover tasks from a previous process (a stopped run's
// pending store entries) would be eligible again, letting a freshly registered
// worker copy them out of band. StartRun flips the state back to running.
func (c *Controller) SetIdle() {
	c.mu.Lock()
	c.runState = RunIdle
	c.mu.Unlock()
}

// WorkerPool returns a copy of the declared worker pool.
func (c *Controller) WorkerPool() []WorkerPoolEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]WorkerPoolEntry, len(c.workerPool))
	copy(out, c.workerPool)
	return out
}

// SetWorkerPool replaces the declared pool, normalizes its IPs and persists it
// to the configured pool file (if any). The caller is responsible for IP
// validity; invalid addresses are dropped.
func (c *Controller) SetWorkerPool(pool []WorkerPoolEntry) error {
	clean := make([]WorkerPoolEntry, 0, len(pool))
	for _, e := range pool {
		e.IP = NormalizeIP(e.IP)
		if e.IP == "" && !e.Local {
			continue // skip anonymous remote declarations
		}
		clean = append(clean, e)
	}
	c.mu.Lock()
	c.workerPool = clean
	file := c.cfg.WorkerPoolFile
	c.mu.Unlock()
	if file == "" {
		return nil
	}
	return saveWorkerPool(file, clean)
}

// poolIPs returns the normalized non-empty IPs of the declared pool. The
// caller must not hold mu.
func (c *Controller) poolIPs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	var ips []string
	for _, e := range c.workerPool {
		ip := NormalizeIP(e.IP)
		if ip == "" || seen[ip] {
			continue
		}
		seen[ip] = true
		ips = append(ips, ip)
	}
	return ips
}

// saveWorkerPool persists the pool as JSON via an atomic rename.
func saveWorkerPool(path string, pool []WorkerPoolEntry) error {
	data, err := json.MarshalIndent(pool, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
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
	now := time.Now()
	peerIP := claimPeerIP(ctx)
	c.mu.Lock()
	if w, ok := c.workers[id]; ok {
		w.address = req.GetAddress()
		w.ip = peerIP
		w.capacity = int(req.GetCapacity())
		w.lastSeen = now
	} else {
		c.workers[id] = &workerState{
			id:       id,
			ip:       peerIP,
			address:  req.GetAddress(),
			capacity: int(req.GetCapacity()),
			joined:   now,
			lastSeen: now,
		}
	}
	c.mu.Unlock()
	return &migratev1.RegisterResponse{WorkerId: id}, nil
}

// Claim implements migratev1.ControlServer.
func (c *Controller) Claim(ctx context.Context, req *migratev1.ClaimRequest) (*migratev1.ClaimResponse, error) {
	c.mu.Lock()
	claimsAllowed := c.runState == RunRunning
	if claimsAllowed && c.active != nil && len(c.active.cfg.WorkerIPs) > 0 {
		// Only the workers the operator included in this run may pull tasks.
		if !IPAllowed(claimPeerIP(ctx), c.active.cfg.WorkerIPs) {
			claimsAllowed = false
		}
	}
	c.mu.Unlock()
	if !claimsAllowed {
		// paused, idle, stopped or outside the run's worker allowlist: hold all
		// tasks until the run resumes.
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

	// Account the payload into the active run (bytes/files) and the owning
	// worker's counters. Fails on the run are double-counted in store.Fail
	// below, but the run summary only cares about this run's outcome totals.
	c.mu.Lock()
	if w, ok := c.workers[req.GetWorkerId()]; ok {
		w.lastSeen = time.Now()
		w.filesDone += int64(len(okIDs))
		w.filesFailed += int64(len(failIDs))
		for _, res := range req.GetResults() {
			if res.GetOk() {
				w.bytesDone += res.GetBytes()
			}
		}
	}
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
	if w, ok := c.workers[req.GetWorkerId()]; ok {
		w.lastSeen = time.Now()
	}
	c.mu.Unlock()
	return &migratev1.HeartbeatResponse{Ok: true}, nil
}

// Stats returns the store's current task distribution.
func (c *Controller) Stats() taskstore.Stats { return c.store.Stats() }

// QueryTasks returns a paginated view of the task store (see
// taskstore.Store.Query).
func (c *Controller) QueryTasks(status taskstore.Status, offset, limit int) ([]taskstore.TaskItem, int) {
	return c.store.Query(status, offset, limit)
}

// Workers returns a snapshot of every registered worker, ordered by id.
func (c *Controller) Workers() []WorkerInfo {
	c.mu.Lock()
	now := time.Now()
	out := make([]WorkerInfo, 0, len(c.workers))
	for _, w := range c.workers {
		out = append(out, WorkerInfo{
			ID:          w.id,
			IP:          w.ip,
			Address:     w.address,
			Capacity:    w.capacity,
			Joined:      w.joined,
			LastSeen:    w.lastSeen,
			Connected:   now.Sub(w.lastSeen) <= c.cfg.WorkerTimeout,
			FilesDone:   w.filesDone,
			FilesFailed: w.filesFailed,
			BytesDone:   w.bytesDone,
		})
	}
	c.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// State returns a composite snapshot of the controller in a single lock hold.
func (c *Controller) State() ServerState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := ServerState{
		RunState: c.runState,
		Stats:    c.store.Stats(),
		Now:      time.Now(),
	}
	if r := c.active; r != nil {
		st.Run = &RunSummary{
			ID:           r.id,
			StartedAt:    r.startedAt,
			FilesDone:    r.filesDone,
			FilesFailed:  r.filesFailed,
			BytesDone:    r.bytesDone,
			BytesFailed:  r.bytesFailed,
			ScannedTasks: r.scanned,
			ScanDone:     r.scanDone,
			WorkerIPs:    r.cfg.WorkerIPs,
		}
	}
	for _, w := range c.workers {
		st.Workers = append(st.Workers, WorkerInfo{
			ID:          w.id,
			IP:          w.ip,
			Address:     w.address,
			Capacity:    w.capacity,
			Joined:      w.joined,
			LastSeen:    w.lastSeen,
			Connected:   st.Now.Sub(w.lastSeen) <= c.cfg.WorkerTimeout,
			FilesDone:   w.filesDone,
			FilesFailed: w.filesFailed,
			BytesDone:   w.bytesDone,
		})
	}
	sort.Slice(st.Workers, func(i, j int) bool { return st.Workers[i].ID < st.Workers[j].ID })
	return st
}

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

// claimPeerIP extracts the remote address of the caller of a gRPC method, as an
// IP host without the port. "::ffff:127.0.0.1" style IPv4-in-IPv6 mappings are
// unwrapped so a worker shows up as plain "127.0.0.1".
func claimPeerIP(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return NormalizeIP(hostOf(p.Addr.String()))
	}
	return ""
}

// hostOf splits "host:port" and returns the host (the full string when there is
// no port).
func hostOf(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// NormalizeIP unwraps IPv4-mapped IPv6 addresses and brackets.
func NormalizeIP(ip string) string {
	ip = strings.TrimSpace(strings.Trim(ip, "[]"))
	if i := strings.LastIndex(ip, "::ffff:"); i >= 0 {
		ip = ip[i+len("::ffff:"):]
	}
	return ip
}

// IPAllowed reports whether ip is present in the allowlist. Both sides are
// normalized (IPv4-mapped IPv6 unwrapped). An empty allowlist means everyone
// qualifies; an empty (unknown) peer IP never qualifies against a non-empty
// list.
func IPAllowed(ip string, allowed []string) bool {
	ip = NormalizeIP(ip)
	if len(allowed) == 0 {
		return true
	}
	if ip == "" {
		return false
	}
	for _, a := range allowed {
		if ip == NormalizeIP(strings.TrimSpace(a)) {
			return true
		}
	}
	return false
}
