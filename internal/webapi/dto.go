package webapi

import (
	"sync"
	"time"

	"datants/internal/control"
	"datants/internal/history"
	"datants/internal/taskstore"
)

// Run-kind names mirroring taskstore.Kind.
var kindNames = map[taskstore.Kind]string{
	taskstore.KindCopy:   "copy",
	taskstore.KindMkdir:  "mkdir",
	taskstore.KindMeta:   "meta",
	taskstore.KindDelete: "delete",
}

// RateMeter computes a decaying instantaneous bytes/sec estimate from periodic
// samples. Safe for concurrent use.
type rateMeter struct {
	mu    sync.Mutex
	lastT time.Time
	lastB int64
	rate  float64
}

func (m *rateMeter) sample(now time.Time, bytes int64) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastT.IsZero() {
		m.lastT, m.lastB, m.rate = now, bytes, 0
		return 0
	}
	d := now.Sub(m.lastT).Seconds()
	if d <= 0 {
		m.lastT, m.lastB = now, bytes
		return m.rate
	}
	delta := float64(bytes - m.lastB)
	if delta < 0 {
		delta = 0
	}
	m.rate = delta / d
	m.lastT, m.lastB = now, bytes
	return m.rate
}

// Task clusters the current task counts while a run is active.
type taskStatsDTO struct {
	Pending    int `json:"pending"`
	InProgress int `json:"in_progress"`
	Done       int `json:"done"`
	Failed     int `json:"failed"`
	Total      int `json:"total"`
}

func toTaskStats(st taskstore.Stats) taskStatsDTO {
	return taskStatsDTO{
		Pending:    st.Pending,
		InProgress: st.InProgress,
		Done:       st.Done,
		Failed:     st.Failed,
		Total:      st.Pending + st.InProgress + st.Done + st.Failed,
	}
}

// Worker short summary carried in every status push (full detail: /api/workers).
type workerBriefDTO struct {
	ID        string `json:"id"`
	IP        string `json:"ip,omitempty"`
	Connected bool   `json:"connected"`
	Ready     bool   `json:"ready"`
	FilesDone int64  `json:"files_done"`
	BytesDone int64  `json:"bytes_done"`
}

type workerDTO struct {
	ID          string    `json:"id"`
	IP          string    `json:"ip,omitempty"`
	Address     string    `json:"address"`
	Capacity    int       `json:"capacity"`
	Connected   bool      `json:"connected"`
	InPool      bool      `json:"in_pool"`
	Ready       bool      `json:"ready"`
	Joined      time.Time `json:"joined_at"`
	LastSeen    time.Time `json:"last_seen"`
	FilesDone   int64     `json:"files_done"`
	FilesFailed int64     `json:"files_failed"`
	BytesDone   int64     `json:"bytes_done"`
}

func toWorkerDTO(w control.WorkerInfo, poolIPs map[string]bool) workerDTO {
	_, inPool := poolIPs[w.IP]
	return workerDTO{
		ID:          w.ID,
		IP:          w.IP,
		Address:     w.Address,
		Capacity:    w.Capacity,
		Connected:   w.Connected,
		InPool:      inPool,
		Ready:       w.Connected && inPool,
		Joined:      w.Joined,
		LastSeen:    w.LastSeen,
		FilesDone:   w.FilesDone,
		FilesFailed: w.FilesFailed,
		BytesDone:   w.BytesDone,
	}
}

// workerPoolDTO is one declared pool entry for the Workers page.
type workerPoolDTO struct {
	ID       string `json:"id,omitempty"`
	IP       string `json:"ip,omitempty"`
	Local    bool   `json:"local,omitempty"`
	Count    int    `json:"count,omitempty"`
	Capacity int    `json:"capacity,omitempty"`
	Batch    int    `json:"batch,omitempty"`
}

func toWorkerPoolDTO(e control.WorkerPoolEntry) workerPoolDTO {
	return workerPoolDTO{ID: e.ID, IP: e.IP, Local: e.Local, Count: e.Count, Capacity: e.Capacity, Batch: e.Batch}
}

func fromWorkerPoolDTO(e workerPoolDTO) control.WorkerPoolEntry {
	return control.WorkerPoolEntry{ID: e.ID, IP: e.IP, Local: e.Local, Count: e.Count, Capacity: e.Capacity, Batch: e.Batch}
}

func toWorkerBriefDTO(w control.WorkerInfo) workerBriefDTO {
	return workerBriefDTO{ID: w.ID, IP: w.IP, Connected: w.Connected, Ready: w.Connected && w.InPool, FilesDone: w.FilesDone, BytesDone: w.BytesDone}
}

// runTargetDTO is one src→dst pair inside the active run.
type runTargetDTO struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

// runSummaryDTO is the live snapshot of the active run.
type runSummaryDTO struct {
	ID           string         `json:"id"`
	Targets      []runTargetDTO `json:"targets,omitempty"`
	Src          string         `json:"src,omitempty"`
	Dst          string         `json:"dst,omitempty"`
	StartedAt    time.Time      `json:"started_at"`
	FilesDone    int64          `json:"files_done"`
	FilesFailed  int64          `json:"files_failed"`
	BytesDone    int64          `json:"bytes_done"`
	BytesFailed  int64          `json:"bytes_failed"`
	ScannedTasks int64          `json:"scanned_tasks"`
	ScanDone     bool           `json:"scan_done"`
	ElapsedMs    int64          `json:"elapsed_ms"`
	WorkerIPs    []string       `json:"worker_ips,omitempty"`
}

func toRunSummaryDTO(r control.RunSummary) runSummaryDTO {
	elapsed := time.Since(r.StartedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	targets := make([]runTargetDTO, 0, len(r.Targets))
	for _, t := range r.Targets {
		targets = append(targets, runTargetDTO{Src: t.Src, Dst: t.Dst})
	}
	var src, dst string
	if len(r.Targets) > 0 {
		src, dst = r.Targets[0].Src, r.Targets[0].Dst
	}
	return runSummaryDTO{
		ID:           r.ID,
		Targets:      targets,
		Src:          src,
		Dst:          dst,
		StartedAt:    r.StartedAt,
		FilesDone:    r.FilesDone,
		FilesFailed:  r.FilesFailed,
		BytesDone:    r.BytesDone,
		BytesFailed:  r.BytesFailed,
		ScannedTasks: r.ScannedTasks,
		ScanDone:     r.ScanDone,
		ElapsedMs:    elapsed.Milliseconds(),
		WorkerIPs:    r.WorkerIPs,
	}
}

// statusDTO is the payload of GET /api/status and every SSE status event.
type statusDTO struct {
	State           string           `json:"state"`
	ProgressPct     float64          `json:"progress_pct"`
	RateBytesPerSec float64          `json:"rate_bytes_per_sec"`
	Run             *runSummaryDTO   `json:"run,omitempty"`
	Tasks           taskStatsDTO     `json:"tasks"`
	Workers         []workerBriefDTO `json:"workers"`
	Now             time.Time        `json:"now"`
}

// runDTO is a history run plus its computed duration in milliseconds.
type runDTO struct {
	*history.Run
	DurationMs int64 `json:"duration_ms"`
}

func toRunDTO(r *history.Run) runDTO {
	return runDTO{Run: r, DurationMs: r.Duration().Milliseconds()}
}

// taskDTO is the public shape of one task as returned by /api/tasks.
type taskDTO struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Status  string `json:"status"`
	SrcPath string `json:"src_path"`
	DstPath string `json:"dst_path"`
	Size    int64  `json:"size"`
	Mode    uint32 `json:"mode"`
	Uid     uint32 `json:"uid"`
	Gid     uint32 `json:"gid"`
	Owner   string `json:"owner,omitempty"`
}

func toTaskDTO(it taskstore.TaskItem) taskDTO {
	kind := kindNames[it.Kind]
	if kind == "" {
		kind = "copy"
	}
	return taskDTO{
		ID:      it.ID,
		Kind:    kind,
		Status:  it.Status.String(),
		SrcPath: it.SrcPath,
		DstPath: it.DstPath,
		Size:    it.Size,
		Mode:    it.Mode,
		Uid:     it.Uid,
		Gid:     it.Gid,
		Owner:   it.Owner,
	}
}
