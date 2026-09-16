// Package history records the outcome of each Controller migration run so the
// results of past migrations can be inspected afterwards (web UI, audit). It is
// intentionally independent of the task store: wiping the task store to force a
// from-scratch re-migration does not erase run history.
//
// Each run is persisted as a single JSON file (run_<id>.json) whose writes are
// atomic (tmp file + rename), so a crash never leaves a half-written record.
package history

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Status is the lifecycle state of a migration run.
type Status string

const (
	// StatusRunning means the run's scan and/or transfer is still in flight.
	StatusRunning Status = "running"
	// StatusDone means the task store emptied and the run finished cleanly.
	StatusDone Status = "done"
	// StatusFailed means the scan phase errored out.
	StatusFailed Status = "failed"
	// StatusStopped means the run was ended early (operator stop or the
	// controller process exited before the run completed).
	StatusStopped Status = "stopped"
)

// Target is one source→destination directory pair within a migration run.
type Target struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

// Config snapshots the scan options a run was started with, so history shows
// exactly how each migration was configured.
type Config struct {
	Workers     int      `json:"workers,omitempty"`
	IncludeDirs bool     `json:"include_dirs,omitempty"`
	SyncMeta    bool     `json:"sync_meta,omitempty"`
	DeleteStale bool     `json:"delete_stale,omitempty"`
	Ignore      []string `json:"ignore,omitempty"`
	WorkerIPs   []string `json:"worker_ips,omitempty"`
}

// Result summarizes the transfer outcome of a run.
type Result struct {
	// FilesDone is the number of tasks completed successfully.
	FilesDone int64 `json:"files_done"`
	// FilesFailed is the number of tasks that errored.
	FilesFailed int64 `json:"files_failed"`
	// BytesDone is the number of payload bytes written successfully.
	BytesDone int64 `json:"bytes_done"`
	// BytesFailed is the number of payload bytes that failed.
	BytesFailed int64 `json:"bytes_failed"`
	// AvgBytesSec is the average copy throughput over the run's lifetime.
	AvgBytesSec float64 `json:"avg_bytes_per_sec,omitempty"`
}

// Run is one Controller migration run.
type Run struct {
	ID         string    `json:"id"`
	Src        string    `json:"src"`
	Dst        string    `json:"dst"`
	Targets    []Target  `json:"targets,omitempty"`
	Config     Config    `json:"config"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Status     Status    `json:"status"`
	Result     Result    `json:"result"`
}

// Duration returns how long the run took (elapsed so far if still running).
func (r *Run) Duration() time.Duration {
	end := r.FinishedAt
	if end.IsZero() {
		end = time.Now()
	}
	if end.Before(r.StartedAt) {
		return 0
	}
	return end.Sub(r.StartedAt)
}

// NewID generates a unique, chronologically sortable run id.
func NewID() string {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is practically unreachable; a zero suffix is
		// still unique enough when combined with the timestamp.
		return "run-" + time.Now().UTC().Format("20060102T150405Z000000000")
	}
	return "run-" + time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

// Store persists runs as individual JSON files under dir and keeps an in-memory
// index for fast listing. It is safe for concurrent use.
type Store struct {
	dir  string
	mu   sync.Mutex
	runs map[string]*Run
	ids  []string // most recent first
}

// New opens the run store rooted at dir, loading any existing run files. Runs
// left "running" by a previous process exit are marked "stopped": a new
// controller process cannot meaningfully adopt an in-flight run.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, runs: map[string]*Run{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "run_") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var r Run
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("history: corrupt %s: %w", e.Name(), err)
		}
		if r.ID == "" {
			continue
		}
		s.runs[r.ID] = &r
		s.ids = append(s.ids, r.ID)
	}
	// Interrupted runs from a previous process become "stopped".
	now := time.Now()
	var rewrites []*Run
	for _, id := range s.ids {
		r := s.runs[id]
		if r.Status == StatusRunning {
			r.Status = StatusStopped
			r.FinishedAt = now
			rewrites = append(rewrites, r)
		}
	}
	sort.Slice(s.ids, func(i, j int) bool {
		return s.runs[s.ids[i]].StartedAt.After(s.runs[s.ids[j]].StartedAt)
	})
	for _, r := range rewrites {
		if err := s.writeLocked(r); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Create records a new run. An empty ID is replaced with a generated one, an
// empty StartedAt with the current time, and an empty Status with running. The
// run is written to disk immediately.
func (s *Store) Create(r *Run) error {
	if r.ID == "" {
		r.ID = NewID()
	}
	if r.StartedAt.IsZero() {
		r.StartedAt = time.Now()
	}
	if r.Status == "" {
		r.Status = StatusRunning
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[r.ID] = r
	s.ids = append(s.ids, r.ID)
	sort.Slice(s.ids, func(i, j int) bool {
		return s.runs[s.ids[i]].StartedAt.After(s.runs[s.ids[j]].StartedAt)
	})
	return s.writeLocked(r)
}

// Get returns a copy of the run with the given id.
func (s *Store) Get(id string) (*Run, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

// List returns all runs, most recent first. Callers must not mutate the runs.
func (s *Store) List() []*Run {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Run, 0, len(s.ids))
	for _, id := range s.ids {
		cp := *s.runs[id]
		out = append(out, &cp)
	}
	return out
}

// Update mutates a run in place and rewrites its file. fn may be nil (useful to
// just persist the latest progress). Returns false if the id is unknown.
func (s *Store) Update(id string, fn func(*Run)) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return false, nil
	}
	if fn != nil {
		fn(r)
	}
	return true, s.writeLocked(r)
}

// Close releases the store. File-handle free; provided for symmetry with other
// stores (e.g. taskstore).
func (s *Store) Close() error { return nil }

// writeLocked atomically writes a run record to disk.
func (s *Store) writeLocked(r *Run) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	path := s.path(r.ID)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func (s *Store) path(id string) string {
	return filepath.Join(s.dir, "run_"+id+".json")
}
