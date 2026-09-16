// Package workerctl manages child worker processes launched by the web console.
// It is deliberately small: a Controller process can start N local da_worker
// processes (co-located with the shared storage the workers migrate between),
// track their liveness, and stop them, without the operator opening a shell on
// the controller host. The processes themselves are ordinary da_worker clients
// that register over gRPC, so the rest of the system is unchanged.
//
// The process factory is injectable so tests can run any executable (including
// the test binary itself) instead of a real da_worker.
package workerctl

import (
	"errors"
	"os/exec"
	"sort"
	"sync"
	"time"
)

// ErrNotSpawned is returned when stopping an id this manager does not own.
var ErrNotSpawned = errors.New("workerctl: not spawned")

// Process is the minimal surface of a supervised child process.
type Process interface {
	Pid() int
	Kill() error
	Wait() error
}

// Factory launches name with args and returns the running process.
type Factory func(name string, args ...string) (Process, error)

// OSFactory spawns real processes. stdout/stderr are left flowing to the daemon
// (worker logs go to their own --log file, so nothing is captured here).
func OSFactory() Factory {
	return func(name string, args ...string) (Process, error) {
		cmd := exec.Command(name, args...)
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return &osProcess{cmd: cmd}, nil
	}
}

type osProcess struct{ cmd *exec.Cmd }

func (p *osProcess) Pid() int    { return p.cmd.Process.Pid }
func (p *osProcess) Kill() error { return p.cmd.Process.Kill() }
func (p *osProcess) Wait() error { return p.cmd.Wait() }

// SpawnedInfo describes one managed child.
type SpawnedInfo struct {
	ID        string     `json:"id"`
	PID       int        `json:"pid"`
	Alive     bool       `json:"alive"`
	StartedAt time.Time  `json:"started_at"`
	Name      string     `json:"name"`
	Args      []string   `json:"args"`
	ExitedAt  *time.Time `json:"exited_at,omitempty"`
}

type tracked struct {
	info SpawnedInfo
	proc Process
}

// Manager supervises child processes keyed by id. Safe for concurrent use.
type Manager struct {
	mu      sync.Mutex
	factory Factory
	procs   map[string]*tracked
}

// NewManager builds a Manager. A nil factory defaults to OSFactory.
func NewManager(factory Factory) *Manager {
	if factory == nil {
		factory = OSFactory()
	}
	return &Manager{factory: factory, procs: make(map[string]*tracked)}
}

// Spawn starts name with args under id. The id must not already be managed.
// Liveness is tracked in the background via Wait.
func (m *Manager) Spawn(id, name string, args ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.procs[id]; exists {
		return errors.New("workerctl: id already spawned: " + id)
	}
	proc, err := m.factory(name, args...)
	if err != nil {
		return err
	}
	tr := &tracked{
		proc: proc,
		info: SpawnedInfo{
			ID:        id,
			PID:       proc.Pid(),
			Alive:     true,
			StartedAt: time.Now(),
			Name:      name,
			Args:      args,
		},
	}
	m.procs[id] = tr
	go func() {
		proc.Wait()
		m.mu.Lock()
		defer m.mu.Unlock()
		if cur, ok := m.procs[id]; ok && cur == tr {
			now := time.Now()
			tr.info.Alive = false
			tr.info.ExitedAt = &now
		}
	}()
	return nil
}

// Stop kills the child if it is still alive and forgets it. Reaping happens in
// the background watcher.
func (m *Manager) Stop(id string) error {
	m.mu.Lock()
	tr, ok := m.procs[id]
	if !ok {
		m.mu.Unlock()
		return ErrNotSpawned
	}
	delete(m.procs, id)
	m.mu.Unlock()
	if tr.info.Alive {
		return tr.proc.Kill()
	}
	return nil
}

// StopAll stops every managed child. Errors are collected; the first is
// returned.
func (m *Manager) StopAll() error {
	m.mu.Lock()
	ids := make([]string, 0, len(m.procs))
	for id := range m.procs {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	var firstErr error
	for _, id := range ids {
		if err := m.Stop(id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Alive reports whether id is a managed, still-running process.
func (m *Manager) Alive(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	tr, ok := m.procs[id]
	return ok && tr.info.Alive
}

// Info returns the current descriptor of a managed process.
func (m *Manager) Info(id string) (SpawnedInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tr, ok := m.procs[id]
	if !ok {
		return SpawnedInfo{}, false
	}
	return tr.info, true
}

// Snapshot returns the current managed processes, newest first.
func (m *Manager) Snapshot() []SpawnedInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SpawnedInfo, 0, len(m.procs))
	for _, tr := range m.procs {
		info := tr.info
		if info.ExitedAt != nil {
			et := *info.ExitedAt
			info.ExitedAt = &et
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out
}
