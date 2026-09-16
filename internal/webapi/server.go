// Package webapi exposes the Controller to a browser UI over plain HTTP: REST
// endpoints for status, workers, tasks and run history plus live command
// endpoints (start/pause/resume/stop/rescan) and a Server-Sent Events stream
// (GET /api/events) that pushes a status snapshot every second while a client
// is connected.
//
// It depends only on the control layer's public surface (via ControlService),
// so the endpoints can be tested against a fake controller and the package can
// be embedded into da_control or reused elsewhere.
package webapi

import (
	"log"
	"net/http"
	"time"

	"datants/internal/control"
	"datants/internal/history"
	"datants/internal/sshrun"
	"datants/internal/taskstore"
	"datants/internal/workerctl"
)

// ControlService is the slice of the Controller the web layer needs. It is the
// seam used by httptest tests to substitute a fake.
type ControlService interface {
	State() control.ServerState
	RunState() control.RunState
	StartRun(src, dst string, cfg control.RunConfig) (string, error)
	StartRuns(targets []control.RunTarget, cfg control.RunConfig) (string, error)
	Pause() error
	Resume() error
	StopRun() error
	ReScan() error
	Workers() []control.WorkerInfo
	WorkerPool() []control.WorkerPoolEntry
	SetWorkerPool(pool []control.WorkerPoolEntry) error
	QueryTasks(status taskstore.Status, offset, limit int) ([]taskstore.TaskItem, int)
	Runs() []*history.Run
	Run(id string) (*history.Run, bool)
}

// Server is the HTTP API server. It is safe for concurrent use.
type Server struct {
	ctl      ControlService
	log      *log.Logger
	rate     *rateMeter
	interval time.Duration // SSE refresh interval
	spawner  *workerctl.Manager
	spawnCfg SpawnConfig
	remote   *sshrun.Manager
}

// SpawnConfig holds what the control process needs to launch local da_worker
// processes from the console.
type SpawnConfig struct {
	// WorkerBin is the path to the da_worker executable. Empty disables the
	// spawn endpoints.
	WorkerBin string
	// ControllerAddr is the gRPC address workers dial, e.g. "127.0.0.1:50051".
	ControllerAddr string
	// LogDir receives per-worker logs as <LogDir>/<id>.log.
	LogDir string
	// IdlePoll keeps spawned workers resident between runs (0 disables the
	// flag, letting them exit once the migration completes).
	IdlePoll time.Duration
}

// Option configures a Server.
type Option func(*Server)

// WithLogger logs operator commands and errors.
func WithLogger(l *log.Logger) Option { return func(s *Server) { s.log = l } }

// WithRefreshInterval sets how often an SSE event is pushed. Default 1s.
func WithRefreshInterval(d time.Duration) Option {
	return func(s *Server) { s.interval = d }
}

// WithWorkerSpawning enables the console to launch local worker processes.
func WithWorkerSpawning(m *workerctl.Manager, cfg SpawnConfig) Option {
	return func(s *Server) {
		s.spawner = m
		s.spawnCfg = cfg
	}
}

// WithRemoteSpawning enables the console to launch workers on the nodes of the
// declared worker pool over SSH (see internal/sshrun).
func WithRemoteSpawning(m *sshrun.Manager) Option {
	return func(s *Server) {
		s.remote = m
	}
}

// NewServer builds the API server over a controller (or fake).
func NewServer(ctl ControlService, opts ...Option) *Server {
	s := &Server{
		ctl:      ctl,
		rate:     &rateMeter{},
		interval: time.Second,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Handler returns the HTTP handler with every API route registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/workers", s.handleWorkers)
	mux.HandleFunc("POST /api/workers/pool", s.handleSetWorkerPool)
	mux.HandleFunc("POST /api/workers/pool/spawn", s.handlePoolSpawn)
	mux.HandleFunc("POST /api/workers/pool/stop", s.handlePoolStop)
	mux.HandleFunc("POST /api/workers/spawn", s.handleSpawn)
	mux.HandleFunc("POST /api/workers/stop", s.handleStopWorker)
	mux.HandleFunc("GET /api/tasks", s.handleTasks)
	mux.HandleFunc("GET /api/runs", s.handleRuns)
	mux.HandleFunc("GET /api/runs/{id}", s.handleRun)
	mux.HandleFunc("POST /api/start", s.handleStart)
	mux.HandleFunc("POST /api/pause", s.handlePause)
	mux.HandleFunc("POST /api/resume", s.handleResume)
	mux.HandleFunc("POST /api/stop", s.handleStop)
	mux.HandleFunc("POST /api/rescan", s.handleRescan)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	return mux
}
