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
	"datants/internal/taskstore"
)

// ControlService is the slice of the Controller the web layer needs. It is the
// seam used by httptest tests to substitute a fake.
type ControlService interface {
	State() control.ServerState
	RunState() control.RunState
	StartRun(src, dst string, cfg control.RunConfig) (string, error)
	Pause() error
	Resume() error
	StopRun() error
	ReScan() error
	Workers() []control.WorkerInfo
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
}

// Option configures a Server.
type Option func(*Server)

// WithLogger logs operator commands and errors.
func WithLogger(l *log.Logger) Option { return func(s *Server) { s.log = l } }

// WithRefreshInterval sets how often an SSE event is pushed. Default 1s.
func WithRefreshInterval(d time.Duration) Option {
	return func(s *Server) { s.interval = d }
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
