package webapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"datants/internal/control"
	"datants/internal/taskstore"
	"datants/internal/workerctl"
)

// abortError marks a clearly-bad request: it maps to 400 instead of 409.
type abortError struct{ msg string }

func (e *abortError) Error() string { return e.msg }

func badRequest(msg string) error { return &abortError{msg: msg} }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Printf("webapi: encode response: %v", err)
	}
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// commandErr maps a control/lifecycle error to an HTTP status: validation
// problems (empty src/dst) are 400, state conflicts (run already active, no
// run to stop) are 409.
func commandErr(e error) int {
	var ab *abortError
	if errors.As(e, &ab) {
		return http.StatusBadRequest
	}
	return http.StatusConflict
}

// buildStatus assembles the live status payload and feeds the rate meter.
func (s *Server) buildStatus() statusDTO {
	st := s.ctl.State()
	var bytes int64
	if st.Run != nil {
		bytes = st.Run.BytesDone
	}
	rate := s.rate.sample(st.Now, bytes)

	dto := statusDTO{
		State:           st.RunState.String(),
		RateBytesPerSec: rate,
		Tasks:           toTaskStats(st.Stats),
		Workers:         make([]workerBriefDTO, 0, len(st.Workers)),
		Now:             st.Now,
	}
	if st.Run != nil {
		run := toRunSummaryDTO(*st.Run)
		dto.Run = &run
		if total := dto.Tasks.Total; total > 0 {
			dto.ProgressPct = float64(st.Stats.Done+st.Stats.Failed) / float64(total) * 100
		} else {
			dto.ProgressPct = 100
		}
	}
	for _, w := range st.Workers {
		dto.Workers = append(dto.Workers, toWorkerBriefDTO(w))
	}
	return dto
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.buildStatus())
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	ws := s.ctl.Workers()
	out := make([]workerDTO, 0, len(ws))
	for _, w := range ws {
		out = append(out, toWorkerDTO(w))
	}
	resp := map[string]any{"workers": out}
	if s.spawner != nil {
		resp["spawned"] = s.spawner.Snapshot()
	}
	writeJSON(w, http.StatusOK, resp)
}

// spawnLimits bound how many workers a single console command may launch.
const (
	maxSpawnPerRequest = 64
	maxCapacity        = 1024
)

// spawnRequestDTO is the JSON body of POST /api/workers/spawn.
type spawnRequestDTO struct {
	Count    int    `json:"count"`
	Capacity int    `json:"capacity"`
	Batch    int    `json:"batch"`
	IDPrefix string `json:"id_prefix"`
}

func (s *Server) handleSpawn(w http.ResponseWriter, r *http.Request) {
	if s.spawner == nil || s.spawnCfg.WorkerBin == "" {
		writeErr(w, http.StatusBadRequest, "worker spawning is not enabled on this controller (--worker-bin)")
		return
	}
	var req spawnRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Count < 1 {
		req.Count = 1
	}
	if req.Count > maxSpawnPerRequest {
		writeErr(w, http.StatusBadRequest, "count exceeds limit "+strconv.Itoa(maxSpawnPerRequest))
		return
	}
	if req.Capacity < 1 || req.Batch < 1 {
		writeErr(w, http.StatusBadRequest, "capacity and batch must be >= 1")
		return
	}
	if req.Capacity > maxCapacity {
		writeErr(w, http.StatusBadRequest, "capacity exceeds limit "+strconv.Itoa(maxCapacity))
		return
	}
	prefix := strings.TrimSpace(req.IDPrefix)
	if prefix == "" {
		prefix = "local"
	}

	spawned := make([]workerctl.SpawnedInfo, 0, req.Count)
	for i := 0; i < req.Count; i++ {
		id := fmt.Sprintf("%s-%02d", prefix, i)
		logFile := filepath.Join(s.spawnCfg.LogDir, id+".log")
		args := []string{
			"--controller", s.spawnCfg.ControllerAddr,
			"--id", id,
			"--capacity", strconv.Itoa(req.Capacity),
			"--batch", strconv.Itoa(req.Batch),
			"--log", logFile,
		}
		if s.spawnCfg.IdlePoll > 0 {
			// keep the worker resident so a worker spawned before a run (or
			// waiting after one) stays online
			args = append(args, "--idle-poll", s.spawnCfg.IdlePoll.String())
		}
		err := s.spawner.Spawn(id, s.spawnCfg.WorkerBin, args...)
		if err != nil {
			_ = s.spawner.StopAll()
			writeErr(w, http.StatusInternalServerError, "failed to spawn worker "+id+": "+err.Error())
			return
		}
		if info, ok := s.spawner.Info(id); ok {
			s.logf("spawn worker %s pid=%d", id, info.PID)
			spawned = append(spawned, info)
		}
	}
	s.logf("spawn workers: count=%d prefix=%q capacity=%d batch=%d", req.Count, prefix, req.Capacity, req.Batch)
	writeJSON(w, http.StatusOK, map[string]any{"spawned": spawned})
}

// stopWorkerRequestDTO is the JSON body of POST /api/workers/stop.
type stopWorkerRequestDTO struct {
	ID string `json:"id"`
}

func (s *Server) handleStopWorker(w http.ResponseWriter, r *http.Request) {
	if s.spawner == nil {
		writeErr(w, http.StatusBadRequest, "worker spawning is not enabled on this controller")
		return
	}
	var req stopWorkerRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if err := s.spawner.Stop(req.ID); err != nil {
		if err == workerctl.ErrNotSpawned {
			writeErr(w, http.StatusNotFound, "no managed worker with id "+req.ID)
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.logf("stopped managed worker %s", req.ID)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	var status taskstore.Status
	switch r.URL.Query().Get("status") {
	case "", "all":
		status = taskstore.AnyStatus
	case "pending":
		status = taskstore.Pending
	case "in-progress":
		status = taskstore.InProgress
	case "done":
		status = taskstore.Done
	case "failed":
		status = taskstore.Failed
	default:
		writeErr(w, http.StatusBadRequest, "status must be one of all, pending, in-progress, done, failed")
		return
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit > 1000 {
		limit = 1000
	}

	items, total := s.ctl.QueryTasks(status, offset, limit)
	out := make([]taskDTO, 0, len(items))
	for _, it := range items {
		out = append(out, toTaskDTO(it))
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": out, "total": total})
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	runs := s.ctl.Runs()
	out := make([]runDTO, 0, len(runs))
	for _, run := range runs {
		out = append(out, toRunDTO(run))
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, ok := s.ctl.Run(id)
	if !ok {
		writeErr(w, http.StatusNotFound, "run not found")
		return
	}
	writeJSON(w, http.StatusOK, toRunDTO(run))
}

// startRequestDTO is the JSON body of POST /api/start.
type startRequestDTO struct {
	Src         string   `json:"src"`
	Dst         string   `json:"dst"`
	Workers     int      `json:"workers"`
	IncludeDirs bool     `json:"include_dirs"`
	SyncMeta    bool     `json:"sync_meta"`
	DeleteStale bool     `json:"delete_stale"`
	Ignore      []string `json:"ignore"`
	Fresh       bool     `json:"fresh"`
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	var req startRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Src == "" || req.Dst == "" {
		writeErr(w, http.StatusBadRequest, "src and dst are required")
		return
	}
	cfg := control.RunConfig{
		Workers:     req.Workers,
		IncludeDirs: req.IncludeDirs,
		SyncMeta:    req.SyncMeta,
		DeleteStale: req.DeleteStale,
		Ignore:      req.Ignore,
		Fresh:       req.Fresh,
	}
	id, err := s.ctl.StartRun(req.Src, req.Dst, cfg)
	if err != nil {
		writeErr(w, commandErr(err), err.Error())
		return
	}
	s.logf("start run %s %s -> %s", id, req.Src, req.Dst)
	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, "pause", s.ctl.Pause)
}
func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, "resume", s.ctl.Resume)
}
func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, "stop", s.ctl.StopRun)
}
func (s *Server) handleRescan(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, "rescan", s.ctl.ReScan)
}

func (s *Server) mutate(w http.ResponseWriter, name string, fn func() error) {
	if err := fn(); err != nil {
		writeErr(w, commandErr(err), err.Error())
		return
	}
	s.logf("command: %s ok", name)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleEvents streams Server-Sent Events: one "status" event per tick with the
// full live status payload. The stream ends when the client disconnects.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	frame := func() {
		data, err := json.Marshal(s.buildStatus())
		if err != nil {
			return
		}
		fmt.Fprintf(w, "event: status\ndata: %s\n\n", data)
		fl.Flush()
	}
	frame()

	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
			frame()
		}
	}
}

func (s *Server) logf(format string, args ...any) {
	if s.log != nil {
		s.log.Printf("webapi: "+format, args...)
	}
}
