package webapi_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"datants/internal/control"
	"datants/internal/history"
	"datants/internal/taskstore"
	"datants/internal/webapi"
)

// fake implements webapi.ControlService with programmable behaviour.
type fake struct {
	state   control.ServerState
	runs    []*history.Run
	workers []control.WorkerInfo
	pool    []control.WorkerPoolEntry
	tasks   []taskstore.TaskItem
	total   int

	startSrc, startDst string
	startCfg           control.RunConfig
	startTargets       []control.RunTarget
	startErr           error

	pauseErr, resumeErr, stopErr, rescanErr error
}

func newFake() *fake {
	now := time.Now()
	return &fake{
		state: control.ServerState{
			RunState: control.RunRunning,
			Run: &control.RunSummary{
				ID: "run-1", StartedAt: now.Add(-time.Minute),
				FilesDone: 10, FilesFailed: 1, BytesDone: 5000, BytesFailed: 10,
				ScannedTasks: 15, ScanDone: true,
			},
			Stats: taskstore.Stats{Pending: 2, InProgress: 1, Done: 10, Failed: 1},
			Now:   now,
		},
		workers: []control.WorkerInfo{
			{
				ID: "w1", IP: "10.0.0.1", Address: "10.0.0.1:1234", Capacity: 8,
				Joined: now.Add(-time.Hour), LastSeen: now, Connected: true,
				FilesDone: 10, FilesFailed: 1, BytesDone: 5000,
			},
		},
		runs: []*history.Run{
			{
				ID: "run-1", Src: "/s", Dst: "/d",
				StartedAt: now.Add(-time.Hour), FinishedAt: now,
				Status: history.StatusDone,
				Result: history.Result{FilesDone: 50, BytesDone: 1000},
			},
		},
		tasks: []taskstore.TaskItem{
			{
				Task: taskstore.Task{
					ID: "t1", Kind: taskstore.KindCopy,
					SrcPath: "/s/a.txt", DstPath: "/d/a.txt", Size: 100,
				},
				Status: taskstore.Done, Owner: "w1",
			},
		},
		total: 1,
	}
}

func (f *fake) State() control.ServerState    { return f.state }
func (f *fake) RunState() control.RunState    { return f.state.RunState }
func (f *fake) Workers() []control.WorkerInfo { return f.workers }
func (f *fake) WorkerPool() []control.WorkerPoolEntry {
	out := make([]control.WorkerPoolEntry, len(f.pool))
	copy(out, f.pool)
	return out
}
func (f *fake) SetWorkerPool(pool []control.WorkerPoolEntry) error {
	f.pool = append([]control.WorkerPoolEntry(nil), pool...)
	return nil
}
func (f *fake) Runs() []*history.Run { return f.runs }
func (f *fake) Run(id string) (*history.Run, bool) {
	for _, r := range f.runs {
		if r.ID == id {
			return r, true
		}
	}
	return nil, false
}
func (f *fake) QueryTasks(status taskstore.Status, offset, limit int) ([]taskstore.TaskItem, int) {
	return f.tasks, f.total
}
func (f *fake) StartRun(src, dst string, cfg control.RunConfig) (string, error) {
	f.startSrc, f.startDst, f.startCfg = src, dst, cfg
	if f.startErr != nil {
		return "", f.startErr
	}
	return "run-new", nil
}
func (f *fake) StartRuns(targets []control.RunTarget, cfg control.RunConfig) (string, error) {
	if len(targets) > 0 {
		f.startSrc, f.startDst = targets[0].Src, targets[0].Dst
	}
	f.startCfg = cfg
	f.startTargets = targets
	if f.startErr != nil {
		return "", f.startErr
	}
	return "run-new", nil
}
func (f *fake) Pause() error   { return f.pauseErr }
func (f *fake) Resume() error  { return f.resumeErr }
func (f *fake) StopRun() error { return f.stopErr }
func (f *fake) ReScan() error  { return f.rescanErr }

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body == "" {
		rdr = bytes.NewReader(nil)
	} else {
		rdr = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func decode[T any](t *testing.T, rr *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rr.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %s: %v", string(rr.Body.Bytes()), err)
	}
	return v
}

func TestStatusEndpoint(t *testing.T) {
	srv := webapi.NewServer(newFake())
	rr := do(t, srv.Handler(), "GET", "/api/status", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d, body %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control = %q, want no-cache", got)
	}
	st := decode[map[string]any](t, rr)
	if st["state"] != "running" {
		t.Fatalf("state = %v", st["state"])
	}
	if st["run"] == nil {
		t.Fatal("run missing from status")
	}
	if _, ok := st["rate_bytes_per_sec"]; !ok {
		t.Fatal("rate missing")
	}
	if _, ok := st["progress_pct"]; !ok {
		t.Fatal("progress_pct missing")
	}
	tasks := st["tasks"].(map[string]any)
	if tasks["done"].(float64) != 10 {
		t.Fatalf("tasks.done = %v", tasks["done"])
	}
}

func TestWorkersEndpoint(t *testing.T) {
	srv := webapi.NewServer(newFake())
	rr := do(t, srv.Handler(), "GET", "/api/workers", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d", rr.Code)
	}
	out := decode[map[string]any](t, rr)
	ws := out["workers"].([]any)
	if len(ws) != 1 {
		t.Fatalf("workers = %d", len(ws))
	}
	w := ws[0].(map[string]any)
	if w["id"] != "w1" || w["capacity"].(float64) != 8 || w["connected"] != true {
		t.Fatalf("worker = %v", w)
	}
	if _, ok := w["address"]; !ok {
		t.Fatal("worker address missing")
	}
	if w["ip"] != "10.0.0.1" {
		t.Fatalf("worker ip = %v", w["ip"])
	}
	// Without a pool the connected worker is online but not ready.
	if w["in_pool"] != false || w["ready"] != false {
		t.Fatalf("worker pool flags = %v", w)
	}
	if pool, ok := out["pool"].([]any); !ok || len(pool) != 0 {
		t.Fatalf("default pool = %v", out["pool"])
	}

	// A declared pool marks its members ready while they stay connected.
	f := newFake()
	f.pool = []control.WorkerPoolEntry{{ID: "node-a", IP: "10.0.0.1", Capacity: 8}}
	srv = webapi.NewServer(f)
	rr = do(t, srv.Handler(), "GET", "/api/workers", "")
	w = decode[map[string]any](t, rr)["workers"].([]any)[0].(map[string]any)
	if w["in_pool"] != true || w["ready"] != true {
		t.Fatalf("pooled worker flags = %v", w)
	}
	pool := decode[map[string]any](t, rr)["pool"].([]any)
	if len(pool) != 1 || pool[0].(map[string]any)["ip"] != "10.0.0.1" {
		t.Fatalf("pool = %v", pool)
	}
}

func TestWorkerPoolEndpoint(t *testing.T) {
	f := newFake()
	srv := webapi.NewServer(f)

	body := `{"workers":[
		{"id":"node-a","ip":"10.0.0.1","capacity":8,"batch":64},
		{"id":"node-b","ip":"[::ffff:192.168.0.9]","local":true,"count":2,"capacity":4}
	]}`
	rr := do(t, srv.Handler(), "POST", "/api/workers/pool", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("set pool code = %d body=%s", rr.Code, rr.Body.String())
	}
	if len(f.pool) != 2 {
		t.Fatalf("stored pool = %+v", f.pool)
	}
	if f.pool[1].IP != "192.168.0.9" {
		t.Fatalf("normalized ip = %q", f.pool[1].IP)
	}

	// entries without an IP are rejected
	rr = do(t, srv.Handler(), "POST", "/api/workers/pool", `{"workers":[{"id":"x"}]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing ip code = %d", rr.Code)
	}
	if len(f.pool) != 2 {
		t.Fatalf("rejected request still mutated pool: %+v", f.pool)
	}
}

func TestTasksEndpoint(t *testing.T) {
	srv := webapi.NewServer(newFake())
	for _, status := range []string{"all", "pending", "in-progress", "done", "failed"} {
		rr := do(t, srv.Handler(), "GET", "/api/tasks?status="+status+"&offset=0&limit=5", "")
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%s code=%d", status, rr.Code)
		}
		out := decode[map[string]any](t, rr)
		if out["total"].(float64) != 1 {
			t.Fatalf("status=%s total=%v", status, out["total"])
		}
		tasks := out["tasks"].([]any)
		tk := tasks[0].(map[string]any)
		if tk["id"] != "t1" || tk["kind"] != "copy" || tk["status"] != "done" || tk["owner"] != "w1" {
			t.Fatalf("task = %v", tk)
		}
	}
	// unknown status -> 400
	rr := do(t, srv.Handler(), "GET", "/api/tasks?status=bogus", "")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bogus status code = %d", rr.Code)
	}
}

func TestRunsEndpoints(t *testing.T) {
	srv := webapi.NewServer(newFake())
	rr := do(t, srv.Handler(), "GET", "/api/runs", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d", rr.Code)
	}
	out := decode[map[string]any](t, rr)
	runs := out["runs"].([]any)
	if len(runs) != 1 {
		t.Fatalf("runs = %d", len(runs))
	}
	run := runs[0].(map[string]any)
	if run["id"] != "run-1" || run["status"] != "done" {
		t.Fatalf("run = %v", run)
	}
	if _, ok := run["duration_ms"]; !ok {
		t.Fatal("duration_ms missing")
	}

	rr = do(t, srv.Handler(), "GET", "/api/runs/run-1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("run detail code = %d", rr.Code)
	}
	detail := decode[map[string]any](t, rr)
	if detail["id"] != "run-1" {
		t.Fatalf("detail = %v", detail)
	}

	rr = do(t, srv.Handler(), "GET", "/api/runs/nope", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing run code = %d", rr.Code)
	}
}

func TestStartEndpoint(t *testing.T) {
	f := newFake()
	srv := webapi.NewServer(f)
	body := `{"src":"/s","dst":"/d","workers":4,"include_dirs":true,"delete_stale":true,"fresh":true,"ignore":["*.tmp"],"worker_ips":["127.0.0.2","10.1.1.1"]}`
	rr := do(t, srv.Handler(), "POST", "/api/start", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	out := decode[map[string]any](t, rr)
	if out["id"] != "run-new" {
		t.Fatalf("id = %v", out["id"])
	}
	if f.startSrc != "/s" || f.startDst != "/d" {
		t.Fatalf("start args = %q %q", f.startSrc, f.startDst)
	}
	cfg := f.startCfg
	if cfg.Workers != 4 || !cfg.IncludeDirs || !cfg.DeleteStale || !cfg.Fresh || len(cfg.Ignore) != 1 {
		t.Fatalf("start cfg = %+v", cfg)
	}
	if len(cfg.WorkerIPs) != 2 || cfg.WorkerIPs[0] != "127.0.0.2" || cfg.WorkerIPs[1] != "10.1.1.1" {
		t.Fatalf("worker_ips = %v", cfg.WorkerIPs)
	}
}

func TestStartEndpointMultiTarget(t *testing.T) {
	f := newFake()
	srv := webapi.NewServer(f)
	body := `{"src":"/s1","dst":"/d1","targets":[{"src":"/s1","dst":"/d1"},{"src":"/s2","dst":"/d2"}],"workers":2}`
	rr := do(t, srv.Handler(), "POST", "/api/start", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d body = %s", rr.Code, rr.Body.String())
	}
	if len(f.startTargets) != 2 {
		t.Fatalf("start targets = %+v", f.startTargets)
	}
	if f.startTargets[0].Src != "/s1" || f.startTargets[0].Dst != "/d1" ||
		f.startTargets[1].Src != "/s2" || f.startTargets[1].Dst != "/d2" {
		t.Fatalf("start targets = %+v", f.startTargets)
	}
	if f.startSrc != "/s1" || f.startDst != "/d1" {
		t.Fatalf("start first src/dst = %q %q", f.startSrc, f.startDst)
	}
	if f.startCfg.Workers != 2 {
		t.Fatalf("workers = %d", f.startCfg.Workers)
	}

	// a targets entry missing src or dst is a 400
	rr = do(t, srv.Handler(), "POST", "/api/start", `{"targets":[{"src":"/s1","dst":""}]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty dst in target code = %d body = %s", rr.Code, rr.Body.String())
	}
}

func TestStartEndpointBadRequests(t *testing.T) {
	srv := webapi.NewServer(newFake())
	// malformed json
	rr := do(t, srv.Handler(), "POST", "/api/start", "{not json")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("malformed body code = %d", rr.Code)
	}
	// controller validation error -> 400
	f2 := newFake()
	f2.startErr = errors.New("src and dst are required")
	srv2 := webapi.NewServer(f2)
	rr = do(t, srv2.Handler(), "POST", "/api/start", `{"src":"","dst":""}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty src code = %d", rr.Code)
	}
	// state conflict -> 409
	f3 := newFake()
	f3.startErr = errors.New("a run is already active")
	srv3 := webapi.NewServer(f3)
	rr = do(t, srv3.Handler(), "POST", "/api/start", `{"src":"/s","dst":"/d"}`)
	if rr.Code != http.StatusConflict {
		t.Fatalf("conflict code = %d", rr.Code)
	}
}

func TestMutationEndpoints(t *testing.T) {
	f := newFake()
	srv := webapi.NewServer(f)
	for _, path := range []string{"/api/pause", "/api/resume", "/api/stop", "/api/rescan"} {
		rr := do(t, srv.Handler(), "POST", path, "")
		if rr.Code != http.StatusOK {
			t.Fatalf("%s code = %d body=%s", path, rr.Code, rr.Body.String())
		}
	}
	// error paths -> 409
	f.pauseErr = errors.New("no running run to pause")
	rr := do(t, srv.Handler(), "POST", "/api/pause", "")
	if rr.Code != http.StatusConflict {
		t.Fatalf("pause conflict code = %d", rr.Code)
	}
}

func TestUnknownRouteAndMethod(t *testing.T) {
	srv := webapi.NewServer(newFake())
	rr := do(t, srv.Handler(), "GET", "/api/nope", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown route code = %d", rr.Code)
	}
	rr = do(t, srv.Handler(), "POST", "/api/status", "")
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/status code = %d", rr.Code)
	}
}

func TestEventsSSE(t *testing.T) {
	srv := httptest.NewServer(webapi.NewServer(newFake()).Handler())
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(srv.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}

	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	if err != nil && n == 0 {
		t.Fatalf("read: %v", err)
	}
	frame := string(buf[:n])
	if !strings.Contains(frame, "event: status") {
		t.Fatalf("missing event line in %q", frame)
	}
	if !strings.Contains(frame, `"state":"running"`) {
		t.Fatalf("missing data payload in %q", frame)
	}
}

// ensure the fake satisfies the interface at compile time
var _ webapi.ControlService = (*fake)(nil)
