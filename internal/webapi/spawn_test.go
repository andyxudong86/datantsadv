package webapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"datants/internal/webapi"
	"datants/internal/workerctl"
)

// TestHelperChild parks the re-invoked test binary so spawn tests can operate on
// real OS processes without shipping an actual da_worker.
func TestHelperChild(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		t.Skip("helper process re-invocation only")
	}
	for {
		time.Sleep(time.Hour)
	}
}

func spawnFactory(t *testing.T) workerctl.Factory {
	t.Helper()
	return func(name string, args ...string) (workerctl.Process, error) {
		full := append([]string{"-test.run=^TestHelperChild$"}, args...)
		cmd := exec.Command(os.Args[0], full...)
		cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return &osProc{cmd: cmd}, nil
	}
}

type osProc struct{ cmd *exec.Cmd }

func (p *osProc) Pid() int    { return p.cmd.Process.Pid }
func (p *osProc) Kill() error { return p.cmd.Process.Kill() }
func (p *osProc) Wait() error { return p.cmd.Wait() }

func newSpawnServer(t *testing.T, wb string) (*httptest.Server, *webapi.Server, *workerctl.Manager) {
	t.Helper()
	m := workerctl.NewManager(spawnFactory(t))
	srv := webapi.NewServer(newFake(),
		webapi.WithWorkerSpawning(m, webapi.SpawnConfig{
			WorkerBin:      wb,
			ControllerAddr: "127.0.0.1:5005",
			LogDir:         t.TempDir(),
		}))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, srv, m
}

func TestSpawnDisabled(t *testing.T) {
	srv := webapi.NewServer(newFake())

	if rr := do(t, srv.Handler(), "POST", "/api/workers/spawn", `{"count":1}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("spawn when disabled: code=%d body=%s", rr.Code, rr.Body)
	}
	if rr := do(t, srv.Handler(), "POST", "/api/workers/stop", `{"id":"x"}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("stop when disabled: code=%d body=%s", rr.Code, rr.Body)
	}
}

func TestSpawnAndStopWorkers(t *testing.T) {
	_, srv, mgr := newSpawnServer(t, os.Args[0])

	rr := do(t, srv.Handler(), "POST", "/api/workers/spawn",
		`{"count":2,"capacity":4,"batch":16,"id_prefix":"web"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("spawn: code=%d body=%s", rr.Code, rr.Body)
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	spawnedSl, _ := resp["spawned"].([]any)
	if len(spawnedSl) != 2 {
		t.Fatalf("spawned len = %d, want 2: %s", len(spawnedSl), rr.Body)
	}

	first, _ := spawnedSl[0].(map[string]any)
	id, _ := first["id"].(string)
	pid, _ := first["pid"].(float64)
	if id != "web-00" || pid == 0 {
		t.Fatalf("first spawned = %+v", first)
	}
	if !mgr.Alive("web-00") || !mgr.Alive("web-01") {
		t.Fatal("spawned workers not alive")
	}

	// GET /api/workers must surface the managed workers too
	wrr := do(t, srv.Handler(), "GET", "/api/workers", "")
	var workers map[string]any
	if err := json.Unmarshal(wrr.Body.Bytes(), &workers); err != nil {
		t.Fatal(err)
	}
	spawnedSl2, _ := workers["spawned"].([]any)
	if len(spawnedSl2) != 2 {
		t.Fatalf("workers.spawned len = %d, want 2", len(spawnedSl2))
	}

	// stop each; second stop is a 404
	if rr := do(t, srv.Handler(), "POST", "/api/workers/stop", `{"id":"web-00"}`); rr.Code != http.StatusOK {
		t.Fatalf("stop web-00: code=%d body=%s", rr.Code, rr.Body)
	}
	if rr := do(t, srv.Handler(), "POST", "/api/workers/stop", `{"id":"web-00"}`); rr.Code != http.StatusNotFound {
		t.Fatalf("second stop web-00: code=%d body=%s", rr.Code, rr.Body)
	}
	if rr := do(t, srv.Handler(), "POST", "/api/workers/stop", `{"id":"web-01"}`); rr.Code != http.StatusOK {
		t.Fatalf("stop web-01: code=%d body=%s", rr.Code, rr.Body)
	}
	if mgr.Alive("web-00") || mgr.Alive("web-01") {
		t.Fatal("workers still alive after stop")
	}
}

func TestSpawnValidation(t *testing.T) {
	_, srv, _ := newSpawnServer(t, os.Args[0])

	cases := []struct {
		body string
		want int
	}{
		{`{"count":999,"capacity":4,"batch":16}`, http.StatusBadRequest},
		{`{"count":1,"capacity":0,"batch":16}`, http.StatusBadRequest},
		{`{"count":1,"capacity":4096,"batch":16}`, http.StatusBadRequest},
		{`{"count":1,"capacity":1,"batch":0}`, http.StatusBadRequest},
		{`{"capacity":2,"batch":8}`, http.StatusOK}, // count defaults to 1
	}
	for _, c := range cases {
		rr := do(t, srv.Handler(), "POST", "/api/workers/spawn", c.body)
		if rr.Code != c.want {
			t.Fatalf("body %s: code=%d want=%d body=%s", c.body, rr.Code, c.want, rr.Body)
		}
	}
}
