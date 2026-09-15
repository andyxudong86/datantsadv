package webapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	migratev1 "datants/gen/migrate/v1"
	"datants/internal/control"
	"datants/internal/history"
	"datants/internal/taskstore"
	"datants/internal/webapi"
)

// TestEndToEndRun drives a real Controller through the HTTP API: start a run
// via POST /api/start, drain the queue as a simulated worker, watch the run
// complete via SSE/status polling and confirm it lands in the history and the
// task store stats match.
func TestEndToEndRun(t *testing.T) {
	src := t.TempDir()
	for i := 0; i < 3; i++ {
		if err := os.WriteFile(filepath.Join(src, fmt.Sprintf("f%d.txt", i)), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dst := t.TempDir()
	state := t.TempDir()

	store, err := taskstore.New(filepath.Join(state, "store"))
	if err != nil {
		t.Fatal(err)
	}
	hist, err := history.New(filepath.Join(state, "history"))
	if err != nil {
		t.Fatal(err)
	}
	ctl := control.NewController(store, control.Config{
		History:          hist,
		ReclaimInterval:  10 * time.Millisecond,
		ClaimTimeout:     200 * time.Millisecond,
		ProgressInterval: 20 * time.Millisecond,
	})
	t.Cleanup(func() {
		ctl.Stop()
		hist.Close()
		store.Done()
	})
	ctl.Start()

	httpSrv := httptest.NewServer(webapi.NewServer(ctl).Handler())
	defer httpSrv.Close()
	s := &client{url: httpSrv.URL, t: t}

	// the controller is "running" by default but has no active run
	if got := s.status().State; got != control.RunRunning.String() {
		t.Fatalf("initial state = %q", got)
	}
	if s.status().Run != nil {
		t.Fatal("no run should be active initially")
	}

	// start a run
	id := s.start(src, dst)
	if id == "" {
		t.Fatal("start returned empty id")
	}

	// register the simulated worker
	ctl.Register(t.Context(), &migratev1.RegisterRequest{
		WorkerId: "w1",
		Address:  "10.0.0.1",
		Capacity: 8,
	})

	// drain the queue as a fake worker once the scan has finished
	ctxDone := make(chan struct{})
	go func() {
		defer close(ctxDone)
		// wait until the scan has ingested everything
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if cur, ok := ctl.CurrentRun(); ok && cur.ScanDone {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		for {
			resp, err := ctl.Claim(t.Context(), &migratev1.ClaimRequest{WorkerId: "w1", BatchSize: 1000})
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if len(resp.Tasks) == 0 {
				return
			}
			var results []*migratev1.Result
			for _, tk := range resp.Tasks {
				results = append(results, &migratev1.Result{TaskId: tk.Id, Ok: true, Bytes: 111})
			}
			if _, err := ctl.Report(t.Context(), &migratev1.ReportRequest{WorkerId: "w1", Results: results}); err != nil {
				t.Errorf("report: %v", err)
				return
			}
		}
	}()

	// wait for the worker goroutine to finish claiming everything
	select {
	case <-ctxDone:
	case <-time.After(10 * time.Second):
		t.Fatal("worker goroutine timed out")
	}

	// wait for the run to finalize via live status polling
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st := s.status()
		if st.State == control.RunIdle.String() && st.Run == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if cur, ok := ctl.CurrentRun(); ok {
		t.Fatalf("run still active: %s", cur.ID)
	}

	// final status and task stats
	st := s.status()
	if st.State != control.RunIdle.String() {
		t.Fatalf("final state = %q, want idle", st.State)
	}
	if st.Tasks.Done != 3 || st.Tasks.Total != 3 {
		t.Fatalf("final tasks = %+v", st.Tasks)
	}

	// the run must be recorded in history through the store
	runs := s.runs()
	if len(runs) != 1 {
		t.Fatalf("runs = %d, want 1", len(runs))
	}
	if runs[0].ID != id || runs[0].Status != string(history.StatusDone) {
		t.Fatalf("run = %+v", runs[0])
	}
	if runs[0].Result.FilesDone != 3 {
		t.Fatalf("files_done = %d", runs[0].Result.FilesDone)
	}

	// worker counters are visible in the API
	ws := s.workers()
	if len(ws) != 1 || ws[0].ID != "w1" || ws[0].FilesDone != 3 {
		t.Fatalf("workers = %+v", ws)
	}
}

// client is a tiny typed HTTP wrapper for the test.
type client struct {
	url string
	t   *testing.T
}

type apiStatus struct {
	State string `json:"state"`
	Run   *struct {
		ID       string `json:"id"`
		ScanDone bool   `json:"scan_done"`
	} `json:"run"`
	Tasks struct {
		Pending    int `json:"pending"`
		InProgress int `json:"in_progress"`
		Done       int `json:"done"`
		Failed     int `json:"failed"`
		Total      int `json:"total"`
	} `json:"tasks"`
}

func (c *client) do(method, path, body string) *http.Response {
	c.t.Helper()
	var resp *http.Response
	var err error
	client := &http.Client{Timeout: 10 * time.Second}
	if method == "GET" && body == "" {
		resp, err = client.Get(c.url + path)
	} else {
		var reqBody *bytes.Reader
		if body != "" {
			reqBody = bytes.NewReader([]byte(body))
		}
		req, err2 := http.NewRequest(method, c.url+path, reqBody)
		if err2 != nil {
			c.t.Fatal(err2)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err = client.Do(req)
	}
	if err != nil {
		c.t.Fatalf("request %s %s: %v", method, path, err)
	}
	return resp
}

func (c *client) status() apiStatus {
	c.t.Helper()
	resp := c.do("GET", "/api/status", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.t.Fatalf("status code = %d", resp.StatusCode)
	}
	var out apiStatus
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.t.Fatal(err)
	}
	return out
}

func (c *client) start(src, dst string) string {
	c.t.Helper()
	body := fmt.Sprintf(`{"src":%q,"dst":%q,"workers":2}`, src, dst)
	resp := c.do("POST", "/api/start", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.t.Fatalf("start code = %d", resp.StatusCode)
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.t.Fatal(err)
	}
	return out.ID
}

type apiRun struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Result struct {
		FilesDone int64 `json:"files_done"`
	} `json:"result"`
}

func (c *client) runs() []apiRun {
	c.t.Helper()
	resp := c.do("GET", "/api/runs", "")
	defer resp.Body.Close()
	var out struct {
		Runs []apiRun `json:"runs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.t.Fatal(err)
	}
	return out.Runs
}

func (c *client) workers() []struct {
	ID        string `json:"id"`
	FilesDone int64  `json:"files_done"`
} {
	c.t.Helper()
	resp := c.do("GET", "/api/workers", "")
	defer resp.Body.Close()
	var out struct {
		Workers []struct {
			ID        string `json:"id"`
			FilesDone int64  `json:"files_done"`
		} `json:"workers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.t.Fatal(err)
	}
	return out.Workers
}
