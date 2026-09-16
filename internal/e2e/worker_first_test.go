package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWorkerUpBeforeRunStart reproduces the operator flow "start a worker first,
// wait until it is online on the Workers page, then start the migration": the
// pre-started resident worker must pick up tasks immediately and stay
// connected (never "go offline") while the run progresses and completes.
func TestWorkerUpBeforeRunStart(t *testing.T) {
	if testing.Short() {
		t.Skip("binary smoke test skipped in -short mode")
	}

	binDir := t.TempDir()
	controlBin := goBuild(t, filepath.Join(binDir, "da_control"), "datants/cmd/da_control")
	workerBin := goBuild(t, filepath.Join(binDir, "da_worker"), "datants/cmd/da_worker")

	src := t.TempDir()
	dst := t.TempDir()
	state := t.TempDir()
	logs := t.TempDir()
	ctlLog := filepath.Join(logs, "control.log")

	for i := 0; i < 40; i++ {
		mustWrite(t, filepath.Join(src, fmt.Sprintf("f%02d.txt", i)), fmt.Sprintf("file %d\n", i))
	}

	grpcPort := freePort(t)
	httpPort := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)

	startProc(t, controlBin, ctlLog,
		"--state", state,
		"--listen", fmt.Sprintf("127.0.0.1:%d", grpcPort),
		"--http", fmt.Sprintf("127.0.0.1:%d", httpPort),
		"--claim-timeout", "5s",
		"--reclaim-interval", "500ms",
		"--worker-bin", workerBin,
		"--log", ctlLog,
	)

	waitFor(t, 20*time.Second, "web console", func() (bool, error) {
		req, _ := http.NewRequest(http.MethodGet, base+"/", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode == http.StatusOK && bytes.Contains(body, []byte("DataNTs 迁移控制台")), nil
	})

	// 1) start the local worker BEFORE any migration is running
	postJSON(t, base+"/api/workers/spawn",
		map[string]any{"count": 1, "capacity": 4, "batch": 8, "id_prefix": "pre"},
		http.StatusOK,
	)
	const workerID = "pre-00"
	t.Cleanup(func() { postJSON(t, base+"/api/workers/stop", map[string]any{"id": workerID}, -1) })

	// 2) wait until it registered and shows online
	waitFor(t, 20*time.Second, "pre-started worker online", func() (bool, error) {
		var out struct {
			Workers []struct {
				ID        string `json:"id"`
				Connected bool   `json:"connected"`
			} `json:"workers"`
		}
		if err := getJSON(t, base+"/api/workers", &out); err != nil {
			return false, err
		}
		for _, w := range out.Workers {
			if w.ID == workerID && w.Connected {
				return true, nil
			}
		}
		return false, nil
	})

	// 3) now start the migration
	postJSON(t, base+"/api/start", map[string]any{
		"src": src, "dst": dst, "workers": 2, "fresh": true,
	}, http.StatusOK)

	// 4) the previously-started worker must stay connected and do the copying
	completed := make(chan struct{})
	go func() {
		var st statusResp
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			_ = getJSON(t, base+"/api/status", &st)
			if st.State == "idle" && st.Tasks.Total > 0 && st.Tasks.Done == st.Tasks.Total && st.Tasks.Failed == 0 {
				close(completed)
				return
			}
			time.Sleep(150 * time.Millisecond)
		}
		t.Errorf("migration did not complete: state=%s tasks=%+v", st.State, st.Tasks)
		close(completed)
	}()

	offline := make(chan string, 4)
	go func() {
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			var out struct {
				Workers []struct {
					ID        string `json:"id"`
					Connected bool   `json:"connected"`
				} `json:"workers"`
			}
			if err := getJSON(t, base+"/api/workers", &out); err == nil {
				for _, w := range out.Workers {
					if w.ID == workerID && !w.Connected {
						offline <- "worker went offline during run"
						return
					}
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
		offline <- ""
	}()

	<-completed
	if msg := <-offline; msg != "" {
		t.Fatalf("%s (worker %s disconnected mid-migration)", msg, workerID)
	}

	// 5) every file must be at the destination
	for i := 0; i < 40; i++ {
		got, err := os.ReadFile(filepath.Join(dst, fmt.Sprintf("f%02d.txt", i)))
		if err != nil {
			t.Fatalf("read dst/f%02d.txt: %v", i, err)
		}
		if string(got) != fmt.Sprintf("file %d\n", i) {
			t.Errorf("dst/f%02d.txt = %q", i, got)
		}
	}

	// 6) and the resident worker is still alive after the run
	var final struct {
		Spawned []struct {
			ID    string `json:"id"`
			Alive bool   `json:"alive"`
		} `json:"spawned"`
	}
	if err := getJSON(t, base+"/api/workers", &final); err != nil {
		t.Fatal(err)
	}
	for _, sp := range final.Spawned {
		if sp.ID == workerID && !sp.Alive {
			t.Errorf("spawned worker %s is no longer alive after the run", workerID)
		}
	}

	if t.Failed() {
		dumpLog(t, ctlLog)
	}
}

// TestStartFormRejectsDuplicateRun checks that starting a second migration while
// one is in flight is refused by the API (409), matching what the console
// enforces client-side by disabling the start button during a run.
func TestStartFormRejectsDuplicateRun(t *testing.T) {
	if testing.Short() {
		t.Skip("binary smoke test skipped in -short mode")
	}
	binDir := t.TempDir()
	controlBin := goBuild(t, filepath.Join(binDir, "da_control"), "datants/cmd/da_control")

	src := t.TempDir()
	dst := t.TempDir()
	for i := 0; i < 5; i++ {
		mustWrite(t, filepath.Join(src, fmt.Sprintf("f%d.txt", i)), "x\n")
	}
	state := t.TempDir()
	logs := t.TempDir()
	httpPort := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	startProc(t, controlBin, filepath.Join(logs, "control.log"),
		"--state", state,
		"--listen", fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		"--http", base[len("http://"):],
		"--log", filepath.Join(logs, "control.log"),
	)
	waitFor(t, 20*time.Second, "web console", func() (bool, error) {
		req, _ := http.NewRequest(http.MethodGet, base+"/", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK, nil
	})

	postJSON(t, base+"/api/start", map[string]any{"src": src, "dst": dst}, http.StatusOK)
	waitFor(t, 20*time.Second, "run active", func() (bool, error) {
		var st statusResp
		if err := getJSON(t, base+"/api/status", &st); err != nil {
			return false, err
		}
		return st.State == "running", nil
	})
	// while in flight a second start must be a 409, not a 200
	postJSON(t, base+"/api/start", map[string]any{"src": src, "dst": dst}, http.StatusConflict)
}
