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

// TestMultiTargetRun starts one migration with two src/src pairs in a single
// /api/start call and verifies both destinations receive their files, the run
// reports both targets, and history keeps both pairs.
func TestMultiTargetRun(t *testing.T) {
	if testing.Short() {
		t.Skip("binary smoke test skipped in -short mode")
	}

	binDir := t.TempDir()
	controlBin := goBuild(t, filepath.Join(binDir, "da_control"), "datants/cmd/da_control")
	workerBin := goBuild(t, filepath.Join(binDir, "da_worker"), "datants/cmd/da_worker")

	state := t.TempDir()
	logs := t.TempDir()
	ctlLog := filepath.Join(logs, "control.log")

	var pair struct {
		Src string
		Dst string
	}
	pair.Src, pair.Dst = makePair(t, 5, "a")
	second := struct{ Src, Dst string }{}
	second.Src, second.Dst = makePair(t, 3, "b")

	grpcPort := freePort(t)
	httpPort := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	startProc(t, controlBin, ctlLog,
		"--state", state,
		"--listen", fmt.Sprintf("127.0.0.1:%d", grpcPort),
		"--http", fmt.Sprintf("127.0.0.1:%d", httpPort),
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
	postJSON(t, base+"/api/workers/spawn",
		map[string]any{"count": 1, "capacity": 4, "batch": 8, "id_prefix": "multi"},
		http.StatusOK,
	)
	const workerID = "multi-00"
	t.Cleanup(func() { postJSON(t, base+"/api/workers/stop", map[string]any{"id": workerID}, -1) })
	waitFor(t, 20*time.Second, "worker online", func() (bool, error) {
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

	// one start request, two targets
	postJSON(t, base+"/api/start", map[string]any{
		"src": pair.Src, "dst": pair.Dst,
		"targets": []map[string]string{
			{"src": pair.Src, "dst": pair.Dst},
			{"src": second.Src, "dst": second.Dst},
		},
		"workers": 2, "fresh": true,
	}, http.StatusOK)

	waitFor(t, 90*time.Second, "multi-target run to complete", func() (bool, error) {
		var st statusResp
		if err := getJSON(t, base+"/api/status", &st); err != nil {
			return false, err
		}
		if st.State == "idle" && st.Tasks.Total > 0 && st.Tasks.Done == st.Tasks.Total && st.Tasks.Failed == 0 {
			return true, nil
		}
		return false, nil
	})

	for i := 0; i < 5; i++ {
		if _, err := os.Stat(filepath.Join(pair.Dst, fmt.Sprintf("a%02d.txt", i))); err != nil {
			t.Errorf("dst %s missing a%02d.txt: %v", pair.Dst, i, err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := os.Stat(filepath.Join(second.Dst, fmt.Sprintf("b%02d.txt", i))); err != nil {
			t.Errorf("dst %s missing b%02d.txt: %v", second.Dst, i, err)
		}
	}

	// history keeps both targets
	var runs struct {
		Runs []struct {
			ID      string `json:"id"`
			Status  string `json:"status"`
			Targets []struct {
				Src string `json:"src"`
				Dst string `json:"dst"`
			} `json:"targets"`
			Result struct {
				FilesDone int64 `json:"files_done"`
			} `json:"result"`
		} `json:"runs"`
	}
	if err := getJSON(t, base+"/api/runs", &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Runs) == 0 {
		t.Fatal("no runs in history")
	}
	last := runs.Runs[0]
	if last.Status != "done" {
		t.Fatalf("last run status = %s", last.Status)
	}
	if len(last.Targets) != 2 {
		t.Fatalf("history targets = %+v, want 2", last.Targets)
	}
	if last.Targets[0].Src != pair.Src || last.Targets[1].Dst != second.Dst {
		t.Fatalf("history targets = %+v", last.Targets)
	}
	if last.Result.FilesDone != 8 {
		t.Fatalf("files_done = %d, want 8", last.Result.FilesDone)
	}

	if t.Failed() {
		dumpLog(t, ctlLog)
	}
}

func makePair(t *testing.T, n int, prefix string) (src, dst string) {
	t.Helper()
	src = t.TempDir()
	dst = t.TempDir()
	for i := 0; i < n; i++ {
		mustWrite(t, filepath.Join(src, fmt.Sprintf("%s%02d.txt", prefix, i)), fmt.Sprintf("%s-%d\n", prefix, i))
	}
	return src, dst
}
