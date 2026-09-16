package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// helpers

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

type statusResp struct {
	State string `json:"state"`
	Tasks struct {
		Pending    int `json:"pending"`
		InProgress int `json:"in_progress"`
		Done       int `json:"done"`
		Failed     int `json:"failed"`
		Total      int `json:"total"`
	} `json:"tasks"`
}

func getJSON(t *testing.T, url string, out any) error {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func postJSON(t *testing.T, url string, body any, want int) ([]byte, int) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if want >= 0 && resp.StatusCode != want {
		t.Fatalf("POST %s = %d (want %d): %s", url, resp.StatusCode, want, tail(string(rb), 500))
	}
	return rb, resp.StatusCode
}

// waitFor polls fn until it reports ok or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, desc string, fn func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := fn()
		if err != nil {
			lastErr = err
		} else if ok {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("timed out waiting for %s: %v", desc, lastErr)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

// proc wraps a child process and guarantees it is killed before the temp dirs
// go away (Windows can otherwise hold log/state files open).
type proc struct {
	cmd *exec.Cmd
}

func startProc(t *testing.T, name, logFile string, args ...string) *proc {
	t.Helper()
	cmd := exec.Command(name, args...)
	lf, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	p := &proc{cmd: cmd}
	t.Cleanup(func() {
		p.kill()
		lf.Close()
	})
	return p
}

func (p *proc) kill() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	p.cmd.Process.Kill()
	p.cmd.Wait()
}

func dumpLog(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	t.Logf("--- log tail %s ---\n%s", filepath.Base(path), tail(string(data), 3000))
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}

// ---------------------------------------------------------------------------
// the smoke test

func TestDistributedBinarySmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("binary smoke test skipped in -short mode")
	}

	binDir := t.TempDir() // separate so binary builds survive while we delete logs
	controlBin := goBuild(t, filepath.Join(binDir, "da_control"), "datants/cmd/da_control")
	workerBin := goBuild(t, filepath.Join(binDir, "da_worker"), "datants/cmd/da_worker")

	src := t.TempDir()
	dst := t.TempDir()
	state := t.TempDir()
	logs := t.TempDir()
	ctlLog := filepath.Join(logs, "control.log")
	workerLog := filepath.Join(logs, "worker.log")

	// --- seed source and (partial, stale) destination trees ---
	mustWrite(t, filepath.Join(src, "a.txt"), "hello e2e\n")
	sub := filepath.Join(src, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	rnd := rand.New(rand.NewSource(42))
	blob := make([]byte, 256<<10)
	if _, err := rnd.Read(blob); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.bin"), blob, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dst, "stale.txt"), "must be deleted")

	grpcPort := freePort(t)
	httpPort := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)

	// --- boot the control plane ---
	p := startProc(t, controlBin, ctlLog,
		"--src", src,
		"--dst", dst,
		"--state", state,
		"--listen", fmt.Sprintf("127.0.0.1:%d", grpcPort),
		"--http", fmt.Sprintf("127.0.0.1:%d", httpPort),
		"--include-empty-dirs",
		"--delete-stale",
		"--claim-timeout", "5s",
		"--reclaim-interval", "500ms",
		"--batch", "8",
		"--log", ctlLog,
	)
	defer p.kill()

	// --- the web console must come up ---
	waitFor(t, 20*time.Second, "web console", func() (bool, error) {
		req, _ := http.NewRequest(http.MethodGet, base+"/", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, fmt.Errorf("console HTTP %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		return bytes.Contains(body, []byte("DataNTs 迁移控制台")), nil
	})

	// --- SSE stream pushes a status frame on connect ---
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/events", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("SSE connect: %v", err)
		}
		defer resp.Body.Close()
		buf := make([]byte, 4096)
		sniff := bytes.Buffer{}
		for {
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				sniff.Write(buf[:n])
				if bytes.Contains(sniff.Bytes(), []byte("event: status")) {
					return
				}
			}
			if rerr != nil {
				break
			}
		}
		t.Fatalf("SSE: no status frame (ctx deadline hit), got %q", sniff.String())
	}()

	// --- boot the worker (it retries until the controller listens) ---
	startProc(t, workerBin, workerLog,
		"--controller", fmt.Sprintf("127.0.0.1:%d", grpcPort),
		"--id", "e2e-w1",
		"--capacity", "4",
		"--batch", "8",
		"--heartbeat", "1s",
		"--log", workerLog,
	)

	// --- worker must register and appear online ---
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
			if w.ID == "e2e-w1" && w.Connected {
				return true, nil
			}
		}
		return false, nil
	})

	// --- the whole migration must complete with zero failures ---
	var final statusResp
	waitFor(t, 90*time.Second, "migration complete", func() (bool, error) {
		st := statusResp{}
		if err := getJSON(t, base+"/api/status", &st); err != nil {
			return false, err
		}
		if st.State == "running" {
			return false, nil
		}
		if st.State == "idle" && st.Tasks.Done > 0 &&
			st.Tasks.Done == st.Tasks.Total && st.Tasks.Failed == 0 {
			final = st
			return true, nil
		}
		return false, fmt.Errorf("state=%s tasks=%+v", st.State, st.Tasks)
	})
	t.Logf("final task counts: total=%d done=%d failed=%d",
		final.Tasks.Total, final.Tasks.Done, final.Tasks.Failed)
	if final.Tasks.Total != 5 {
		// 2 copy (a.txt, sub/b.bin) + 2 mkdir (sub, emptydir; both missing in
		// dst and created because of --include-empty-dirs) + 1 delete (stale.txt)
		t.Errorf("expected 5 tasks, got %d", final.Tasks.Total)
	}

	// --- commands after completion must be rejected deterministically ---
	for _, cmdPath := range []string{"/api/stop", "/api/rescan"} {
		resp, err := http.Post(base+cmdPath, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Errorf("POST %s = %d, want 409", cmdPath, resp.StatusCode)
		}
	}

	// --- the destination must match the expectation on disk ---
	gotA, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil {
		t.Fatalf("read dst/a.txt: %v", err)
	}
	if string(gotA) != "hello e2e\n" {
		t.Errorf("dst/a.txt = %q", gotA)
	}
	gotB, err := os.ReadFile(filepath.Join(dst, "sub", "b.bin"))
	if err != nil {
		t.Fatalf("read dst/sub/b.bin: %v", err)
	}
	if !bytes.Equal(gotB, blob) {
		t.Errorf("dst/sub/b.bin content mismatch (%d bytes)", len(gotB))
	}
	empty, err := os.Stat(filepath.Join(dst, "emptydir"))
	if err != nil {
		t.Fatalf("stat dst/emptydir: %v", err)
	}
	if !empty.IsDir() {
		t.Errorf("dst/emptydir is not a directory")
	}
	if entries, _ := os.ReadDir(filepath.Join(dst, "emptydir")); len(entries) != 0 {
		t.Errorf("dst/emptydir not empty: %d entries", len(entries))
	}
	if _, err := os.Stat(filepath.Join(dst, "stale.txt")); !os.IsNotExist(err) {
		t.Errorf("dst/stale.txt should have been deleted")
	}

	// --- history must record a completed run ---
	var runs struct {
		Runs []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"runs"`
	}
	if err := getJSON(t, base+"/api/runs", &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Runs) != 1 {
		t.Errorf("expected exactly 1 history run, got %d", len(runs.Runs))
		for _, r := range runs.Runs {
			t.Logf("  run %s status=%s", r.ID, r.Status)
		}
	} else if runs.Runs[0].Status != "done" {
		t.Errorf("history run status = %q, want done", runs.Runs[0].Status)
	}

	// --- task query shows a mixed inventory ---
	var tasks struct {
		Tasks []struct {
			Kind   string `json:"kind"`
			Status string `json:"status"`
		} `json:"tasks"`
		Total int `json:"total"`
	}
	if err := getJSON(t, base+"/api/tasks?status=all&limit=1000", &tasks); err != nil {
		t.Fatal(err)
	}
	if tasks.Total != 5 {
		t.Errorf("/api/tasks total = %d, want 5", tasks.Total)
	}
	seen := map[string]bool{}
	mkdirs := 0
	for _, tk := range tasks.Tasks {
		seen[tk.Kind+":"+tk.Status] = true
		if tk.Kind == "mkdir" {
			mkdirs++
		}
	}
	for _, want := range []string{"copy:done", "mkdir:done", "delete:done"} {
		if !seen[want] {
			t.Errorf("task inventory missing %q (got %v)", want, seen)
		}
	}
	if mkdirs != 2 {
		t.Errorf("expected 2 mkdir tasks, got %d", mkdirs)
	}

	if t.Failed() {
		dumpLog(t, ctlLog)
		dumpLog(t, workerLog)
	}
}

func TestWebonlyStandbySmoke(t *testing.T) {
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

	mustWrite(t, filepath.Join(src, "note.txt"), "started from standby\n")
	blob := make([]byte, 32<<10)
	for i := range blob {
		blob[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(src, "big"), blob, 0o644); err != nil {
		t.Fatal(err)
	}

	grpcPort := freePort(t)
	httpPort := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)

	// Standby mode: NO --src/--dst — the run must NOT start by itself. The
	// console is allowed to launch local workers via --worker-bin.
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

	// POST /api/start must succeed with a 200 — it would 409 if da_control had
	// auto-started a run at boot, so this proves standby mode.
	body, _ := json.Marshal(map[string]any{
		"src":       src,
		"dst":       dst,
		"workers":   2,
		"fresh":     true,
		"sync_meta": false,
		// The console-spawned worker connects from the loopback, so it must be
		// reachable under a run restricted to its own IP.
		"worker_ips": []string{"127.0.0.1"},
	})
	resp, err := http.Post(base+"/api/start", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /api/start: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/start = %d, want 200 (a run already active?)", resp.StatusCode)
	}

	// Worker comes up through the CONSOLE, not the shell: the control process
	// spawns da_worker as a child (—worker-bin).
	postJSON(t, base+"/api/workers/spawn",
		map[string]any{"count": 1, "capacity": 4, "batch": 8, "id_prefix": "guispawn"},
		http.StatusOK,
	)
	spawnedWorker := "guispawn-00"
	t.Cleanup(func() {
		// best-effort: never leave a managed worker running after the test even
		// if the assertions above fail early.
		postJSON(t, base+"/api/workers/stop", map[string]any{"id": spawnedWorker}, -1)
	})

	// The spawned worker must register through gRPC and come online and the
	// console must track it as a managed process.
	waitFor(t, 20*time.Second, "console-spawned worker online", func() (bool, error) {
		var out struct {
			Workers []struct {
				ID        string `json:"id"`
				Connected bool   `json:"connected"`
			} `json:"workers"`
			Spawned []struct {
				ID    string `json:"id"`
				Alive bool   `json:"alive"`
				PID   int    `json:"pid"`
			} `json:"spawned"`
		}
		if err := getJSON(t, base+"/api/workers", &out); err != nil {
			return false, err
		}
		registered := false
		for _, w := range out.Workers {
			if w.ID == spawnedWorker && w.Connected {
				registered = true
			}
		}
		managed := false
		for _, sp := range out.Spawned {
			if sp.ID == spawnedWorker && sp.Alive && sp.PID > 0 {
				managed = true
			}
		}
		return registered && managed, nil
	})

	var final statusResp
	waitFor(t, 90*time.Second, "migration complete", func() (bool, error) {
		st := statusResp{}
		if err := getJSON(t, base+"/api/status", &st); err != nil {
			return false, err
		}
		if st.State == "running" {
			return false, nil
		}
		if st.State == "idle" && st.Tasks.Done > 0 &&
			st.Tasks.Done == st.Tasks.Total && st.Tasks.Failed == 0 {
			final = st
			return true, nil
		}
		return false, fmt.Errorf("state=%s tasks=%+v", st.State, st.Tasks)
	})
	t.Logf("standby run: total=%d done=%d failed=%d",
		final.Tasks.Total, final.Tasks.Done, final.Tasks.Failed)

	if got, err := os.ReadFile(filepath.Join(dst, "note.txt")); err != nil || string(got) != "started from standby\n" {
		t.Errorf("dst/note.txt = %q, err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(dst, "big")); err != nil || !bytes.Equal(got, blob) {
		t.Errorf("dst/big mismatch (err=%v, %d bytes)", err, len(got))
	}

	var runs struct {
		Runs []struct {
			Status string `json:"status"`
		} `json:"runs"`
	}
	if err := getJSON(t, base+"/api/runs", &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs.Runs) != 1 || runs.Runs[0].Status != "done" {
		t.Errorf("history = %+v, want one done run", runs.Runs)
	}

	// The console can also shut the worker down again.
	postJSON(t, base+"/api/workers/stop", map[string]any{"id": spawnedWorker}, http.StatusOK)

	if t.Failed() {
		dumpLog(t, ctlLog)
		dumpLog(t, filepath.Join(state, "workers", spawnedWorker+".log"))
	}
}
