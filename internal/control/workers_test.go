package control_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	migratev1 "datants/gen/migrate/v1"
	"datants/internal/control"
	"datants/internal/history"
	"datants/internal/taskstore"
)

func freshParts(t *testing.T) (*taskstore.Store, *history.Store) {
	t.Helper()
	store, err := taskstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hist, err := history.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store, hist
}

func registerWorker(t *testing.T, ctl *control.Controller, id string) {
	t.Helper()
	if _, err := ctl.Register(context.Background(), &migratev1.RegisterRequest{
		WorkerId: id,
		Address:  "10.0.0." + id,
		Capacity: 8,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerTracking(t *testing.T) {
	ctl, _, _, src, dst := newStory(t)
	registerWorker(t, ctl, "w1")

	if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2}); err != nil {
		t.Fatal(err)
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}
	done, bytes := claimAndRun(t, ctl, "w1")
	if done != 5 {
		t.Fatalf("processed %d, want 5", done)
	}
	waitForRunEnd(t, ctl, 5*time.Second)

	ws := ctl.Workers()
	if len(ws) != 1 {
		t.Fatalf("workers = %d, want 1", len(ws))
	}
	w := ws[0]
	if w.ID != "w1" || w.Address != "10.0.0.w1" || w.Capacity != 8 {
		t.Fatalf("worker meta wrong: %+v", w)
	}
	if !w.Connected {
		t.Fatal("freshly reporting worker should be connected")
	}
	if w.FilesDone != 5 || w.BytesDone != bytes {
		t.Fatalf("worker counters wrong: %+v", w)
	}
	if w.Joined.IsZero() || w.LastSeen.IsZero() {
		t.Fatal("worker timestamps not set")
	}
}

func TestWorkerConnectedTimeout(t *testing.T) {
	store, hist := freshParts(t)
	defer store.Done()
	defer hist.Close()
	ctl := control.NewController(store, control.Config{History: hist, WorkerTimeout: 40 * time.Millisecond})
	t.Cleanup(func() { ctl.Stop() })
	ctl.Start()

	registerWorker(t, ctl, "w1")
	if ws := ctl.Workers(); len(ws) != 1 || !ws[0].Connected {
		t.Fatalf("worker should be connected right after register: %+v", ws)
	}
	time.Sleep(100 * time.Millisecond)
	if ws := ctl.Workers(); len(ws) != 1 || ws[0].Connected {
		t.Fatalf("worker should be disconnected after timeout: %+v", ws)
	}

	// a heartbeat brings it back
	if _, err := ctl.Heartbeat(context.Background(), &migratev1.HeartbeatRequest{WorkerId: "w1"}); err != nil {
		t.Fatal(err)
	}
	if ws := ctl.Workers(); !ws[0].Connected {
		t.Fatalf("worker should reconnect after heartbeat: %+v", ws)
	}
}

func TestServerState(t *testing.T) {
	ctl, _, _, src, dst := newStory(t)
	registerWorker(t, ctl, "w1")

	st := ctl.State()
	if st.RunState != control.RunRunning {
		t.Fatalf("state runstate = %v", st.RunState)
	}
	if st.Run != nil {
		t.Fatalf("no run expected, got %+v", st.Run)
	}
	if len(st.Workers) != 1 || st.Workers[0].ID != "w1" {
		t.Fatalf("state workers = %+v", st.Workers)
	}
	if st.Now.IsZero() {
		t.Fatal("state now missing")
	}

	if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2}); err != nil {
		t.Fatal(err)
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}
	claimAndRun(t, ctl, "w1")
	waitForRunEnd(t, ctl, 5*time.Second)

	st = ctl.State()
	if st.RunState != control.RunIdle || st.Run != nil {
		t.Fatalf("state after completion wrong: %+v", st)
	}
}

func TestReScanRequeuesChangedFiles(t *testing.T) {
	ctl, _, _, src, dst := newStory(t)
	if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2}); err != nil {
		t.Fatal(err)
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}

	// hold 4 tasks in-progress (never reported), leaving 1 pending: the run
	// stays active and there is a live claim slot to rescan into.
	ctx := context.Background()
	resp, err := ctl.Claim(ctx, &migratev1.ClaimRequest{WorkerId: "w1", BatchSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 4 {
		t.Fatalf("claimed %d, want 4", len(resp.Tasks))
	}

	// a new file appears mid-run
	if err := os.WriteFile(filepath.Join(src, "fX.txt"), []byte("new-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ctl.ReScan(); err != nil {
		t.Fatal(err)
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("rescan did not finish")
	}

	// everything pending (the leftover + the new file) must be processable
	done, _ := claimAndRun(t, ctl, "w1")
	if done != 2 {
		t.Fatalf("processed %d after rescan, want 2", done)
	}
	// finish the 4 held tasks
	results := make([]*migratev1.Result, 0, len(resp.Tasks))
	for _, tk := range resp.Tasks {
		results = append(results, &migratev1.Result{TaskId: tk.Id, Ok: true, Bytes: 1})
	}
	if _, err := ctl.Report(ctx, &migratev1.ReportRequest{WorkerId: "w1", Results: results}); err != nil {
		t.Fatal(err)
	}
	waitForRunEnd(t, ctl, 5*time.Second)
	if done+len(results) != 6 {
		t.Fatalf("total processed = %d, want 6", done+len(results))
	}
}

func TestReScanErrors(t *testing.T) {
	ctl, _, _, src, dst := newStory(t)
	if err := ctl.ReScan(); err == nil {
		t.Fatal("ReScan with no active run should fail")
	}
	if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2}); err != nil {
		t.Fatal(err)
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}
	if err := ctl.StopRun(); err != nil {
		t.Fatal(err)
	}
	if err := ctl.ReScan(); err == nil {
		t.Fatal("ReScan after stop should fail")
	}
}
