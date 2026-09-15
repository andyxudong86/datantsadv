package control_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	migratev1 "datants/gen/migrate/v1"
	"datants/internal/control"
	"datants/internal/history"
	"datants/internal/taskstore"
)

// claimAndRun simulates a fleet of workers by repeatedly claiming tasks via the
// controller RPC handlers and reporting them as successful. It returns the
// number of tasks processed and the total payload bytes reported.
func claimAndRun(t *testing.T, ctl *control.Controller, worker string) (int, int64) {
	t.Helper()
	ctx := context.Background()
	done := 0
	var bytes int64
	for {
		resp, err := ctl.Claim(ctx, &migratev1.ClaimRequest{WorkerId: worker, BatchSize: 1000})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if len(resp.Tasks) == 0 {
			return done, bytes
		}
		var results []*migratev1.Result
		for _, tk := range resp.Tasks {
			results = append(results, &migratev1.Result{TaskId: tk.Id, Ok: true, Bytes: 111})
		}
		if _, err := ctl.Report(ctx, &migratev1.ReportRequest{WorkerId: worker, Results: results}); err != nil {
			t.Fatalf("report: %v", err)
		}
		done += len(results)
		bytes += int64(len(results)) * 111
	}
}

// waitForRunEnd waits until the controller reports no active run, failing after
// timeout.
func waitForRunEnd(t *testing.T, ctl *control.Controller, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := ctl.CurrentRun(); !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("run did not end before timeout")
}

func newStory(t *testing.T) (*control.Controller, *history.Store, *taskstore.Store, string, string) {
	t.Helper()
	src := t.TempDir()
	for i := 0; i < 5; i++ {
		if err := os.WriteFile(filepath.Join(src, fmt.Sprintf("f%d.txt", i)), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dst := t.TempDir()

	state := t.TempDir()
	store, err := taskstore.New(state)
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
		ClaimTimeout:     250 * time.Millisecond,
		ProgressInterval: 25 * time.Millisecond,
	})
	t.Cleanup(func() {
		ctl.Stop()
		hist.Close()
		store.Done()
	})
	ctl.Start()
	return ctl, hist, store, src, dst
}

func TestRunLifecycleCompletes(t *testing.T) {
	ctl, hist, _, src, dst := newStory(t)

	// paused shouldn't matter here; a fresh controller is running by default.
	if got := ctl.RunState(); got != control.RunRunning {
		t.Fatalf("default state = %v, want running", got)
	}

	id, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("StartRun returned empty id")
	}

	// the run must be active while it drains
	if _, ok := ctl.CurrentRun(); !ok {
		t.Fatal("run should be active right after StartRun")
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}

	done, bytes := claimAndRun(t, ctl, "test-worker")
	if done != 5 {
		t.Fatalf("processed %d tasks, want 5", done)
	}

	waitForRunEnd(t, ctl, 5*time.Second)

	if got := ctl.RunState(); got != control.RunIdle {
		t.Fatalf("state after completion = %v, want idle", got)
	}

	r, ok := hist.Get(id)
	if !ok {
		t.Fatal("run missing from history")
	}
	if r.Status != history.StatusDone {
		t.Fatalf("run status = %s, want done", r.Status)
	}
	if r.Result.FilesDone != 5 {
		t.Fatalf("files_done = %d, want 5", r.Result.FilesDone)
	}
	if r.Result.BytesDone != bytes {
		t.Fatalf("bytes_done = %d, want %d", r.Result.BytesDone, bytes)
	}
	if r.Duration() <= 0 {
		t.Fatalf("run duration = %v, want > 0", r.Duration())
	}
	if r.Src != src || r.Dst != dst {
		t.Fatalf("run src/dst wrong: %q %q", r.Src, r.Dst)
	}
	if r.Result.AvgBytesSec <= 0 {
		t.Fatalf("avg rate = %v, want > 0", r.Result.AvgBytesSec)
	}
}

func TestStartRunArgValidation(t *testing.T) {
	ctl, _, _, _, _ := newStory(t)
	if _, err := ctl.StartRun("", "/d", control.RunConfig{}); err == nil {
		t.Fatal("StartRun with empty src should fail")
	}
	if _, err := ctl.StartRun("/s", "", control.RunConfig{}); err == nil {
		t.Fatal("StartRun with empty dst should fail")
	}
}

func TestStartRunRejectsConcurrentRun(t *testing.T) {
	ctl, _, _, src, dst := newStory(t)
	if _, err := ctl.StartRun(src, dst, control.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.StartRun(src, dst, control.RunConfig{}); err == nil {
		t.Fatal("second StartRun while one is active should fail")
	}
	// stopping frees the slot for a new run
	if err := ctl.StopRun(); err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.StartRun(src, dst, control.RunConfig{}); err != nil {
		t.Fatalf("StartRun after stop failed: %v", err)
	}
}

func TestPauseHoldsClaims(t *testing.T) {
	ctl, _, _, src, dst := newStory(t)
	if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2}); err != nil {
		t.Fatal(err)
	}
	// wait for scan to enqueue the 5 tasks
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}

	if err := ctl.Pause(); err != nil {
		t.Fatal(err)
	}
	if got := ctl.RunState(); got != control.RunPaused {
		t.Fatalf("state = %v, want paused", got)
	}

	// no tasks may be claimed while paused
	resp, err := ctl.Claim(context.Background(), &migratev1.ClaimRequest{WorkerId: "w", BatchSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 0 {
		t.Fatalf("claimed %d tasks while paused", len(resp.Tasks))
	}
	// pause twice must fail (already paused)
	if err := ctl.Pause(); err == nil {
		t.Fatal("double pause should fail")
	}

	if err := ctl.Resume(); err != nil {
		t.Fatal(err)
	}
	done, _ := claimAndRun(t, ctl, "w")
	if done != 5 {
		t.Fatalf("processed %d after resume, want 5", done)
	}
	waitForRunEnd(t, ctl, 5*time.Second)
}

func TestStopRunFinalizesStopped(t *testing.T) {
	ctl, hist, _, src, dst := newStory(t)
	id, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}

	if err := ctl.StopRun(); err != nil {
		t.Fatal(err)
	}
	if got := ctl.RunState(); got != control.RunIdle {
		t.Fatalf("state after stop = %v, want idle", got)
	}
	if _, ok := ctl.CurrentRun(); ok {
		t.Fatal("run still active after stop")
	}

	// claims must be held after stop
	resp, _ := ctl.Claim(context.Background(), &migratev1.ClaimRequest{WorkerId: "w", BatchSize: 1000})
	if len(resp.Tasks) != 0 {
		t.Fatalf("claimed %d tasks after stop", len(resp.Tasks))
	}

	r, ok := hist.Get(id)
	if !ok {
		t.Fatal("stopped run missing from history")
	}
	if r.Status != history.StatusStopped {
		t.Fatalf("run status = %s, want stopped", r.Status)
	}
	if r.FinishedAt.IsZero() {
		t.Fatal("stopped run has no FinishedAt")
	}
	// double stop is a no-op error
	if err := ctl.StopRun(); err == nil {
		t.Fatal("StopRun with no active run should fail")
	}
}

func TestFreshRunClearsTaskStore(t *testing.T) {
	ctl, _, store, src, dst := newStory(t)
	// pre-seed a task as if a previous migration left residue
	if err := ctl.IngestTasks([]taskstore.Task{
		{ID: "stale", Kind: taskstore.KindCopy, SrcPath: "/old/s", DstPath: "/old/d", Size: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2, Fresh: true}); err != nil {
		t.Fatal(err)
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}
	st := store.Stats()
	if st.Done != 0 || st.Failed != 0 {
		t.Fatalf("fresh run kept old task state: %+v", st)
	}
	done, _ := claimAndRun(t, ctl, "fresh-worker")
	if done != 5 {
		t.Fatalf("processed %d tasks in fresh run, want 5", done)
	}
	waitForRunEnd(t, ctl, 5*time.Second)
}

func TestReportFailureAccounting(t *testing.T) {
	ctl, hist, _, src, dst := newStory(t)
	id, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !waitScanDone(t, ctl) {
		t.Fatal("scan did not finish")
	}

	// process first two fine, fail the rest
	ctx := context.Background()
	var okOk, okFail []*migratev1.Result
	for i := 0; i < 5; i++ {
		resp, err := ctl.Claim(ctx, &migratev1.ClaimRequest{WorkerId: "w", BatchSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Tasks) == 0 {
			continue
		}
		tk := resp.Tasks[0]
		if i < 2 {
			okOk = append(okOk, &migratev1.Result{TaskId: tk.Id, Ok: true, Bytes: 10})
		} else {
			okFail = append(okFail, &migratev1.Result{TaskId: tk.Id, Ok: false, Error: "boom"})
		}
	}
	if _, err := ctl.Report(ctx, &migratev1.ReportRequest{WorkerId: "w", Results: append(okOk, okFail...)}); err != nil {
		t.Fatal(err)
	}
	// the store drained: the run auto-completes once the reclaim ticker fires.
	waitForRunEnd(t, ctl, 5*time.Second)

	r, ok := hist.Get(id)
	if !ok {
		t.Fatal("run missing from history")
	}
	if r.Status != history.StatusDone {
		t.Fatalf("run status = %s, want done", r.Status)
	}
	if r.Result.FilesFailed != 3 {
		t.Fatalf("files_failed = %d, want 3", r.Result.FilesFailed)
	}
	if r.Result.FilesDone != 2 {
		t.Fatalf("files_done = %d, want 2", r.Result.FilesDone)
	}
	if r.Result.BytesDone != 20 {
		t.Fatalf("bytes_done = %d, want 20", r.Result.BytesDone)
	}
}

func waitScanDone(t *testing.T, ctl *control.Controller) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s, ok := ctl.CurrentRun()
		if ok && s.ScanDone {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
