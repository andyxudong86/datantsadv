package control_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	migratev1 "datants/gen/migrate/v1"
	"datants/internal/control"
	"datants/internal/taskstore"
	"datants/internal/worker"
)

func startServer(t *testing.T, ctl *control.Controller) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	ctl.RegisterServer(s)
	ctl.Start()
	go s.Serve(lis)
	t.Cleanup(func() { s.Stop(); ctl.Stop() })
	return lis.Addr().String()
}

func runWorkers(t *testing.T, target string, n int, batch, capacity int) {
	t.Helper()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		conn, client, err := worker.DialControl(target, true)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		w := worker.New(client, worker.Config{BatchSize: batch, Capacity: capacity})
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(ctx); err != nil {
				t.Errorf("worker run: %v", err)
			}
		}()
	}
	wg.Wait()
}

func countFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	filepath.Walk(root, func(_ string, info os.FileInfo, _ error) error {
		if !info.IsDir() {
			n++
		}
		return nil
	})
	return n
}

func setupSource(t *testing.T) string {
	t.Helper()
	src := t.TempDir()

	// single large directory with many small files (the hard case)
	big := filepath.Join(src, "big")
	if err := os.MkdirAll(big, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if err := os.WriteFile(filepath.Join(big, fmt.Sprintf("f%04d.txt", i)), []byte("payload-"+itos(i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// nested tree
	nested := filepath.Join(src, "nested", "a")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(nested, "x.txt"), []byte("nested-content"), 0o644)

	// empty directory (MKDIR task)
	os.MkdirAll(filepath.Join(src, "empty"), 0o755)

	return src
}

func TestDistributedMigrationEndToEnd(t *testing.T) {
	src := setupSource(t)
	dst := t.TempDir()
	state := t.TempDir()

	store, err := taskstore.New(state)
	if err != nil {
		t.Fatal(err)
	}
	ctl := control.NewController(store, control.Config{})
	t.Cleanup(func() { store.Done() })

	ingested, err := ctl.ScanAndIngest(src, dst, control.ScanConfig{Workers: 4, IncludeDirs: true})
	if err != nil {
		t.Fatalf("scan/ingest: %v", err)
	}
	if ingested != 305 { // 301 files + 4 dirs (big, nested, nested/a, empty)
		t.Fatalf("ingested %d tasks, want 302", ingested)
	}

	target := startServer(t, ctl)
	runWorkers(t, target, 3, 16, 8)

	// every task must be done, none failed, none left pending/in-progress
	st := ctl.Stats()
	if st.Pending != 0 || st.InProgress != 0 || st.Failed != 0 {
		t.Fatalf("unfinished work: %+v", st)
	}
	if st.Done != 305 {
		t.Fatalf("done = %d, want 305", st.Done)
	}

	// destination must contain all 301 files (plus empty dir)
	if got := countFiles(t, dst); got != 301 {
		t.Fatalf("dst has %d files, want 301", got)
	}
	if _, err := os.Stat(filepath.Join(dst, "empty")); err != nil {
		t.Fatalf("empty dir not created: %v", err)
	}

	// content integrity spot check
	b, err := os.ReadFile(filepath.Join(dst, "big", "f0150.txt"))
	if err != nil || string(b) != "payload-150" {
		t.Fatalf("content mismatch: %q err=%v", b, err)
	}
	b2, _ := os.ReadFile(filepath.Join(dst, "nested", "a", "x.txt"))
	if string(b2) != "nested-content" {
		t.Fatalf("nested content mismatch: %q", b2)
	}
}

// TestReportFailureLogging verifies the Controller logs per-report failure
// details when a logger is configured, and that failed tasks are persisted as
// failed. This is the primary diagnostic when workers report copy errors.
func TestReportFailureLogging(t *testing.T) {
	state := t.TempDir()
	store, err := taskstore.New(state)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Done()

	var logBuf bytes.Buffer
	l := log.New(&logBuf, "", 0)

	if err := store.Ingest([]taskstore.Task{
		{ID: "a", Kind: taskstore.KindCopy, SrcPath: "/s/a", DstPath: "/d/a"},
		{ID: "b", Kind: taskstore.KindCopy, SrcPath: "/s/b", DstPath: "/d/b"},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	ctl := control.NewController(store, control.Config{Logger: l, ProgressInterval: 10 * time.Hour})
	ctx := context.Background()

	if _, err := ctl.Report(ctx, &migratev1.ReportRequest{
		WorkerId: "w1",
		Results: []*migratev1.Result{
			{TaskId: "a", Ok: true, Bytes: 10},
			{TaskId: "b", Ok: false, Error: "open /s/b: no such file or directory"},
		},
	}); err != nil {
		t.Fatalf("report: %v", err)
	}

	got := logBuf.String()
	if !strings.Contains(got, "worker=w1 ok=1 failed=1") {
		t.Fatalf("report log missing summary: %q", got)
	}
	if !strings.Contains(got, "open /s/b") {
		t.Fatalf("report log missing error detail: %q", got)
	}

	st := ctl.Stats()
	if st.Done != 1 || st.Failed != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestDistributedMigrationIncremental(t *testing.T) {
	src := setupSource(t)
	dst := t.TempDir()
	state := t.TempDir()

	store, _ := taskstore.New(state)
	ctl := control.NewController(store, control.Config{})
	t.Cleanup(func() { store.Done() })

	if _, err := ctl.ScanAndIngest(src, dst, control.ScanConfig{Workers: 4, IncludeDirs: true}); err != nil {
		t.Fatal(err)
	}
	target := startServer(t, ctl)
	runWorkers(t, target, 2, 16, 8)

	// re-run incremental: nothing should be enqueued
	if _, err := ctl.ScanAndIngest(src, dst, control.ScanConfig{Workers: 4, IncludeDirs: true}); err != nil {
		t.Fatal(err)
	}
	st := ctl.Stats()
	if st.Pending != 0 {
		t.Fatalf("incremental re-scan enqueued %d new tasks", st.Pending)
	}

	// modify one file and add one new file; only these must be picked up
	mod := filepath.Join(src, "big", "f0005.txt")
	os.WriteFile(mod, []byte("CHANGED"), 0o644)
	os.WriteFile(filepath.Join(src, "big", "fnew.txt"), []byte("brand-new"), 0o644)

	n, err := ctl.ScanAndIngest(src, dst, control.ScanConfig{Workers: 4, IncludeDirs: true})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("incremental ingest enqueued %d, want 2 (1 modified + 1 new)", n)
	}
	runWorkers(t, target, 2, 16, 8)

	b, _ := os.ReadFile(filepath.Join(dst, "big", "f0005.txt"))
	if string(b) != "CHANGED" {
		t.Fatalf("modified file not updated: %q", b)
	}
	if _, err := os.Stat(filepath.Join(dst, "big", "fnew.txt")); err != nil {
		t.Fatalf("new file not migrated: %v", err)
	}

	// regression: a brand-new directory (with files) must be fully migrated on
	// the next incremental scan, not just have its empty dir created.
	newdir := filepath.Join(src, "newdir")
	if err := os.MkdirAll(newdir, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(newdir, "a.txt"), []byte("AAA"), 0o644)
	os.WriteFile(filepath.Join(newdir, "b.txt"), []byte("BBB"), 0o644)

	n, err = ctl.ScanAndIngest(src, dst, control.ScanConfig{Workers: 4, IncludeDirs: true})
	if err != nil {
		t.Fatal(err)
	}
	// mkdir newdir (1) + a.txt + b.txt copies (2) = 3
	if n != 3 {
		t.Fatalf("new-directory ingest enqueued %d, want 3", n)
	}
	runWorkers(t, target, 2, 16, 8)

	for _, f := range []string{"a.txt", "b.txt"} {
		b, err := os.ReadFile(filepath.Join(dst, "newdir", f))
		if err != nil {
			t.Fatalf("newdir/%s not migrated: %v", f, err)
		}
		want := map[string]string{"a.txt": "AAA", "b.txt": "BBB"}[f]
		if string(b) != want {
			t.Fatalf("newdir/%s content %q, want %q", f, b, want)
		}
	}
}

func TestDistributedMigrationSyncMetaAndDelete(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	state := t.TempDir()

	// initial tree: a.txt (will get a metadata change), delme.txt (will be deleted)
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "a.txt"), []byte("AAA"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "delme.txt"), []byte("DDD"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, _ := taskstore.New(state)
	ctl := control.NewController(store, control.Config{})
	t.Cleanup(func() { store.Done() })

	cfg := control.ScanConfig{Workers: 4, IncludeDirs: true, SyncMeta: true, DeleteStale: true}
	if _, err := ctl.ScanAndIngest(src, dst, cfg); err != nil {
		t.Fatal(err)
	}
	target := startServer(t, ctl)
	runWorkers(t, target, 2, 16, 4)

	st := ctl.Stats()
	if st.Done != 3 || st.Pending != 0 || st.Failed != 0 {
		t.Fatalf("initial migration stats: %+v", st)
	}

	// 1) mode change on an unchanged-content file -> MetaTask (no recopy).
	//    0o444 is a real, stat-visible change on both unix and windows.
	os.Chmod(filepath.Join(src, "sub", "a.txt"), 0o444)
	n, err := ctl.ScanAndIngest(src, dst, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("meta scan ingested %d tasks, want 1", n)
	}
	runWorkers(t, target, 2, 16, 4)
	st = ctl.Stats()
	if st.Pending != 0 || st.Failed != 0 || st.Done != 4 {
		t.Fatalf("after meta scan stats: %+v", st)
	}
	info, err := os.Stat(filepath.Join(dst, "sub", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o444 {
		t.Fatalf("mode not synced: got %o want %o", info.Mode().Perm(), 0o444)
	}
	// content must be untouched by the meta pass
	b, _ := os.ReadFile(filepath.Join(dst, "sub", "a.txt"))
	if string(b) != "AAA" {
		t.Fatalf("meta pass changed content: %q", b)
	}

	// 2) delete a source file -> DeleteTask removes it from destination
	if err := os.Remove(filepath.Join(src, "sub", "delme.txt")); err != nil {
		t.Fatal(err)
	}
	n, err = ctl.ScanAndIngest(src, dst, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("delete scan ingested %d tasks, want 1", n)
	}
	runWorkers(t, target, 2, 16, 4)
	if _, err := os.Stat(filepath.Join(dst, "sub", "delme.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale file not deleted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "sub", "a.txt")); err != nil {
		t.Fatalf("kept file missing after delete pass: %v", err)
	}
}

func TestControllerRestartsFromState(t *testing.T) {
	// simulate a controller crash + restart: ingest+claim, then reopen store.
	state := t.TempDir()

	store1, _ := taskstore.New(state)
	ctl1 := control.NewController(store1, control.Config{})
	// claim a couple tasks to leave in-progress state
	_ = ctl1.IngestTasks([]taskstore.Task{
		{ID: "t1", Kind: taskstore.KindCopy, SrcPath: "s1", DstPath: "d1", Size: 1},
		{ID: "t2", Kind: taskstore.KindCopy, SrcPath: "s2", DstPath: "d2", Size: 1},
	})
	if _, err := store1.Claim("w1", 2); err != nil {
		t.Fatal(err)
	}
	_ = store1.Complete([]string{"t1"})
	if err := store1.Done(); err != nil {
		t.Fatal(err)
	}

	// restart
	store2, err := taskstore.New(state)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Done()
	ctl2 := control.NewController(store2, control.Config{})
	ctl2.Start()
	defer ctl2.Stop()

	// in-progress t2 should be reclaimed after timeout and become pending again
	time.Sleep(50 * time.Millisecond)
	st := ctl2.Stats()
	if st.Done != 1 {
		t.Fatalf("done = %d, want 1", st.Done)
	}
	if st.InProgress != 1 {
		t.Fatalf("t2 should be in-progress after restart, got %+v", st)
	}
}

func TestScanAndIngestRelativePaths(t *testing.T) {
	// Regression: relative src/dst roots must be absolutized before planning,
	// otherwise joining a relative dst with an absolute source-relative path
	// produces an invalid destination (e.g. `targettest\D:\...\sourcetest`).
	work := t.TempDir()
	prev, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(work); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(prev) })

	if err := os.MkdirAll("sourcetest", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("sourcetest", "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("targettest", 0o755); err != nil {
		t.Fatal(err)
	}

	store, err := taskstore.New(filepath.Join(work, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Done()
	ctl := control.NewController(store, control.Config{})

	n, err := ctl.ScanAndIngest("sourcetest", "targettest", control.ScanConfig{Workers: 2, IncludeDirs: true})
	if err != nil {
		t.Fatalf("scan/ingest with relative roots: %v", err)
	}
	// at least a copy task for a.txt must be produced
	if n < 1 {
		t.Fatalf("expected tasks from relative roots, got %d", n)
	}

	target := startServer(t, ctl)
	runWorkers(t, target, 1, 16, 4)

	// a.txt must land in the (relative) target dir with correct content
	b, err := os.ReadFile(filepath.Join("targettest", "a.txt"))
	if err != nil {
		t.Fatalf("relative migration did not copy into targettest: %v", err)
	}
	if string(b) != "hello" {
		t.Fatalf("content mismatch: %q", b)
	}

	// with absolute roots the same job must not re-enqueue (idempotent)
	absSrc, _ := filepath.Abs("sourcetest")
	absDst, _ := filepath.Abs("targettest")
	n2, err := ctl.ScanAndIngest(absSrc, absDst, control.ScanConfig{Workers: 2, IncludeDirs: true})
	if err != nil {
		t.Fatal(err)
	}
	st := ctl.Stats()
	if n2 != 0 || st.Pending != 0 {
		t.Fatalf("idempotency broken with absolute roots: n=%d stats=%+v", n2, st)
	}
}

func itos(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
