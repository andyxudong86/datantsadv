package taskstore

import (
	"path/filepath"
	"testing"
	"time"
)

func mkTasks(n int) []Task {
	out := make([]Task, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, Task{ID: itos(i), Kind: KindCopy, SrcPath: "/s/" + itos(i), DstPath: "/d/" + itos(i), Size: 4})
	}
	return out
}

func TestIngestAndClaim(t *testing.T) {
	s := mustNew(t)
	if err := s.Ingest(mkTasks(10)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Claim("w1", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("claimed %d, want 4", len(got))
	}
	st := s.Stats()
	if st.Pending != 6 || st.InProgress != 4 {
		t.Fatalf("stats wrong: %+v", st)
	}
	// atomic: a second claim never returns the same ids
	taken := map[string]bool{}
	for _, tk := range got {
		taken[tk.ID] = true
	}
	got2, _ := s.Claim("w2", 10)
	for _, tk := range got2 {
		if taken[tk.ID] {
			t.Fatalf("task %s double-assigned", tk.ID)
		}
	}
}

func TestFailThenReingestRequeues(t *testing.T) {
	// Regression: a task that failed must return to the pending queue when it
	// is re-ingested on the next scan, otherwise a restarted controller would
	// never re-copy what was lost (users saw "only one directory migrated").
	s := mustNew(t)
	ts := mkTasks(2)
	if err := s.Ingest(ts); err != nil {
		t.Fatal(err)
	}
	claimed, _ := s.Claim("w1", 2)
	var ids []string
	for _, tk := range claimed {
		ids = append(ids, tk.ID)
	}
	if err := s.Fail(ids); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.Failed != 2 {
		t.Fatalf("expected 2 failed, got %+v", st)
	}

	// re-ingest the same tasks (as an incremental re-scan would)
	if err := s.Ingest(ts); err != nil {
		t.Fatal(err)
	}
	st = s.Stats()
	if st.Failed != 0 || st.Pending != 2 {
		t.Fatalf("failed tasks not requeued: %+v", st)
	}

	// and they must be claimable again
	got, _ := s.Claim("w2", 2)
	if len(got) != 2 {
		t.Fatalf("reclaimed %d tasks, want 2", len(got))
	}
}

func TestLifecycleComplete(t *testing.T) {
	s := mustNew(t)
	_ = s.Ingest(mkTasks(5))
	tks, _ := s.Claim("w1", 5)
	var ids []string
	for _, tk := range tks {
		ids = append(ids, tk.ID)
	}
	if err := s.Complete(ids); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.Done != 5 {
		t.Fatalf("expected 5 done, got %+v", st)
	}
}

func TestClaimEmpty(t *testing.T) {
	s := mustNew(t)
	got, err := s.Claim("w1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no tasks, got %d", len(got))
	}
}

func TestReclaimStale(t *testing.T) {
	s := mustNew(t)
	_ = s.Ingest(mkTasks(3))
	_, _ = s.Claim("w1", 3)
	now := time.Now()
	// nothing stale yet
	n, err := s.ReclaimStale(time.Minute, now)
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	// pretend 10 minutes passed
	n, _ = s.ReclaimStale(5*time.Minute, now.Add(10*time.Minute))
	if n != 3 {
		t.Fatalf("reclaimed %d, want 3", n)
	}
	st := s.Stats()
	if st.Pending != 3 {
		t.Fatalf("expected 3 pending after reclaim, got %+v", st)
	}
	// can re-claim now
	got, _ := s.Claim("w1", 3)
	if len(got) != 3 {
		t.Fatalf("reclaim-claim got %d", len(got))
	}
}

func TestFail(t *testing.T) {
	s := mustNew(t)
	_ = s.Ingest(mkTasks(2))
	tks, _ := s.Claim("w1", 2)
	ids := []string{tks[0].ID}
	if err := s.Fail(ids); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.Failed != 1 || st.InProgress != 1 {
		t.Fatalf("stats wrong: %+v", st)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s1, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = s1.Ingest(mkTasks(5))
	tks, _ := s1.Claim("w1", 2)
	_ = s1.Complete([]string{tks[0].ID})
	st := s1.Stats()
	if st.Done != 1 || st.InProgress != 1 || st.Pending != 3 {
		t.Fatalf("pre-close stats: %+v", st)
	}
	if err := s1.Done(); err != nil {
		t.Fatal(err)
	}

	// reopen; state must be reconstructed from journal
	s2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Done()
	st2 := s2.Stats()
	if st2 != st {
		t.Fatalf("after reopen stats %+v != pre-close %+v", st2, st)
	}
	// the done task must remain done, in-progress stays in-progress
	if _, stat, ok := s2.Get(tks[0].ID); !ok || stat != Done {
		t.Fatalf("task %s status=%v", tks[0].ID, stat)
	}
	if _, stat, ok := s2.Get(tks[1].ID); !ok || stat != InProgress {
		t.Fatalf("task %s status=%v", tks[1].ID, stat)
	}
}

func TestSnapshotCompaction(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	// force enough entries to trigger compaction
	_ = s.Ingest(mkTasks(20000))
	if _, err := s.Claim("w1", 100); err != nil {
		t.Fatal(err)
	}
	if err := s.Done(); err != nil {
		t.Fatal(err)
	}
	snap, _ := filepath.Glob(filepath.Join(dir, "*.dat"))
	if len(snap) != 1 {
		t.Fatalf("expected 1 snapshot file, found %v", snap)
	}

	// reopen and verify counts survive
	s2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Done()
	st := s2.Stats()
	if st.Pending+st.InProgress != 20000 {
		t.Fatalf("recovered %+v, want 20000 total", st)
	}
	if st.InProgress != 100 {
		t.Fatalf("in-progress = %d, want 100", st.InProgress)
	}
}

func TestQuery(t *testing.T) {
	s := mustNew(t)
	ts := mkTasks(5)
	if err := s.Ingest(ts); err != nil {
		t.Fatal(err)
	}
	claimed, _ := s.Claim("w1", 2)
	var inProgress, done []string
	for i, tk := range claimed {
		if i == 0 {
			inProgress = append(inProgress, tk.ID)
		} else {
			done = append(done, tk.ID)
		}
	}
	_ = s.Complete(done)

	// status filter
	items, total := s.Query(Pending, 0, 0)
	if total != 3 || len(items) != 3 {
		t.Fatalf("pending: total=%d items=%d, want 3/3", total, len(items))
	}
	for _, it := range items {
		if it.Status != Pending || it.Owner != "" {
			t.Fatalf("unexpected pending item %+v", it)
		}
	}
	items, total = s.Query(InProgress, 0, 0)
	if total != 1 || len(items) != 1 || items[0].ID != inProgress[0] || items[0].Owner != "w1" {
		t.Fatalf("in-progress: total=%d items=%v", total, items)
	}
	if _, total = s.Query(AnyStatus, 0, 0); total != 5 {
		t.Fatalf("any total=%d, want 5", total)
	}

	// ordering by id
	items, _ = s.Query(AnyStatus, 0, 100)
	for i := 1; i < len(items); i++ {
		if items[i-1].ID >= items[i].ID {
			t.Fatalf("not sorted by id: %v", items)
		}
	}

	// pagination: offset skips, limit caps
	items, total = s.Query(AnyStatus, 2, 2)
	if total != 5 || len(items) != 2 {
		t.Fatalf("page: total=%d items=%d, want 5/2", total, len(items))
	}
	full, _ := s.Query(AnyStatus, 0, 100)
	if items[0].ID != full[2].ID || items[1].ID != full[3].ID {
		t.Fatalf("offset page misaligned: %v vs %v", items, full)
	}
	if got, _ := s.Query(AnyStatus, 10, 10); got != nil {
		t.Fatalf("offset beyond range should be empty, got %v", got)
	}
}

func TestReset(t *testing.T) {
	s := mustNew(t)
	if err := s.Ingest(mkTasks(5)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim("w1", 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Reset(); err != nil {
		t.Fatal(err)
	}
	if got := s.Stats(); got.Pending+got.InProgress+got.Done+got.Failed != 0 {
		t.Fatalf("stats after reset: %+v", got)
	}

	// fresh store must survive a reload with empty state
	dir := filepath.Dir(s.snapshotPath)
	if err := s.Done(); err != nil {
		t.Fatal(err)
	}
	s2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Done()
	if got := s2.Stats(); got.Pending+got.InProgress+got.Done+got.Failed != 0 {
		t.Fatalf("reloaded store not empty: %+v", got)
	}
}

func mustNew(t *testing.T) *Store {
	t.Helper()
	s, err := New(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Done() })
	return s
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
