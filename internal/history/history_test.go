package history

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreCreateGetList(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}

	older := &Run{
		ID:        "run-a",
		StartedAt: time.Unix(100, 0),
		Status:    StatusDone,
	}
	newer := &Run{
		ID:        "run-b",
		StartedAt: time.Unix(200, 0),
		Status:    StatusFailed,
	}
	if err := s.Create(older); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(newer); err != nil {
		t.Fatal(err)
	}

	// newest first
	all := s.List()
	if len(all) != 2 {
		t.Fatalf("list len = %d, want 2", len(all))
	}
	if all[0].ID != "run-b" || all[1].ID != "run-a" {
		t.Fatalf("list order = [%s %s], want [run-b run-a]", all[0].ID, all[1].ID)
	}

	got, ok := s.Get("run-a")
	if !ok || got.ID != "run-a" {
		t.Fatalf("get: ok=%v got=%+v", ok, got)
	}
	if _, ok := s.Get("nope"); ok {
		t.Fatal("get of unknown id unexpectedly succeeded")
	}

	// Create fills in defaults.
	auto := &Run{Dst: "/x"}
	if err := s.Create(auto); err != nil {
		t.Fatal(err)
	}
	if auto.ID == "" || auto.StartedAt.IsZero() || auto.Status != StatusRunning {
		t.Fatalf("defaults not applied: %+v", auto)
	}
}

func TestStoreUpdateAndReload(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	r := &Run{ID: "run-1", Src: "/s", Dst: "/d"}
	if err := s.Create(r); err != nil {
		t.Fatal(err)
	}

	end := time.Now()
	ok, err := s.Update("run-1", func(r *Run) {
		r.Status = StatusDone
		r.FinishedAt = end
		r.Result.FilesDone = 42
		r.Result.BytesDone = 99
	})
	if err != nil || !ok {
		t.Fatalf("update: ok=%v err=%v", ok, err)
	}

	// reopen
	s2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Get("run-1")
	if !ok {
		t.Fatal("run-1 missing after reload")
	}
	if got.Status != StatusDone || got.Result.FilesDone != 42 || got.Result.BytesDone != 99 {
		t.Fatalf("reloaded run wrong: %+v", got)
	}
	if !got.FinishedAt.Equal(end) {
		t.Fatalf("finished_at lost: %v want %v", got.FinishedAt, end)
	}

	// updating an unknown id reports false
	if ok, _ := s2.Update("nope", nil); ok {
		t.Fatal("update of unknown id should return false")
	}
}

func TestStoreReloadMarksInterruptedRunsStopped(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	running := &Run{ID: "run-inflight", StartedAt: time.Now(), Status: StatusRunning}
	if err := s.Create(running); err != nil {
		t.Fatal(err)
	}

	s2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := s2.Get("run-inflight")
	if !ok {
		t.Fatal("run missing after reload")
	}
	if got.Status != StatusStopped {
		t.Fatalf("interrupted run status = %s, want stopped", got.Status)
	}
	if got.FinishedAt.IsZero() {
		t.Fatal("interrupted run has no FinishedAt")
	}
}

func TestStoreCorruptFile(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(&Run{ID: "ok"}); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(dir, "run_bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := New(dir); err == nil {
		t.Fatal("expected error opening store with corrupt run file")
	}
	os.Remove(bad)

	s3, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(s3.List()) != 1 {
		t.Fatalf("list len = %d, want 1", len(s3.List()))
	}
}

func TestRunDuration(t *testing.T) {
	start := time.Now()
	r := &Run{StartedAt: start.Add(-time.Minute)}
	if d := r.Duration(); d != time.Minute {
		t.Fatalf("running duration = %v, want 1m", d)
	}
	r.FinishedAt = start
	if d := r.Duration(); d != time.Minute {
		t.Fatalf("finished duration = %v, want 1m", d)
	}
}

func TestNewID(t *testing.T) {
	a := NewID()
	b := NewID()
	if a == b {
		t.Fatalf("ids collided: %s", a)
	}
	if !strings.HasPrefix(a, "run-") {
		t.Fatalf("id %q lacks run- prefix", a)
	}
}
