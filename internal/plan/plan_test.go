package plan

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"datants/internal/manifest"
)

func makeFile(t *testing.T, path, content string, mod time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.Chtimes(path, mod, mod)
}

func runCompare(t *testing.T, srcRoot, dstRoot string, verify bool, recs []manifest.Record) []Task {
	t.Helper()
	return runCompareOpts(t, srcRoot, dstRoot, verify, false, false, false, recs)
}

func runCompareOpts(t *testing.T, srcRoot, dstRoot string, verify, syncMeta, deleteStale, dirsIncluded bool, recs []manifest.Record) []Task {
	t.Helper()
	p := &Planner{Source: Source{
		SrcRoot:      srcRoot,
		DstRoot:      dstRoot,
		Verify:       verify,
		SyncMeta:     syncMeta,
		DeleteStale:  deleteStale,
		DirsIncluded: dirsIncluded,
	}}
	var tasks []Task
	p.Emit = func(tk Task) error {
		tasks = append(tasks, tk)
		return nil
	}
	dir := srcRoot
	if len(recs) > 0 {
		dir = filepath.Dir(recs[0].Path)
	}
	if err := p.CompareDir(dir, recs); err != nil {
		t.Fatal(err)
	}
	return tasks
}

func TestPlanCopiesMissingFile(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "new.txt"), "content", mod)

	rec := manifest.Record{Path: filepath.Join(src, "new.txt"), Size: 7, ModTime: mod.Unix(), Mode: 0o644}
	tasks := runCompare(t, src, dst, false, []manifest.Record{rec})

	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	tk := tasks[0]
	if tk.Kind != CopyTask {
		t.Errorf("expected CopyTask, got %v", tk.Kind)
	}
	if filepath.Base(tk.DstPath) != "new.txt" {
		t.Errorf("dst path = %q", tk.DstPath)
	}
}

func TestPlanSkipsIdenticalFile(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "same.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "same.txt"), "data", mod)

	rec := manifest.Record{Path: filepath.Join(src, "same.txt"), Size: 4, ModTime: mod.Unix(), Mode: 0o644}
	tasks := runCompare(t, src, dst, false, []manifest.Record{rec})
	if len(tasks) != 0 {
		t.Fatalf("expected 0 tasks for identical file, got %d", len(tasks))
	}
}

func TestPlanRecopiesChangedSize(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "f.bin"), "longer-content", mod)
	makeFile(t, filepath.Join(dst, "f.bin"), "short", mod)

	rec := manifest.Record{Path: filepath.Join(src, "f.bin"), Size: 14, ModTime: mod.Unix(), Mode: 0o644}
	tasks := runCompare(t, src, dst, false, []manifest.Record{rec})
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task for size change, got %d", len(tasks))
	}
}

func TestPlanRecopiesChangedMtime(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	oldMod := time.Unix(1600000000, 0)
	newMod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "m.txt"), "data", newMod)
	makeFile(t, filepath.Join(dst, "m.txt"), "data", oldMod)

	rec := manifest.Record{Path: filepath.Join(src, "m.txt"), Size: 4, ModTime: newMod.Unix(), Mode: 0o644}
	tasks := runCompare(t, src, dst, false, []manifest.Record{rec})
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task for mtime change, got %d", len(tasks))
	}
}

func TestPlanMkdirForMissingDir(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	rec := manifest.Record{Path: filepath.Join(src, "empty") + string(os.PathSeparator), Size: 0, ModTime: 0, Mode: 0o755, IsDir: true}
	tasks := runCompare(t, src, dst, false, []manifest.Record{rec})
	if len(tasks) != 1 || tasks[0].Kind != MkdirTask {
		t.Fatalf("expected 1 MkdirTask, got %+v", tasks)
	}
}

func TestPlanForceCopiesEverything(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "f.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "f.txt"), "data", mod)

	rec := manifest.Record{Path: filepath.Join(src, "f.txt"), Size: 4, ModTime: mod.Unix(), Mode: 0o644}
	tasks := runCompare(t, src, dst, true, []manifest.Record{rec})
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task in force mode, got %d", len(tasks))
	}
}

func TestPlanMetaOnModeChange(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "m.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "m.txt"), "data", mod)
	// dst explicitly differs in mode (read-only vs 0644)
	if err := os.Chmod(filepath.Join(dst, "m.txt"), 0o444); err != nil {
		t.Fatal(err)
	}

	rec := manifest.Record{Path: filepath.Join(src, "m.txt"), Size: 4, ModTime: mod.Unix(), Mode: 0o644}
	tasks := runCompareOpts(t, src, dst, false, true, false, false, []manifest.Record{rec})
	if len(tasks) != 1 || tasks[0].Kind != MetaTask {
		t.Fatalf("expected 1 MetaTask, got %+v", tasks)
	}
	if tasks[0].DstPath != filepath.Join(dst, "m.txt") {
		t.Errorf("meta dst = %q", tasks[0].DstPath)
	}
}

func TestPlanNoMetaChangeIgnoredWhenSyncMetaOff(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "m.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "m.txt"), "data", mod)
	if err := os.Chmod(filepath.Join(dst, "m.txt"), 0o444); err != nil {
		t.Fatal(err)
	}

	rec := manifest.Record{Path: filepath.Join(src, "m.txt"), Size: 4, ModTime: mod.Unix(), Mode: 0o644}
	tasks := runCompareOpts(t, src, dst, false, false, false, false, []manifest.Record{rec})
	if len(tasks) != 0 {
		t.Fatalf("expected 0 tasks without sync-meta, got %+v", tasks)
	}
}

func TestPlanDeletesStaleFile(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "keep.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "keep.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "stale.txt"), "old", mod)

	rec := manifest.Record{Path: filepath.Join(src, "keep.txt"), Size: 4, ModTime: mod.Unix(), Mode: 0o644}
	tasks := runCompareOpts(t, src, dst, false, false, true, false, []manifest.Record{rec})
	if len(tasks) != 1 || tasks[0].Kind != DeleteTask {
		t.Fatalf("expected 1 DeleteTask, got %+v", tasks)
	}
	if filepath.Base(tasks[0].DstPath) != "stale.txt" {
		t.Errorf("delete dst = %q", tasks[0].DstPath)
	}
}

func TestPlanKeepsStaleWhenDeleteStaleOff(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	makeFile(t, filepath.Join(src, "keep.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "keep.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "stale.txt"), "old", mod)

	rec := manifest.Record{Path: filepath.Join(src, "keep.txt"), Size: 4, ModTime: mod.Unix(), Mode: 0o644}
	tasks := runCompareOpts(t, src, dst, false, false, false, false, []manifest.Record{rec})
	if len(tasks) != 0 {
		t.Fatalf("expected 0 tasks, got %+v", tasks)
	}
}

func TestPlanDirDeletionRequiresDirsIncluded(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	mod := time.Unix(1700000000, 0)
	// both sides have an identical file and a directory `keepdir`
	makeFile(t, filepath.Join(src, "keep.txt"), "data", mod)
	makeFile(t, filepath.Join(dst, "keep.txt"), "data", mod)
	if err := os.MkdirAll(filepath.Join(src, "keepdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dst, "keepdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	// dst has a stale directory
	if err := os.MkdirAll(filepath.Join(dst, "olddir"), 0o755); err != nil {
		t.Fatal(err)
	}
	makeFile(t, filepath.Join(dst, "olddir", "x.txt"), "old", mod)
	recs := []manifest.Record{
		{Path: filepath.Join(src, "keep.txt"), Size: 4, ModTime: mod.Unix(), Mode: 0o644},
		{Path: filepath.Join(src, "keepdir") + string(os.PathSeparator), Size: 0, ModTime: 0, Mode: 0o755, IsDir: true},
	}

	// without DirsIncluded the stale dir must NOT be deleted
	tasks := runCompareOpts(t, src, dst, false, false, true, false, recs)
	if len(tasks) != 0 {
		t.Fatalf("expected no deletes without DirsIncluded, got %+v", tasks)
	}

	// with DirsIncluded the stale dir is deleted, keepdir is kept
	tasks2 := runCompareOpts(t, src, dst, false, false, true, true, recs)
	if len(tasks2) != 1 || tasks2[0].Kind != DeleteTask {
		t.Fatalf("expected 1 DeleteTask with DirsIncluded, got %+v", tasks2)
	}
	if filepath.Base(tasks2[0].DstPath) != "olddir" {
		t.Errorf("delete dst = %q", tasks2[0].DstPath)
	}
}

func TestRel(t *testing.T) {
	p := &Planner{Source: Source{SrcRoot: `/mnt/src`, DstRoot: `/mnt/dst`}}
	rel := p.Rel(`/mnt/src/a/b.txt`)
	want := filepath.Join("a", "b.txt")
	if rel != want {
		t.Errorf("rel = %q, want %q", rel, want)
	}
}
