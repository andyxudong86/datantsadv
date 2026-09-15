package transfer

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"datants/internal/fsinfo"
	"datants/internal/plan"
)

func TestPoolCopiesFile(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	src := filepath.Join(srcDir, "f.txt")
	if err := os.WriteFile(src, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	mod := time.Unix(1700000000, 0)
	_ = os.Chtimes(src, mod, mod)

	var got []TaskResult
	var mu sync.Mutex
	pool := NewPool(Config{
		Workers:    2,
		BufferSize: 64,
		OnResult: func(r TaskResult) {
			mu.Lock()
			got = append(got, r)
			mu.Unlock()
		},
	})
	dst := filepath.Join(dstDir, "sub", "f.txt")
	pool.Submit(plan.Task{Kind: plan.CopyTask, SrcPath: src, DstPath: dst, Size: 11, ModTime: mod.Unix(), Mode: 0o644})
	pool.Close()

	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d", len(got))
	}
	if got[0].Err != nil {
		t.Fatalf("copy error: %v", got[0].Err)
	}
	content, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(content) != "hello world" {
		t.Errorf("dst content = %q", string(content))
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().Unix() != mod.Unix() {
		t.Errorf("mtime not preserved: %d", info.ModTime().Unix())
	}
	if _, err := os.Stat(dst + ".mig.tmp"); !os.IsNotExist(err) {
		t.Errorf("temp file not cleaned up")
	}
}

func TestPoolMkdirTask(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "a", "b")
	var got []TaskResult
	var mu sync.Mutex
	pool := NewPool(Config{
		Workers:  1,
		OnResult: func(r TaskResult) { mu.Lock(); got = append(got, r); mu.Unlock() },
	})
	pool.Submit(plan.Task{Kind: plan.MkdirTask, DstPath: dst, Mode: 0o755})
	pool.Close()
	if len(got) != 1 || got[0].Err != nil {
		t.Fatalf("unexpected results: %+v", got)
	}
	if info, err := os.Stat(dst); err != nil || !info.IsDir() {
		t.Errorf("mkdir failed: %v", err)
	}
}

func TestPoolMetaTask(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "m.txt")
	if err := os.WriteFile(dst, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 0o444 is observable on both Windows (read-only attr) and unix (perms)
	// and is distinct from the 0o644 write mode. Pass the file's real owner so
	// the unconditional chown in applyMeta is a no-op on unix (a non-root CI
	// runner must not be asked to reassign ownership).
	const want = 0o444
	info0, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	uid0, gid0 := fsinfo.Owner(info0)
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "freebsd" {
		// windows: only read-only flag is meaningful; check content + no error
		var got []TaskResult
		var mu sync.Mutex
		pool := NewPool(Config{
			Workers:  1,
			OnResult: func(r TaskResult) { mu.Lock(); got = append(got, r); mu.Unlock() },
		})
		pool.Submit(plan.Task{Kind: plan.MetaTask, DstPath: dst, Mode: want})
		pool.Close()
		if len(got) != 1 || got[0].Err != nil {
			t.Fatalf("unexpected results: %+v", got)
		}
		content, err := os.ReadFile(dst)
		if err != nil || string(content) != "keep" {
			t.Errorf("content changed by MetaTask: %q err=%v", string(content), err)
		}
		return
	}
	var got []TaskResult
	var mu sync.Mutex
	pool := NewPool(Config{
		Workers:  1,
		OnResult: func(r TaskResult) { mu.Lock(); got = append(got, r); mu.Unlock() },
	})
	pool.Submit(plan.Task{Kind: plan.MetaTask, DstPath: dst, Mode: want, Uid: uid0, Gid: gid0})
	pool.Close()
	if len(got) != 1 || got[0].Err != nil {
		t.Fatalf("unexpected results: %+v", got)
	}
	// the file's content is untouched
	content, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "keep" {
		t.Errorf("content changed by MetaTask: %q", string(content))
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != want {
		t.Errorf("mode not applied: got %o want %o", info.Mode().Perm(), want)
	}
}

func TestPoolMetaTaskChownWithoutPreserveOwner(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "freebsd" {
		t.Skip("owner application only meaningful on unix")
	}
	// A MetaTask must attempt the requested owner even when PreserveOwner (the
	// copy-path owner option) is off. It used to be gated behind that flag and
	// silently skipped, so a group change reported success without happening.
	// We can't force a chown outcome portably (root CI vs non-root): the point
	// is that the attempt happens — either the ownership changed or the task
	// failed with a chown error. A silent no-op success would fail this.
	dst := filepath.Join(t.TempDir(), "owner.txt")
	if err := os.WriteFile(dst, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	curUID, curGID := fsinfo.Owner(info)
	targetGID := curGID + 1 // definitely different, may not be settable by non-root

	var gotErr error
	pool := NewPool(Config{Workers: 1}) // PreserveOwner defaults to false
	pool.SetOnResult(func(r TaskResult) { gotErr = r.Err })
	pool.Submit(plan.Task{Kind: plan.MetaTask, DstPath: dst, Mode: 0o644, Uid: curUID, Gid: targetGID})
	pool.Close()

	gotInfo, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	_, gotGID := fsinfo.Owner(gotInfo)

	whose := "chown succeeded"
	if gotErr != nil {
		whose = "chown failed (reported)"
	}
	if gotGID == targetGID || gotErr != nil {
		t.Logf("MetaTask owner attempt observed: %s", whose)
		return
	}
	t.Fatalf("MetaTask silently ignored owner change: err=%v gid=%d target=%d", gotErr, gotGID, targetGID)
}

func TestPoolDeleteTask(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "gone", "file.txt")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// also delete a whole subtree via the same task kind
	sub := filepath.Join(t.TempDir(), "tree", "a", "b")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	var got []TaskResult
	var mu sync.Mutex
	pool := NewPool(Config{
		Workers:  2,
		OnResult: func(r TaskResult) { mu.Lock(); got = append(got, r); mu.Unlock() },
	})
	pool.Submit(plan.Task{Kind: plan.DeleteTask, DstPath: dst})
	pool.Submit(plan.Task{Kind: plan.DeleteTask, DstPath: filepath.Dir(filepath.Dir(sub))})
	pool.Close()
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d", len(got))
	}
	for _, r := range got {
		if r.Err != nil {
			t.Errorf("delete failed: %v", r.Err)
		}
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Errorf("file not deleted")
	}
	if _, err := os.Stat(filepath.Dir(filepath.Dir(sub))); !os.IsNotExist(err) {
		t.Errorf("directory tree not deleted")
	}
}

func TestPoolCopyPreservesOwnerWhenAsked(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "freebsd" {
		t.Skip("owner preservation only meaningful on unix")
	}
	src := filepath.Join(t.TempDir(), "s.txt")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	uid, gid := fsinfo.Owner(info)
	dst := filepath.Join(t.TempDir(), "d.txt")
	pool := NewPool(Config{Workers: 1, PreserveOwner: true})
	pool.Submit(plan.Task{Kind: plan.CopyTask, SrcPath: src, DstPath: dst, Mode: 0o644, Uid: uid, Gid: gid})
	pool.Close()
	dinfo, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	duid, dgid := fsinfo.Owner(dinfo)
	if duid != uid || dgid != gid {
		t.Errorf("owner not preserved: got %d:%d want %d:%d", duid, dgid, uid, gid)
	}
}

func TestPoolRetriesFailedSource(t *testing.T) {
	var got []TaskResult
	var mu sync.Mutex
	pool := NewPool(Config{
		Workers:  1,
		Retries:  2,
		OnResult: func(r TaskResult) { mu.Lock(); got = append(got, r); mu.Unlock() },
	})
	pool.Submit(plan.Task{
		Kind:    plan.CopyTask,
		SrcPath: filepath.Join(t.TempDir(), "missing.txt"),
		DstPath: filepath.Join(t.TempDir(), "out.txt"),
	})
	pool.Close()
	if len(got) != 1 {
		t.Fatalf("expected 1 result")
	}
	if got[0].Err == nil {
		t.Fatal("expected an error for missing source")
	}
	if got[0].Retries != 2 {
		t.Errorf("expected 2 retries, got %d", got[0].Retries)
	}
}

func TestPoolManySmallFiles(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	const n = 200
	var mu sync.Mutex
	var errCount int
	pool := NewPool(Config{
		Workers:    8,
		BufferSize: 1024,
		OnResult: func(r TaskResult) {
			if r.Err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
			}
		},
	})
	for i := 0; i < n; i++ {
		src := filepath.Join(srcDir, "x", string(rune('a'+i%26)), "f"+string(rune('a'+i%26))+"_"+itos(i)+".txt")
		if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(src, []byte("tiny"), 0o644); err != nil {
			t.Fatal(err)
		}
		pool.Submit(plan.Task{
			Kind:    plan.CopyTask,
			SrcPath: src,
			DstPath: filepath.Join(dstDir, relTo(srcDir, src)),
			Size:    4,
		})
	}
	pool.Close()

	if errCount != 0 {
		t.Fatalf("unexpected errors: %d", errCount)
	}
	copied, _ := pool.Counters()
	if copied != n {
		t.Errorf("copied = %d, want %d", copied, n)
	}
	got := 0
	filepath.Walk(dstDir, func(_ string, info os.FileInfo, _ error) error {
		if !info.IsDir() {
			got++
		}
		return nil
	})
	if got != n {
		t.Errorf("found %d files on dst, want %d", got, n)
	}
}

func relTo(root, path string) string {
	r, _ := filepath.Rel(root, path)
	return r
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

func TestPoolCounters(t *testing.T) {
	pool := NewPool(Config{Workers: 2})
	src := filepath.Join(t.TempDir(), "ok.txt")
	os.WriteFile(src, []byte("data"), 0o644)
	pool.Submit(plan.Task{Kind: plan.CopyTask, SrcPath: src, DstPath: filepath.Join(t.TempDir(), "ok.txt")})
	pool.Submit(plan.Task{Kind: plan.CopyTask, SrcPath: filepath.Join(t.TempDir(), "bad.txt"), DstPath: filepath.Join(t.TempDir(), "bad.txt")})
	pool.Close()
	copied, failed := pool.Counters()
	if copied != 1 {
		t.Errorf("copied = %d, want 1", copied)
	}
	if failed != 1 {
		t.Errorf("failed = %d, want 1", failed)
	}
}

func TestPoolNoDeadlockWithUnboundedSubmission(t *testing.T) {
	// submitting far more tasks than the internal buffers without draining
	// results must not deadlock (consumer goroutine drains continuously).
	pool := NewPool(Config{Workers: 4})
	src := filepath.Join(t.TempDir(), "a.txt")
	os.WriteFile(src, []byte("x"), 0o644)
	for i := 0; i < 2000; i++ {
		pool.Submit(plan.Task{
			Kind:    plan.CopyTask,
			SrcPath: src,
			DstPath: filepath.Join(t.TempDir(), "out", itos(i)+".txt"),
			Size:    1,
		})
	}
	pool.Close()
	copied, failed := pool.Counters()
	if copied+failed != 2000 {
		t.Errorf("expected 2000 tasks, got copied=%d failed=%d", copied, failed)
	}
}
