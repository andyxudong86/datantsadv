package scanner

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"datants/internal/manifest"
)

func setupTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write := func(p, content string) {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a.txt", "hello")
	write("sub/b.txt", "world")
	write("sub/deep/c.txt", "12345")
	write("sub/deep/d.txt", "zzz")
	return root
}

func TestScanAllFiles(t *testing.T) {
	root := setupTree(t)
	got := map[string]int{}
	err := Scan(root, Options{Workers: 4, IncludeDirs: false}, func(dir string, recs []manifest.Record) error {
		for _, r := range recs {
			if !r.IsDir {
				got[filepath.Base(r.Path)] = int(r.Size)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	want := map[string]int{"a.txt": 5, "b.txt": 5, "c.txt": 5, "d.txt": 3}
	for name, size := range want {
		if got[name] != size {
			t.Errorf("file %q size = %d, want %d", name, got[name], size)
		}
	}
}

func TestScanSortsPerDirectory(t *testing.T) {
	root := setupTree(t)
	var dirOrder [][]string
	err := Scan(root, Options{Workers: 2}, func(dir string, recs []manifest.Record) error {
		var names []string
		for _, r := range recs {
			if !r.IsDir {
				names = append(names, filepath.Base(r.Path))
			}
		}
		dirOrder = append(dirOrder, names)
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	// ensure each directory's records are sorted ascending
	for _, names := range dirOrder {
		if !sort.StringsAreSorted(names) {
			t.Errorf("records not sorted: %v", names)
		}
	}
}

func TestScanIncludeDirs(t *testing.T) {
	root := setupTree(t)
	var dirs int
	err := Scan(root, Options{Workers: 2, IncludeDirs: true}, func(dir string, recs []manifest.Record) error {
		for _, r := range recs {
			if r.IsDir {
				dirs++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if dirs < 2 {
		t.Errorf("expected at least 2 subdirectories, got %d", dirs)
	}
}

func TestScanIgnore(t *testing.T) {
	root := setupTree(t)
	got := map[string]bool{}
	err := Scan(root, Options{Workers: 2, Ignore: []string{"deep"}}, func(dir string, recs []manifest.Record) error {
		for _, r := range recs {
			if !r.IsDir {
				got[r.Path] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 files after ignoring 'deep', got %d", len(got))
	}
	if _, ok := got[filepath.Join(root, "sub", "deep", "c.txt")]; ok {
		t.Errorf("ignored subtree still scanned")
	}
}

func TestScanMissingRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	err := Scan(missing, Options{Workers: 2}, func(string, []manifest.Record) error { return nil })
	if err == nil {
		t.Fatal("expected error for missing root")
	}
}

// TestScanSingleWorkerDeepTree covers a deadlock that occurred when a single
// worker (Workers=1) discovered more child directories than the internal queue
// could buffer: the sole worker deadlocked while enqueuing its own children.
// Regression test: the scan must complete and report every file.
func TestScanSingleWorkerDeepTree(t *testing.T) {
	root := t.TempDir()
	// One root with far more subdirectories than any bounded-queue buffer, each
	// with a couple of files, plus a second nesting level.
	const dirs = 50
	for i := 0; i < dirs; i++ {
		sub := filepath.Join(root, "d"+string(rune('a'+i%26))+fmt.Sprint(i))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 3; j++ {
			f := filepath.Join(sub, fmt.Sprintf("f%d.txt", j))
			if err := os.WriteFile(f, []byte(fmt.Sprintf("content of %d-%d", i, j)), 0o644); err != nil {
				t.Fatal(err)
			}
			nested := filepath.Join(sub, fmt.Sprintf("n%d", j))
			if err := os.MkdirAll(nested, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(nested, "x.txt"), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	var files int
	done := make(chan error, 1)
	go func() {
		done <- Scan(root, Options{Workers: 1}, func(_ string, recs []manifest.Record) error {
			for _, r := range recs {
				if !r.IsDir {
					files++
				}
			}
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("scan deadlocked (no completion within 30s)")
	}
	want := dirs * (3 + 3) // 3 files + 3 nested x.txt per subdirectory
	if files != want {
		t.Errorf("scanned %d files, want %d", files, want)
	}
}
