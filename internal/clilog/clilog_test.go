package clilog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolve(t *testing.T) {
	cases := []struct {
		name, dest, want string
	}{
		{"control", "", filepath.Join(string(filepath.Separator)+"var", "log", "da_control.log")},
		{"worker", "", filepath.Join(string(filepath.Separator)+"var", "log", "da_worker.log")},
		{"migrate", "-", ""},
		{"control", "stderr", ""},
		{"control", "/tmp/my.log", "/tmp/my.log"},
		{"control", "relative/out.log", "relative/out.log"},
	}
	for _, c := range cases {
		if got := Resolve(c.name, c.dest); got != c.want {
			t.Errorf("Resolve(%q, %q) = %q, want %q", c.name, c.dest, got, c.want)
		}
	}
}

func TestNewWritesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "da_test.log")
	logger, closeFn := New("test", path)
	defer closeFn()
	logger.Printf("hello %d", 42)

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(b), "hello 42") {
		t.Errorf("log file does not contain written line: %q", string(b))
	}
}

func TestNewAltresAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "da_append.log")
	logger, closeFn := New("test", path)
	logger.Printf("first")
	closeFn()

	logger, closeFn = New("test", path)
	defer closeFn()
	logger.Printf("second")

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(b), "first") || !strings.Contains(string(b), "second") {
		t.Errorf("expected both lines after reopen, got: %q", string(b))
	}
}

func TestNewUnwritableDirFallsBack(t *testing.T) {
	// A nonexistent parent directory must not abort the program: the logger
	// falls back to stderr. We can't observe stderr cleanly in-process, so we
	// only assert that New returns without panicking.
	logger, closeFn := New("test", filepath.Join(t.TempDir(), "no", "sub", "dir", "x.log"))
	defer closeFn()
	logger.Printf("ok")
}
