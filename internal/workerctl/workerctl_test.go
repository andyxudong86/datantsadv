package workerctl

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestHelperChild re-invokes the test binary as a pretend da_worker: when the
// GO_WANT_HELPER_PROCESS env var is set it sleeps forever (until killed), so the
// Manager can be exercised on real OS processes from any platform.
func TestHelperChild(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		t.Skip("helper process re-invocation only")
	}
	for {
		time.Sleep(time.Hour)
	}
}

// fakeFactory starts the current test binary parked in TestHelperChild.
func fakeFactory(t *testing.T) Factory {
	t.Helper()
	return func(name string, args ...string) (Process, error) {
		full := append([]string{"-test.run=^TestHelperChild$"}, args...)
		cmd := exec.Command(os.Args[0], full...)
		cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return &osProcess{cmd: cmd}, nil
	}
}

func TestSpawnTrackStop(t *testing.T) {
	m := NewManager(fakeFactory(t))

	if err := m.Spawn("one", "helper", "-flag", "value"); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if !m.Alive("one") {
		t.Fatal("expected spawned process alive")
	}
	snap := m.Snapshot()
	if len(snap) != 1 || snap[0].ID != "one" || snap[0].PID == 0 || !snap[0].Alive {
		t.Fatalf("snapshot = %+v", snap)
	}

	// duplicate id must fail
	if err := m.Spawn("one", "helper"); err == nil {
		t.Fatal("duplicate spawn id accepted")
	}

	if err := m.Stop("one"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := m.Stop("one"); err != ErrNotSpawned {
		t.Fatalf("second stop err = %v, want ErrNotSpawned", err)
	}

	// liveness flips as the watcher reaps the child
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && m.Alive("one") {
		time.Sleep(10 * time.Millisecond)
	}
	if m.Alive("one") {
		t.Fatal("process still reported alive after Stop")
	}
	if len(m.Snapshot()) != 0 {
		t.Fatalf("snapshot not empty after stop: %+v", m.Snapshot())
	}
}

func TestStopAll(t *testing.T) {
	m := NewManager(fakeFactory(t))
	for _, id := range []string{"a", "b", "c"} {
		if err := m.Spawn(id, "helper"); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.StopAll(); err != nil {
		t.Fatalf("StopAll: %v", err)
	}
	if len(m.Snapshot()) != 0 {
		t.Fatalf("snapshot not empty: %+v", m.Snapshot())
	}
}

func TestSpawnMissingBinary(t *testing.T) {
	m := NewManager(OSFactory())
	path := "/definitely/missing/da_worker"
	if runtime.GOOS == "windows" {
		path = filepath.Join("Z:", "definitely-missing", "da_worker.exe")
	}
	if err := m.Spawn("x", path); err == nil {
		t.Fatal("spawn of missing binary succeeded")
	}
	if m.Alive("x") {
		t.Fatal("missing binary reported alive")
	}
}
