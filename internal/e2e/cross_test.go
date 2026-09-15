package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestCrossCompileRHEL7Artifacts proves the RHEL 7.9 deployment artifacts are
// produced the way the README describes: linux/amd64, cgo disabled (pure static,
// no glibc dependency to match RHEL 7.9's old glibc 2.17). It validates the
// binary's recorded build settings through "go version -m" so nothing silently
// regresses to a dynamically-linked build.
func TestCrossCompileRHEL7Artifacts(t *testing.T) {
	if testing.Short() {
		t.Skip("cross compile skipped in -short mode")
	}
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
		// the host is already the target: nothing to cross-compile, and go
		// refuses GOOS/GOARCH equal to the host anyway
		t.Skip("host is linux/amd64, no cross compile needed")
	}

	outDir := t.TempDir()
	bins := make([]string, 0, 3)
	for _, b := range []struct{ name, pkg string }{
		{"da_migrate", "datants/cmd/da_migrate"},
		{"da_control", "datants/cmd/da_control"},
		{"da_worker", "datants/cmd/da_worker"},
	} {
		bins = append(bins, goBuild(t, filepath.Join(outDir, b.name), b.pkg, "GOOS=linux", "GOARCH=amd64"))
	}
	for _, bin := range bins {
		verifyLinuxStatic(t, bin)
	}
}

// verifyLinuxStatic asserts the recorded build settings of a linux/amd64 binary.
func verifyLinuxStatic(t *testing.T, bin string) {
	t.Helper()
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("artifact missing: %v", err)
	}
	cmd := exec.Command("go", "version", "-m", bin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go version -m %s: %v\n%s", filepath.Base(bin), err, out)
	}
	s := string(out)
	settings := []string{
		"GOOS=linux",
		"GOARCH=amd64",
		"CGO_ENABLED=0",
	}
	for _, want := range settings {
		if !strings.Contains(s, "\t"+want+"\n") {
			t.Errorf("%s: build setting %q missing, not a static linux/amd64 artifact\n%s",
				filepath.Base(bin), want, s)
		}
	}
}
