// Package e2e runs end-to-end smoke tests against the real binaries: it builds
// da_control and da_worker for the host platform, boots them as subprocesses
// over a real source/destination tree, and verifies the whole pipeline through
// the web API plus the resulting files on disk. A companion test cross-compiles
// the RHEL 7.9 (linux/amd64, pure static) artifacts and verifies their build
// settings via "go version -m".
package e2e

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// goBuild builds pkg into out with the given extra env over a deterministic
// base. It appends .exe when the TARGET GOOS is windows, so cross-compile
// callers keep plain output names on a Windows host.
func goBuild(t *testing.T, out, pkg string, env ...string) string {
	t.Helper()
	absOut := out
	targetOS := runtime.GOOS
	for _, kv := range env {
		if strings.HasPrefix(kv, "GOOS=") {
			targetOS = strings.TrimPrefix(kv, "GOOS=")
		}
	}
	if targetOS == "windows" && !strings.HasSuffix(strings.ToLower(absOut), ".exe") {
		absOut += ".exe"
	}
	base := []string{"GOFLAGS=-mod=vendor", "CGO_ENABLED=0", "GOWORK=off"}
	cmd := exec.Command("go", "build", "-o", absOut, pkg)
	cmd.Env = append(os.Environ(), append(base, env...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, out)
	}
	return absOut
}
