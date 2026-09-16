package control_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	migratev1 "datants/gen/migrate/v1"
	"datants/internal/control"
	"datants/internal/taskstore"
	"datants/internal/worker"
)

func TestNormalizeIP(t *testing.T) {
	cases := []struct{ in, want string }{
		{"127.0.0.1", "127.0.0.1"},
		{"::ffff:127.0.0.1", "127.0.0.1"},
		{"[::1]", "::1"},
		{"::1", "::1"},
		{"192.168.1.5", "192.168.1.5"},
		{" 10.0.0.7 ", "10.0.0.7"},
	}
	for _, c := range cases {
		if got := control.NormalizeIP(c.in); got != c.want {
			t.Errorf("NormalizeIP(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestIPAllowed(t *testing.T) {
	allowed := []string{"127.0.0.1", "::ffff:10.0.0.5"}
	for _, ip := range []string{"127.0.0.1", "10.0.0.5", "::ffff:10.0.0.5"} {
		if !control.IPAllowed(ip, allowed) {
			t.Errorf("IPAllowed(%q) should be true", ip)
		}
	}
	for _, ip := range []string{"10.0.0.6", "192.168.1.9", ""} {
		if control.IPAllowed(ip, allowed) {
			t.Errorf("IPAllowed(%q) should be false", ip)
		}
	}
	if !control.IPAllowed("anything", nil) {
		t.Fatal("empty allowlist should allow all")
	}
}

// helperPeerCtx wraps a normal context with a fabricated gRPC peer address, so
// Claim's peer-IP extraction sees the caller as a given IP without sockets.
func peerCtx(ip string) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 4000},
	})
}

// TestClaimPeerIPGating drives the real Claim request through the real
// peer-IP extraction with synthetic peers, proving a run's WorkerIPs gate
// admits in-list callers and returns an empty claim for anyone else.
func TestClaimPeerIPGating(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		os.WriteFile(filepath.Join(src, name), []byte("x"), 0o644)
	}
	store, err := taskstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Done()
	cases := []struct {
		name      string
		allowIPs  []string
		callerIP  string
		wantTasks bool
	}{
		{"in-list caller admitted", []string{"127.0.0.1"}, "127.0.0.1", true},
		{"out-of-list caller denied", []string{"127.0.0.1"}, "10.0.0.5", false},
		{"mapped-v6 written as v4 allowed", []string{"127.0.0.1"}, "::ffff:127.0.0.1", true},
		{"no allowlist means everyone", nil, "10.0.0.5", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := taskstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer st.Done()
			ctl := control.NewController(st, control.Config{})
			ctl.Start()
			defer ctl.Stop()
			if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2, WorkerIPs: tc.allowIPs}); err != nil {
				t.Fatalf("start run: %v", err)
			}
			waitFor(t, 20*time.Second, func() bool {
				ss := ctl.State()
				return ss.Run != nil && ss.Run.ScanDone
			}, "scan to finish")

			resp, err := ctl.Claim(peerCtx(tc.callerIP), &migratev1.ClaimRequest{})
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			got := len(resp.GetTasks()) > 0
			if got != tc.wantTasks {
				t.Fatalf("caller %s with allowlist %v got tasks=%v, want %v", tc.callerIP, tc.allowIPs, got, tc.wantTasks)
			}
		})
	}
}

// TestRunWorkerAllowlistAllowsMatchingIP runs a real worker whose IP is in the
// allowlist and requires the whole migration to complete under it.
func TestRunWorkerAllowlistAllowsMatchingIP(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		os.WriteFile(filepath.Join(src, name), []byte("hello-"+name), 0o644)
	}
	store, err := taskstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Done()
	ctl := control.NewController(store, control.Config{})
	ctl.Start()
	defer ctl.Stop()

	target, lis := startServerReadAddr(t, ctl)
	defer lis.Close()

	if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2, WorkerIPs: []string{"127.0.0.1"}}); err != nil {
		t.Fatalf("start run: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool {
		st := ctl.State()
		return st.Run != nil && st.Run.ScanDone
	}, "scan to finish")

	var wg sync.WaitGroup
	wk, closer := dialWorker(t, target, "w-ok", 8, 4)
	defer closer()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := wk.Run(context.Background()); err != nil {
			t.Errorf("worker: %v", err)
		}
	}()
	wg.Wait()

	st := ctl.Stats()
	if st.Pending != 0 || st.InProgress != 0 || st.Failed != 0 || st.Done != 3 {
		t.Fatalf("stats after run: %+v", st)
	}
	workers := ctl.Workers()
	if len(workers) != 1 || workers[0].IP != "127.0.0.1" || workers[0].FilesDone != 3 {
		t.Fatalf("worker info wrong: %+v", workers)
	}
}

// TestRunWorkerAllowlistDeniesNonmatchingIP proves a worker whose IP is NOT in
// the allowlist can register but never receives a single task: the run leaves
// all tasks pending and no files land in the destination tree.
func TestRunWorkerAllowlistDeniesNonmatchingIP(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		os.WriteFile(filepath.Join(src, name), []byte("hello-"+name), 0o644)
	}
	store, err := taskstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Done()
	ctl := control.NewController(store, control.Config{})
	ctl.Start()
	defer ctl.Stop()

	target, lis := startServerReadAddr(t, ctl)
	defer lis.Close()

	if _, err := ctl.StartRun(src, dst, control.RunConfig{Workers: 2, WorkerIPs: []string{"10.9.9.9"}}); err != nil {
		t.Fatalf("start run: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool {
		st := ctl.State()
		return st.Run != nil && st.Run.ScanDone
	}, "scan to finish")

	wk, closer := dialWorker(t, target, "w-denied", 8, 4)
	defer closer()
	if err := wk.Run(context.Background()); err != nil {
		t.Fatalf("worker run: %v", err)
	}

	st := ctl.Stats()
	if st.Done != 0 || st.Pending != 3 {
		t.Fatalf("denied worker must not touch tasks, stats: %+v", st)
	}
	if got := countFiles(t, dst); got != 0 {
		t.Fatalf("denied worker copied %d files into dst, want 0", got)
	}
	workers := ctl.Workers()
	if len(workers) != 1 || workers[0].IP != "127.0.0.1" {
		t.Fatalf("worker info wrong: %+v", workers)
	}
}

// startServerReadAddr mirrors internal/control/distributed_test's startServer
// but returns the net.Listener so the caller can close it.
func startServerReadAddr(t *testing.T, ctl *control.Controller) (string, net.Listener) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	ctl.RegisterServer(s)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String(), lis
}

func dialWorker(t *testing.T, target, id string, batch, capacity int) (*worker.Worker, func()) {
	t.Helper()
	conn, client, err := worker.DialControl(target, true)
	if err != nil {
		t.Fatal(err)
	}
	return worker.New(client, worker.Config{WorkerID: id, BatchSize: batch, Capacity: capacity}), func() { conn.Close() }
}

// waitFor polls cond until it is satisfied or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		t.Helper()
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
