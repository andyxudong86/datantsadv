package worker_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	migratev1 "datants/gen/migrate/v1"
	"datants/internal/worker"
)

// TestWorkerRetriesWhenControllerLate verifies that a worker starting before the
// controller listener exists keeps retrying instead of exiting, then exits
// cleanly once the controller becomes available and hands out no tasks.
func TestWorkerRetriesWhenControllerLate(t *testing.T) {
	// grab a free address by binding and immediately closing
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	ctl := newStaticController()

	// worker starts immediately; the controller listener is brought up later
	var wg sync.WaitGroup
	runErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, client, err := worker.DialControl(addr, true)
		if err != nil {
			runErr <- err
			return
		}
		defer conn.Close()
		runErr <- worker.New(client, worker.Config{}).Run(context.Background())
	}()

	// controller comes up after worker has been retrying Register
	time.Sleep(300 * time.Millisecond)
	srv := grpc.NewServer()
	migratev1.RegisterControlServer(srv, ctl)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("controller listen: %v", err)
	}
	go srv.Serve(lis)

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("worker run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not finish after controller came up")
	}

	if ctl.claimed != 1 {
		t.Fatalf("worker claimed %d batches, want 1", ctl.claimed)
	}
	srv.Stop()
}

// TestWorkerStaysResidentUntilCancelled verifies the --idle-poll behaviour used
// by console-launched workers: with nothing to do the worker must NOT exit, and
// only a context cancellation (i.e. Stop) returns.
func TestWorkerStaysResidentUntilCancelled(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	ctl := newStaticController()
	srv := grpc.NewServer()
	migratev1.RegisterControlServer(srv, ctl)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("controller listen: %v", err)
	}
	go srv.Serve(lis)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	conn, client, err := worker.DialControl(addr, true)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	w := worker.New(client, worker.Config{IdlePoll: 50 * time.Millisecond})
	go func() { runErr <- w.Run(ctx) }()

	// it must process at least the one hand-out batch...
	waitFor(t, 5*time.Second, "first claim", func() bool { return ctl.claimed >= 1 })
	// ...then stay resident instead of exiting even though the store is drained
	time.Sleep(500 * time.Millisecond)
	select {
	case err := <-runErr:
		t.Fatalf("resident worker exited early: %v", err)
	default:
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("worker run after cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}
	srv.Stop()
}

func waitFor(t *testing.T, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

type staticController struct {
	migratev1.UnimplementedControlServer
	mu      sync.Mutex
	claimed int
}

func newStaticController() *staticController { return &staticController{} }

func (c *staticController) Register(ctx context.Context, req *migratev1.RegisterRequest) (*migratev1.RegisterResponse, error) {
	id := req.GetWorkerId()
	if id == "" {
		id = "w"
	}
	return &migratev1.RegisterResponse{WorkerId: id}, nil
}

func (c *staticController) Claim(ctx context.Context, req *migratev1.ClaimRequest) (*migratev1.ClaimResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.claimed > 0 {
		return &migratev1.ClaimResponse{}, nil
	}
	c.claimed++
	return &migratev1.ClaimResponse{Tasks: []*migratev1.Task{
		{Id: "t1", Kind: migratev1.Task_KIND_COPY, SrcPath: "/nonexistent/src", DstPath: "/nonexistent/dst"},
	}}, nil
}

func (c *staticController) Report(ctx context.Context, req *migratev1.ReportRequest) (*migratev1.ReportResponse, error) {
	return &migratev1.ReportResponse{}, nil
}

func (c *staticController) Heartbeat(ctx context.Context, req *migratev1.HeartbeatRequest) (*migratev1.HeartbeatResponse, error) {
	return &migratev1.HeartbeatResponse{Ok: true}, nil
}
