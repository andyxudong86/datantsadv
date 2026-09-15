// Package worker implements the Worker side of the distributed migration
// system. A Worker is an independent process that runs on a machine with
// access to the shared source/destination storage. It connects to a Controller
// over gRPC, claims batches of file-level tasks, copies file content directly
// between source and destination (the data plane never passes through the
// Controller), and reports results.
//
// To keep control-plane overhead low for "massive small file" jobs, tasks are
// claimed and reported in batches rather than one RPC per file.
package worker

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	migratev1 "datants/gen/migrate/v1"
	"datants/internal/plan"
	"datants/internal/transfer"
)

// Config controls a Worker's behaviour.
type Config struct {
	// WorkerID to register with (empty lets the Controller assign one).
	WorkerID string
	// Address is a human-readable identity for logs.
	Address string
	// BatchSize is how many tasks are claimed per control-plane round-trip.
	BatchSize int
	// Capacity is the number of concurrent local copy workers.
	Capacity int
	// BufferSize is the per-copy IO buffer size.
	BufferSize int
	// Rate limits local bytes/sec (0 = unlimited).
	Rate int64
	// HeartbeatInterval controls liveness telemetry cadence.
	HeartbeatInterval time.Duration
	// PreserveOwner applies the tasks' uid/gid on copy and meta update.
	PreserveOwner bool
}

// Worker runs the claim-copy-report loop.
type Worker struct {
	client migratev1.ControlClient
	cfg    Config
	id     string
}

// New creates a Worker bound to the given control-plane client.
func New(client migratev1.ControlClient, cfg Config) *Worker {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = 8
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	return &Worker{client: client, cfg: cfg}
}

// ID returns the registered worker id.
func (w *Worker) ID() string { return w.id }

// Run registers, heartbeats, and processes tasks until ctx is cancelled or the
// controller returns an unrecoverable error. Startup registration and any
// transient control-plane connectivity error are retried with backoff so a
// worker may start before (or outlive) a controller restart: it keeps trying
// until the controller is reachable again or ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	for {
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		resp, err := w.client.Register(rpcCtx, &migratev1.RegisterRequest{
			WorkerId: w.cfg.WorkerID,
			Address:  w.cfg.Address,
			Capacity: int32(w.cfg.Capacity),
		})
		cancel()
		if err == nil {
			w.id = resp.GetWorkerId()
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable(err) {
			return err
		}
		if !backoff(ctx, 500*time.Millisecond, 30*time.Second) {
			return ctx.Err()
		}
	}

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeatLoop(hbCtx)
	}()

	for {
		if ctx.Err() != nil {
			break
		}
		proceed, err := w.processBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			if retryable(err) {
				if !backoff(ctx, 500*time.Millisecond, 30*time.Second) {
					break
				}
				continue
			}
			return err
		}
		if !proceed {
			break // no more tasks; migration complete
		}
	}
	hbCancel()
	<-hbDone
	return nil
}

// processBatch claims one batch, copies it, and reports. Returns (false, nil)
// when the controller has no more work.
func (w *Worker) processBatch(ctx context.Context) (bool, error) {
	claimCtx, claimCancel := context.WithTimeout(ctx, 30*time.Second)
	claim, err := w.client.Claim(claimCtx, &migratev1.ClaimRequest{
		WorkerId:  w.id,
		BatchSize: int32(w.cfg.BatchSize),
	})
	claimCancel()
	if err != nil {
		return false, err
	}
	if len(claim.GetTasks()) == 0 {
		return false, nil
	}

	// build transfer tasks and run them locally with a bounded pool
	pool := transfer.NewPool(transfer.Config{
		Workers:       w.cfg.Capacity,
		BufferSize:    w.cfg.BufferSize,
		Rate:          w.cfg.Rate,
		PreserveOwner: w.cfg.PreserveOwner,
	})
	var mu sync.Mutex
	results := make([]*migratev1.Result, 0, len(claim.GetTasks()))
	pool.SetOnResult(func(r transfer.TaskResult) {
		mu.Lock()
		results = append(results, &migratev1.Result{
			TaskId: r.Task.ID,
			Ok:     r.Err == nil,
			Error:  errString(r.Err),
			Bytes:  r.Bytes,
		})
		mu.Unlock()
	})
	for _, pt := range claim.GetTasks() {
		pool.Submit(toPlanTask(pt))
	}
	pool.Close()

	repCtx, repCancel := context.WithTimeout(ctx, 30*time.Second)
	_, err = w.client.Report(repCtx, &migratev1.ReportRequest{
		WorkerId: w.id,
		Results:  results,
	})
	repCancel()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = w.client.Heartbeat(ctx, &migratev1.HeartbeatRequest{WorkerId: w.id})
		}
	}
}

// toPlanTask converts a protobuf task into the local transfer plan task form.
func toPlanTask(t *migratev1.Task) plan.Task {
	kind := plan.CopyTask
	switch t.GetKind() {
	case migratev1.Task_KIND_MKDIR:
		kind = plan.MkdirTask
	case migratev1.Task_KIND_META:
		kind = plan.MetaTask
	case migratev1.Task_KIND_DELETE:
		kind = plan.DeleteTask
	}
	return plan.Task{
		ID:      t.GetId(),
		Kind:    kind,
		SrcPath: t.GetSrcPath(),
		DstPath: t.GetDstPath(),
		Size:    t.GetSize(),
		ModTime: t.GetModTime(),
		Mode:    t.GetMode(),
		Uid:     t.GetUid(),
		Gid:     t.GetGid(),
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// DialControl creates a gRPC connection to the controller at target (e.g.
// "host:port"). Pass allowInsecure=true for MVP (TLS can be layered on later).
func DialControl(target string, allowInsecure bool) (*grpc.ClientConn, migratev1.ControlClient, error) {
	var creds grpc.DialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	_ = allowInsecure
	conn, err := grpc.NewClient(target, creds)
	if err != nil {
		return nil, nil, err
	}
	return conn, migratev1.NewControlClient(conn), nil
}

// retryable reports whether a control-plane error looks transient and is worth
// retrying with backoff (controller restarting, network blip, worker starting
// before the controller binds its listener).
func retryable(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown:
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "transport: ") ||
		strings.Contains(msg, "connection reset")
}

// backoff sleeps once for base (clamped to max) before each retry. The caller
// invokes it between attempts; no internal doubling state is kept, so it always
// returns true after the wait unless ctx is cancelled.
func backoff(ctx context.Context, base, max time.Duration) bool {
	if base > max {
		base = max
	}
	t := time.NewTimer(base)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
