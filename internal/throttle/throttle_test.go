package throttle

import (
	"testing"
	"time"
)

func TestDisabledLimiterReturnsImmediately(t *testing.T) {
	l := New(0) // disabled
	start := time.Now()
	l.Wait(1 << 20)
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("disabled limiter blocked: %v", elapsed)
	}
}

func TestNilLimiterSafe(t *testing.T) {
	var l *Limiter
	l.Wait(1000) // must not panic
}

func TestLimiterThrottles(t *testing.T) {
	const rate = 1 << 20 // 1 MiB/s
	l := New(rate)
	start := time.Now()
	l.Wait(2 << 20) // asking for 2 MiB at 1 MiB/s burst capacity 1 MiB
	// The second MiB must be delayed by ~1s (burst capacity is 1 MiB).
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond {
		t.Errorf("limiter did not throttle: elapsed=%v", elapsed)
	}
}

func TestLimiterAllowsWithinBurst(t *testing.T) {
	l := New(1 << 20)
	start := time.Now()
	// small burst within capacity should be near-instant
	l.Wait(4096)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("small burst unexpectedly slow: %v", elapsed)
	}
}
