// Package throttle provides a token-bucket rate limiter that lets a migration
// cap its aggregate bandwidth so it does not saturate the underlying
// storage/network unnecessarily.
package throttle

import (
	"sync"
	"time"
)

// Limiter is a token bucket used to throttle byte throughput. It is safe for
// concurrent use; total consumption across all workers is bounded to the
// configured rate.
//
// A single Wait(n) where n exceeds the current burst capacity is allowed by
// growing the bucket to match n — this guarantees Wait always terminates for
// any request size while still enforcing the configured aggregate rate.
type Limiter struct {
	mu       sync.Mutex
	rate     float64 // bytes per second
	capacity float64 // burst capacity in bytes
	tokens   float64
	last     time.Time
}

// New creates a limiter with the given rate (bytes/second). A rate <= 0 disables
// throttling entirely (Wait returns immediately).
func New(rate int64) *Limiter {
	r := float64(rate)
	if r > 0 {
		return &Limiter{rate: r, capacity: r, tokens: r, last: time.Now()}
	}
	return &Limiter{last: time.Now()}
}

// Wait blocks until n bytes can be consumed (tokens available) and then returns.
// If the limiter is disabled it returns immediately. It always terminates.
func (l *Limiter) Wait(n int64) {
	if l == nil || l.rate <= 0 || n <= 0 {
		return
	}
	fn := float64(n)
	l.mu.Lock()
	for {
		l.refill()
		if l.tokens >= fn {
			l.tokens -= fn
			l.mu.Unlock()
			return
		}
		// grow the bucket to guarantee eventual satisfaction of the request
		if l.capacity < fn {
			l.capacity = fn
		}
		deficit := fn - l.tokens
		wait := time.Duration(deficit / l.rate * float64(time.Second))
		l.mu.Unlock()
		if wait > 0 {
			time.Sleep(wait + time.Millisecond)
		} else {
			time.Sleep(time.Millisecond)
		}
		l.mu.Lock()
	}
}

// refill must be called with the mutex held.
func (l *Limiter) refill() {
	now := time.Now()
	elapsed := now.Sub(l.last).Seconds()
	l.last = now
	l.tokens += elapsed * l.rate
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
}
