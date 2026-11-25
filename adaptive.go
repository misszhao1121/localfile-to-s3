package main

import (
	"context"
	"sync"
)

// adaptiveLimiter is an AIMD (additive-increase, multiplicative-decrease)
// concurrency limiter for S3 PUT traffic.
//
// Why: with a fixed worker pool, once the cluster starts throttling (503),
// all workers keep firing retries — the retry traffic alone keeps the
// cluster saturated, so nothing recovers (retry storm). The limiter shrinks
// the number of in-flight PUTs on each 503 and slowly regains capacity on
// success, converging to what the cluster can actually absorb.
//
// Semantics:
//   - Acquire blocks until a slot is available (bounded by ctx).
//   - Release reports the outcome of the operation that used the slot:
//     OnSuccess grows the limit by +1 every success-streak window,
//     OnThrottle halves it (min floor).
//   - The limit is shared by all upload workers via one limiter instance.
type adaptiveLimiter struct {
	mu      sync.Mutex
	sem     chan struct{} // token semaphore sized to max
	limit   int           // current allowed concurrency
	max     int
	min     int
	success int // consecutive successes since last growth
}

func newAdaptiveLimiter(max, min int) *adaptiveLimiter {
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	l := &adaptiveLimiter{
		sem:   make(chan struct{}, max),
		limit: max,
		max:   max,
		min:   min,
	}
	// pre-fill the semaphore with (max - limit) tokens so only `limit`
	// acquisitions can be in flight at once
	for i := 0; i < max-l.limit; i++ {
		l.sem <- struct{}{}
	}
	return l
}

// Acquire takes a slot, blocking while the limiter is shrunk below the
// number of active workers. Extra workers park here — that IS the
// backpressure, preventing retry storms.
func (l *adaptiveLimiter) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.sem <- struct{}{}:
		return nil
	}
}

// Release returns the slot and reports the outcome.
func (l *adaptiveLimiter) Release(success bool) {
	l.mu.Lock()
	if success {
		l.success++
		// grow by 1 every 32 consecutive successes (slow ramp-up)
		if l.success >= 32 && l.limit < l.max {
			l.limit++
			l.success = 0
			<-l.sem // free one token => one more concurrent op allowed
		}
	} else {
		l.success = 0
		// multiplicative decrease: halve, floor at min
		newLimit := l.limit / 2
		if newLimit < l.min {
			newLimit = l.min
		}
		if newLimit < l.limit {
			shrink := l.limit - newLimit
			l.limit = newLimit
			for i := 0; i < shrink; i++ {
				select {
				case l.sem <- struct{}{}: // take tokens => fewer slots
				default:
				}
			}
		}
	}
	l.mu.Unlock()
	// always give the acquisition token back
	<-l.sem
}

// Limit returns the current in-flight cap (for progress display).
func (l *adaptiveLimiter) Limit() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.limit
}
