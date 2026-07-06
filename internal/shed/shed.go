// Package shed implements load shedding by hand (an assignment requirement:
// no rate-limiting libraries): a counting semaphore bounding concurrent work
// plus a bounded wait queue. A request that finds both full is rejected
// immediately with 503 — the node degrades by refusing excess work instead of
// building an unbounded backlog and collapsing (goroutine pileup, OOM,
// latency death spiral).
package shed

import (
	"context"
	"net/http"
)

// Limiter admits at most maxConcurrent requests at a time; up to maxQueue
// more may wait for a slot. Anything beyond that is shed. The zero value is
// not usable; construct with New.
type Limiter struct {
	slots chan struct{} // counting semaphore: one token per running request
	queue chan struct{} // bounded wait queue: one token per waiting request
}

// New returns a Limiter admitting maxConcurrent concurrent requests with a
// wait queue of maxQueue (0 = no queue, pure semaphore).
func New(maxConcurrent, maxQueue int) *Limiter {
	return &Limiter{
		slots: make(chan struct{}, maxConcurrent),
		queue: make(chan struct{}, maxQueue),
	}
}

// Acquire reports whether the caller may proceed. It returns false
// immediately when every slot is busy and the wait queue is full (shed), or
// when ctx is cancelled while waiting in the queue. After a true return the
// caller must call Release exactly once.
func (l *Limiter) Acquire(ctx context.Context) bool {
	select {
	case l.slots <- struct{}{}: // fast path: free slot
		return true
	default:
	}
	select {
	case l.queue <- struct{}{}: // no slot: try to join the bounded queue
	default:
		return false // saturated: shed immediately, don't block the caller
	}
	defer func() { <-l.queue }() // leave the queue whichever way this ends
	select {
	case l.slots <- struct{}{}:
		return true
	case <-ctx.Done(): // caller gave up (client timeout/disconnect)
		return false
	}
}

// Release frees a slot acquired with Acquire.
func (l *Limiter) Release() { <-l.slots }

// Middleware guards next with the limiter: requests that cannot be admitted
// are answered with 503 without touching the handler.
func Middleware(l *Limiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.Acquire(r.Context()) {
			http.Error(w, "overloaded", http.StatusServiceUnavailable)
			return
		}
		defer l.Release()
		next.ServeHTTP(w, r)
	})
}
