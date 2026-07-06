package shed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAcquireRelease(t *testing.T) {
	l := New(2, 0)
	ctx := context.Background()

	for i := range 2 {
		if !l.Acquire(ctx) {
			t.Fatalf("acquire %d within the limit must succeed", i+1)
		}
	}
	if l.Acquire(ctx) {
		t.Fatal("third acquire must shed: no free slot, no queue")
	}
	l.Release()
	if !l.Acquire(ctx) {
		t.Fatal("acquire after release must succeed")
	}
}

// TestQueueAdmitsWaiterOnRelease: a waiter parked in the queue gets the slot
// once a running request finishes; anything beyond the queue sheds instantly.
func TestQueueAdmitsWaiterOnRelease(t *testing.T) {
	l := New(1, 1)
	ctx := context.Background()

	if !l.Acquire(ctx) {
		t.Fatal("first acquire")
	}
	admitted := make(chan bool, 1)
	go func() { admitted <- l.Acquire(ctx) }()
	waitFor(t, "waiter to join the queue", func() bool { return len(l.queue) == 1 })

	// Slot busy + queue full: next caller is shed without blocking.
	if l.Acquire(ctx) {
		t.Fatal("expected shed while slot and queue are both full")
	}

	l.Release()
	if !<-admitted {
		t.Fatal("queued waiter must be admitted after a release")
	}
}

// TestCtxCancelWhileQueued: a waiter that gives up (client timeout) must
// return false and free its queue spot for others.
func TestCtxCancelWhileQueued(t *testing.T) {
	l := New(1, 1)
	if !l.Acquire(context.Background()) {
		t.Fatal("first acquire")
	}

	ctx, cancel := context.WithCancel(context.Background())
	admitted := make(chan bool, 1)
	go func() { admitted <- l.Acquire(ctx) }()
	waitFor(t, "waiter to join the queue", func() bool { return len(l.queue) == 1 })

	cancel()
	if <-admitted {
		t.Fatal("cancelled waiter must not be admitted")
	}
	waitFor(t, "queue spot to be freed", func() bool { return len(l.queue) == 0 })
}

// TestInFlightNeverExceedsLimit hammers the limiter from many goroutines and
// asserts the concurrency bound holds. Run under -race.
func TestInFlightNeverExceedsLimit(t *testing.T) {
	const limit = 4
	l := New(limit, limit)

	var cur, peak atomic.Int32
	var admitted, shed atomic.Int32
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !l.Acquire(context.Background()) {
				shed.Add(1)
				return
			}
			admitted.Add(1)
			c := cur.Add(1)
			for {
				p := peak.Load()
				if c <= p || peak.CompareAndSwap(p, c) {
					break
				}
			}
			time.Sleep(200 * time.Microsecond) // hold the slot briefly
			cur.Add(-1)
			l.Release()
		}()
	}
	wg.Wait()

	if p := peak.Load(); p > limit {
		t.Fatalf("in-flight peaked at %d, limit is %d", p, limit)
	}
	if admitted.Load() == 0 || shed.Load() == 0 {
		t.Fatalf("expected both admitted and shed under overload, got admitted=%d shed=%d",
			admitted.Load(), shed.Load())
	}
}

// TestMiddleware verifies the HTTP behavior end to end: while the limiter is
// saturated the middleware answers 503 without invoking the handler; once
// capacity frees, requests flow again.
func TestMiddleware(t *testing.T) {
	l := New(2, 0)
	entered := make(chan struct{}, 2)
	unblock := make(chan struct{})
	var handled atomic.Int32
	h := Middleware(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handled.Add(1)
		entered <- struct{}{}
		<-unblock
	}))

	// Occupy both slots with in-flight requests.
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
			if rec.Code != http.StatusOK {
				t.Errorf("in-flight request got %d, want 200", rec.Code)
			}
		}()
	}
	<-entered
	<-entered

	// Saturated: excess requests shed synchronously, handler untouched.
	before := handled.Load()
	for range 3 {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("saturated request got %d, want 503", rec.Code)
		}
	}
	if handled.Load() != before {
		t.Fatal("shed requests must not reach the handler")
	}

	close(unblock)
	wg.Wait()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("request after drain got %d, want 200", rec.Code)
	}
}
