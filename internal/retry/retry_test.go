package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// deterministic makes jitter return its upper bound minus one and records the
// bounds and sleeps the policy asked for.
func deterministic(p Policy, bounds *[]time.Duration, sleeps *[]time.Duration) Policy {
	p.randN = func(n int64) int64 {
		*bounds = append(*bounds, time.Duration(n))
		return n - 1
	}
	p.sleep = func(d time.Duration) { *sleeps = append(*sleeps, d) }
	return p
}

func TestFirstTrySuccessMakesOneCall(t *testing.T) {
	calls := 0
	err := Policy{MaxAttempts: 5}.Do(context.Background(), func() error {
		calls++
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("err=%v calls=%d, want nil and 1", err, calls)
	}
}

func TestZeroPolicyMeansSingleAttempt(t *testing.T) {
	calls := 0
	err := Policy{}.Do(context.Background(), func() error {
		calls++
		return errBoom
	})
	if !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want errBoom and exactly 1 attempt", err, calls)
	}
}

func TestRetriesUntilSuccess(t *testing.T) {
	var bounds, sleeps []time.Duration
	p := deterministic(Policy{MaxAttempts: 5, BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second}, &bounds, &sleeps)

	calls := 0
	err := p.Do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return errBoom
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want nil and 3", err, calls)
	}
	if len(sleeps) != 2 {
		t.Fatalf("slept %d times, want 2 (between 3 attempts)", len(sleeps))
	}
}

func TestExhaustsAttemptsAndReturnsLastError(t *testing.T) {
	var bounds, sleeps []time.Duration
	p := deterministic(Policy{MaxAttempts: 3, BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second}, &bounds, &sleeps)

	calls := 0
	err := p.Do(context.Background(), func() error {
		calls++
		return errBoom
	})
	if !errors.Is(err, errBoom) || calls != 3 {
		t.Fatalf("err=%v calls=%d, want errBoom and 3 attempts", err, calls)
	}
}

func TestPermanentStopsImmediately(t *testing.T) {
	calls := 0
	err := Policy{MaxAttempts: 5, BaseDelay: time.Millisecond}.Do(context.Background(), func() error {
		calls++
		return Permanent(errBoom)
	})
	if calls != 1 {
		t.Fatalf("calls=%d, want 1: permanent errors must not be retried", calls)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("err=%v, want the original error unwrapped", err)
	}
	var perm *permanentError
	if errors.As(err, &perm) {
		t.Fatal("returned error must not stay wrapped in permanentError")
	}
}

// TestBackoffGrowsExponentiallyWithCap pins the jitter bounds: base·2^attempt,
// clamped at MaxDelay. Full jitter draws uniformly below the bound.
func TestBackoffGrowsExponentiallyWithCap(t *testing.T) {
	var bounds, sleeps []time.Duration
	p := deterministic(Policy{MaxAttempts: 5, BaseDelay: 50 * time.Millisecond, MaxDelay: 120 * time.Millisecond}, &bounds, &sleeps)

	_ = p.Do(context.Background(), func() error { return errBoom })

	want := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 120 * time.Millisecond, 120 * time.Millisecond}
	if len(bounds) != len(want) {
		t.Fatalf("got %d jitter draws (%v), want %d", len(bounds), bounds, len(want))
	}
	for i := range want {
		if bounds[i] != want[i] {
			t.Fatalf("jitter bound %d = %v, want %v (sequence %v)", i, bounds[i], want[i], bounds)
		}
	}
	for i, s := range sleeps {
		if s >= bounds[i] {
			t.Fatalf("sleep %d = %v not below its bound %v: jitter must be rand(0, bound)", i, s, bounds[i])
		}
	}
}

func TestCtxCancelStopsRetrying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	p := Policy{MaxAttempts: 10, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
	p.sleep = func(time.Duration) {} // fast
	err := p.Do(ctx, func() error {
		calls++
		if calls == 2 {
			cancel() // caller disappears mid-loop
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("err=%v, want the operation error", err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want 2: no attempts after cancellation", calls)
	}
}
