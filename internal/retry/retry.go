// Package retry implements bounded retries with exponential backoff and full
// jitter, by hand (assignment requirement: no resilience libraries).
//
// Full jitter — sleep = rand(0, base·2^attempt) — is deliberate: retriers that
// failed together (e.g. a replica that shed a burst) would otherwise retry in
// lockstep and reproduce the very overload that failed them. Randomizing the
// whole delay spreads them apart.
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Policy bounds the retry loop. The zero value performs exactly one attempt:
// no retries, no sleeps — callers that want plain calls pass Policy{}.
type Policy struct {
	MaxAttempts int           // total attempts including the first; <1 acts as 1
	BaseDelay   time.Duration // jitter upper bound before the first retry
	MaxDelay    time.Duration // cap on the jitter upper bound

	// Test hooks; nil selects the real implementation.
	randN func(int64) int64
	sleep func(time.Duration)
}

// Default is the policy for router->storage internal calls: three attempts
// absorb transient blips (connection refused, shed 503, timeout) while
// bounding the extra latency a genuinely dead replica adds to the quorum
// fail path.
func Default() Policy {
	return Policy{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not retryable: Do stops immediately and returns the
// original error. Use for failures a retry cannot improve — a 4xx replica
// answer will be a 4xx again.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// Do runs fn until it returns nil, returns a Permanent error, attempts are
// exhausted, or ctx ends. The last observed error is returned (unwrapped for
// Permanent ones). Retries only make sense for idempotent operations; both
// internal ops qualify (a PUT re-sends the same key/timestamp/value, which
// LWW deduplicates).
func (p Policy) Do(ctx context.Context, fn func() error) error {
	attempts := max(p.MaxAttempts, 1)
	var err error
	for attempt := range attempts {
		if err = fn(); err == nil {
			return nil
		}
		var perm *permanentError
		if errors.As(err, &perm) {
			return perm.err
		}
		if attempt == attempts-1 {
			break
		}
		if !p.pause(ctx, p.jitter(attempt)) {
			break // caller gave up; err already holds the operation failure
		}
	}
	return err
}

// jitter returns a uniform random delay in [0, min(BaseDelay·2^attempt, MaxDelay)).
func (p Policy) jitter(attempt int) time.Duration {
	bound := p.BaseDelay << attempt
	// The shift overflows for huge attempt counts; treat overflow as "capped".
	if p.MaxDelay > 0 && (bound > p.MaxDelay || bound <= 0) {
		bound = p.MaxDelay
	}
	if bound <= 0 {
		return 0
	}
	randN := p.randN
	if randN == nil {
		randN = rand.Int64N
	}
	return time.Duration(randN(int64(bound)))
}

// pause sleeps for d, reporting false if ctx ended first.
func (p Policy) pause(ctx context.Context, d time.Duration) bool {
	if p.sleep != nil {
		p.sleep(d)
		return ctx.Err() == nil
	}
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
