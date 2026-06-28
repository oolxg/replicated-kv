package store

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestStoreGetMissing(t *testing.T) {
	s := New()
	if _, ok := s.Get("absent"); ok {
		t.Fatal("expected miss on empty store")
	}
}

// TestStorePutLWW drives a sequence of writes against one key and asserts both
// the per-write applied flag and the converged final state. This covers the
// whole LWW decision matrix: first write, newer-wins, older-ignored, the
// equal-timestamp value tie-break, and idempotent re-put.
func TestStorePutLWW(t *testing.T) {
	type write struct {
		val         string
		ts          int64
		wantApplied bool
	}
	tests := []struct {
		name      string
		writes    []write
		wantValue string
		wantTS    int64
	}{
		{
			name:      "single put applies",
			writes:    []write{{"v1", 10, true}},
			wantValue: "v1", wantTS: 10,
		},
		{
			name:      "newer timestamp wins",
			writes:    []write{{"old", 10, true}, {"new", 20, true}},
			wantValue: "new", wantTS: 20,
		},
		{
			name:      "older timestamp ignored",
			writes:    []write{{"new", 20, true}, {"old", 10, false}},
			wantValue: "new", wantTS: 20,
		},
		{
			name:      "equal timestamp, greater value wins",
			writes:    []write{{"aaa", 50, true}, {"bbb", 50, true}, {"aaa", 50, false}},
			wantValue: "bbb", wantTS: 50,
		},
		{
			name:      "identical re-put is a no-op",
			writes:    []write{{"v", 5, true}, {"v", 5, false}},
			wantValue: "v", wantTS: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			const key = "k"
			for i, w := range tt.writes {
				if got := s.Put(key, []byte(w.val), w.ts); got != w.wantApplied {
					t.Fatalf("write %d (%q@%d): applied = %v, want %v", i, w.val, w.ts, got, w.wantApplied)
				}
			}
			v, ok := s.Get(key)
			if !ok {
				t.Fatalf("key %q absent after writes", key)
			}
			if string(v.Value) != tt.wantValue || v.Timestamp != tt.wantTS {
				t.Fatalf("final state = %q@%d, want %q@%d", v.Value, v.Timestamp, tt.wantValue, tt.wantTS)
			}
		})
	}
}

// TestPutDoesNotAliasCallerBuffer guards against the store retaining the
// caller's slice: mutating the buffer after Put must not corrupt stored data.
func TestPutDoesNotAliasCallerBuffer(t *testing.T) {
	s := New()
	buf := []byte("orig")
	s.Put("k", buf, 1)
	buf[0] = 'X'

	v, _ := s.Get("k")
	if string(v.Value) != "orig" {
		t.Fatalf("store aliased caller buffer: got %q", v.Value)
	}
}

// TestConcurrentWritersConvergeToMaxTimestamp races many writers on a single
// key. Regardless of the (non-deterministic) commit order, LWW guarantees the
// largest timestamp wins. Run under -race to catch data races.
func TestConcurrentWritersConvergeToMaxTimestamp(t *testing.T) {
	s := New()
	const writers = 64
	const key = "hot"

	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 1; i <= writers; i++ {
		go func(ts int64) {
			defer wg.Done()
			s.Put(key, []byte(fmt.Sprintf("v%d", ts)), ts)
		}(int64(i))
	}
	wg.Wait()

	v, ok := s.Get(key)
	if !ok {
		t.Fatal("expected key present after concurrent writes")
	}
	if v.Timestamp != writers {
		t.Fatalf("winning ts = %d, want %d (max)", v.Timestamp, writers)
	}
}

// TestConcurrentDistinctKeys exercises shard striping: independent keys written
// concurrently must all survive intact.
func TestConcurrentDistinctKeys(t *testing.T) {
	s := New()
	const n = 1000

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			s.Put(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)), int64(i+1))
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		v, ok := s.Get(fmt.Sprintf("key-%d", i))
		want := fmt.Sprintf("val-%d", i)
		if !ok || !bytes.Equal(v.Value, []byte(want)) {
			t.Fatalf("key-%d: got %q ok=%v, want %q", i, v.Value, ok, want)
		}
	}
}
