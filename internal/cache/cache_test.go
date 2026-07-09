package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock lets tests move time by hand.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

func newTestCache(capacity int, ttl time.Duration) (*Cache[string], *fakeClock) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	c := New[string](capacity, ttl)
	c.now = clk.now
	return c, clk
}

func TestHitAndMiss(t *testing.T) {
	c, _ := newTestCache(4, time.Minute)

	if _, ok := c.Get("absent"); ok {
		t.Fatal("empty cache must miss")
	}
	c.Put("k", "v")
	v, ok := c.Get("k")
	if !ok || v != "v" {
		t.Fatalf("got %q ok=%v, want v", v, ok)
	}
}

func TestTTLExpiry(t *testing.T) {
	c, clk := newTestCache(4, time.Minute)

	c.Put("k", "v")
	clk.advance(59 * time.Second)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("entry must still be fresh before the TTL")
	}
	clk.advance(2 * time.Second) // 61s total
	if _, ok := c.Get("k"); ok {
		t.Fatal("entry must expire after the TTL")
	}
}

func TestPutRefreshesValueAndTTL(t *testing.T) {
	c, clk := newTestCache(4, time.Minute)

	c.Put("k", "v1")
	clk.advance(50 * time.Second)
	c.Put("k", "v2") // refresh: new value, new TTL window
	clk.advance(30 * time.Second)

	v, ok := c.Get("k")
	if !ok || v != "v2" {
		t.Fatalf("got %q ok=%v, want v2 still fresh 30s after refresh", v, ok)
	}
}

func TestLRUEviction(t *testing.T) {
	c, _ := newTestCache(2, time.Minute)

	c.Put("a", "1")
	c.Put("b", "2")
	if _, ok := c.Get("a"); !ok { // a becomes most-recently-used
		t.Fatal("a must be present")
	}
	c.Put("c", "3") // over capacity: evicts b, the LRU

	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted as least recently used")
	}
	for _, k := range []string{"a", "c"} {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("%s must have survived eviction", k)
		}
	}
}

func TestInvalidate(t *testing.T) {
	c, _ := newTestCache(4, time.Minute)

	c.Put("k", "v")
	c.Invalidate("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("invalidated key must miss")
	}
	c.Invalidate("never-existed") // must not panic
}

func TestCapacityHolds(t *testing.T) {
	const capacity = 8
	c, _ := newTestCache(capacity, time.Minute)

	for i := range 100 {
		c.Put(fmt.Sprintf("k%d", i), "v")
	}
	if n := c.order.Len(); n != capacity {
		t.Fatalf("cache holds %d entries, cap is %d", n, capacity)
	}
	if n := len(c.items); n != capacity {
		t.Fatalf("index holds %d entries, cap is %d", n, capacity)
	}
}

// TestConcurrentAccess exercises the mutex under -race.
func TestConcurrentAccess(t *testing.T) {
	c := New[int](64, time.Minute)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := range 500 {
				k := fmt.Sprintf("k%d", (base+j)%100)
				c.Put(k, j)
				c.Get(k)
				if j%17 == 0 {
					c.Invalidate(k)
				}
			}
		}(i * 13)
	}
	wg.Wait()
}
