// Package cache implements a read-through LRU cache with TTL, by hand
// (assignment requirement: no caching libraries). The router keeps hot keys
// here: a hit answers the client without any quorum fan-out, cutting both
// latency and load on the stateful tier.
//
// The cache is per-router process. With several routers, one router's write
// does not invalidate another router's cached copy — the TTL bounds that
// staleness window. This is a documented limitation, not an accident.
package cache

import (
	"container/list"
	"sync"
	"time"
)

type entry[V any] struct {
	key     string
	value   V
	expires time.Time
}

// Cache is a fixed-capacity LRU with per-entry TTL. Safe for concurrent use.
// A single mutex is fine here: operations are a map lookup plus a couple of
// pointer swaps, orders of magnitude cheaper than the quorum read a hit saves.
type Cache[V any] struct {
	mu    sync.Mutex
	cap   int
	ttl   time.Duration
	order *list.List // front = most recently used
	items map[string]*list.Element

	now func() time.Time // test hook
}

// New returns an empty cache holding at most capacity entries, each fresh for
// ttl after (re)insertion. capacity must be >= 1 and ttl > 0.
func New[V any](capacity int, ttl time.Duration) *Cache[V] {
	return &Cache[V]{
		cap:   capacity,
		ttl:   ttl,
		order: list.New(),
		items: make(map[string]*list.Element, capacity),
		now:   time.Now,
	}
}

// Get returns the cached value for key and whether a fresh one was present.
// A hit marks the entry most-recently-used; an expired entry is dropped.
func (c *Cache[V]) Get(key string) (V, bool) {
	var zero V
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return zero, false
	}
	ent := el.Value.(*entry[V])
	if c.now().After(ent.expires) {
		c.remove(el) // expired: evict now so it cannot squat in the LRU list
		return zero, false
	}
	c.order.MoveToFront(el)
	return ent.value, true
}

// Put inserts or refreshes key with a fresh TTL, evicting the least recently
// used entry when over capacity.
func (c *Cache[V]) Put(key string, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		ent := el.Value.(*entry[V])
		ent.value = v
		ent.expires = c.now().Add(c.ttl)
		c.order.MoveToFront(el)
		return
	}
	c.items[key] = c.order.PushFront(&entry[V]{key: key, value: v, expires: c.now().Add(c.ttl)})
	if c.order.Len() > c.cap {
		c.remove(c.order.Back())
	}
}

// Invalidate drops key if present. The router calls this on every successful
// write, so its own clients read their own writes despite the cache.
func (c *Cache[V]) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.remove(el)
	}
}

func (c *Cache[V]) remove(el *list.Element) {
	delete(c.items, el.Value.(*entry[V]).key)
	c.order.Remove(el)
}
