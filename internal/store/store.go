// Package store implements a sharded, in-memory key-value store with
// last-writer-wins (LWW) conflict resolution driven by a caller-supplied
// timestamp. It is the stateful core of a storage node.
//
// The store is intentionally "dumb": it applies versioned writes and serves
// versioned reads, but does not know about replication, the hash ring, or the
// network. Those concerns live in higher layers (router/coordinator). Keeping
// the store free of those responsibilities is what makes the stateful tier
// easy to reason about and test in isolation.
package store

import (
	"bytes"
	"sync"
)

// shardCount is the number of independently-locked shards. Striping the
// keyspace across many shards lets concurrent writers to different keys
// proceed in parallel instead of serialising on a single global lock.
const shardCount = 32

// Versioned is a value tagged with the logical timestamp at which it was
// written. The timestamp is assigned once, by the coordinator, on the write
// path; fanning the same timestamp out to every replica is what makes LWW
// well-defined and convergent across replicas.
type Versioned struct {
	Value     []byte
	Timestamp int64
}

type shard struct {
	mu sync.RWMutex
	m  map[string]Versioned
}

// Store is a concurrency-safe sharded map. The zero value is not usable;
// construct one with New.
type Store struct {
	shards [shardCount]shard
}

// New returns a ready-to-use, empty Store.
func New() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i].m = make(map[string]Versioned)
	}
	return s
}

// fnv32a computes the 32-bit FNV-1a hash of key without allocating. We hash
// the string bytes directly (rather than via hash/fnv, which would allocate a
// hasher and a []byte conversion on every call) because sharding sits on the
// hot path of every read and write.
func fnv32a(key string) uint32 {
	const (
		offsetBasis uint32 = 2166136261
		prime       uint32 = 16777619
	)
	h := offsetBasis
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime
	}
	return h
}

func (s *Store) shardFor(key string) *shard {
	return &s.shards[fnv32a(key)%shardCount]
}

// Put applies a last-writer-wins write of val@ts for key. The write takes
// effect iff ts is strictly newer than the stored timestamp, or equal to it
// with a lexicographically-greater value.
//
// The value tie-break on equal timestamps is deliberate: it makes the merge
// total and order-independent, so replicas that receive the same set of
// equal-timestamp writes in different orders still converge to the same value.
// (A single coordinator clock makes equal timestamps rare, but two routers can
// still collide on time.Now().UnixNano(); this keeps that case deterministic.)
//
// Put reports whether the store was mutated. A return of false means an
// equal-or-newer version was already present, so the write was a safe no-op.
func (s *Store) Put(key string, val []byte, ts int64) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if cur, ok := sh.m[key]; ok {
		if ts < cur.Timestamp ||
			(ts == cur.Timestamp && bytes.Compare(val, cur.Value) <= 0) {
			return false
		}
	}

	// Copy the incoming bytes so a caller that reuses its buffer after the
	// call cannot mutate what we have stored.
	stored := make([]byte, len(val))
	copy(stored, val)
	sh.m[key] = Versioned{Value: stored, Timestamp: ts}
	return true
}

// Get returns the stored version for key and whether it was present.
//
// The returned Versioned.Value aliases the slice held in the store and must
// not be mutated by the caller. This is safe because Put never mutates an
// existing slice in place; it always replaces the whole map entry with a
// freshly-allocated one. Avoiding a copy here keeps the read path (the
// throughput-critical path) allocation-free.
func (s *Store) Get(key string) (Versioned, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	v, ok := sh.m[key]
	return v, ok
}
