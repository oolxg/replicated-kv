// Package ring implements a consistent-hash ring with virtual nodes. It maps a
// key to the ordered list of physical nodes responsible for it (the preference
// list). Adding or removing a node only remaps that node's share of the
// keyspace, not the whole thing.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
)

// virtualNodes is how many points each physical node occupies on the ring. More
// points smooth out key distribution; 150 is the usual sweet spot.
const virtualNodes = 150

type point struct {
	hash uint64
	node string
}

// Ring is an immutable consistent-hash ring. It is built once from a static
// node list and never mutated, so concurrent readers need no locking.
type Ring struct {
	points []point // sorted ascending by hash
	size   int     // number of distinct physical nodes
}

// New builds a ring over the given node addresses. Empty and duplicate entries
// are ignored.
func New(nodes []string) *Ring {
	r := &Ring{}
	seen := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		r.size++
		for i := 0; i < virtualNodes; i++ {
			r.points = append(r.points, point{hash: hash64(n + "#" + strconv.Itoa(i)), node: n})
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i].hash < r.points[j].hash })
	return r
}

// Size reports the number of distinct physical nodes on the ring.
func (r *Ring) Size() int { return r.size }

// PreferenceList returns up to n distinct physical nodes responsible for key,
// walking the ring clockwise from hash(key). It returns fewer than n only when
// the ring has fewer than n nodes, and nil for an empty ring or n <= 0.
func (r *Ring) PreferenceList(key string, n int) []string {
	if len(r.points) == 0 || n <= 0 {
		return nil
	}
	if n > r.size {
		n = r.size
	}

	h := hash64(key)
	// Index of the first point with hash >= h; wrap to the start if none.
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if start == len(r.points) {
		start = 0
	}

	out := make([]string, 0, n)
	seen := make(map[string]bool, n)
	for scanned := 0; scanned < len(r.points) && len(out) < n; scanned++ {
		node := r.points[(start+scanned)%len(r.points)].node
		if !seen[node] {
			seen[node] = true
			out = append(out, node)
		}
	}
	return out
}

// hash64 maps a string to a ring position. SHA-256 gives strong avalanche, so
// virtual-node points spread evenly regardless of how similar node names are
// (FNV clustered near-identical inputs and skewed the distribution). It is
// deterministic across processes, so every router builds an identical ring.
// Used for load spreading, not security; truncating to 64 bits is fine here.
func hash64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
