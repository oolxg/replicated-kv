package ring

import (
	"fmt"
	"testing"
)

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPreferenceListDistinctCount(t *testing.T) {
	r := New([]string{"a:1", "b:1", "c:1", "d:1"})
	tests := []struct {
		name string
		n    int
		want int
	}{
		{"one", 1, 1},
		{"two", 2, 2},
		{"all", 4, 4},
		{"more than nodes caps at size", 10, 4},
		{"zero", 0, 0},
		{"negative", -1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := r.PreferenceList("some-key", tt.n)
			if len(pl) != tt.want {
				t.Fatalf("len = %d, want %d (%v)", len(pl), tt.want, pl)
			}
			seen := map[string]bool{}
			for _, node := range pl {
				if seen[node] {
					t.Fatalf("duplicate physical node %q in %v", node, pl)
				}
				seen[node] = true
			}
		})
	}
}

func TestPreferenceListDeterministic(t *testing.T) {
	r := New([]string{"a:1", "b:1", "c:1"})
	want := r.PreferenceList("k", 2)
	for i := 0; i < 100; i++ {
		if got := r.PreferenceList("k", 2); !equal(got, want) {
			t.Fatalf("non-deterministic: %v vs %v", got, want)
		}
	}
}

func TestEmptyRing(t *testing.T) {
	r := New(nil)
	if r.Size() != 0 {
		t.Fatalf("size = %d, want 0", r.Size())
	}
	if pl := r.PreferenceList("k", 1); pl != nil {
		t.Fatalf("want nil preference list, got %v", pl)
	}
}

func TestDedupAndEmptyEntries(t *testing.T) {
	r := New([]string{"a:1", "a:1", "", "b:1"})
	if r.Size() != 2 {
		t.Fatalf("size = %d, want 2 (dedup + drop empty)", r.Size())
	}
}

// TestDistribution feeds 100k keys and asserts each node's share of primaries
// is within +/-20% of the mean — the sharding must actually spread load.
func TestDistribution(t *testing.T) {
	nodes := []string{"a:1", "b:1", "c:1", "d:1", "e:1"}
	r := New(nodes)

	counts := map[string]int{}
	const total = 100_000
	for i := 0; i < total; i++ {
		counts[r.PreferenceList(fmt.Sprintf("key-%d", i), 1)[0]]++
	}

	mean := float64(total) / float64(len(nodes))
	for _, n := range nodes {
		dev := float64(counts[n])/mean - 1
		if dev < -0.2 || dev > 0.2 {
			t.Errorf("node %s got %d keys (%.1f%% off mean %.0f)", n, counts[n], dev*100, mean)
		}
	}
}

// TestMinimalRemapOnNodeRemoval verifies consistent hashing's core property:
// removing a node moves only the keys that node owned, nothing else.
func TestMinimalRemapOnNodeRemoval(t *testing.T) {
	before := New([]string{"a:1", "b:1", "c:1", "d:1", "e:1"})
	after := New([]string{"a:1", "b:1", "c:1", "d:1"}) // removed e:1

	const total = 50_000
	moved, ownedByRemoved := 0, 0
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("key-%d", i)
		b := before.PreferenceList(k, 1)[0]
		a := after.PreferenceList(k, 1)[0]
		if b == "e:1" {
			ownedByRemoved++
		}
		if a != b {
			moved++
		}
	}
	if moved != ownedByRemoved {
		t.Fatalf("remap not minimal: %d keys moved but only %d were owned by the removed node", moved, ownedByRemoved)
	}
}
