package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oolxg/replicated-kv/internal/cache"
	"github.com/oolxg/replicated-kv/internal/retry"
	"github.com/oolxg/replicated-kv/internal/ring"
	"github.com/oolxg/replicated-kv/internal/shed"
	"github.com/oolxg/replicated-kv/internal/storage"
	"github.com/oolxg/replicated-kv/internal/store"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// wideShed returns a limiter generous enough to never shed in tests that are
// not about shedding.
func wideShed() *shed.Limiter { return shed.New(1024, 1024) }

// opts builds coordinator Options with the given quorum and harmless
// defaults: no cache, single-attempt calls (retry-specific tests set their
// own policy).
func opts(rf, w, r int) Options {
	return Options{RF: rf, W: w, R: r, Limiter: wideShed(), Log: discardLog()}
}

// testNode is one real storage node: its HTTP server plus a handle on the
// underlying store, so tests can seed or inspect replica state directly.
type testNode struct {
	addr  string
	store *store.Store
	srv   *httptest.Server
}

// startNodes spins up n real storage nodes. Using the real storage handler
// makes these true router->storage integration tests.
func startNodes(t *testing.T, n int) []*testNode {
	t.Helper()
	nodes := make([]*testNode, 0, n)
	for i := 0; i < n; i++ {
		st := store.New()
		srv := httptest.NewServer(storage.NewHandler(st, wideShed(), discardLog()).Routes())
		t.Cleanup(srv.Close)
		nodes = append(nodes, &testNode{
			addr:  strings.TrimPrefix(srv.URL, "http://"),
			store: st,
			srv:   srv,
		})
	}
	return nodes
}

func addrsOf(nodes []*testNode) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.addr
	}
	return out
}

// newCoord builds a coordinator with the deployment defaults:
// RF = min(3, nodes), W = R = majority of RF.
func newCoord(t *testing.T, nodes []*testNode) *Coordinator {
	t.Helper()
	a := addrsOf(nodes)
	rf := min(3, len(a))
	return New(ring.New(a), opts(rf, rf/2+1, rf/2+1))
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNewerTotalOrder(t *testing.T) {
	tests := []struct {
		name string
		a, b Versioned
		want bool
	}{
		{"higher timestamp wins", Versioned{"x", 2}, Versioned{"y", 1}, true},
		{"lower timestamp loses", Versioned{"x", 1}, Versioned{"y", 2}, false},
		{"equal ts, greater value wins", Versioned{"b", 5}, Versioned{"a", 5}, true},
		{"equal ts, smaller value loses", Versioned{"a", 5}, Versioned{"b", 5}, false},
		{"identical is not newer", Versioned{"a", 5}, Versioned{"a", 5}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newer(tt.a, tt.b); got != tt.want {
				t.Fatalf("newer(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestCoordinatorPutGet(t *testing.T) {
	c := newCoord(t, startNodes(t, 3))
	ctx := context.Background()

	if err := c.Put(ctx, "alpha", "one"); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, found, err := c.Get(ctx, "alpha")
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if v.Value != "one" {
		t.Fatalf("value = %q, want one", v.Value)
	}
	if v.Timestamp == 0 {
		t.Fatal("coordinator must assign a non-zero timestamp")
	}
}

func TestCoordinatorSpreadOfKeys(t *testing.T) {
	c := newCoord(t, startNodes(t, 3))
	ctx := context.Background()

	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		if err := c.Put(ctx, k, k+"-val"); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("key-%d", i)
		v, found, err := c.Get(ctx, k)
		if err != nil || !found || v.Value != k+"-val" {
			t.Fatalf("get %s: %q found=%v err=%v", k, v.Value, found, err)
		}
	}
}

func TestCoordinatorGetMissing(t *testing.T) {
	c := newCoord(t, startNodes(t, 3))
	_, found, err := c.Get(context.Background(), "ghost")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func TestSingleNodeDegeneratesToLayer2(t *testing.T) {
	nodes := startNodes(t, 1)
	c := New(ring.New(addrsOf(nodes)), opts(1, 1, 1))
	ctx := context.Background()

	if err := c.Put(ctx, "k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, found, err := c.Get(ctx, "k")
	if err != nil || !found || v.Value != "v" {
		t.Fatalf("get: %q found=%v err=%v", v.Value, found, err)
	}
}

// TestPutReplicatesToAllReplicas: Put returns at W acks, but the fan-out goes
// to all RF replicas — eventually every one of them holds the value.
func TestPutReplicatesToAllReplicas(t *testing.T) {
	nodes := startNodes(t, 3) // RF=3 -> every node replicates every key
	c := newCoord(t, nodes)

	if err := c.Put(context.Background(), "k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	for i, n := range nodes {
		waitFor(t, fmt.Sprintf("replica %d to hold the value", i), func() bool {
			got, ok := n.store.Get("k")
			return ok && string(got.Value) == "v"
		})
	}
}

// TestPutQuorumCounting drives the W-counting logic against dead replicas.
// With RF=3, W=2: one dead replica is tolerated, two are not.
func TestPutQuorumCounting(t *testing.T) {
	tests := []struct {
		name    string
		down    int // how many of the 3 replicas are dead
		wantErr bool
	}{
		{"all up", 0, false},
		{"one down", 1, false},
		{"two down", 2, true},
		{"all down", 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := startNodes(t, 3)
			c := newCoord(t, nodes)
			for i := 0; i < tt.down; i++ {
				nodes[i].srv.Close()
			}
			err := c.Put(context.Background(), "k", "v")
			if tt.wantErr {
				if !errors.Is(err, errNoQuorum) {
					t.Fatalf("err = %v, want errNoQuorum", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestGetQuorumCounting drives the R-counting logic against dead replicas.
// With RF=3, R=2: one dead replica is tolerated, two are not.
func TestGetQuorumCounting(t *testing.T) {
	tests := []struct {
		name    string
		down    int
		wantErr bool
	}{
		{"all up", 0, false},
		{"one down", 1, false},
		{"two down", 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := startNodes(t, 3)
			c := newCoord(t, nodes)
			for _, n := range nodes {
				n.store.Put("k", []byte("v"), 5)
			}
			for i := 0; i < tt.down; i++ {
				nodes[i].srv.Close()
			}
			v, found, err := c.Get(context.Background(), "k")
			if tt.wantErr {
				if !errors.Is(err, errNoQuorum) {
					t.Fatalf("err = %v, want errNoQuorum", err)
				}
				return
			}
			if err != nil || !found || v.Value != "v" {
				t.Fatalf("get: %q found=%v err=%v", v.Value, found, err)
			}
		})
	}
}

// TestQuorumSurvivesOneReplicaDown is the layer's key integration test: with
// RF=3/W=2/R=2, killing one replica keeps both reads and writes available.
func TestQuorumSurvivesOneReplicaDown(t *testing.T) {
	nodes := startNodes(t, 3)
	c := newCoord(t, nodes)
	ctx := context.Background()

	if err := c.Put(ctx, "k", "v1"); err != nil {
		t.Fatalf("put before kill: %v", err)
	}
	nodes[0].srv.Close() // kill one replica

	v, found, err := c.Get(ctx, "k")
	if err != nil || !found || v.Value != "v1" {
		t.Fatalf("get after kill: %q found=%v err=%v", v.Value, found, err)
	}
	if err := c.Put(ctx, "k", "v2"); err != nil {
		t.Fatalf("put after kill: %v", err)
	}
	v, found, err = c.Get(ctx, "k")
	if err != nil || !found || v.Value != "v2" {
		t.Fatalf("get after second put: %q found=%v err=%v", v.Value, found, err)
	}
}

// TestGetReconcileNewestWins: one replica holds a stale version (as after a
// missed write with W=2), the others the newest. Any R=2 read set overlaps a
// newest copy, and reconcile must return it.
func TestGetReconcileNewestWins(t *testing.T) {
	nodes := startNodes(t, 3)
	c := newCoord(t, nodes)

	nodes[0].store.Put("k", []byte("stale"), 1)
	nodes[1].store.Put("k", []byte("fresh"), 2)
	nodes[2].store.Put("k", []byte("fresh"), 2)

	for i := 0; i < 20; i++ { // repeat: replica answer order is racy
		v, found, err := c.Get(context.Background(), "k")
		if err != nil || !found {
			t.Fatalf("get: found=%v err=%v", found, err)
		}
		if v.Value != "fresh" || v.Timestamp != 2 {
			t.Fatalf("reconcile returned %q@%d, want fresh@2", v.Value, v.Timestamp)
		}
	}
}

// TestReadRepairHeals covers both repair branches: a replica that missed the
// write entirely, and one holding a stale version. R=RF here so the read
// always sees every replica's answer — with R<RF repair is opportunistic
// (only replicas inside the quorum window get repaired), which would make
// these tests racy.
func TestReadRepairHeals(t *testing.T) {
	t.Run("missing replica", func(t *testing.T) {
		nodes := startNodes(t, 3)
		c := New(ring.New(addrsOf(nodes)), opts(3, 2, 3))

		nodes[0].store.Put("k", []byte("v"), 5)
		nodes[1].store.Put("k", []byte("v"), 5)
		// nodes[2] missed the write

		v, found, err := c.Get(context.Background(), "k")
		if err != nil || !found || v.Value != "v" {
			t.Fatalf("get: %q found=%v err=%v", v.Value, found, err)
		}
		waitFor(t, "read-repair to heal the missing replica", func() bool {
			got, ok := nodes[2].store.Get("k")
			return ok && string(got.Value) == "v" && got.Timestamp == 5
		})
	})

	t.Run("stale replica", func(t *testing.T) {
		nodes := startNodes(t, 3)
		c := New(ring.New(addrsOf(nodes)), opts(3, 2, 3))

		nodes[0].store.Put("k", []byte("stale"), 1)
		nodes[1].store.Put("k", []byte("fresh"), 2)
		nodes[2].store.Put("k", []byte("fresh"), 2)

		v, found, err := c.Get(context.Background(), "k")
		if err != nil || !found || v.Value != "fresh" {
			t.Fatalf("get: %q found=%v err=%v", v.Value, found, err)
		}
		waitFor(t, "read-repair to overwrite the stale replica", func() bool {
			got, ok := nodes[0].store.Get("k")
			return ok && string(got.Value) == "fresh" && got.Timestamp == 2
		})
	})
}

// TestTimeoutCountsAsFailure: a hanging replica must count against the quorum
// exactly like a dead one — for both tolerated (1 of 3) and fatal (2 of 3)
// amounts of hanging. The hanging handler blocks until the caller's timeout
// cancels the request, so no fixed sleeps are involved.
func TestTimeoutCountsAsFailure(t *testing.T) {
	tests := []struct {
		name    string
		healthy int
		hanging int
		wantErr bool
	}{
		{"one hanging of three tolerated", 2, 1, false},
		{"two hanging of three break quorum", 1, 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := startNodes(t, tt.healthy)
			all := addrsOf(nodes)
			for i := 0; i < tt.hanging; i++ {
				hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// The server only detects a client disconnect (and cancels
					// r.Context()) once the request body has been consumed —
					// without the drain, a PUT would hang here forever and
					// deadlock the server's Close in t.Cleanup.
					_, _ = io.Copy(io.Discard, r.Body)
					select {
					case <-r.Context().Done(): // caller timed out and hung up
					case <-time.After(5 * time.Second): // safety net for Close
					}
				}))
				t.Cleanup(hang.Close)
				all = append(all, strings.TrimPrefix(hang.URL, "http://"))
			}
			c := New(ring.New(all), opts(3, 2, 2))
			c.client.Timeout = 250 * time.Millisecond // fail hanging calls fast

			err := c.Put(context.Background(), "k", "v")
			if tt.wantErr {
				if !errors.Is(err, errNoQuorum) {
					t.Fatalf("put err = %v, want errNoQuorum", err)
				}
			} else if err != nil {
				t.Fatalf("put with %d hanging replicas: %v", tt.hanging, err)
			}

			// Seed the healthy stores directly so the GET quorum has a value
			// to agree on regardless of the PUT outcome above.
			for _, n := range nodes {
				n.store.Put("k", []byte("v"), 5)
			}
			v, found, err := c.Get(context.Background(), "k")
			if tt.wantErr {
				if !errors.Is(err, errNoQuorum) {
					t.Fatalf("get err = %v, want errNoQuorum", err)
				}
				return
			}
			if err != nil || !found || v.Value != "v" {
				t.Fatalf("get with %d hanging replicas: %q found=%v err=%v", tt.hanging, v.Value, found, err)
			}
		})
	}
}

// TestLargeValuesSurviveInternalReencoding is the regression test for the body
// inflation bug: the coordinator re-encodes the value for the internal PUT, and
// json's default HTML escaping would expand '<', '>', '&' six-fold — a value
// the router accepted then blew past the storage node's body cap and failed
// the write quorum. The client bodies here are built without HTML escaping,
// so they fit the router's 1 MiB cap while being escape-hostile.
func TestLargeValuesSurviveInternalReencoding(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"html-escape-hostile value", strings.Repeat("<&>", 100_000)}, // 300 KB raw; ~1.8 MB if HTML-escaped
		{"ascii value near the client cap", strings.Repeat("a", (1<<20)-1024)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newCoord(t, startNodes(t, 3)).Routes()

			var clientBody bytes.Buffer
			enc := json.NewEncoder(&clientBody)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(map[string]string{"value": tt.value}); err != nil {
				t.Fatal(err)
			}
			if clientBody.Len() > maxBodyBytes {
				t.Fatalf("test bug: client body %d exceeds the router cap", clientBody.Len())
			}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/kv/big", &clientBody))
			if rec.Code != http.StatusOK {
				t.Fatalf("put status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
			}

			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/kv/big", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("get status = %d, want 200", rec.Code)
			}
			var got Versioned
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Value != tt.value {
				t.Fatalf("value corrupted in transit: len %d, want %d", len(got.Value), len(tt.value))
			}
		})
	}
}

// TestReplicaShedCountsAsFailure: a replica answering 503 (load shedding)
// must count against the quorum exactly like a dead one — the router must
// neither treat 503 as success nor retry into the saturated node here.
func TestReplicaShedCountsAsFailure(t *testing.T) {
	tests := []struct {
		name     string
		healthy  int
		shedding int
		wantErr  bool
	}{
		{"one shedding of three tolerated", 2, 1, false},
		{"two shedding of three break quorum", 1, 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := startNodes(t, tt.healthy)
			all := addrsOf(nodes)
			for range tt.shedding {
				overloaded := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Error(w, "overloaded", http.StatusServiceUnavailable)
				}))
				t.Cleanup(overloaded.Close)
				all = append(all, strings.TrimPrefix(overloaded.URL, "http://"))
			}
			c := New(ring.New(all), opts(3, 2, 2))

			err := c.Put(context.Background(), "k", "v")
			if tt.wantErr {
				if !errors.Is(err, errNoQuorum) {
					t.Fatalf("put err = %v, want errNoQuorum", err)
				}
			} else if err != nil {
				t.Fatalf("put with %d shedding replicas: %v", tt.shedding, err)
			}

			for _, n := range nodes {
				n.store.Put("k", []byte("v"), 5)
			}
			v, found, err := c.Get(context.Background(), "k")
			if tt.wantErr {
				if !errors.Is(err, errNoQuorum) {
					t.Fatalf("get err = %v, want errNoQuorum", err)
				}
				return
			}
			if err != nil || !found || v.Value != "v" {
				t.Fatalf("get with %d shedding replicas: %q found=%v err=%v", tt.shedding, v.Value, found, err)
			}
		})
	}
}

// TestCacheServesRepeatedReadsWithoutQuorum: after one quorum read the value
// must come from the router's cache — proven by killing the entire cluster
// and reading again.
func TestCacheServesRepeatedReadsWithoutQuorum(t *testing.T) {
	nodes := startNodes(t, 3)
	o := opts(3, 2, 2)
	o.Cache = cache.New[Versioned](64, time.Minute)
	c := New(ring.New(addrsOf(nodes)), o)
	ctx := context.Background()

	if err := c.Put(ctx, "hot", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, found, err := c.Get(ctx, "hot"); err != nil || !found {
		t.Fatalf("first get: found=%v err=%v", found, err)
	}

	for _, n := range nodes {
		n.srv.Close() // no cluster left: only the cache can answer now
	}
	v, found, err := c.Get(ctx, "hot")
	if err != nil || !found || v.Value != "v1" {
		t.Fatalf("cached get after cluster death: %q found=%v err=%v", v.Value, found, err)
	}
}

// TestPutInvalidatesCache: a write through this router must not leave a stale
// cached version behind.
func TestPutInvalidatesCache(t *testing.T) {
	nodes := startNodes(t, 3)
	o := opts(3, 2, 2)
	o.Cache = cache.New[Versioned](64, time.Minute)
	c := New(ring.New(addrsOf(nodes)), o)
	ctx := context.Background()

	if err := c.Put(ctx, "k", "v1"); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	if v, _, _ := c.Get(ctx, "k"); v.Value != "v1" {
		t.Fatalf("expected v1 cached, got %q", v.Value)
	}
	if err := c.Put(ctx, "k", "v2"); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	v, found, err := c.Get(ctx, "k")
	if err != nil || !found || v.Value != "v2" {
		t.Fatalf("get after overwrite: %q found=%v err=%v — stale cache?", v.Value, found, err)
	}
}

// TestRetryRecoversTransientFailures: a replica that sheds twice and then
// recovers must be invisible to the client when retries are enabled. RF=1
// so the flaky node is the only replica — success is attributable to retry.
func TestRetryRecoversTransientFailures(t *testing.T) {
	inner := storage.NewHandler(store.New(), wideShed(), discardLog()).Routes()
	var calls atomic.Int32
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			http.Error(w, "overloaded", http.StatusServiceUnavailable)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(flaky.Close)

	o := opts(1, 1, 1)
	o.Retry = retry.Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	c := New(ring.New([]string{strings.TrimPrefix(flaky.URL, "http://")}), o)
	ctx := context.Background()

	if err := c.Put(ctx, "k", "v"); err != nil {
		t.Fatalf("put through flaky replica: %v (attempts made: %d)", err, calls.Load())
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("replica saw %d calls, want exactly 3 (2 shed + 1 success)", got)
	}
	v, found, err := c.Get(ctx, "k")
	if err != nil || !found || v.Value != "v" {
		t.Fatalf("get after retried put: %q found=%v err=%v", v.Value, found, err)
	}
}

// TestClientErrorsAreNotRetried: a deterministic 4xx from a replica must fail
// after exactly one attempt — retrying reproduces it and only adds latency.
func TestClientErrorsAreNotRetried(t *testing.T) {
	var calls atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	t.Cleanup(bad.Close)

	o := opts(1, 1, 1)
	o.Retry = retry.Policy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	c := New(ring.New([]string{strings.TrimPrefix(bad.URL, "http://")}), o)

	if err := c.Put(context.Background(), "k", "v"); err == nil {
		t.Fatal("expected error from a 400 replica")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("replica saw %d calls, want exactly 1: 4xx must not be retried", got)
	}
}

func TestRouterHTTP(t *testing.T) {
	h := newCoord(t, startNodes(t, 3)).Routes()

	tests := []struct {
		name       string
		method     string
		target     string
		body       string
		wantStatus int
	}{
		{"put ok", http.MethodPut, "/kv/foo", `{"value":"bar"}`, http.StatusOK},
		{"get present", http.MethodGet, "/kv/foo", "", http.StatusOK},
		{"get missing", http.MethodGet, "/kv/absent", "", http.StatusNotFound},
		{"put bad json", http.MethodPut, "/kv/foo", "{bad", http.StatusBadRequest},
		{"put unknown field", http.MethodPut, "/kv/foo", `{"value":"v","x":1}`, http.StatusBadRequest},
		{"put oversized body", http.MethodPut, "/kv/foo", `{"value":"` + strings.Repeat("a", 2<<20) + `"}`, http.StatusRequestEntityTooLarge},
		{"healthz", http.MethodGet, "/healthz", "", http.StatusOK},
		{"method not allowed", http.MethodPost, "/kv/foo", "", http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.target, body))
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
		})
	}
}

// TestRouterHTTPNoQuorum: quorum failures surface to the client as 504.
func TestRouterHTTPNoQuorum(t *testing.T) {
	nodes := startNodes(t, 3)
	h := newCoord(t, nodes).Routes()
	for _, n := range nodes {
		n.srv.Close()
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/kv/foo", strings.NewReader(`{"value":"v"}`)))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("put status = %d, want 504", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/kv/foo", nil))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("get status = %d, want 504", rec.Code)
	}
}

func TestRouterGetReturnsValueAndTimestamp(t *testing.T) {
	h := newCoord(t, startNodes(t, 3)).Routes()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/kv/foo", strings.NewReader(`{"value":"bar"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("put status %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/kv/foo", nil))
	body := rec.Body.String()
	if !strings.Contains(body, `"value":"bar"`) || !strings.Contains(body, `"timestamp":`) {
		t.Fatalf("unexpected body: %s", body)
	}
}
