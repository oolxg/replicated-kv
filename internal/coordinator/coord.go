// Package coordinator implements the stateless request coordinator (the
// router's core): quorum replication across the storage nodes chosen by the
// ring, last-writer-wins reconciliation, and opportunistic read-repair.
//
// Writes fan out to all RF replicas of a key and succeed once W acks arrive;
// the remaining replicas keep applying the write in the background. Reads fan
// out to all RF replicas and return once R have answered, picking the newest
// version among them. With W+R > RF the read set always overlaps the newest
// committed write. The coordinator assigns write timestamps, so LWW has
// exactly one clock per write path.
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
	"net/url"
	"time"

	"github.com/oolxg/replicated-kv/internal/ring"
)

const (
	nodeRequestTimeout = 2 * time.Second
	maxBodyBytes       = 1 << 20 // 1 MiB cap on client request bodies
)

var (
	errNoNodes  = errors.New("no storage nodes available")
	errNoQuorum = errors.New("quorum not reached")
)

// Versioned is a value with the timestamp it was written at. Its JSON shape
// matches the storage node's request and response bodies.
type Versioned struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

// newer reports whether a should win over b. It applies the same total order
// as store.Put (timestamp, then value bytes), so the version reconcile picks
// is the same one the replicas themselves converge to.
func newer(a, b Versioned) bool {
	return a.Timestamp > b.Timestamp ||
		(a.Timestamp == b.Timestamp && a.Value > b.Value)
}

// replicaRead is one replica's answer to a GET fan-out. err == nil and
// found == false means the replica answered "no such key", which counts
// toward the read quorum.
type replicaRead struct {
	node  string
	v     Versioned
	found bool
	err   error
}

// Coordinator fans client operations out to the key's replicas and collects
// quorum. The quorum contract (1 <= W,R <= RF <= number of ring nodes) is
// validated by config at startup.
type Coordinator struct {
	ring     *ring.Ring
	rf, w, r int
	client   *http.Client
	log      *slog.Logger
}

// New returns a Coordinator over rg with the given quorum parameters.
// log must be non-nil.
func New(rg *ring.Ring, rf, w, r int, log *slog.Logger) *Coordinator {
	return &Coordinator{
		ring: rg,
		rf:   rf,
		w:    w,
		r:    r,
		client: &http.Client{
			Timeout: nodeRequestTimeout,
			// Tuned for connection reuse to each storage node under load; the
			// stdlib default of 2 idle conns/host throttles throughput.
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		log: log,
	}
}

// Put assigns a timestamp and replicates value to the key's RF replicas,
// returning nil once W of them have acked. Replica calls deliberately ignore
// ctx cancellation: once we answer the client, stragglers should still apply
// the write (best-effort durability); each call is bounded by the client
// timeout instead. ctx only bounds how long we wait for the quorum.
func (c *Coordinator) Put(ctx context.Context, key, value string) error {
	replicas := c.ring.PreferenceList(key, c.rf)
	if len(replicas) == 0 {
		return errNoNodes
	}
	body, err := marshalBody(Versioned{Value: value, Timestamp: time.Now().UnixNano()})
	if err != nil {
		return err
	}

	acks := make(chan error, len(replicas)) // buffered: stragglers never block
	for _, node := range replicas {
		go func(node string) { acks <- c.putReplica(node, key, body) }(node)
	}

	oks, fails := 0, 0
	var lastErr error
	for range replicas {
		select {
		case err := <-acks:
			if err != nil {
				fails++
				lastErr = err
				// Fail fast once the write quorum is unreachable.
				if fails > len(replicas)-c.w {
					return fmt.Errorf("%w: %d of %d acks needed: %v", errNoQuorum, oks, c.w, lastErr)
				}
			} else {
				oks++
				if oks >= c.w {
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("%w: %d of %d acks needed: %v", errNoQuorum, oks, c.w, lastErr)
}

func (c *Coordinator) putReplica(node, key string, body []byte) error {
	req, err := http.NewRequest(http.MethodPut, nodeURL(node, "/internal/put/", key), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("put to %s: %w", node, err)
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("node %s returned status %d", node, resp.StatusCode)
	}
	return nil
}

// Get fans the read out to the key's RF replicas, waits for R answers, and
// reconciles them by the LWW order. A replica's "not found" counts toward the
// quorum. found is false with a nil error when the read quorum agrees the key
// does not exist. Replicas observed stale or missing are repaired
// asynchronously with the winning version.
func (c *Coordinator) Get(ctx context.Context, key string) (Versioned, bool, error) {
	replicas := c.ring.PreferenceList(key, c.rf)
	if len(replicas) == 0 {
		return Versioned{}, false, errNoNodes
	}

	results := make(chan replicaRead, len(replicas)) // buffered: stragglers never block
	for _, node := range replicas {
		go func(node string) { results <- c.getReplica(ctx, node, key) }(node)
	}

	reads := make([]replicaRead, 0, len(replicas))
	oks := 0
	for range replicas {
		var rd replicaRead
		select {
		case rd = <-results:
		case <-ctx.Done():
			return Versioned{}, false, ctx.Err()
		}
		reads = append(reads, rd)
		if rd.err == nil {
			oks++
			if oks >= c.r {
				break
			}
		} else if len(reads)-oks > len(replicas)-c.r {
			// Fail fast once the read quorum is unreachable.
			return Versioned{}, false, fmt.Errorf("%w: %d of %d reads needed: %v", errNoQuorum, oks, c.r, rd.err)
		}
	}
	if oks < c.r {
		return Versioned{}, false, fmt.Errorf("%w: %d of %d reads needed", errNoQuorum, oks, c.r)
	}

	var winner Versioned
	found := false
	for _, rd := range reads {
		if rd.err != nil || !rd.found {
			continue
		}
		if !found || newer(rd.v, winner) {
			winner, found = rd.v, true
		}
	}
	if !found {
		return Versioned{}, false, nil
	}
	c.repair(key, winner, reads)
	return winner, true, nil
}

func (c *Coordinator) getReplica(ctx context.Context, node, key string) replicaRead {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeURL(node, "/internal/get/", key), nil)
	if err != nil {
		return replicaRead{node: node, err: err}
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return replicaRead{node: node, err: fmt.Errorf("get from %s: %w", node, err)}
	}
	defer drainClose(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		var v Versioned
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
			return replicaRead{node: node, err: fmt.Errorf("decode from %s: %w", node, err)}
		}
		return replicaRead{node: node, v: v, found: true}
	case http.StatusNotFound:
		return replicaRead{node: node}
	default:
		return replicaRead{node: node, err: fmt.Errorf("node %s returned status %d", node, resp.StatusCode)}
	}
}

// repair asynchronously writes winner back to replicas that answered with a
// stale or missing version (Dynamo-style read-repair). It is opportunistic:
// only replicas whose answer arrived within the quorum window are considered,
// and replicas that errored are skipped — they are likely down and will be
// healed by a later read. Failures are best-effort by design.
func (c *Coordinator) repair(key string, winner Versioned, reads []replicaRead) {
	var body []byte
	for _, rd := range reads {
		if rd.err != nil || (rd.found && !newer(winner, rd.v)) {
			continue
		}
		if body == nil {
			var err error
			if body, err = marshalBody(winner); err != nil {
				return
			}
		}
		go func(node string) {
			if err := c.putReplica(node, key, body); err != nil {
				c.log.Debug("read-repair failed", "node", node, "key", key, "err", err)
			}
		}(rd.node)
	}
}

// --- client-facing HTTP API ---

// Routes returns the client-facing HTTP handler.
func (c *Coordinator) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", c.handleGet)
	mux.HandleFunc("PUT /kv/{key}", c.handlePut)
	mux.HandleFunc("GET /healthz", c.handleHealth)
	return mux
}

func (c *Coordinator) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	v, found, err := c.Get(r.Context(), key)
	if err != nil {
		c.log.Error("get failed", "key", key, "err", err)
		http.Error(w, "quorum not reached", http.StatusGatewayTimeout)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	c.writeJSON(w, http.StatusOK, v)
}

func (c *Coordinator) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req struct {
		Value string `json:"value"`
	}
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := c.Put(r.Context(), key, req.Value); err != nil {
		c.log.Error("put failed", "key", key, "err", err)
		http.Error(w, "quorum not reached", http.StatusGatewayTimeout)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Coordinator) handleHealth(w http.ResponseWriter, _ *http.Request) {
	c.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (c *Coordinator) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		c.log.Error("encode response body", "err", err)
	}
}

// marshalBody encodes the internal write envelope without HTML escaping.
// json.Marshal would expand '<', '>', '&' to \u-sequences (1 byte -> 6), so a
// value that fit the router's client-facing body cap could inflate past the
// storage node's cap and turn an accepted write into a quorum failure.
func marshalBody(v Versioned) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func nodeURL(node, path, key string) string {
	return "http://" + node + path + url.PathEscape(key)
}

// drainClose reads and discards any remaining body before closing, so the
// underlying keep-alive connection can be reused.
func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()
}
