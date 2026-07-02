// Package coordinator implements the stateless request coordinator (the
// router's core). At this layer (RF=1) it forwards each key to the single
// primary node from the ring's preference list and proxies the result. It
// assigns the write timestamp so LWW has exactly one clock per write path.
//
// It also serves the client-facing HTTP API (/kv/{key}), translating client
// requests into internal calls to storage nodes.
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

var errNoNodes = errors.New("no storage nodes available")

// Versioned is a value with the timestamp it was written at. JSON shape matches
// the storage node's response.
type Versioned struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

// Coordinator forwards client operations to storage nodes chosen by the ring.
type Coordinator struct {
	ring   *ring.Ring
	client *http.Client
	log    *slog.Logger
}

// New returns a Coordinator over the given ring. log must be non-nil.
func New(r *ring.Ring, log *slog.Logger) *Coordinator {
	return &Coordinator{
		ring: r,
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

// Put assigns a timestamp and writes value to the primary replica for key.
func (c *Coordinator) Put(ctx context.Context, key, value string) error {
	node := c.primary(key)
	if node == "" {
		return errNoNodes
	}

	body, err := json.Marshal(Versioned{Value: value, Timestamp: time.Now().UnixNano()})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, nodeURL(node, "/internal/put/", key), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("forward put to %s: %w", node, err)
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("node %s returned status %d", node, resp.StatusCode)
	}
	return nil
}

// Get reads the value for key from the primary replica. found is false with a
// nil error when the key does not exist.
func (c *Coordinator) Get(ctx context.Context, key string) (v Versioned, found bool, err error) {
	node := c.primary(key)
	if node == "" {
		return Versioned{}, false, errNoNodes
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeURL(node, "/internal/get/", key), nil)
	if err != nil {
		return Versioned{}, false, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return Versioned{}, false, fmt.Errorf("forward get to %s: %w", node, err)
	}
	defer drainClose(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
			return Versioned{}, false, fmt.Errorf("decode node %s response: %w", node, err)
		}
		return v, true, nil
	case http.StatusNotFound:
		return Versioned{}, false, nil
	default:
		return Versioned{}, false, fmt.Errorf("node %s returned status %d", node, resp.StatusCode)
	}
}

func (c *Coordinator) primary(key string) string {
	pl := c.ring.PreferenceList(key, 1)
	if len(pl) == 0 {
		return ""
	}
	return pl[0]
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
		http.Error(w, "upstream error", http.StatusBadGateway)
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
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := c.Put(r.Context(), key, req.Value); err != nil {
		c.log.Error("put failed", "key", key, "err", err)
		http.Error(w, "upstream error", http.StatusBadGateway)
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

func nodeURL(node, path, key string) string {
	return "http://" + node + path + url.PathEscape(key)
}

// drainClose reads and discards any remaining body before closing, so the
// underlying keep-alive connection can be reused.
func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()
}
