// Package storage exposes the internal HTTP API of a single storage node.
//
// It is a thin, stateless adapter over a store.Store: it decodes a request,
// applies it to the store, and encodes the response. All durable state lives
// in the store. The /internal/* endpoints are intended to be called only by
// the router (coordinator); they are not part of the public client API.
package storage

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/oolxg/replicated-kv/internal/shed"
	"github.com/oolxg/replicated-kv/internal/store"
)

// maxBodyBytes caps the request body the node will read. Bounding per-request
// memory keeps a malformed or hostile client from exhausting the heap; it is a
// basic robustness measure, distinct from the load-shedding added in a later
// layer. It is deliberately larger than the router's 1 MiB client-facing cap:
// the internal envelope adds a timestamp field, so a value the router accepted
// must never be rejected here.
const maxBodyBytes = 2 << 20 // 2 MiB

type putRequest struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

type getResponse struct {
	Value     string `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

type putResponse struct {
	Applied bool `json:"applied"`
}

// Handler serves the storage node's internal API.
type Handler struct {
	store *store.Store
	lim   *shed.Limiter
	log   *slog.Logger
}

// NewHandler returns a Handler backed by s, load-shedding through lim.
// lim and log must be non-nil.
func NewHandler(s *store.Store, lim *shed.Limiter, log *slog.Logger) *Handler {
	return &Handler{store: s, lim: lim, log: log}
}

// Routes builds the HTTP handler. Patterns use the method-aware matching of
// the Go 1.22+ ServeMux, so a method/path mismatch yields 404/405 for free and
// no third-party router is required. The data endpoints sit behind the load
// shedder; /healthz deliberately does not — a saturated node is overloaded,
// not dead, and its health checks must keep saying so.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /internal/get/{key}", shed.Middleware(h.lim, http.HandlerFunc(h.handleGet)))
	mux.Handle("PUT /internal/put/{key}", shed.Middleware(h.lim, http.HandlerFunc(h.handlePut)))
	mux.HandleFunc("GET /healthz", h.handleHealth)
	return mux
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	v, ok := h.store.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	h.writeJSON(w, http.StatusOK, getResponse{Value: string(v.Value), Timestamp: v.Timestamp})
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req putRequest
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Timestamp <= 0 {
		http.Error(w, "timestamp must be a positive value", http.StatusBadRequest)
		return
	}

	applied := h.store.Put(key, []byte(req.Value), req.Timestamp)
	h.writeJSON(w, http.StatusOK, putResponse{Applied: applied})
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already written, so we can only record the
		// failure, not recover the response.
		h.log.Error("encode response body", "err", err)
	}
}
