package storage

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oolxg/replicated-kv/internal/shed"
	"github.com/oolxg/replicated-kv/internal/store"
)

func newTestHandler() http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(store.New(), shed.New(64, 64), logger).Routes()
}

func do(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, r)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestHTTPStatusCases covers the request → status-code matrix: routing, method
// matching, and request validation.
func TestHTTPStatusCases(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		body       string
		wantStatus int
	}{
		{"healthz", http.MethodGet, "/healthz", "", http.StatusOK},
		{"get missing key", http.MethodGet, "/internal/get/nope", "", http.StatusNotFound},
		{"put ok", http.MethodPut, "/internal/put/k", `{"value":"v","timestamp":1}`, http.StatusOK},
		{"put malformed json", http.MethodPut, "/internal/put/k", "{not json", http.StatusBadRequest},
		{"put unknown field", http.MethodPut, "/internal/put/k", `{"value":"v","timestamp":1,"x":true}`, http.StatusBadRequest},
		{"put missing timestamp", http.MethodPut, "/internal/put/k", `{"value":"v"}`, http.StatusBadRequest},
		{"put zero timestamp", http.MethodPut, "/internal/put/k", `{"value":"v","timestamp":0}`, http.StatusBadRequest},
		{"put negative timestamp", http.MethodPut, "/internal/put/k", `{"value":"v","timestamp":-1}`, http.StatusBadRequest},
		{"method not allowed", http.MethodPost, "/internal/get/k", "", http.StatusMethodNotAllowed},
		{"unknown route", http.MethodGet, "/nope", "", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler()
			rec := do(t, h, tt.method, tt.target, tt.body)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

// TestHTTPPutThenGet checks the end-to-end behavior the status table cannot:
// the applied flag and the value/timestamp read back.
func TestHTTPPutThenGet(t *testing.T) {
	h := newTestHandler()

	rec := do(t, h, http.MethodPut, "/internal/put/foo", `{"value":"hello","timestamp":1000}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("put status = %d, want 200", rec.Code)
	}
	var pr putResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &pr); err != nil {
		t.Fatalf("decode put response: %v", err)
	}
	if !pr.Applied {
		t.Fatal("expected applied = true on first put")
	}

	rec = do(t, h, http.MethodGet, "/internal/get/foo", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200", rec.Code)
	}
	var gr getResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &gr); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if gr.Value != "hello" || gr.Timestamp != 1000 {
		t.Fatalf("got %q@%d, want hello@1000", gr.Value, gr.Timestamp)
	}
}

// TestHTTPPutLWW confirms the LWW rule is enforced through the HTTP layer: an
// older-timestamp write returns applied=false and does not change the value.
func TestHTTPPutLWW(t *testing.T) {
	h := newTestHandler()

	if rec := do(t, h, http.MethodPut, "/internal/put/k", `{"value":"new","timestamp":20}`); rec.Code != http.StatusOK {
		t.Fatalf("put new status = %d", rec.Code)
	}

	rec := do(t, h, http.MethodPut, "/internal/put/k", `{"value":"old","timestamp":10}`)
	var pr putResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &pr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pr.Applied {
		t.Fatal("older write reported applied=true; LWW violated")
	}

	rec = do(t, h, http.MethodGet, "/internal/get/k", "")
	var gr getResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &gr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gr.Value != "new" || gr.Timestamp != 20 {
		t.Fatalf("got %q@%d, want new@20", gr.Value, gr.Timestamp)
	}
}

// TestHTTPValueRoundTrip ensures payloads with JSON/special characters survive
// the encode/decode path byte-for-byte.
func TestHTTPValueRoundTrip(t *testing.T) {
	h := newTestHandler()
	payload := putRequest{Value: `{"nested":"json with spaces & symbols €"}`, Timestamp: 7}
	body, _ := json.Marshal(payload)

	if rec := do(t, h, http.MethodPut, "/internal/put/doc", string(body)); rec.Code != http.StatusOK {
		t.Fatalf("put status = %d", rec.Code)
	}
	rec := do(t, h, http.MethodGet, "/internal/get/doc", "")
	var gr getResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &gr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gr.Value != payload.Value {
		t.Fatalf("value not preserved:\n got %q\nwant %q", gr.Value, payload.Value)
	}
}

// TestShedWiring pins down which routes sit behind the limiter: data
// endpoints shed with 503 when the node is saturated, /healthz never does.
func TestShedWiring(t *testing.T) {
	lim := shed.New(1, 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(store.New(), lim, logger).Routes()

	if !lim.Acquire(context.Background()) {
		t.Fatal("failed to occupy the only slot")
	}

	if rec := do(t, h, http.MethodPut, "/internal/put/k", `{"value":"v","timestamp":1}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated put status = %d, want 503", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/internal/get/k", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated get status = %d, want 503", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("healthz under saturation status = %d, want 200 (must bypass shedding)", rec.Code)
	}

	lim.Release()
	if rec := do(t, h, http.MethodPut, "/internal/put/k", `{"value":"v","timestamp":1}`); rec.Code != http.StatusOK {
		t.Fatalf("put after release status = %d, want 200", rec.Code)
	}
}
