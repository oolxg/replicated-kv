package coordinator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oolxg/replicated-kv/internal/ring"
	"github.com/oolxg/replicated-kv/internal/storage"
	"github.com/oolxg/replicated-kv/internal/store"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startNodes spins up n real storage nodes and returns their addresses
// (host:port) plus a cleanup func. Using the real storage handler makes these
// true router->storage integration tests.
func startNodes(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s := httptest.NewServer(storage.NewHandler(store.New(), discardLog()).Routes())
		t.Cleanup(s.Close)
		addrs = append(addrs, strings.TrimPrefix(s.URL, "http://"))
	}
	return addrs
}

func newCoord(t *testing.T, addrs []string) *Coordinator {
	t.Helper()
	return New(ring.New(addrs), discardLog())
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

func TestCoordinatorStableRouting(t *testing.T) {
	c := newCoord(t, startNodes(t, 5))
	ctx := context.Background()

	c.Put(ctx, "k", "v1")
	c.Put(ctx, "k", "v2") // overwrite: newer timestamp wins, same node
	v, found, _ := c.Get(ctx, "k")
	if !found || v.Value != "v2" {
		t.Fatalf("got %q found=%v, want v2", v.Value, found)
	}
}

func TestCoordinatorNodeDown(t *testing.T) {
	// Address with nothing listening -> forward fails.
	c := New(ring.New([]string{"127.0.0.1:1"}), discardLog())
	if err := c.Put(context.Background(), "k", "v"); err == nil {
		t.Fatal("expected error putting to a down node")
	}
	if _, _, err := c.Get(context.Background(), "k"); err == nil {
		t.Fatal("expected error getting from a down node")
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
