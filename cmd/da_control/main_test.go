package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"datants/internal/control"
	"datants/internal/history"
	"datants/internal/taskstore"
	"datants/internal/webapi"
)

// testController builds a fresh controller over temp state for handler tests.
func testController(t *testing.T) *control.Controller {
	t.Helper()
	state := t.TempDir()
	store, err := taskstore.New(filepath.Join(state, "store"))
	if err != nil {
		t.Fatal(err)
	}
	hist, err := history.New(filepath.Join(state, "history"))
	if err != nil {
		t.Fatal(err)
	}
	ctl := control.NewController(store, control.Config{
		History:          hist,
		ReclaimInterval:  10 * time.Millisecond,
		ClaimTimeout:     200 * time.Millisecond,
		ProgressInterval: 20 * time.Millisecond,
	})
	t.Cleanup(func() {
		ctl.Stop()
		hist.Close()
		store.Done()
	})
	ctl.Start()
	return ctl
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestBuildWebHandlerNoAuth(t *testing.T) {
	h := buildWebHandler(testController(t), "", "", nil, webapi.SpawnConfig{})

	// SPA shell served
	rr := get(h, "/")
	if rr.Code != http.StatusOK {
		t.Fatalf("GET / code = %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("content-type = %q", rr.Header().Get("Content-Type"))
	}

	// asset served
	rr = get(h, "/app.js")
	if rr.Code != http.StatusOK || rr.Body.Len() == 0 {
		t.Fatalf("GET /app.js code=%d len=%d", rr.Code, rr.Body.Len())
	}

	// api routed
	rr = get(h, "/api/status")
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /api/status code = %d", rr.Code)
	}
	var out struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v (%s)", err, rr.Body.String())
	}
	if out.State != control.RunRunning.String() {
		t.Fatalf("state = %q, want running", out.State)
	}
}

func TestBuildWebHandlerBasicAuth(t *testing.T) {
	h := buildWebHandler(testController(t), "admin", "s3cret", nil, webapi.SpawnConfig{})

	// no credentials -> 401 with WWW-Authenticate
	rr := get(h, "/")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no creds code = %d", rr.Code)
	}
	if rr.Header().Get("WWW-Authenticate") == "" {
		t.Fatal("missing WWW-Authenticate header")
	}

	// wrong credentials -> 401
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "wrong")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong creds code = %d", rr.Code)
	}

	// correct credentials -> 200 on both SPA and API
	req = httptest.NewRequest(http.MethodGet, "/api/status", nil)
	req.SetBasicAuth("admin", "s3cret")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("authed api code = %d", rr.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "s3cret")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("authed page code = %d", rr.Code)
	}
}

func TestBasicAuthRequiredPass(t *testing.T) {
	h := basicAuth("u", "p", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("u", "")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("empty pass code = %d", rr.Code)
	}
}

func TestLocalConnectAddr(t *testing.T) {
	cases := []struct{ in, want string }{
		{"0.0.0.0:50051", "127.0.0.1:50051"},
		{":50051", "127.0.0.1:50051"},
		{"[::]:50051", "127.0.0.1:50051"},
		{"127.0.0.1:50051", "127.0.0.1:50051"},
		{"192.168.1.5:50051", "192.168.1.5:50051"},
		{"localhost:50051", "localhost:50051"},
		{"garbage", "garbage"},
	}
	for _, c := range cases {
		if got := localConnectAddr(c.in); got != c.want {
			t.Errorf("localConnectAddr(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
