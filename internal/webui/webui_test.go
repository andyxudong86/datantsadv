package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// stubAPI is a marker handler to prove /api/* routing works.
type stubAPI struct{}

func (stubAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Routed", "api")
	w.Write([]byte("api-ok"))
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestServesIndex(t *testing.T) {
	h := Handler(stubAPI{})
	for _, path := range []string{"/", "/index.html"} {
		rr := get(h, path)
		if rr.Code != http.StatusOK {
			t.Fatalf("%q code = %d", path, rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "<title>DataNTs") {
			t.Fatalf("%q response lacks page title", path)
		}
		ct := rr.Header().Get("Content-Type")
		if !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("%q content-type = %q", path, ct)
		}
	}
}

func TestServesAssets(t *testing.T) {
	h := Handler(stubAPI{})
	cases := []struct{ path, mime string }{
		{"/app.js", "text/javascript"},
		{"/style.css", "text/css"},
	}
	for _, c := range cases {
		rr := get(h, c.path)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s code = %d", c.path, rr.Code)
		}
		if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, c.mime) {
			t.Fatalf("%s content-type = %q, want prefix %q", c.path, ct, c.mime)
		}
		if rr.Body.Len() == 0 {
			t.Fatalf("%s empty body", c.path)
		}
	}
}

func TestRoutesAPI(t *testing.T) {
	h := Handler(stubAPI{})
	rr := get(h, "/api/status")
	if rr.Code != http.StatusOK {
		t.Fatalf("code = %d", rr.Code)
	}
	if hdr := rr.Header().Get("X-Routed"); hdr != "api" {
		t.Fatalf("X-Routed = %q, want api", hdr)
	}
	if rr.Body.String() != "api-ok" {
		t.Fatalf("body = %q, want api-ok", rr.Body.String())
	}
}

func TestSPAFallback(t *testing.T) {
	// a client-side route or unknown path must fall back to the SPA shell
	h := Handler(stubAPI{})
	for _, path := range []string{"/some/route", "/nonexistent.js", "/favicon.ico"} {
		rr := get(h, path)
		if rr.Code != http.StatusOK {
			t.Fatalf("%q code = %d", path, rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "<title>DataNTs") {
			t.Fatalf("%q did not fall back to index", path)
		}
	}
}

func TestPathTraversalIsSafe(t *testing.T) {
	h := Handler(stubAPI{})
	rr := get(h, "/../../etc/passwd")
	if rr.Code == http.StatusInternalServerError && rr.Body.Len() == 0 {
		// still fine: must not leak anything outside the embedded FS
		return
	}
	if strings.Contains(rr.Body.String(), "root:") {
		t.Fatalf("path traversal leaked file contents: %q", rr.Body.String())
	}
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "<title>DataNTs") {
		t.Fatalf("traversal path not handled by fallback: code=%d", rr.Code)
	}
}

func TestConsoleStatusDiagnostics(t *testing.T) {
	h := Handler(stubAPI{})
	rr := get(h, "/index.html")
	html := rr.Body.String()
	for _, want := range []string{`id="run-updated"`, `id="state-err"`} {
		if !strings.Contains(html, want) {
			t.Fatalf("index.html missing %s", want)
		}
	}
	rr = get(h, "/app.js")
	js := rr.Body.String()
	for _, want := range []string{`$("#run-updated")`, `$("#state-err")`} {
		if !strings.Contains(js, want) {
			t.Fatalf("app.js missing %s", want)
		}
	}
	if !strings.Contains(js, "catch (err)") {
		t.Fatal("app.js applyStatus lacks the error banner")
	}
	if strings.Contains(js, `$("#run-badge").outerHTML`) {
		t.Fatal("app.js must not replace #run-badge via outerHTML: badge() drops the id and breaks the button updates")
	}
}
