// Package webui serves the embedded single-page web console for the Controller.
//
// The console is a dependency-free vanilla JS SPA (no build step, no CDN) that
// talks to the webapi package: it renders the live dashboard from the
// /api/events Server-Sent Events stream and drives migration start/pause/
// resume/stop/rescan through the REST endpoints.
//
// Handler takes the API handler and serves it at /api/* while everything else
// is resolved from the embedded assets; unknown paths fall back to index.html
// so client-side navigation works without server-side rewrites.
package webui

import (
	"embed"
	"io/fs"
	"mime"
	"net/http"
	"path/filepath"
	"strings"
)

//go:embed assets
var assets embed.FS

// Handler returns an HTTP handler that serves the web console. Requests under
// /api/ are delegated to the given API handler; all other paths are served from
// the embedded assets with an index.html fallback.
func Handler(api http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			api.ServeHTTP(w, r)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/")
		if name == "" {
			name = "index.html"
		}
		data, err := fs.ReadFile(assets, filepath.ToSlash(filepath.Join("assets", name)))
		if err != nil {
			// Fault-tolerant: fall back to the SPA shell so any path (or
			// reload on a client-side route) still renders.
			data, err = fs.ReadFile(assets, "assets/index.html")
			if err != nil {
				http.Error(w, "console asset missing", http.StatusNotFound)
				return
			}
			name = "index.html"
		}
		ct := mime.TypeByExtension(filepath.Ext(name))
		if ct == "" {
			ct = "text/plain; charset=utf-8"
		}
		// no-cache so updates to the UI surface immediately on redeploy
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Cache-Control", "no-cache")
		w.Write(data)
	})
}
