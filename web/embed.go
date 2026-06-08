// Package web embeds the built dashboard (web/dist) and serves it with an SPA
// fallback. The Vite build output is committed so `go build` needs no Node step.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var dist embed.FS

// assets returns the embedded files rooted at dist/.
func assets() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic("web: embed dist subtree: " + err.Error())
	}
	return sub
}

// Handler serves the dashboard. Real asset paths are served directly; any other
// path falls back to index.html so client-side routing works (single-page app).
func Handler() http.Handler {
	root := assets()
	fileServer := http.FileServerFS(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := path.Clean(strings.TrimPrefix(r.URL.Path, "/"))
		if clean == "." || clean == "" {
			clean = "index.html"
		}
		if _, err := fs.Stat(root, clean); err != nil {
			// Not a real asset — serve the SPA shell.
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})
}
