// Package web serves the demo console: a page that tails a run's event stream, narrates it in the
// language of the workload rather than the language of the harness, and lets a presenter kill the
// control plane on purpose.
//
// It exists so that replay-on-resume is observable rather than only assertable in a test. The raw
// trajectory viewer is still served at /raw.html -- it renders the same stream without the demo
// framing, which is what you want when something is wrong and the narration is in the way.
package web

import (
	"embed"
	"net/http"
)

//go:embed console.html raw.html architecture.html
var files embed.FS

func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, files, "console.html")
	})
	mux.Handle("GET /", http.FileServerFS(files))
	return mux
}
