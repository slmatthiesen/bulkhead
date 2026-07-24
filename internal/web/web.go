// Package web serves the trajectory viewer: a single page that tails a run's event stream and
// draws it. It exists so that the replay-on-resume behaviour is observable rather than only
// assertable in a test.
package web

import (
	"embed"
	"net/http"
)

//go:embed index.html
var files embed.FS

func Handler() http.Handler { return http.FileServerFS(files) }
