// Package status serves the read-only status page, health checks and metrics.
package status

import (
	"embed"
	"encoding/json"
	"html/template"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/tpural/possum/internal/provider"
)

//go:embed index.html
var assets embed.FS

var page = template.Must(template.New("index.html").Funcs(template.FuncMap{
	"ago": func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return time.Since(t).Round(time.Second).String()
	},
}).ParseFS(assets, "index.html"))

// Report is the JSON document served at /api/status.
type Report struct {
	DryRun  bool              `json:"dryRun"`
	Groups  []Group           `json:"groups"`
	Members []provider.Member `json:"members"`
}

// Group summarises a node group.
type Group struct {
	Name    string `json:"name"`
	MinSize int    `json:"minSize"`
	MaxSize int    `json:"maxSize"`
	Target  int    `json:"targetSize"`
}

// Handler returns the HTTP handler for the status server.
func Handler(f *provider.Fleet, gatherer prometheus.Gatherer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !f.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(report(f))
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = page.Execute(w, report(f))
	})
	return mux
}

func report(f *provider.Fleet) Report {
	r := Report{DryRun: f.DryRun(), Members: f.Members()}
	for _, g := range f.Groups() {
		size, _ := f.TargetSize(g.Name)
		r.Groups = append(r.Groups, Group{Name: g.Name, MinSize: g.MinSize, MaxSize: g.MaxSize, Target: size})
	}
	return r
}
