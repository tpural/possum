// Package gate scales an HTTP backend to zero when idle and wakes it on the first
// real request, answering model listings from configuration so they never wake it.
package gate

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
)

// Scaler reads and changes the backend's replica count.
type Scaler interface {
	Replicas(ctx context.Context) (desired, ready int, err error)
	Scale(ctx context.Context, replicas int) error
}

// Options configures a Gate.
type Options struct {
	Upstream     *url.URL
	Scaler       Scaler
	Models       []Model
	Idle         time.Duration
	WakeTimeout  time.Duration
	PollInterval time.Duration
	Metrics      *Metrics
	Log          logr.Logger
}

// Gate is an http.Handler that proxies to the upstream, waking it first when needed.
type Gate struct {
	opts   Options
	proxy  *httputil.ReverseProxy
	models []byte

	inflight atomic.Int64
	lastSeen atomic.Int64

	wakeMu sync.Mutex
}

// New returns a Gate.
func New(opts Options) (*Gate, error) {
	if opts.Upstream == nil || opts.Scaler == nil {
		return nil, errors.New("gate: upstream and scaler are required")
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 2 * time.Second
	}
	if opts.Metrics == nil {
		opts.Metrics = NewMetrics(nil)
	}
	models, err := json.Marshal(modelList(opts.Models))
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(opts.Upstream)
	proxy.FlushInterval = -1
	g := &Gate{opts: opts, proxy: proxy, models: models}
	g.touch()
	return g, nil
}

func (g *Gate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && (r.URL.Path == "/v1/models" || r.URL.Path == "/models") {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(g.models)
		return
	}

	g.inflight.Add(1)
	g.opts.Metrics.inflight.Inc()
	defer func() {
		g.touch()
		g.inflight.Add(-1)
		g.opts.Metrics.inflight.Dec()
	}()
	g.touch()

	if err := g.wake(r.Context()); err != nil {
		g.opts.Log.Error(err, "wake backend")
		http.Error(w, "backend is waking up, try again shortly: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	g.proxy.ServeHTTP(w, r)
}

// Run scales the backend to zero once it has been idle for the configured period.
func (g *Gate) Run(ctx context.Context) {
	t := time.NewTicker(min(g.opts.Idle/4, time.Minute))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.sleepIfIdle(ctx)
		}
	}
}

func (g *Gate) sleepIfIdle(ctx context.Context) {
	if g.inflight.Load() > 0 || time.Since(g.last()) < g.opts.Idle {
		return
	}
	g.wakeMu.Lock()
	defer g.wakeMu.Unlock()
	desired, _, err := g.opts.Scaler.Replicas(ctx)
	if err != nil || desired == 0 || g.inflight.Load() > 0 {
		return
	}
	if err := g.opts.Scaler.Scale(ctx, 0); err != nil {
		g.opts.Log.Error(err, "scale to zero")
		return
	}
	g.opts.Metrics.sleeps.Inc()
	g.opts.Log.Info("backend idle, scaled to zero", "idle", time.Since(g.last()).Round(time.Second))
}

func (g *Gate) wake(ctx context.Context) error {
	g.wakeMu.Lock()
	defer g.wakeMu.Unlock()

	desired, ready, err := g.opts.Scaler.Replicas(ctx)
	if err != nil {
		return err
	}
	if ready > 0 {
		return nil
	}
	start := time.Now()
	if desired == 0 {
		g.opts.Log.Info("waking backend")
		g.opts.Metrics.wakes.Inc()
		if err := g.opts.Scaler.Scale(ctx, 1); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, g.opts.WakeTimeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return errors.New("backend did not become ready in time")
		case <-time.After(g.opts.PollInterval):
		}
		if _, ready, err := g.opts.Scaler.Replicas(ctx); err == nil && ready > 0 && g.reachable(ctx) == nil {
			g.opts.Metrics.wakeSeconds.Observe(time.Since(start).Seconds())
			g.opts.Log.Info("backend ready", "took", time.Since(start).Round(time.Second))
			return nil
		}
	}
}

func (g *Gate) reachable(ctx context.Context) error {
	u := g.opts.Upstream
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	d := net.Dialer{Timeout: 2 * time.Second}
	c, err := d.DialContext(ctx, "tcp", net.JoinHostPort(u.Hostname(), port))
	if err != nil {
		return err
	}
	return c.Close()
}

func (g *Gate) touch()          { g.lastSeen.Store(time.Now().UnixNano()) }
func (g *Gate) last() time.Time { return time.Unix(0, g.lastSeen.Load()) }
