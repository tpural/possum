package gate

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the Prometheus collectors exported by the gate.
type Metrics struct {
	wakes       prometheus.Counter
	sleeps      prometheus.Counter
	wakeSeconds prometheus.Histogram
	inflight    prometheus.Gauge
}

// NewMetrics creates the collectors and registers them with reg when it is not nil.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		wakes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "possum_gate_wakes_total",
			Help: "Times the backend was scaled up from zero.",
		}),
		sleeps: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "possum_gate_sleeps_total",
			Help: "Times the backend was scaled to zero after being idle.",
		}),
		wakeSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "possum_gate_wake_seconds",
			Help:    "Time from the first request until the backend was ready.",
			Buckets: []float64{5, 15, 30, 60, 120, 180, 300, 600, 900},
		}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "possum_gate_inflight_requests",
			Help: "Requests currently proxied or waiting for the backend.",
		}),
	}
	if reg != nil {
		reg.MustRegister(m.wakes, m.sleeps, m.wakeSeconds, m.inflight)
	}
	return m
}
