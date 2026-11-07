package provider

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the Prometheus collectors exported by the provider.
type Metrics struct {
	nodes        *prometheus.GaugeVec
	powerActions *prometheus.CounterVec
	driverErrors *prometheus.CounterVec
}

// NewMetrics creates the collectors and registers them with reg when it is not nil.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		nodes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "possum_nodes",
			Help: "Managed nodes by group and phase.",
		}, []string{"group", "phase"}),
		powerActions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "possum_power_actions_total",
			Help: "Power changes performed, by group and resulting state.",
		}, []string{"group", "state"}),
		driverErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "possum_driver_errors_total",
			Help: "Errors returned by power drivers.",
		}, []string{"driver"}),
	}
	if reg != nil {
		reg.MustRegister(m.nodes, m.powerActions, m.driverErrors)
	}
	return m
}

func (m *Metrics) powerAction(group, state string) {
	m.powerActions.WithLabelValues(group, state).Inc()
}

func (m *Metrics) driverError(driver string) { m.driverErrors.WithLabelValues(driver).Inc() }
