package ristretto

import (
	"go.opentelemetry.io/otel/metric"
)

// Option - Option types for cache
type Option func(ristretto *Ristretto)

// MaxCost - Defines maximum cost for ristretto
func MaxCost(n string) Option {
	return func(c *Ristretto) {
		c.maxCost = n
	}
}

// NumberOfCounters - Number of keys to track frequency.
func NumberOfCounters(n int64) Option {
	return func(c *Ristretto) {
		c.numCounters = n
	}
}

// WithMetrics - Enables ristretto internal metrics and exports them with the given meter.
// The name is recorded in the "cache" attribute, for example "schema" or "permission".
// Ristretto metrics add some overhead (atomic counters on each Get and Set).
func WithMetrics(meter metric.Meter, name string) Option {
	return func(c *Ristretto) {
		c.meter = meter
		c.name = name
	}
}
