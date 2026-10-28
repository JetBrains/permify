package gc

import (
	"time"

	"go.opentelemetry.io/otel/metric"
)

// Option represents a function that configures a GC (Garbage Collector) instance.
type Option func(gc *GC)

// WithMeter sets the meter used to create the GC metrics. The default is internal.Meter.
func WithMeter(meter metric.Meter) Option {
	return func(gc *GC) {
		gc.meter = meter
	}
}

// Interval is an option that sets the interval duration for the GC.
func Interval(n time.Duration) Option {
	return func(gc *GC) {
		gc.interval = n
	}
}

// Window is an option that sets the window duration for the GC.
func Window(n time.Duration) Option {
	return func(gc *GC) {
		gc.window = n
	}
}

// Timeout is an option that sets the timeout duration for the GC.
func Timeout(n time.Duration) Option {
	return func(gc *GC) {
		gc.timeout = n
	}
}
