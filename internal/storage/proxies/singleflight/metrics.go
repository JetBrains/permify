package singleflight

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/pkg/telemetry"
)

// Attribute sets of the singleflight counters, one per deduplicated operation.
// They are created once, because the readers are on the hot path.
var (
	headSnapshotAttrs                        = opAttrs("head_snapshot")
	queryRelationshipsAttrs                  = opAttrs("query_relationships")
	queryRelationshipsWithSubjectFilterAttrs = opAttrs("query_relationships_with_subject_filter")
	querySingleAttributeAttrs                = opAttrs("query_single_attribute")
	queryAttributesAttrs                     = opAttrs("query_attributes")
	headVersionAttrs                         = opAttrs("head_version")
)

func opAttrs(op string) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(attribute.String("op", op)))
}

// Option - Option type for the singleflight readers
type Option func(*options)

type options struct {
	meter metric.Meter
}

// WithMeter - Sets the meter used to create the singleflight metrics. The default is internal.Meter.
func WithMeter(meter metric.Meter) Option {
	return func(o *options) {
		o.meter = meter
	}
}

// metrics - Counters of the singleflight readers.
// Saved delegate calls are calls - executions.
type metrics struct {
	// calls is incremented before each singleflight call
	calls metric.Int64Counter
	// executions is incremented only when the delegate is called (once per deduplicated group)
	executions metric.Int64Counter
}

func newMetrics(opts ...Option) metrics {
	o := &options{meter: internal.Meter}
	for _, opt := range opts {
		opt(o)
	}

	return metrics{
		calls:      telemetry.NewCounter(o.meter, "singleflight_calls", "Number of storage calls that go through singleflight, by operation"),
		executions: telemetry.NewCounter(o.meter, "singleflight_executions", "Number of storage calls that singleflight passed to the delegate, by operation"),
	}
}
