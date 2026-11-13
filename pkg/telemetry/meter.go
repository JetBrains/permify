package telemetry

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"

	"go.opentelemetry.io/contrib/instrumentation/host"
	orn "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/attribute"
	omt "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/Permify/permify/internal"
)

var durationHistogramAggregation = metric.AggregationBase2ExponentialHistogram{
	MaxSize:  160,
	MaxScale: 20,
}

// NewMeter - Creates new meter
func NewMeter(exporter metric.Exporter, interval time.Duration) func(context.Context) error {
	hostName, err := os.Hostname()
	if err != nil {
		return func(context.Context) error { return nil }
	}

	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(
			exporter,
			metric.WithInterval(interval),
		)),
		metric.WithView(HistogramViews()...),
		metric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("permify"),
			attribute.String("id", internal.Identifier),
			attribute.String("project.id", internal.Identifier),
			attribute.String("version", internal.Version),
			attribute.String("host_name", hostName),
			attribute.String("os", runtime.GOOS),
			attribute.String("arch", runtime.GOARCH),
		)),
	)

	if err = orn.Start(
		orn.WithMinimumReadMemStatsInterval(time.Second),
		orn.WithMeterProvider(mp),
	); err != nil {
		return func(context.Context) error { return nil }
	}

	if err = host.Start(host.WithMeterProvider(mp)); err != nil {
		return func(context.Context) error { return nil }
	}

	otel.SetMeterProvider(mp)

	return mp.Shutdown
}

// HistogramViews configures duration histograms to use exponential buckets.
func HistogramViews() []metric.View {
	return []metric.View{
		metric.NewView(
			metric.Instrument{Name: "gc_run_duration_seconds", Kind: metric.InstrumentKindHistogram},
			metric.Stream{Aggregation: durationHistogramAggregation},
		),
		metric.NewView(
			metric.Instrument{Kind: metric.InstrumentKindHistogram, Unit: "s"},
			metric.Stream{Aggregation: durationHistogramAggregation},
		),
		metric.NewView(
			metric.Instrument{Kind: metric.InstrumentKindHistogram, Unit: "ms"},
			metric.Stream{Aggregation: durationHistogramAggregation},
		),
	}
}

// NewNoopMeter - Creates new noop meter
func NewNoopMeter() omt.Meter {
	mp := noop.MeterProvider{}
	return mp.Meter("permify")
}

func NewCounter(meter omt.Meter, name, description string) omt.Int64Counter {
	counter, err := meter.Int64Counter(
		name,
		omt.WithDescription(description),
	)
	if err != nil {
		slog.Error("failed to create request counter", slog.String("error", err.Error()))
		panic(err)
	}

	return counter
}

func NewHistogram(meter omt.Meter, name, unit, description string) omt.Int64Histogram {
	histogram, err := meter.Int64Histogram(
		name,
		omt.WithUnit(unit),
		omt.WithDescription(description),
	)
	if err != nil {
		slog.Error("failed to create request histogram", slog.String("error", err.Error()))
		panic(err)
	}

	return histogram
}
