package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestHistogramViews(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithView(HistogramViews()...),
	)
	t.Cleanup(func() {
		assert.NoError(t, provider.Shutdown(context.Background()))
	})

	meter := provider.Meter("test")
	recordFloat64 := func(name, unit string, opts ...metric.Float64HistogramOption) {
		opts = append(opts, metric.WithUnit(unit))
		histogram, err := meter.Float64Histogram(name, opts...)
		require.NoError(t, err)
		histogram.Record(context.Background(), 1)
	}
	recordInt64 := func(name, unit string) {
		histogram, err := meter.Int64Histogram(name, metric.WithUnit(unit))
		require.NoError(t, err)
		histogram.Record(context.Background(), 1)
	}

	recordFloat64("rpc.server.call.duration", "s", metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.1, 1))
	recordFloat64("http.server.request.duration", "s", metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.1, 1))
	recordFloat64("rpc.server.duration", "ms")
	recordFloat64("gc_run_duration_seconds", "")
	recordInt64("db.client.operation.duration", "s")
	recordInt64("payload.size", "By")

	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	require.Len(t, resourceMetrics.ScopeMetrics, 1)
	metrics := resourceMetrics.ScopeMetrics[0].Metrics
	require.Len(t, metrics, 6)

	for _, m := range metrics {
		switch m.Name {
		case "rpc.server.call.duration", "http.server.request.duration", "rpc.server.duration", "gc_run_duration_seconds":
			histogram, ok := m.Data.(metricdata.ExponentialHistogram[float64])
			require.True(t, ok, m.Name)
			require.Len(t, histogram.DataPoints, 1)
			assert.Equal(t, uint64(1), histogram.DataPoints[0].Count)
		case "db.client.operation.duration":
			histogram, ok := m.Data.(metricdata.ExponentialHistogram[int64])
			require.True(t, ok, m.Name)
			require.Len(t, histogram.DataPoints, 1)
			assert.Equal(t, uint64(1), histogram.DataPoints[0].Count)
		case "payload.size":
			histogram, ok := m.Data.(metricdata.Histogram[int64])
			require.True(t, ok, m.Name)
			require.Len(t, histogram.DataPoints, 1)
			assert.Equal(t, uint64(1), histogram.DataPoints[0].Count)
		default:
			assert.Failf(t, "unexpected metric", "unexpected metric %q", m.Name)
		}
	}
}
