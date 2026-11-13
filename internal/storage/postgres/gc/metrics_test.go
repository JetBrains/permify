package gc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Permify/permify/pkg/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestGCPartialTenantFailureMarksRunAsError(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	collector := NewGC(nil, WithMeter(meter))

	var visited []string
	err := collector.runWithMetrics(context.Background(), func() error {
		return collector.runTenants(context.Background(), []string{"t1", "t2", "t3"}, time.Now(), func(_ context.Context, tenantID string, _ time.Time) error {
			visited = append(visited, tenantID)
			if tenantID == "t2" {
				return errors.New("tenant cleanup failed")
			}
			return nil
		})
	})
	require.Error(t, err)
	assert.Equal(t, []string{"t1", "t2", "t3"}, visited)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	runsByResult := map[string]int64{}
	tenantRunsByResult := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			sum, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				result, _ := dp.Attributes.Value(attribute.Key("result"))
				switch metric.Name {
				case "gc_runs":
					runsByResult[result.AsString()] = dp.Value
				case "gc_tenant_runs":
					tenantRunsByResult[result.AsString()] = dp.Value
				}
			}
		}
	}
	assert.Equal(t, map[string]int64{"error": 1}, runsByResult)
	assert.Equal(t, map[string]int64{"ok": 2, "error": 1}, tenantRunsByResult)
}

func TestGCRunMetrics(t *testing.T) {
	t.Run("successful run records duration and last success", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		meter := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(reader),
			sdkmetric.WithView(telemetry.HistogramViews()...),
		).Meter("test")
		collector := NewGC(nil, WithMeter(meter))

		require.NoError(t, collector.runWithMetrics(context.Background(), func() error { return nil }))

		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		var sawRun bool
		var sawDuration bool
		var sawLastSuccess bool
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				switch m.Name {
				case "gc_runs":
					sum, ok := m.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					require.Len(t, sum.DataPoints, 1)
					assert.Equal(t, int64(1), sum.DataPoints[0].Value)
					sawRun = true
				case "gc_run_duration_seconds":
					histogram, ok := m.Data.(metricdata.ExponentialHistogram[float64])
					require.True(t, ok)
					require.Len(t, histogram.DataPoints, 1)
					assert.Equal(t, uint64(1), histogram.DataPoints[0].Count)
					result, _ := histogram.DataPoints[0].Attributes.Value(attribute.Key("result"))
					assert.Equal(t, "ok", result.AsString())
					sawDuration = true
				case "gc_last_success_timestamp_seconds":
					gauge, ok := m.Data.(metricdata.Gauge[int64])
					require.True(t, ok)
					require.Len(t, gauge.DataPoints, 1)
					assert.Positive(t, gauge.DataPoints[0].Value)
					sawLastSuccess = true
				}
			}
		}
		assert.True(t, sawRun)
		assert.True(t, sawDuration)
		assert.True(t, sawLastSuccess)
	})

	t.Run("failed run records error but no last success", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		meter := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(reader),
			sdkmetric.WithView(telemetry.HistogramViews()...),
		).Meter("test")
		collector := NewGC(nil, WithMeter(meter))

		require.Error(t, collector.runWithMetrics(context.Background(), func() error {
			return errors.New("gc failed")
		}))

		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		var sawError bool
		var sawDuration bool
		var sawLastSuccess bool
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				switch m.Name {
				case "gc_runs":
					sum, ok := m.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					for _, point := range sum.DataPoints {
						result, _ := point.Attributes.Value(attribute.Key("result"))
						if result.AsString() == "error" && point.Value == 1 {
							sawError = true
						}
					}
				case "gc_run_duration_seconds":
					histogram, ok := m.Data.(metricdata.ExponentialHistogram[float64])
					require.True(t, ok)
					require.Len(t, histogram.DataPoints, 1)
					assert.Equal(t, uint64(1), histogram.DataPoints[0].Count)
					result, _ := histogram.DataPoints[0].Attributes.Value(attribute.Key("result"))
					assert.Equal(t, "error", result.AsString())
					sawDuration = true
				case "gc_last_success_timestamp_seconds":
					sawLastSuccess = true
				}
			}
		}
		assert.True(t, sawError)
		assert.True(t, sawDuration)
		assert.False(t, sawLastSuccess)
	})
}

func TestGCRecordsDeletedRowsByTable(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	collector := NewGC(nil, WithMeter(meter))
	collector.recordDeletedRows(context.Background(), "relation_tuples", 7)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "gc_deleted_rows" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			table, ok := sum.DataPoints[0].Attributes.Value(attribute.Key("table"))
			require.True(t, ok)
			assert.Equal(t, "relation_tuples", table.AsString())
			assert.Equal(t, int64(7), sum.DataPoints[0].Value)
			return
		}
	}
	t.Fatal("gc_deleted_rows metric not found")
}
