package gc

import (
	"context"
	"errors"
	"testing"
	"time"

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
	err := collector.runTenants(context.Background(), []string{"t1", "t2", "t3"}, time.Now(), func(_ context.Context, tenantID string, _ time.Time) error {
		visited = append(visited, tenantID)
		if tenantID == "t2" {
			return errors.New("tenant cleanup failed")
		}
		return nil
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
