package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRateLimiterMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	limiter := NewRateLimiter(1, WithLimiterMeter(meter))

	require.NoError(t, limiter.Limit(context.Background()))
	require.Error(t, limiter.Limit(context.Background()))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	results := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "rate_limit_requests" {
				continue
			}
			assert.Empty(t, m.Unit)
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.True(t, sum.IsMonotonic)
			for _, dp := range sum.DataPoints {
				res, _ := dp.Attributes.Value(attribute.Key("result"))
				results[res.AsString()] = dp.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{
		"allowed":  1,
		"exceeded": 1,
	}, results)
}
