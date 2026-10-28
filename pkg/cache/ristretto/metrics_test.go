package ristretto

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	metrics := map[string]metricdata.Aggregation{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			assert.Empty(t, m.Unit, "metric %s should not have a unit", m.Name)
			metrics[m.Name] = m.Data
		}
	}
	return metrics
}

func sumValue(t *testing.T, data metricdata.Aggregation, cache string) int64 {
	t.Helper()
	sum, ok := data.(metricdata.Sum[int64])
	require.True(t, ok, "%T is not an int64 sum", data)
	assert.True(t, sum.IsMonotonic)
	set := attribute.NewSet(attribute.String("cache", cache))
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&set) {
			return dp.Value
		}
	}
	return 0
}

func gaugeValue(t *testing.T, data metricdata.Aggregation, cache string) int64 {
	t.Helper()
	gauge, ok := data.(metricdata.Gauge[int64])
	require.True(t, ok, "%T is not an int64 gauge", data)
	set := attribute.NewSet(attribute.String("cache", cache))
	for _, dp := range gauge.DataPoints {
		if dp.Attributes.Equals(&set) {
			return dp.Value
		}
	}
	return 0
}

func TestRistretto_WithMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	schemaCache, err := New(WithMetrics(meter, "schema"))
	require.NoError(t, err)
	permissionCache, err := New(WithMetrics(meter, "permission"))
	require.NoError(t, err)
	defer permissionCache.Close()

	assert.True(t, schemaCache.Set("foo", "bar", 1))
	schemaCache.Wait()

	_, found := schemaCache.Get("foo")
	assert.True(t, found)

	assert.True(t, permissionCache.Set("baz", "qux", 2))
	permissionCache.Wait()

	metrics := collect(t, reader)
	assert.NotContains(t, metrics, "cache_hits")
	assert.NotContains(t, metrics, "cache_misses")
	assert.Equal(t, int64(1), sumValue(t, metrics["cache_keys_added"], "schema"))
	assert.Equal(t, int64(0), sumValue(t, metrics["cache_keys_evicted"], "schema"))
	assert.Equal(t, int64(0), sumValue(t, metrics["cache_cost_evicted"], "schema"))
	assert.Equal(t, int64(0), sumValue(t, metrics["cache_sets_dropped"], "schema"))
	assert.Equal(t, int64(0), sumValue(t, metrics["cache_sets_rejected"], "schema"))
	assert.Equal(t, int64(1), sumValue(t, metrics["cache_keys_added"], "permission"))
	assert.Equal(t, int64(_defaultNumCounters), gaugeValue(t, metrics["cache_num_counters"], "schema"))
	assert.Equal(t, int64(64), gaugeValue(t, metrics["cache_buffer_items"], "schema"))
	assert.Positive(t, gaugeValue(t, metrics["cache_max_cost"], "schema"))
	initialCost := gaugeValue(t, metrics["cache_cost_used"], "schema")
	assert.Positive(t, initialCost)

	assert.True(t, schemaCache.Set("foo", "updated", 3))
	schemaCache.Wait()
	metrics = collect(t, reader)
	assert.Equal(t, initialCost+2, gaugeValue(t, metrics["cache_cost_used"], "schema"))
	assert.Equal(t, int64(0), sumValue(t, metrics["cache_cost_evicted"], "schema"))

	// After Close, the closed cache is no longer observed.
	schemaCache.Close()
	metrics = collect(t, reader)
	assert.Equal(t, int64(0), sumValue(t, metrics["cache_keys_added"], "schema"))
	assert.Equal(t, int64(1), sumValue(t, metrics["cache_keys_added"], "permission"))
}

func TestRistretto_CostUsedOnEviction(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	cache, err := New(MaxCost("100000"), NumberOfCounters(1024), WithMetrics(meter, "eviction"))
	require.NoError(t, err)
	defer cache.Close()

	assert.True(t, cache.Set("old", "value", 60000))
	cache.Wait()
	initial := collect(t, reader)
	initialCost := gaugeValue(t, initial["cache_cost_used"], "eviction")
	assert.Positive(t, initialCost)

	for i := 0; i < 100; i++ {
		_, found := cache.Get("old")
		assert.True(t, found)
	}
	for i := 0; i < 200; i++ {
		_, found := cache.Get("new")
		assert.False(t, found)
	}
	assert.True(t, cache.Set("new", "value", 60000))
	cache.Wait()

	metrics := collect(t, reader)
	assert.Equal(t, int64(1), sumValue(t, metrics["cache_keys_evicted"], "eviction"))
	assert.Equal(t, initialCost, sumValue(t, metrics["cache_cost_evicted"], "eviction"))
	assert.Equal(t, initialCost, gaugeValue(t, metrics["cache_cost_used"], "eviction"))
}

func TestRistretto_WithoutMetrics(t *testing.T) {
	cache, err := New()
	require.NoError(t, err)
	defer cache.Close()

	assert.Nil(t, cache.registration)
	assert.Nil(t, cache.c.Metrics)
}
