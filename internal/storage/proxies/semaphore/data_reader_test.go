package semaphore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sync/semaphore"

	"github.com/Permify/permify/internal/invoke"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/database"
	base "github.com/Permify/permify/pkg/pb/base/v1"
)

func TestSemaphoreDataReaderConcurrencyWaitsMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	dr := NewDataReader(storage.NewNoopRelationshipReader(), WithMeter(meter))

	sem := semaphore.NewWeighted(1)
	ctx := invoke.WithConcurrencySemaphore(context.Background(), sem)

	// Uncontended call: TryAcquire succeeds immediately, 0 waits.
	_, err := dr.QueryRelationships(ctx, "t1", &base.TupleFilter{}, "snap", database.NewCursorPagination())
	require.NoError(t, err)

	// Contended call: hold the single semaphore slot so TryAcquire fails and Acquire blocks until release.
	require.True(t, sem.TryAcquire(1))
	go func() {
		time.Sleep(10 * time.Millisecond)
		sem.Release(1)
	}()

	_, err = dr.QueryRelationships(ctx, "t1", &base.TupleFilter{}, "snap", database.NewCursorPagination())
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var waits, attempts int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "concurrency_limit_waits" && m.Name != "concurrency_limit_acquire_attempts" {
				continue
			}
			assert.Empty(t, m.Unit)
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.True(t, sum.IsMonotonic)
			for _, dp := range sum.DataPoints {
				scope, _ := dp.Attributes.Value(attribute.Key("scope"))
				if scope.AsString() == "storage" {
					if m.Name == "concurrency_limit_waits" {
						waits += dp.Value
					} else {
						attempts += dp.Value
					}
				}
			}
		}
	}
	assert.Equal(t, int64(1), waits)
	assert.Equal(t, int64(2), attempts)
}

func TestSemaphoreDataReaderPreservesPreCancelledContext(t *testing.T) {
	sem := semaphore.NewWeighted(1)
	ctx, cancel := context.WithCancel(invoke.WithConcurrencySemaphore(context.Background(), sem))
	cancel()

	dr := NewDataReader(storage.NewNoopRelationshipReader())
	_, err := dr.acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, sem.TryAcquire(1), "cancelled call must not consume the available semaphore slot")
}
