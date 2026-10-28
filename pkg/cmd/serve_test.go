package cmd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Permify/permify/internal/config"
	PQDatabase "github.com/Permify/permify/pkg/database/postgres"
)

func TestCircuitBreakerStateChangeRecorder(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	record := circuitBreakerStateChangeRecorder(meter)
	breaker := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:    "storage",
		Timeout: 10 * time.Millisecond,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.TotalFailures > 0
		},
		OnStateChange: record,
	})
	_, err := breaker.Execute(func() (any, error) {
		return nil, errors.New("storage failure")
	})
	require.Error(t, err)
	assert.Equal(t, gobreaker.StateOpen, breaker.State())

	// First half-open probe fails, transitioning open -> half-open -> open.
	assert.Eventually(t, func() bool {
		_, err := breaker.Execute(func() (any, error) {
			return nil, errors.New("probe failure")
		})
		return err != nil && !errors.Is(err, gobreaker.ErrOpenState) && breaker.State() == gobreaker.StateOpen
	}, time.Second, time.Millisecond)

	// Next half-open probe succeeds, transitioning open -> half-open -> closed.
	assert.Eventually(t, func() bool {
		_, err := breaker.Execute(func() (any, error) { return nil, nil })
		return err == nil && breaker.State() == gobreaker.StateClosed
	}, time.Second, time.Millisecond)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)

	m := rm.ScopeMetrics[0].Metrics[0]
	assert.Equal(t, "circuit_breaker_state_changes", m.Name)
	assert.Empty(t, m.Unit)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	assert.True(t, sum.IsMonotonic)

	values := map[string]int64{}
	for _, dp := range sum.DataPoints {
		name, _ := dp.Attributes.Value(attribute.Key("name"))
		assert.Equal(t, "storage", name.AsString())
		from, _ := dp.Attributes.Value(attribute.Key("from"))
		to, _ := dp.Attributes.Value(attribute.Key("to"))
		values[from.AsString()+"->"+to.AsString()] = dp.Value
	}
	assert.Equal(t, map[string]int64{
		"closed->open":      1,
		"open->half-open":   2,
		"half-open->open":   1,
		"half-open->closed": 1,
	}, values)
}

func TestRegisterLimitMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	cfg := config.DefaultConfig()
	reg, err := registerLimitMetrics(meter, cfg, nil)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, reg.Unregister())
	}()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)

	got := map[string]int64{}
	for _, m := range rm.ScopeMetrics[0].Metrics {
		assert.Empty(t, m.Unit, "metric %s should not have a unit", m.Name)
		gauge, ok := m.Data.(metricdata.Gauge[int64])
		require.True(t, ok, "metric %s should be Gauge[int64]", m.Name)
		require.Len(t, gauge.DataPoints, 1)
		got[m.Name] = gauge.DataPoints[0].Value
	}

	assert.Equal(t, map[string]int64{
		"permission_bulk_limit":          int64(cfg.Service.Permission.BulkLimit),
		"permission_concurrency_limit":   int64(cfg.Service.Permission.ConcurrencyLimit),
		"server_rate_limit":              cfg.Server.RateLimit,
		"database_max_connections":       int64(cfg.Database.MaxOpenConnections),
		"database_min_connections":       int64(cfg.Database.MaxIdleConnections),
		"database_max_data_per_write":    int64(cfg.Database.MaxDataPerWrite),
		"database_max_retries":           int64(cfg.Database.MaxRetries),
		"database_watch_buffer_size":     int64(cfg.Database.WatchBufferSize),
		"authn_oidc_backoff_max_retries": int64(cfg.Authn.Oidc.BackoffMaxRetries),
	}, got)
}

func TestRegisterLimitMetricsUsesEffectivePoolSettings(t *testing.T) {
	newPool := func(uri string) *pgxpool.Pool {
		cfg, err := pgxpool.ParseConfig(uri)
		require.NoError(t, err)
		pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
		require.NoError(t, err)
		t.Cleanup(pool.Close)
		return pool
	}

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	cfg := config.DefaultConfig()
	postgresDB := &PQDatabase.Postgres{
		WritePool: newPool("postgres://user:pass@127.0.0.1:1/permify?pool_max_conns=7&pool_min_conns=2"),
		ReadPool:  newPool("postgres://user:pass@127.0.0.1:1/permify?pool_max_conns=11&pool_min_conns=3"),
	}
	reg, err := registerLimitMetrics(meter, cfg, postgresDB)
	require.NoError(t, err)
	defer func() { assert.NoError(t, reg.Unregister()) }()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "database_max_connections" && metric.Name != "database_min_connections" {
				continue
			}
			gauge, ok := metric.Data.(metricdata.Gauge[int64])
			require.True(t, ok)
			for _, point := range gauge.DataPoints {
				pool, ok := point.Attributes.Value(attribute.Key("pool"))
				require.True(t, ok)
				if got[metric.Name] == nil {
					got[metric.Name] = make(map[string]int64)
				}
				got[metric.Name][pool.AsString()] = point.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"read": 11, "write": 7}, got["database_max_connections"])
	assert.Equal(t, map[string]int64{"read": 3, "write": 2}, got["database_min_connections"])
}
