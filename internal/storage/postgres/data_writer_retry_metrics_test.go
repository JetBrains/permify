package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Permify/permify/pkg/database"
	PQDatabase "github.com/Permify/permify/pkg/database/postgres"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
)

type retryableDataWriterTestError struct{}

func (retryableDataWriterTestError) Error() string     { return "retryable before server execution" }
func (retryableDataWriterTestError) SafeToRetry() bool { return true }

func TestDataWriterRetryCountersCountOnlyAttemptsPerformed(t *testing.T) {
	for _, tc := range []struct {
		name        string
		maxRetries  int
		wantRetries int64
	}{
		{name: "zero retry budget", maxRetries: 0, wantRetries: 0},
		{name: "one retry performed", maxRetries: 1, wantRetries: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
			pg := &PQDatabase.Postgres{}
			PQDatabase.MaxRetries(tc.maxRetries)(pg)
			PQDatabase.MaxDataPerWrite(10)(pg)

			writer := NewDataWriter(pg, WithDataWriterMeter(meter))
			writer.writeAttempt = func(context.Context, string, *database.TupleCollection, *database.AttributeCollection) (token.EncodedSnapToken, error) {
				return nil, retryableDataWriterTestError{}
			}
			_, err := writer.Write(context.Background(), "t1", database.NewTupleCollection(), database.NewAttributeCollection())
			require.Error(t, err)

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))
			metrics := make(map[string]int64)
			for _, scope := range rm.ScopeMetrics {
				for _, metric := range scope.Metrics {
					sum, ok := metric.Data.(metricdata.Sum[int64])
					require.True(t, ok, metric.Name)
					for _, point := range sum.DataPoints {
						metrics[metric.Name] += point.Value
					}
				}
			}
			require.Equal(t, int64(1), metrics["database_max_retries_exceeded"])
			require.Equal(t, tc.wantRetries, metrics["database_tx_retries"])
		})
	}
}

func TestDataWriterReturnsCancelledErrorCodeWhenContextIsCancelledDuringRetries(t *testing.T) {
	pg := &PQDatabase.Postgres{}
	PQDatabase.MaxRetries(3)(pg)
	PQDatabase.MaxDataPerWrite(10)(pg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := NewDataWriter(pg)
	attempts := 0
	writer.writeAttempt = func(context.Context, string, *database.TupleCollection, *database.AttributeCollection) (token.EncodedSnapToken, error) {
		attempts++
		cancel()
		return nil, retryableDataWriterTestError{}
	}
	_, err := writer.Write(ctx, "t1", database.NewTupleCollection(), database.NewAttributeCollection())
	require.EqualError(t, err, base.ErrorCode_ERROR_CODE_CANCELLED.String())
	require.Equal(t, 1, attempts)

	_, err = writer.Write(ctx, "t1", database.NewTupleCollection(), database.NewAttributeCollection())
	require.EqualError(t, err, base.ErrorCode_ERROR_CODE_CANCELLED.String())
	require.Equal(t, 1, attempts)
}
