package servers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/metadata"

	"github.com/Permify/permify/internal"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
)

type watchMetricsTestStream struct {
	ctx     context.Context
	sendErr error
}

func (s *watchMetricsTestStream) Send(*v1.WatchResponse) error { return s.sendErr }
func (s *watchMetricsTestStream) SetHeader(metadata.MD) error  { return nil }
func (s *watchMetricsTestStream) SendHeader(metadata.MD) error { return nil }
func (s *watchMetricsTestStream) SetTrailer(metadata.MD)       {}
func (s *watchMetricsTestStream) Context() context.Context     { return s.ctx }
func (s *watchMetricsTestStream) SendMsg(any) error            { return nil }
func (s *watchMetricsTestStream) RecvMsg(any) error            { return nil }

func TestWatchServerCountsValidationFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	originalMeter := internal.Meter
	internal.Meter = meter
	defer func() { internal.Meter = originalMeter }()

	server := NewWatchServer(nil, nil)
	err := server.Watch(&v1.WatchRequest{}, &watchMetricsTestStream{ctx: context.Background()})
	require.Error(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "watch_server_runs" {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			result, _ := sum.DataPoints[0].Attributes.Value(attribute.Key("result"))
			assert.Equal(t, "error", result.AsString())
			assert.Equal(t, int64(1), sum.DataPoints[0].Value)
			return
		}
	}
	t.Fatal("watch_server_runs metric was not collected")
}
