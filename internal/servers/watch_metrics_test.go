package servers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Permify/permify/internal"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
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

type watchMetricsTestWatcher struct {
	changes <-chan *v1.DataChanges
	errors  <-chan error
}

func (w watchMetricsTestWatcher) Watch(context.Context, string, string) (<-chan *v1.DataChanges, <-chan error) {
	return w.changes, w.errors
}

func assertWatchServerRunsMetric(t *testing.T, reader *sdkmetric.ManualReader, expectedResult string, expectedValue int64) {
	t.Helper()
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
			assert.Equal(t, expectedResult, result.AsString())
			assert.Equal(t, expectedValue, sum.DataPoints[0].Value)
			return
		}
	}
	t.Fatal("watch_server_runs metric was not collected")
}

func TestWatchServerCountsValidationFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	originalMeter := internal.Meter
	internal.Meter = meter
	defer func() { internal.Meter = originalMeter }()

	server := NewWatchServer(nil, nil)
	err := server.Watch(&v1.WatchRequest{}, &watchMetricsTestStream{ctx: context.Background()})
	require.Error(t, err)

	assertWatchServerRunsMetric(t, reader, "error", 1)
}

func TestWatchServerReturnsSendFailure(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	originalMeter := internal.Meter
	internal.Meter = meter
	defer func() { internal.Meter = originalMeter }()

	changes := make(chan *v1.DataChanges, 1)
	changes <- &v1.DataChanges{}
	close(changes)
	errorsCh := make(chan error)
	close(errorsCh)
	sendErr := errors.New("stream closed")
	server := NewWatchServer(watchMetricsTestWatcher{changes: changes, errors: errorsCh}, nil)
	stream := &watchMetricsTestStream{ctx: context.Background(), sendErr: sendErr}
	err := server.Watch(&v1.WatchRequest{
		TenantId:  "t1",
		SnapToken: token.NewNoopToken().Encode().String(),
	}, stream)
	require.Equal(t, codes.Internal, status.Code(err))
	assert.Equal(t, sendErr.Error(), status.Convert(err).Message())
	assertWatchServerRunsMetric(t, reader, "error", 1)
}

func TestWatchServerStopsSenderWhenWatcherErrorsWithOpenChangesChannel(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	originalMeter := internal.Meter
	internal.Meter = meter
	defer func() { internal.Meter = originalMeter }()

	// Leave changes open to simulate an early watcher error before changes is closed.
	changes := make(chan *v1.DataChanges)
	errorsCh := make(chan error, 1)
	watchErr := errors.New("invalid snapshot token")
	errorsCh <- watchErr

	server := NewWatchServer(watchMetricsTestWatcher{changes: changes, errors: errorsCh}, nil)
	stream := &watchMetricsTestStream{ctx: context.Background()}
	err := server.Watch(&v1.WatchRequest{
		TenantId:  "t1",
		SnapToken: token.NewNoopToken().Encode().String(),
	}, stream)
	require.Equal(t, codes.Internal, status.Code(err))
	assert.Equal(t, watchErr.Error(), status.Convert(err).Message())
	assertWatchServerRunsMetric(t, reader, "error", 1)
}
