package balancer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	grpcbalancer "google.golang.org/grpc/balancer"

	"github.com/Permify/permify/internal/config"
	"github.com/Permify/permify/internal/invoke"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/balancer"
	base "github.com/Permify/permify/pkg/pb/base/v1"
)

// fakeBuilder returns the given picker. It embeds the builder interface, other methods are not used.
type fakeBuilder struct {
	balancer.Builder
	picker balancer.NodePicker
}

func (b *fakeBuilder) Picker() balancer.NodePicker {
	return b.picker
}

type failingPicker struct{}

func (failingPicker) Pick([]byte) (grpcbalancer.SubConn, error) {
	return nil, errors.New("no subconn")
}

type fakeSubConn struct {
	grpcbalancer.SubConn
}

type staticPicker struct {
	sc grpcbalancer.SubConn
}

func (p staticPicker) Pick([]byte) (grpcbalancer.SubConn, error) {
	return p.sc, nil
}

type localChecker struct {
	calls int
}

func (c *localChecker) Check(_ context.Context, request *invoke.BatchCheckRequest) (*invoke.BatchCheckResponse, error) {
	c.calls++
	return invoke.NewBatchCheckResponse(base.CheckResult_CHECK_RESULT_ALLOWED, request.EntityIDs...), nil
}

type fakePermissionClient struct {
	base.PermissionClient
	checkErr     error
	bulkCheckErr error
}

func (f *fakePermissionClient) Check(_ context.Context, _ *base.PermissionCheckRequest, _ ...grpc.CallOption) (*base.PermissionCheckResponse, error) {
	if f.checkErr != nil {
		return nil, f.checkErr
	}
	return &base.PermissionCheckResponse{
		Can:      base.CheckResult_CHECK_RESULT_ALLOWED,
		Metadata: &base.PermissionCheckResponseMetadata{CheckCount: 1},
	}, nil
}

func (f *fakePermissionClient) BulkCheck(_ context.Context, req *base.PermissionBulkCheckRequest, _ ...grpc.CallOption) (*base.PermissionBulkCheckResponse, error) {
	if f.bulkCheckErr != nil {
		return nil, f.bulkCheckErr
	}
	results := make([]*base.PermissionCheckResponse, len(req.GetItems()))
	for i := range req.GetItems() {
		results[i] = &base.PermissionCheckResponse{
			Can:      base.CheckResult_CHECK_RESULT_ALLOWED,
			Metadata: &base.PermissionCheckResponseMetadata{CheckCount: 1},
		}
	}
	return &base.PermissionBulkCheckResponse{Results: results}, nil
}

func TestBalancerFallbacksMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	checker := &localChecker{}
	builder := &fakeBuilder{}
	client := &fakePermissionClient{}
	b, err := NewCheckEngineWithBalancer(
		context.Background(),
		checker,
		storage.NewNoopSchemaReader(),
		builder,
		"",
		&config.Distributed{},
		&config.GRPC{},
		nil,
		WithMeter(meter),
		WithClient(client),
	)
	require.NoError(t, err)

	request := &invoke.BatchCheckRequest{
		TenantID:   "t1",
		EntityType: "doc",
		EntityIDs:  []string{"1"},
		Permission: "read",
		Subject:    &base.Subject{Type: "user", Id: "1"},
		Metadata:   &base.PermissionCheckRequestMetadata{},
	}

	// Picker is not ready.
	_, err = b.Check(context.Background(), request)
	require.NoError(t, err)

	// Pick fails twice.
	builder.picker = failingPicker{}
	_, err = b.Check(context.Background(), request)
	require.NoError(t, err)
	_, err = b.Check(context.Background(), request)
	require.NoError(t, err)

	assert.Equal(t, 3, checker.calls)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	values := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			assert.Empty(t, m.Unit)
			require.NotEqual(t, "balancer_remote_calls", m.Name, "no remote call was made")
			if m.Name != "balancer_fallbacks" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.True(t, sum.IsMonotonic)
			for _, dp := range sum.DataPoints {
				reason, _ := dp.Attributes.Value(attribute.Key("reason"))
				values[reason.AsString()] = dp.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"picker_not_ready": 1, "pick_failed": 2}, values)
}

func TestBalancerRemoteCallsMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	checker := &localChecker{}
	builder := &fakeBuilder{picker: staticPicker{sc: &fakeSubConn{}}}
	client := &fakePermissionClient{}
	b, err := NewCheckEngineWithBalancer(
		context.Background(),
		checker,
		storage.NewNoopSchemaReader(),
		builder,
		"",
		&config.Distributed{},
		&config.GRPC{},
		nil,
		WithMeter(meter),
		WithClient(client),
	)
	require.NoError(t, err)

	singleReq := &invoke.BatchCheckRequest{
		TenantID:   "t1",
		EntityType: "doc",
		EntityIDs:  []string{"1"},
		Permission: "read",
		Subject:    &base.Subject{Type: "user", Id: "1"},
		Metadata:   &base.PermissionCheckRequestMetadata{},
	}
	batchReq := &invoke.BatchCheckRequest{
		TenantID:   "t1",
		EntityType: "doc",
		EntityIDs:  []string{"1", "2"},
		Permission: "read",
		Subject:    &base.Subject{Type: "user", Id: "1"},
		Metadata:   &base.PermissionCheckRequestMetadata{},
	}

	// Single entity: Check RPC (ok, then error).
	resp, err := b.Check(context.Background(), singleReq)
	require.NoError(t, err)
	assert.Equal(t, base.CheckResult_CHECK_RESULT_ALLOWED, resp.Results["1"])

	client.checkErr = errors.New("rpc check failed")
	_, err = b.Check(context.Background(), singleReq)
	require.Error(t, err)
	client.checkErr = nil

	// Multiple entities on same SubConn: BulkCheck RPC (ok, then error).
	resp, err = b.Check(context.Background(), batchReq)
	require.NoError(t, err)
	assert.Equal(t, base.CheckResult_CHECK_RESULT_ALLOWED, resp.Results["1"])
	assert.Equal(t, base.CheckResult_CHECK_RESULT_ALLOWED, resp.Results["2"])

	client.bulkCheckErr = errors.New("rpc bulk check failed")
	_, err = b.Check(context.Background(), batchReq)
	require.Error(t, err)

	assert.Equal(t, 0, checker.calls)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	values := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			assert.Empty(t, m.Unit)
			require.NotEqual(t, "balancer_fallbacks", m.Name, "no local fallback was made")
			if m.Name != "balancer_remote_calls" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			assert.True(t, sum.IsMonotonic)
			for _, dp := range sum.DataPoints {
				rpc, _ := dp.Attributes.Value(attribute.Key("rpc"))
				result, _ := dp.Attributes.Value(attribute.Key("result"))
				values[rpc.AsString()+"/"+result.AsString()] = dp.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{
		"check/ok":         1,
		"check/error":      1,
		"bulk_check/ok":    1,
		"bulk_check/error": 1,
	}, values)
}
