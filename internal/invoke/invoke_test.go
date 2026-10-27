package invoke

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Permify/permify/internal/storage"
	base "github.com/Permify/permify/pkg/pb/base/v1"
)

type noopChecker struct{}

func (noopChecker) Check(_ context.Context, request *BatchCheckRequest) (*BatchCheckResponse, error) {
	return NewBatchCheckResponse(base.CheckResult_CHECK_RESULT_ALLOWED, request.EntityIDs...), nil
}

type noopLookup struct{}

func (noopLookup) LookupEntity(_ context.Context, _ *base.PermissionLookupEntityRequest) (*base.PermissionLookupEntityResponse, error) {
	return &base.PermissionLookupEntityResponse{}, nil
}

func (noopLookup) LookupEntityStream(_ context.Context, _ *base.PermissionLookupEntityRequest, _ base.Permission_LookupEntityStreamServer) error {
	return nil
}

func (noopLookup) LookupSubject(_ context.Context, _ *base.PermissionLookupSubjectRequest) (*base.PermissionLookupSubjectResponse, error) {
	return &base.PermissionLookupSubjectResponse{}, nil
}

type noopSubjectPermission struct{}

func (noopSubjectPermission) SubjectPermission(_ context.Context, _ *base.PermissionSubjectPermissionRequest) (*base.PermissionSubjectPermissionResponse, error) {
	return &base.PermissionSubjectPermissionResponse{}, nil
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func TestDirectInvokerCounters(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	invoker := NewDirectInvoker(
		storage.NewNoopSchemaReader(),
		storage.NewNoopRelationshipReader(),
		noopChecker{},
		nil,
		noopLookup{},
		noopSubjectPermission{},
		WithMeter(meter),
	)

	checkMeta := &base.PermissionCheckRequestMetadata{
		SnapToken:     "snap",
		SchemaVersion: "v1",
		Depth:         5,
	}
	for _, subjectID := range []string{"alice", "bob"} {
		_, err := invoker.Check(ctx, &BatchCheckRequest{
			TenantID:   "t1",
			EntityType: "doc",
			EntityIDs:  []string{"1"},
			Permission: "read",
			Subject:    &base.Subject{Type: "user", Id: subjectID},
			Metadata:   checkMeta,
		})
		require.NoError(t, err)
	}
	_, err := invoker.Check(ctx, &BatchCheckRequest{
		TenantID:   "t1",
		EntityType: "doc",
		EntityIDs:  []string{"1"},
		Permission: "read",
		Subject:    &base.Subject{Type: "user", Id: "invalid-depth"},
		Metadata: &base.PermissionCheckRequestMetadata{
			Depth: -1,
		},
	})
	require.Error(t, err)

	_, err = invoker.LookupEntity(ctx, &base.PermissionLookupEntityRequest{
		TenantId: "t1",
		Metadata: &base.PermissionLookupEntityRequestMetadata{SnapToken: "snap", SchemaVersion: "v1"},
	})
	require.NoError(t, err)

	require.NoError(t, invoker.LookupEntityStream(ctx, &base.PermissionLookupEntityRequest{
		TenantId: "t1",
		Metadata: &base.PermissionLookupEntityRequestMetadata{SnapToken: "snap", SchemaVersion: "v1"},
	}, nil))

	_, err = invoker.LookupSubject(ctx, &base.PermissionLookupSubjectRequest{
		TenantId: "t1",
		Metadata: &base.PermissionLookupSubjectRequestMetadata{SnapToken: "snap", SchemaVersion: "v1"},
	})
	require.NoError(t, err)

	_, err = invoker.SubjectPermission(ctx, &base.PermissionSubjectPermissionRequest{
		TenantId: "t1",
		Metadata: &base.PermissionSubjectPermissionRequestMetadata{SnapToken: "snap", SchemaVersion: "v1"},
	})
	require.NoError(t, err)

	metrics := collectMetrics(t, reader)
	for name, expected := range map[string]int64{
		"check":              3,
		"lookup_entity":      2,
		"lookup_subject":     1,
		"subject_permission": 1,
	} {
		m, ok := metrics[name]
		require.True(t, ok, "missing metric %s", name)
		assert.Empty(t, m.Unit, "metric %s should not have a unit", name)
		sum, ok := m.Data.(metricdata.Sum[int64])
		require.True(t, ok, "metric %s is %T, not Sum[int64]", name, m.Data)
		assert.True(t, sum.IsMonotonic)
		require.Len(t, sum.DataPoints, 1)
		assert.Equal(t, expected, sum.DataPoints[0].Value)
		if name == "check" {
			assert.False(t, sum.DataPoints[0].Attributes.HasValue("subject_id"))
			subjectType, ok := sum.DataPoints[0].Attributes.Value(attribute.Key("subject_type"))
			assert.True(t, ok)
			assert.Equal(t, "user", subjectType.AsString())
		}
	}
}

func TestDirectInvokerSubjectTypeCacheBound(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	invoker := NewDirectInvoker(
		storage.NewNoopSchemaReader(),
		storage.NewNoopRelationshipReader(),
		noopChecker{},
		nil,
		noopLookup{},
		noopSubjectPermission{},
		WithMeter(meter),
	)

	request := func(subjectType string) *BatchCheckRequest {
		return &BatchCheckRequest{
			TenantID:   "t1",
			EntityType: "doc",
			EntityIDs:  []string{"1"},
			Permission: "read",
			Subject:    &base.Subject{Type: subjectType, Id: "subject"},
			Metadata: &base.PermissionCheckRequestMetadata{
				SnapToken:     "snap",
				SchemaVersion: "v1",
				Depth:         5,
			},
		}
	}

	// Passing "__other__" before the cache is full must not consume a cache slot.
	_, err := invoker.Check(context.Background(), request(otherSubjectType))
	require.NoError(t, err)
	assert.Zero(t, invoker.subjectTypeAttrsCount.Load())

	for i := 0; i < 2; i++ {
		_, err := invoker.Check(context.Background(), request("user"))
		require.NoError(t, err)
	}

	const uniqueSubjectTypes = maxCachedSubjectTypes*2 + 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(uniqueSubjectTypes)
	for i := 0; i < uniqueSubjectTypes; i++ {
		subjectType := fmt.Sprintf("type_%d", i)
		go func() {
			defer wg.Done()
			<-start
			_, err := invoker.Check(context.Background(), request(subjectType))
			assert.NoError(t, err)
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int32(maxCachedSubjectTypes), invoker.subjectTypeAttrsCount.Load())
	var cachedSubjectTypes int
	invoker.subjectTypeAttrs.Range(func(k, _ any) bool {
		assert.NotEqual(t, otherSubjectType, k)
		cachedSubjectTypes++
		return true
	})
	assert.Equal(t, maxCachedSubjectTypes, cachedSubjectTypes)

	metrics := collectMetrics(t, reader)
	check, ok := metrics["check"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	assert.Len(t, check.DataPoints, maxCachedSubjectTypes+1)
	var total int64
	var other int64
	for _, point := range check.DataPoints {
		total += point.Value
		subjectType, ok := point.Attributes.Value(attribute.Key("subject_type"))
		if ok && subjectType.AsString() == otherSubjectType {
			other = point.Value
		}
	}
	assert.Equal(t, int64(uniqueSubjectTypes+3), total)
	assert.Equal(t, int64(uniqueSubjectTypes-maxCachedSubjectTypes+2), other)

	repeatedMetrics := collectMetrics(t, reader)
	repeatedCheck, ok := repeatedMetrics["check"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	var repeatedTotal int64
	for _, point := range repeatedCheck.DataPoints {
		repeatedTotal += point.Value
	}
	assert.Equal(t, total, repeatedTotal)
}

func BenchmarkDirectInvokerCheckSubjectTypeAttrs(b *testing.B) {
	invoker := NewDirectInvoker(
		storage.NewNoopSchemaReader(),
		storage.NewNoopRelationshipReader(),
		noopChecker{},
		nil,
		noopLookup{},
		noopSubjectPermission{},
	)
	_ = invoker.checkSubjectTypeAttrs("user")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = invoker.checkSubjectTypeAttrs("user")
	}
}
