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

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/invoke"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/database"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
)

type stubDataWriter struct {
	storage.NoopDataWriter
}

func (stubDataWriter) RunBundle(_ context.Context, _ string, _ map[string]string, _ *v1.DataBundle) (token.EncodedSnapToken, error) {
	return token.NewNoopToken().Encode(), nil
}

type stubBundleReader struct {
	storage.NoopBundleReader
}

func (stubBundleReader) Read(_ context.Context, _, name string) (*v1.DataBundle, error) {
	return &v1.DataBundle{Name: name}, nil
}

type stubSchemaReader struct {
	storage.NoopSchemaReader
}

func (stubSchemaReader) ReadEntityDefinition(_ context.Context, _, _, entityName, version string) (*v1.EntityDefinition, string, error) {
	return &v1.EntityDefinition{
		Name: entityName,
		Relations: map[string]*v1.RelationDefinition{
			"owner": {
				Name: "owner",
				RelationReferences: []*v1.RelationReference{
					{Type: "user"},
				},
			},
		},
	}, version, nil
}

func (stubSchemaReader) ListSchemas(_ context.Context, _, _ string, _ database.Pagination) ([]*v1.SchemaList, database.EncodedContinuousToken, error) {
	return nil, database.NewNoopContinuousToken().Encode(), nil
}

func TestServerCountersUseCounterInstruments(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	originalMeter := internal.Meter
	internal.Meter = meter
	defer func() { internal.Meter = originalMeter }()

	data := NewDataServer(
		storage.NewNoopRelationshipReader(),
		&stubDataWriter{},
		&stubBundleReader{},
		&stubSchemaReader{},
	)
	schema := NewSchemaServer(storage.NewNoopSchemaWriter(), &stubSchemaReader{})
	sharedSchema := NewSharedSchemaServer(storage.NewNoopSharedSchemaWriter(), storage.NewNoopSharedSchemaReader())

	_, err := data.ReadRelationships(ctx, &v1.RelationshipReadRequest{
		TenantId: "t1",
		Metadata: &v1.RelationshipReadRequestMetadata{SnapToken: "snap"},
		Filter:   &v1.TupleFilter{Entity: &v1.EntityFilter{Type: "doc"}},
	})
	require.NoError(t, err)

	_, err = data.ReadAttributes(ctx, &v1.AttributeReadRequest{
		TenantId: "t1",
		Metadata: &v1.AttributeReadRequestMetadata{SnapToken: "snap"},
		Filter:   &v1.AttributeFilter{Entity: &v1.EntityFilter{Type: "doc"}},
	})
	require.NoError(t, err)

	validTuple := &v1.Tuple{
		Entity:   &v1.Entity{Type: "doc", Id: "1"},
		Relation: "owner",
		Subject:  &v1.Subject{Type: "user", Id: "1"},
	}

	_, err = data.Write(ctx, &v1.DataWriteRequest{
		TenantId: "t1",
		Metadata: &v1.DataWriteRequestMetadata{SchemaVersion: "v1"},
		Tuples:   []*v1.Tuple{validTuple},
	})
	require.NoError(t, err)

	_, err = data.WriteRelationships(ctx, &v1.RelationshipWriteRequest{
		TenantId: "t1",
		Metadata: &v1.RelationshipWriteRequestMetadata{SchemaVersion: "v1"},
		Tuples:   []*v1.Tuple{validTuple},
	})
	require.NoError(t, err)

	_, err = data.Delete(ctx, &v1.DataDeleteRequest{
		TenantId:        "t1",
		TupleFilter:     &v1.TupleFilter{Entity: &v1.EntityFilter{Type: "doc"}},
		AttributeFilter: &v1.AttributeFilter{Entity: &v1.EntityFilter{Type: "doc"}},
	})
	require.NoError(t, err)

	_, err = data.DeleteRelationships(ctx, &v1.RelationshipDeleteRequest{
		TenantId: "t1",
		Filter:   &v1.TupleFilter{Entity: &v1.EntityFilter{Type: "doc"}},
	})
	require.NoError(t, err)

	_, err = data.RunBundle(ctx, &v1.BundleRunRequest{
		TenantId: "t1",
		Name:     "bundle1",
	})
	require.NoError(t, err)

	_, err = schema.Write(ctx, &v1.SchemaWriteRequest{
		TenantId: "t1",
		Schema:   "entity user {}",
	})
	require.NoError(t, err)

	_, err = schema.Read(ctx, &v1.SchemaReadRequest{
		TenantId: "t1",
		Metadata: &v1.SchemaReadRequestMetadata{SchemaVersion: "v1"},
	})
	require.NoError(t, err)

	_, err = schema.List(ctx, &v1.SchemaListRequest{
		TenantId: "t1",
	})
	require.NoError(t, err)

	_, err = sharedSchema.Write(ctx, &v1.SharedSchemaWriteRequest{
		SharedSchemaId: "shared1",
		Schema:         "entity user {}",
	})
	require.NoError(t, err)

	_, err = sharedSchema.Read(ctx, &v1.SharedSchemaReadRequest{
		SharedSchemaId: "shared1",
		Metadata:       &v1.SharedSchemaReadRequestMetadata{SchemaVersion: "v1"},
	})
	require.NoError(t, err)

	_, err = sharedSchema.List(ctx, &v1.SharedSchemaListRequest{})
	require.NoError(t, err)

	_, err = sharedSchema.Assign(ctx, &v1.SharedSchemaAssignRequest{
		SharedSchemaId: "shared1",
		TenantIds:      []string{"t1"},
	})
	require.NoError(t, err)

	expectedNames := map[string]bool{
		"write_data":           true,
		"delete_data":          true,
		"read_attributes":      true,
		"read_relationships":   true,
		"write_relationships":  true,
		"delete_relationships": true,
		"run_bundle":           true,
		"write_schema":         true,
		"read_schema":          true,
		"list_schema":          true,
		"write_shared_schema":  true,
		"read_shared_schema":   true,
		"list_shared_schema":   true,
		"assign_shared_schema": true,
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, len(expectedNames))
	for _, m := range rm.ScopeMetrics[0].Metrics {
		require.True(t, expectedNames[m.Name], "unexpected metric %s", m.Name)
		require.Empty(t, m.Unit, "metric %s should not have a unit", m.Name)
		sum, ok := m.Data.(metricdata.Sum[int64])
		require.True(t, ok, "metric %s is %T, not a counter sum", m.Name, m.Data)
		require.True(t, sum.IsMonotonic, "metric %s should be monotonic", m.Name)
		require.Len(t, sum.DataPoints, 1)
		require.Empty(t, sum.DataPoints[0].Attributes.ToSlice(), "metric %s should not have attributes", m.Name)
		require.Equal(t, int64(1), sum.DataPoints[0].Value, "metric %s value mismatch", m.Name)
	}
}

type failingInvoker struct {
	invoke.Invoker
}

func (failingInvoker) Check(_ context.Context, _ *invoke.BatchCheckRequest) (*invoke.BatchCheckResponse, error) {
	return nil, errors.New("engine error")
}

func TestServerSLOAndErrorCounters(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	originalMeter := internal.Meter
	internal.Meter = meter
	defer func() { internal.Meter = originalMeter }()

	bundleSrv := NewBundleServer(&stubBundleReader{}, storage.NewNoopBundleWriter())
	tenancySrv := NewTenancyServer(storage.NewNoopTenantReader(), storage.NewNoopTenantWriter())
	permSrv := NewPermissionServer(failingInvoker{}, 10, 1)

	_, err := bundleSrv.Write(ctx, &v1.BundleWriteRequest{
		TenantId: "t1",
		Bundles:  []*v1.DataBundle{{Name: "b1"}},
	})
	require.NoError(t, err)

	_, err = bundleSrv.Read(ctx, &v1.BundleReadRequest{TenantId: "t1", Name: "b1"})
	require.NoError(t, err)

	_, err = bundleSrv.Delete(ctx, &v1.BundleDeleteRequest{TenantId: "t1", Name: "b1"})
	require.NoError(t, err)

	// Invalid bundle request triggers bundle_server_errors
	_, err = bundleSrv.Read(ctx, &v1.BundleReadRequest{})
	require.Error(t, err)

	_, err = tenancySrv.Create(ctx, &v1.TenantCreateRequest{Id: "t1", Name: "Tenant 1"})
	require.NoError(t, err)

	_, err = tenancySrv.Delete(ctx, &v1.TenantDeleteRequest{Id: "t1"})
	require.NoError(t, err)

	_, err = tenancySrv.List(ctx, &v1.TenantListRequest{})
	require.NoError(t, err)

	// Exceeding bulk_limit=1 triggers bulk_check_limit_exceeded and permission_server_errors
	_, err = permSrv.BulkCheck(ctx, &v1.PermissionBulkCheckRequest{
		TenantId: "t1",
		Items: []*v1.PermissionBulkCheckRequestItem{
			{Entity: &v1.Entity{Type: "doc", Id: "1"}, Permission: "view", Subject: &v1.Subject{Type: "user", Id: "1"}},
			{Entity: &v1.Entity{Type: "doc", Id: "2"}, Permission: "view", Subject: &v1.Subject{Type: "user", Id: "1"}},
		},
	})
	require.Error(t, err)

	// Batch check failure inside BulkCheck is reported as a denied item, not an RPC error.
	_, err = permSrv.BulkCheck(ctx, &v1.PermissionBulkCheckRequest{
		TenantId: "t1",
		Items: []*v1.PermissionBulkCheckRequestItem{
			{Entity: &v1.Entity{Type: "doc", Id: "1"}, Permission: "view", Subject: &v1.Subject{Type: "user", Id: "1"}},
		},
	})
	require.NoError(t, err)

	// Invalid BulkCheck items are counted separately from RPC failures.
	_, err = permSrv.BulkCheck(ctx, &v1.PermissionBulkCheckRequest{
		TenantId: "t1",
		Items:    []*v1.PermissionBulkCheckRequestItem{{}},
	})
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	metrics := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			assert.Empty(t, m.Unit)
			metrics[m.Name] = m
		}
	}

	for _, name := range []string{
		"write_bundle",
		"read_bundle",
		"delete_bundle",
		"create_tenant",
		"delete_tenant",
		"list_tenant",
		"bulk_check_limit_exceeded",
		"bulk_check_batch_errors",
		"bulk_check_item_errors",
	} {
		m, ok := metrics[name]
		require.True(t, ok, "missing metric %s", name)
		sum, ok := m.Data.(metricdata.Sum[int64])
		require.True(t, ok)
		require.Len(t, sum.DataPoints, 1)
		assert.Equal(t, int64(1), sum.DataPoints[0].Value, "metric %s mismatch", name)
	}

	permErrSum, ok := metrics["permission_server_errors"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, permErrSum.DataPoints, 1)
	method, _ := permErrSum.DataPoints[0].Attributes.Value(attribute.Key("method"))
	code, _ := permErrSum.DataPoints[0].Attributes.Value(attribute.Key("code"))
	assert.Equal(t, "bulk_check", method.AsString())
	assert.Equal(t, "InvalidArgument", code.AsString())
	assert.Equal(t, int64(1), permErrSum.DataPoints[0].Value)

	itemErrSum, ok := metrics["bulk_check_item_errors"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, itemErrSum.DataPoints, 1)
	assert.Equal(t, int64(1), itemErrSum.DataPoints[0].Value)
}
