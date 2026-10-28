package engines

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/metadata"

	"github.com/Permify/permify/internal/config"
	"github.com/Permify/permify/internal/factories"
	"github.com/Permify/permify/internal/invoke"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/database"
	"github.com/Permify/permify/pkg/dsl/compiler"
	"github.com/Permify/permify/pkg/dsl/parser"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
	"github.com/Permify/permify/pkg/tuple"
)

type lookupMetricsTestStream struct {
	err error
}

func (s *lookupMetricsTestStream) Send(*base.PermissionLookupEntityStreamResponse) error {
	return s.err
}
func (s *lookupMetricsTestStream) SetHeader(metadata.MD) error  { return nil }
func (s *lookupMetricsTestStream) SendHeader(metadata.MD) error { return nil }
func (s *lookupMetricsTestStream) SetTrailer(metadata.MD)       {}
func (s *lookupMetricsTestStream) Context() context.Context     { return context.Background() }
func (s *lookupMetricsTestStream) SendMsg(any) error            { return nil }
func (s *lookupMetricsTestStream) RecvMsg(any) error            { return nil }

func collectEngineMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			assert.Empty(t, m.Unit, "metric %s should not have a unit", m.Name)
			out[m.Name] = m
		}
	}
	return out
}

func TestEngineMetrics(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	db, err := factories.DatabaseFactory(config.Database{Engine: "memory"})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	dsl := `
entity user {}

rule is_public(public boolean) {
	public == true
}

entity group {
	relation member @user
}

entity doc {
	relation parent @group#member
	attribute public boolean
	permission view = parent and is_public(public)
}
`
	sch, err := parser.NewParser(dsl).Parse()
	require.NoError(t, err)
	_, _, err = compiler.NewCompiler(true, sch).Compile()
	require.NoError(t, err)

	var defs []storage.SchemaDefinition
	for _, st := range sch.Statements {
		defs = append(defs, storage.SchemaDefinition{
			TenantID:             "t1",
			Version:              "v1",
			Name:                 st.GetName(),
			SerializedDefinition: []byte(st.String()),
		})
	}
	sw := factories.SchemaWriterFactory(db)
	require.NoError(t, sw.WriteSchema(ctx, defs))

	sr := factories.SchemaReaderFactory(db)
	dr := factories.DataReaderFactory(db)
	dw := factories.DataWriterFactory(db)

	var tuples []*base.Tuple
	for _, s := range []string{
		"doc:1#parent@group:g1#member",
		"doc:1#parent@group:g2#member",
		"doc:1#parent@group:g3#member",
		"group:g3#member@user:alice",
		"doc:2#parent@group:g3#member",
	} {
		tup, err := tuple.Tuple(s)
		require.NoError(t, err)
		tuples = append(tuples, tup)
	}
	boolVal, err := ConvertToAnyPB(true)
	require.NoError(t, err)
	attrs := []*base.Attribute{
		{Entity: &base.Entity{Type: "doc", Id: "1"}, Attribute: "public", Value: boolVal},
		{Entity: &base.Entity{Type: "doc", Id: "2"}, Attribute: "public", Value: boolVal},
	}
	_, err = dw.Write(ctx, "t1", database.NewTupleCollection(tuples...), database.NewAttributeCollection(attrs...))
	require.NoError(t, err)

	// Set maxBatchSize=1 to trigger batch splits across the 3 userset groups (g1, g2, g3 -> 2 splits).
	checkEngine := NewCheckEngine(sr, dr, CheckMaxBatchSize(1), CheckConcurrencyLimit(1), CheckMeter(meter))
	expandEngine := NewExpandEngine(sr, dr, ExpandMaxBatchSize(1), ExpandConcurrencyLimit(1), ExpandMeter(meter))
	lookupEngine := NewLookupEngine(checkEngine, sr, dr, LookupMaxBatchSize(10), LookupConcurrencyLimit(2), LookupMeter(meter))
	invoker := invoke.NewDirectInvoker(sr, dr, checkEngine, expandEngine, lookupEngine, nil, invoke.WithMeter(meter))
	checkEngine.SetInvoker(invoker)

	snap := token.NewNoopToken().Encode().String()
	_, err = invoker.Check(ctx, &invoke.BatchCheckRequest{
		TenantID:   "t1",
		EntityType: "doc",
		EntityIDs:  []string{"1"},
		Permission: "view",
		Subject:    &base.Subject{Type: "user", Id: "alice"},
		Metadata:   &base.PermissionCheckRequestMetadata{SnapToken: snap, SchemaVersion: "v1", Depth: 20},
	})
	require.NoError(t, err)

	_, err = invoker.Expand(ctx, &base.PermissionExpandRequest{
		TenantId:   "t1",
		Entity:     &base.Entity{Type: "doc", Id: "1"},
		Permission: "view",
		Metadata:   &base.PermissionExpandRequestMetadata{SnapToken: snap, SchemaVersion: "v1"},
	})
	require.NoError(t, err)

	// Run LookupEntity twice to hit LookupEngine.schemaMap on the second call.
	for i := 0; i < 2; i++ {
		resp, err := invoker.LookupEntity(ctx, &base.PermissionLookupEntityRequest{
			TenantId:   "t1",
			EntityType: "doc",
			Permission: "view",
			Subject:    &base.Subject{Type: "user", Id: "alice"},
			Metadata:   &base.PermissionLookupEntityRequestMetadata{SnapToken: snap, SchemaVersion: "v1", Depth: 20},
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"1", "2"}, resp.GetEntityIds())
	}

	streamErr := errors.New("stream closed")
	err = lookupEngine.LookupEntityStream(ctx, &base.PermissionLookupEntityRequest{
		TenantId:   "t1",
		EntityType: "doc",
		Permission: "view",
		Subject:    &base.Subject{Type: "user", Id: "alice"},
		Metadata:   &base.PermissionLookupEntityRequestMetadata{SnapToken: snap, SchemaVersion: "v1", Depth: 20},
	}, &lookupMetricsTestStream{err: streamErr})
	require.ErrorIs(t, err, streamErr)

	metrics := collectEngineMetrics(t, reader)

	// Verify engine_batch_splits for both check and expand
	splitsSum, ok := metrics["engine_batch_splits"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	splitsByEngine := map[string]int64{}
	for _, dp := range splitsSum.DataPoints {
		eng, _ := dp.Attributes.Value(attribute.Key("engine"))
		splitsByEngine[eng.AsString()] = dp.Value
	}
	assert.Positive(t, splitsByEngine["check"])
	assert.Positive(t, splitsByEngine["expand"])

	// Verify cel_evaluations
	celSum, ok := metrics["cel_evaluations"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	celByResult := map[string]int64{}
	for _, dp := range celSum.DataPoints {
		res, _ := dp.Attributes.Value(attribute.Key("result"))
		celByResult[res.AsString()] = dp.Value
	}
	assert.Positive(t, celByResult["ok"])

	// Verify lookup_schema_cache_lookups (1 miss, 2 hits including LookupEntityStream)
	lookupSchemaSum, ok := metrics["lookup_schema_cache_lookups"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	lookupSchemaByResult := map[string]int64{}
	for _, dp := range lookupSchemaSum.DataPoints {
		res, _ := dp.Attributes.Value(attribute.Key("result"))
		lookupSchemaByResult[res.AsString()] = dp.Value
	}
	assert.Equal(t, map[string]int64{"miss": 1, "hit": 2}, lookupSchemaByResult)

	// Verify lookup_entity_candidates and lookup_entity_matched
	candSum, ok := metrics["lookup_entity_candidates"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, candSum.DataPoints, 1)
	assert.Equal(t, int64(6), candSum.DataPoints[0].Value)

	checkedSum, ok := metrics["lookup_entity_checked"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, checkedSum.DataPoints, 1)
	assert.Equal(t, int64(6), checkedSum.DataPoints[0].Value)

	matchedSum, ok := metrics["lookup_entity_matched"].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, matchedSum.DataPoints, 1)
	assert.Equal(t, int64(4), matchedSum.DataPoints[0].Value)
}

func TestBulkCheckerCountsPrecomputedCandidatesAsChecked(t *testing.T) {
	ctx := context.Background()
	checker, err := NewBulkChecker(ctx, NewCheckEngine(nil, nil), BulkCheckerTypeEntity, func(string, string) {}, BulkCheckerConfig{
		ConcurrencyLimit: 2,
		BufferSize:       10,
	})
	require.NoError(t, err)
	defer checker.Close()

	publisher := NewBulkEntityPublisher(ctx, &base.PermissionLookupEntityRequest{
		TenantId:   "t1",
		EntityType: "doc",
		Permission: "view",
		Subject:    &base.Subject{Type: "user", Id: "alice"},
		Metadata:   &base.PermissionLookupEntityRequestMetadata{},
	}, checker)
	publisher.Publish(&base.Entity{Type: "doc", Id: "1"}, &base.PermissionCheckRequestMetadata{}, nil, base.CheckResult_CHECK_RESULT_ALLOWED)
	publisher.Publish(&base.Entity{Type: "doc", Id: "2"}, &base.PermissionCheckRequestMetadata{}, nil, base.CheckResult_CHECK_RESULT_DENIED)

	require.NoError(t, checker.ExecuteRequests(10))
	assert.Equal(t, int64(2), publisher.PublishedCount())
	assert.Equal(t, int64(2), checker.CheckedCount())
}
