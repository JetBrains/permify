package cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/cache/ristretto"
	base "github.com/Permify/permify/pkg/pb/base/v1"
)

// fakeSchemaReader returns definitions with the head version "v1" when no version is given.
type fakeSchemaReader struct {
	storage.NoopSchemaReader
}

func (f *fakeSchemaReader) ReadEntityDefinition(_ context.Context, _, _, entityName, version string) (*base.EntityDefinition, string, error) {
	if version == "" {
		version = "v1"
	}
	return &base.EntityDefinition{Name: entityName}, version, nil
}

func (f *fakeSchemaReader) ReadRuleDefinition(_ context.Context, _, _, ruleName, version string) (*base.RuleDefinition, string, error) {
	if version == "" {
		version = "v1"
	}
	return &base.RuleDefinition{Name: ruleName}, version, nil
}

func collectSchemaCacheLookups(t *testing.T, reader *sdkmetric.ManualReader) metricdata.Sum[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "schema_cache_lookups" {
				assert.Empty(t, m.Unit)
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok, "schema_cache_lookups is %T, not an int64 sum", m.Data)
				return sum
			}
		}
	}
	require.Fail(t, "schema_cache_lookups not found")
	return metricdata.Sum[int64]{}
}

func lookupsValue(sum metricdata.Sum[int64], kind, result string) int64 {
	set := attribute.NewSet(attribute.String("kind", kind), attribute.String("result", result))
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&set) {
			return dp.Value
		}
	}
	return 0
}

func TestSchemaReaderCacheLookupsMetric(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	c, err := ristretto.New()
	require.NoError(t, err)
	defer c.Close()

	schemaReader := NewSchemaReader(&fakeSchemaReader{}, c, WithMeter(meter))

	// No version: the cache is not read.
	_, v, err := schemaReader.ReadEntityDefinition(ctx, "t1", "", "doc", "")
	require.NoError(t, err)
	assert.Equal(t, "v1", v)
	_, _, err = schemaReader.ReadRuleDefinition(ctx, "t1", "", "is_public", "")
	require.NoError(t, err)

	// First read with a version: miss.
	_, _, err = schemaReader.ReadEntityDefinition(ctx, "t1", "", "user", "v1")
	require.NoError(t, err)
	_, _, err = schemaReader.ReadRuleDefinition(ctx, "t1", "", "is_admin", "v1")
	require.NoError(t, err)
	c.Wait()

	// Second read with the same version: hit.
	definition, _, err := schemaReader.ReadEntityDefinition(ctx, "t1", "", "user", "v1")
	require.NoError(t, err)
	assert.Equal(t, "user", definition.GetName())
	rule, _, err := schemaReader.ReadRuleDefinition(ctx, "t1", "", "is_admin", "v1")
	require.NoError(t, err)
	assert.Equal(t, "is_admin", rule.GetName())

	sum := collectSchemaCacheLookups(t, reader)
	assert.True(t, sum.IsMonotonic)
	for _, kind := range []string{"entity", "rule"} {
		assert.Equal(t, int64(1), lookupsValue(sum, kind, "bypass"), kind)
		assert.Equal(t, int64(1), lookupsValue(sum, kind, "miss"), kind)
		assert.Equal(t, int64(1), lookupsValue(sum, kind, "hit"), kind)
	}
	assert.Len(t, sum.DataPoints, 6)
}
