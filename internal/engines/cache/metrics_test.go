package cache

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Permify/permify/internal/config"
	"github.com/Permify/permify/internal/engines"
	"github.com/Permify/permify/internal/factories"
	"github.com/Permify/permify/internal/invoke"
	"github.com/Permify/permify/pkg/cache/ristretto"
	"github.com/Permify/permify/pkg/database"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
	"github.com/Permify/permify/pkg/tuple"
)

// collectInt64Sum collects the metrics of the reader and returns the int64 sum with the given name.
func collectInt64Sum(reader *sdkmetric.ManualReader, name string) (metricdata.Sum[int64], bool) {
	var rm metricdata.ResourceMetrics
	Expect(reader.Collect(context.Background(), &rm)).Should(Succeed())
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			Expect(m.Unit).Should(BeEmpty())
			sum, ok := m.Data.(metricdata.Sum[int64])
			Expect(ok).Should(BeTrue(), "metric %s is %T, not an int64 sum", name, m.Data)
			return sum, true
		}
	}
	return metricdata.Sum[int64]{}, false
}

// int64SumValue returns the value of the data point with exactly the given attributes.
func int64SumValue(sum metricdata.Sum[int64], attrs ...attribute.KeyValue) int64 {
	set := attribute.NewSet(attrs...)
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&set) {
			return dp.Value
		}
	}
	return 0
}

var _ = Describe("metrics", func() {
	metricsSchema := `
	entity user {}

	entity doc {
		relation owner @user
	}
	`

	var (
		reader  *sdkmetric.ManualReader
		invoker invoke.Check
		cache   *ristretto.Ristretto
	)

	BeforeEach(func() {
		reader = sdkmetric.NewManualReader()
		meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

		db, err := factories.DatabaseFactory(config.Database{Engine: "memory"})
		Expect(err).ShouldNot(HaveOccurred())

		conf, err := newSchema(metricsSchema)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(factories.SchemaWriterFactory(db).WriteSchema(context.Background(), conf)).Should(Succeed())

		var tuples []*base.Tuple
		for _, relationship := range []string{"doc:1#owner@user:1", "doc:2#owner@user:1"} {
			t, err := tuple.Tuple(relationship)
			Expect(err).ShouldNot(HaveOccurred())
			tuples = append(tuples, t)
		}
		_, err = factories.DataWriterFactory(db).Write(context.Background(), "t1", database.NewTupleCollection(tuples...), database.NewAttributeCollection())
		Expect(err).ShouldNot(HaveOccurred())

		schemaReader := factories.SchemaReaderFactory(db)
		dataReader := factories.DataReaderFactory(db)

		cache, err = ristretto.New(ristretto.NumberOfCounters(1_000), ristretto.MaxCost("10MiB"))
		Expect(err).ShouldNot(HaveOccurred())

		checkEngine := engines.NewCheckEngine(schemaReader, dataReader)
		checkEngineWithCache := NewCheckEngineWithCache(checkEngine, schemaReader, cache, WithMeter(meter))
		directInvoker := invoke.NewDirectInvoker(schemaReader, dataReader, checkEngineWithCache, nil, nil, nil, invoke.WithMeter(meter))
		checkEngine.SetInvoker(directInvoker)
		invoker = directInvoker
	})

	AfterEach(func() {
		cache.Close()
	})

	check := func(subjectID string, entityIDs ...string) *invoke.BatchCheckResponse {
		response, err := invoker.Check(context.Background(), &invoke.BatchCheckRequest{
			TenantID:   "t1",
			EntityType: "doc",
			EntityIDs:  entityIDs,
			Permission: "owner",
			Subject:    &base.Subject{Type: "user", Id: subjectID},
			Metadata: &base.PermissionCheckRequestMetadata{
				SnapToken: token.NewNoopToken().Encode().String(),
				Depth:     20,
			},
		})
		Expect(err).ShouldNot(HaveOccurred())
		return response
	}

	It("check is a counter without subject_id", func() {
		Expect(check("1", "1").UnionResult()).Should(Equal(base.CheckResult_CHECK_RESULT_ALLOWED))
		Expect(check("2", "1").UnionResult()).Should(Equal(base.CheckResult_CHECK_RESULT_DENIED))

		sum, found := collectInt64Sum(reader, "check")
		Expect(found).Should(BeTrue())
		Expect(sum.IsMonotonic).Should(BeTrue())
		Expect(sum.Temporality).Should(Equal(metricdata.CumulativeTemporality))

		// Checks for different subjects of the same type go into one series.
		Expect(sum.DataPoints).Should(HaveLen(1))
		Expect(sum.DataPoints[0].Attributes.HasValue("subject_id")).Should(BeFalse())
		Expect(int64SumValue(sum, attribute.String("subject_type", "user"))).Should(Equal(int64(2)))
	})

	It("permission_cache_lookups counts hits and misses per entity ID", func() {
		check("1", "1", "2", "3")
		cache.Wait()

		sum, found := collectInt64Sum(reader, "permission_cache_lookups")
		Expect(found).Should(BeTrue())
		Expect(sum.IsMonotonic).Should(BeTrue())
		Expect(int64SumValue(sum, attribute.String("result", "miss"))).Should(Equal(int64(3)))
		Expect(int64SumValue(sum, attribute.String("result", "hit"))).Should(Equal(int64(0)))

		response := check("1", "1", "2", "3")
		Expect(response.Results).Should(Equal(map[string]base.CheckResult{
			"1": base.CheckResult_CHECK_RESULT_ALLOWED,
			"2": base.CheckResult_CHECK_RESULT_ALLOWED,
			"3": base.CheckResult_CHECK_RESULT_DENIED,
		}))

		sum, found = collectInt64Sum(reader, "permission_cache_lookups")
		Expect(found).Should(BeTrue())
		Expect(int64SumValue(sum, attribute.String("result", "miss"))).Should(Equal(int64(3)))
		Expect(int64SumValue(sum, attribute.String("result", "hit"))).Should(Equal(int64(3)))

		_, found = collectInt64Sum(reader, "cache_hit")
		Expect(found).Should(BeFalse())
	})
})
