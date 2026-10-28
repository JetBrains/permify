package singleflight

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/database"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/token"
)

// blockingDataReader signals started when HeadSnapshot begins and blocks until release is closed.
type blockingDataReader struct {
	storage.NoopDataReader
	startedOnce sync.Once
	started     chan struct{}
	release     chan struct{}
}

func newBlockingDataReader() *blockingDataReader {
	return &blockingDataReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingDataReader) HeadSnapshot(_ context.Context, tenantID string) (token.SnapToken, error) {
	b.startedOnce.Do(func() { close(b.started) })
	<-b.release
	return token.NoopToken{Value: "snapshot-" + tenantID}, nil
}

// blockingSchemaReader signals started when HeadVersion begins and blocks until release is closed.
type blockingSchemaReader struct {
	storage.NoopSchemaReader
	startedOnce sync.Once
	started     chan struct{}
	release     chan struct{}
}

func newBlockingSchemaReader() *blockingSchemaReader {
	return &blockingSchemaReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingSchemaReader) HeadVersion(_ context.Context, _ string) (string, string, error) {
	b.startedOnce.Do(func() { close(b.started) })
	<-b.release
	return "", "v1", nil
}

// awaitingContext signals onWait the first time Done() is evaluated.
// resenje.org/singleflight v0.4.3 evaluates ctx.Done() in Group.wait only after the caller
// has joined the in-flight group under its mutex; the test barrier depends on that behavior.
type awaitingContext struct {
	context.Context
	once   sync.Once
	onWait func()
}

func (c *awaitingContext) Done() <-chan struct{} {
	c.once.Do(c.onWait)
	return c.Context.Done()
}

func singleflightCounterValue(reader *sdkmetric.ManualReader, name, op string) int64 {
	var rm metricdata.ResourceMetrics
	Expect(reader.Collect(context.Background(), &rm)).Should(Succeed())
	set := attribute.NewSet(attribute.String("op", op))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			Expect(m.Unit).Should(BeEmpty())
			sum, ok := m.Data.(metricdata.Sum[int64])
			Expect(ok).Should(BeTrue(), "metric %s is %T, not an int64 sum", name, m.Data)
			Expect(sum.IsMonotonic).Should(BeTrue())
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Equals(&set) {
					return dp.Value
				}
			}
		}
	}
	return 0
}

// runConcurrently starts n calls, waits deterministically until the delegate execution has started
// and all n callers have joined the singleflight wait group, then releases the delegate.
func runConcurrently(ctx context.Context, n int, started <-chan struct{}, release chan struct{}, call func(context.Context)) {
	const timeout = 5 * time.Second
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	var joined atomic.Int32
	allJoined := make(chan struct{})
	var completed atomic.Int32
	allDone := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer GinkgoRecover()
			defer func() {
				if completed.Add(1) == int32(n) {
					close(allDone)
				}
			}()
			call(&awaitingContext{Context: ctx, onWait: func() {
				if joined.Add(1) == int32(n) {
					close(allJoined)
				}
			}})
		}()
	}

	Eventually(started).WithTimeout(timeout).Should(BeClosed())
	Eventually(allJoined).WithTimeout(timeout).Should(BeClosed())
	releaseOnce.Do(func() { close(release) })
	Eventually(allDone).WithTimeout(timeout).Should(BeClosed())
}

var _ = Describe("Singleflight metrics", func() {
	var (
		reader *sdkmetric.ManualReader
		opts   []Option
		ctx    context.Context
	)

	BeforeEach(func() {
		reader = sdkmetric.NewManualReader()
		opts = []Option{WithMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))}
		ctx = context.Background()
	})

	It("counts calls and executions of HeadSnapshot", func() {
		delegate := newBlockingDataReader()
		dataReader := NewDataReader(delegate, opts...)

		runConcurrently(ctx, 10, delegate.started, delegate.release, func(callCtx context.Context) {
			_, err := dataReader.HeadSnapshot(callCtx, "tenant1")
			Expect(err).ShouldNot(HaveOccurred())
		})

		Expect(singleflightCounterValue(reader, "singleflight_calls", "head_snapshot")).Should(Equal(int64(10)))
		Expect(singleflightCounterValue(reader, "singleflight_executions", "head_snapshot")).Should(Equal(int64(1)))

		// A call after the shared call is finished is executed again.
		delegate.release = make(chan struct{})
		close(delegate.release)
		_, err := dataReader.HeadSnapshot(ctx, "tenant1")
		Expect(err).ShouldNot(HaveOccurred())
		Expect(singleflightCounterValue(reader, "singleflight_calls", "head_snapshot")).Should(Equal(int64(11)))
		Expect(singleflightCounterValue(reader, "singleflight_executions", "head_snapshot")).Should(Equal(int64(2)))
	})

	It("counts calls and executions of HeadVersion", func() {
		delegate := newBlockingSchemaReader()
		schemaReader := NewSchemaReader(delegate, opts...)

		runConcurrently(ctx, 5, delegate.started, delegate.release, func(callCtx context.Context) {
			_, version, err := schemaReader.HeadVersion(callCtx, "tenant1")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(version).Should(Equal("v1"))
		})

		Expect(singleflightCounterValue(reader, "singleflight_calls", "head_version")).Should(Equal(int64(5)))
		Expect(singleflightCounterValue(reader, "singleflight_executions", "head_version")).Should(Equal(int64(1)))
	})

	It("allows a waiting caller to cancel without cancelling the shared execution", func() {
		delegate := newBlockingDataReader()
		dataReader := NewDataReader(delegate, opts...)
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(delegate.release) })

		leaderDone := make(chan error, 1)
		go func() {
			_, err := dataReader.HeadSnapshot(ctx, "tenant1")
			leaderDone <- err
		}()
		Eventually(delegate.started).WithTimeout(5 * time.Second).Should(BeClosed())

		waiterCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		waiterJoined := make(chan struct{})
		waiterDone := make(chan error, 1)
		go func() {
			callCtx := &awaitingContext{Context: waiterCtx, onWait: func() { close(waiterJoined) }}
			_, err := dataReader.HeadSnapshot(callCtx, "tenant1")
			waiterDone <- err
		}()
		Eventually(waiterJoined).WithTimeout(5 * time.Second).Should(BeClosed())
		cancel()
		Eventually(waiterDone).WithTimeout(5 * time.Second).Should(Receive(Equal(context.Canceled)))

		releaseOnce.Do(func() { close(delegate.release) })
		Eventually(leaderDone).WithTimeout(5 * time.Second).Should(Receive(BeNil()))
		Expect(singleflightCounterValue(reader, "singleflight_calls", "head_snapshot")).Should(Equal(int64(2)))
		Expect(singleflightCounterValue(reader, "singleflight_executions", "head_snapshot")).Should(Equal(int64(1)))
	})

	It("counts query operations with their own op attribute", func() {
		dataReader := NewDataReader(NewMockDataReader(), opts...)

		_, err := dataReader.QueryRelationships(ctx, "tenant1", &base.TupleFilter{}, "token", database.NewCursorPagination())
		Expect(err).ShouldNot(HaveOccurred())
		_, err = dataReader.QueryRelationshipsWithSubjectFilter(ctx, "tenant1", &base.TupleFilter{}, &base.Subject{Type: "user", Id: "alice"}, "token", database.NewCursorPagination())
		Expect(err).ShouldNot(HaveOccurred())
		_, err = dataReader.QuerySingleAttribute(ctx, "tenant1", &base.AttributeFilter{}, "token")
		Expect(err).ShouldNot(HaveOccurred())
		_, err = dataReader.QueryAttributes(ctx, "tenant1", &base.AttributeFilter{}, "token", database.NewCursorPagination())
		Expect(err).ShouldNot(HaveOccurred())

		Expect(singleflightCounterValue(reader, "singleflight_calls", "query_relationships")).Should(Equal(int64(1)))
		Expect(singleflightCounterValue(reader, "singleflight_executions", "query_relationships")).Should(Equal(int64(1)))
		Expect(singleflightCounterValue(reader, "singleflight_calls", "query_relationships_with_subject_filter")).Should(Equal(int64(1)))
		Expect(singleflightCounterValue(reader, "singleflight_executions", "query_relationships_with_subject_filter")).Should(Equal(int64(1)))
		Expect(singleflightCounterValue(reader, "singleflight_calls", "query_single_attribute")).Should(Equal(int64(1)))
		Expect(singleflightCounterValue(reader, "singleflight_executions", "query_single_attribute")).Should(Equal(int64(1)))
		Expect(singleflightCounterValue(reader, "singleflight_calls", "query_attributes")).Should(Equal(int64(1)))
		Expect(singleflightCounterValue(reader, "singleflight_executions", "query_attributes")).Should(Equal(int64(1)))
		Expect(singleflightCounterValue(reader, "singleflight_calls", "head_snapshot")).Should(Equal(int64(0)))
	})
})
