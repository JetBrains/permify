package semaphore

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/semaphore"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/invoke"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/database"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
	"github.com/Permify/permify/pkg/token"
)

var (
	storageWaitAttrs    = metric.WithAttributeSet(attribute.NewSet(attribute.String("scope", "storage")))
	storageAttemptAttrs = storageWaitAttrs
)

// Option - Option type for the semaphore DataReader
type Option func(*options)

type options struct {
	meter metric.Meter
}

// WithMeter - Sets the meter used to create the semaphore DataReader metrics. The default is internal.Meter.
func WithMeter(meter metric.Meter) Option {
	return func(o *options) {
		o.meter = meter
	}
}

// DataReader wraps a storage.DataReader with per-request semaphore protection.
// Each DB query acquires a slot from the request-scoped semaphore (stored in context),
// preventing connection pool exhaustion from concurrent goroutines.
type DataReader struct {
	delegate                   storage.DataReader
	concurrencyWaitsCounter    metric.Int64Counter
	concurrencyAttemptsCounter metric.Int64Counter
}

// NewDataReader creates a semaphore-protected DataReader.
func NewDataReader(delegate storage.DataReader, opts ...Option) *DataReader {
	o := &options{meter: internal.Meter}
	for _, opt := range opts {
		opt(o)
	}
	return &DataReader{
		delegate:                   delegate,
		concurrencyWaitsCounter:    telemetry.NewCounter(o.meter, "concurrency_limit_waits", "Number of times a request had to wait for a concurrency limit slot, by scope"),
		concurrencyAttemptsCounter: telemetry.NewCounter(o.meter, "concurrency_limit_acquire_attempts", "Number of attempts to acquire a request concurrency slot, by scope"),
	}
}

func (r *DataReader) acquire(ctx context.Context) (*semaphore.Weighted, error) {
	sem := invoke.ConcurrencySemaphoreFromContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.concurrencyAttemptsCounter.Add(ctx, 1, storageAttemptAttrs)
	if sem.TryAcquire(1) {
		if err := ctx.Err(); err != nil {
			sem.Release(1)
			return nil, err
		}
		return sem, nil
	}
	r.concurrencyWaitsCounter.Add(ctx, 1, storageWaitAttrs)
	if err := sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return sem, nil
}

func (r *DataReader) QueryRelationships(ctx context.Context, tenantID string, filter *base.TupleFilter, snap string, pagination database.CursorPagination) (*database.TupleIterator, error) {
	sem, err := r.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer sem.Release(1)
	return r.delegate.QueryRelationships(ctx, tenantID, filter, snap, pagination)
}

func (r *DataReader) QueryRelationshipsWithSubjectFilter(ctx context.Context, tenantID string, filter *base.TupleFilter, subject *base.Subject, snap string, pagination database.CursorPagination) (*database.TupleIterator, error) {
	sem, err := r.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer sem.Release(1)
	return r.delegate.QueryRelationshipsWithSubjectFilter(ctx, tenantID, filter, subject, snap, pagination)
}

func (r *DataReader) ReadRelationships(ctx context.Context, tenantID string, filter *base.TupleFilter, snap string, pagination database.Pagination) (*database.TupleCollection, database.EncodedContinuousToken, error) {
	sem, err := r.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer sem.Release(1)
	return r.delegate.ReadRelationships(ctx, tenantID, filter, snap, pagination)
}

func (r *DataReader) QuerySingleAttribute(ctx context.Context, tenantID string, filter *base.AttributeFilter, snap string) (*base.Attribute, error) {
	sem, err := r.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer sem.Release(1)
	return r.delegate.QuerySingleAttribute(ctx, tenantID, filter, snap)
}

func (r *DataReader) QueryAttributes(ctx context.Context, tenantID string, filter *base.AttributeFilter, snap string, pagination database.CursorPagination) (*database.AttributeIterator, error) {
	sem, err := r.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer sem.Release(1)
	return r.delegate.QueryAttributes(ctx, tenantID, filter, snap, pagination)
}

func (r *DataReader) ReadAttributes(ctx context.Context, tenantID string, filter *base.AttributeFilter, snap string, pagination database.Pagination) (*database.AttributeCollection, database.EncodedContinuousToken, error) {
	sem, err := r.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer sem.Release(1)
	return r.delegate.ReadAttributes(ctx, tenantID, filter, snap, pagination)
}

func (r *DataReader) QueryUniqueSubjectReferences(ctx context.Context, tenantID string, subjectReference *base.RelationReference, excluded []string, snap string, pagination database.Pagination) ([]string, database.EncodedContinuousToken, error) {
	sem, err := r.acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer sem.Release(1)
	return r.delegate.QueryUniqueSubjectReferences(ctx, tenantID, subjectReference, excluded, snap, pagination)
}

func (r *DataReader) HeadSnapshot(ctx context.Context, tenantID string) (token.SnapToken, error) {
	sem, err := r.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer sem.Release(1)
	return r.delegate.HeadSnapshot(ctx, tenantID)
}
