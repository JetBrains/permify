package ristretto

import (
	"context"
	"math"

	"github.com/dgraph-io/ristretto"
	"github.com/dustin/go-humanize"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Ristretto - Structure for Ristretto
type Ristretto struct {
	numCounters int64
	maxCost     string
	bufferItems int64

	// metrics, enabled by WithMetrics
	meter        metric.Meter
	name         string
	registration metric.Registration

	c *ristretto.Cache[string, any]
}

// New - Creates new ristretto cache
func New(opts ...Option) (*Ristretto, error) {
	rs := &Ristretto{
		numCounters: _defaultNumCounters,
		maxCost:     _defaultMaxCost,
		bufferItems: 64,
	}

	// Custom options
	for _, opt := range opts {
		opt(rs)
	}

	mc, err := humanize.ParseBytes(rs.maxCost)
	if err != nil {
		return nil, err
	}

	c, err := ristretto.NewCache(&ristretto.Config[string, any]{
		NumCounters: rs.numCounters,
		MaxCost:     int64(mc),
		BufferItems: rs.bufferItems,
		Metrics:     rs.meter != nil,
	})

	rs.c = c

	if err != nil || rs.meter == nil {
		return rs, err
	}

	if err = rs.registerMetrics(mc); err != nil {
		c.Close()
		return nil, err
	}

	return rs, nil
}

// registerMetrics - Exports the ristretto internal eviction, admission, and cost metrics as observable instruments.
// Hit and miss counts are recorded by the domain cache wrappers (permission_cache_lookups and schema_cache_lookups).
func (r *Ristretto) registerMetrics(maxCost uint64) error {
	type observableCounter struct {
		name        string
		description string
		value       func(m *ristretto.Metrics) uint64
		instrument  metric.Int64ObservableCounter
	}

	counters := []*observableCounter{
		{name: "cache_keys_added", description: "Number of new keys added to the cache", value: (*ristretto.Metrics).KeysAdded},
		{name: "cache_keys_evicted", description: "Number of keys evicted from the cache", value: (*ristretto.Metrics).KeysEvicted},
		{name: "cache_cost_evicted", description: "Sum of the costs of the keys evicted from the cache", value: (*ristretto.Metrics).CostEvicted},
		{name: "cache_sets_dropped", description: "Number of cache Set calls dropped because the set buffer was full", value: (*ristretto.Metrics).SetsDropped},
		{name: "cache_sets_rejected", description: "Number of cache Set calls rejected by the admission policy", value: (*ristretto.Metrics).SetsRejected},
	}

	instruments := make([]metric.Observable, 0, len(counters)+4)
	for _, counter := range counters {
		instrument, err := r.meter.Int64ObservableCounter(
			counter.name,
			metric.WithDescription(counter.description),
		)
		if err != nil {
			return err
		}
		counter.instrument = instrument
		instruments = append(instruments, instrument)
	}

	costUsed, err := r.meter.Int64ObservableGauge(
		"cache_cost_used",
		metric.WithDescription("Sum of the costs of the keys in the cache (cost added minus cost evicted)"),
	)
	if err != nil {
		return err
	}
	instruments = append(instruments, costUsed)

	maxCostGauge, err := r.meter.Int64ObservableGauge(
		"cache_max_cost",
		metric.WithDescription("Configured maximum cost of the cache"),
	)
	if err != nil {
		return err
	}
	instruments = append(instruments, maxCostGauge)

	numCountersGauge, err := r.meter.Int64ObservableGauge(
		"cache_num_counters",
		metric.WithDescription("Configured number of TinyLFU frequency counters in the cache"),
	)
	if err != nil {
		return err
	}
	instruments = append(instruments, numCountersGauge)

	bufferItemsGauge, err := r.meter.Int64ObservableGauge(
		"cache_buffer_items",
		metric.WithDescription("Configured number of Get operations buffered before updating TinyLFU frequency counters"),
	)
	if err != nil {
		return err
	}
	instruments = append(instruments, bufferItemsGauge)

	attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("cache", r.name)))
	metrics := r.c.Metrics
	configuredMaxCost := toInt64(maxCost)
	configuredNumCounters := r.numCounters
	configuredBufferItems := r.bufferItems

	r.registration, err = r.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, counter := range counters {
			o.ObserveInt64(counter.instrument, toInt64(counter.value(metrics)), attrs)
		}
		added, evicted := metrics.CostAdded(), metrics.CostEvicted()
		used := int64(0)
		if added > evicted {
			used = toInt64(added - evicted)
		}
		o.ObserveInt64(costUsed, used, attrs)
		o.ObserveInt64(maxCostGauge, configuredMaxCost, attrs)
		o.ObserveInt64(numCountersGauge, configuredNumCounters, attrs)
		o.ObserveInt64(bufferItemsGauge, configuredBufferItems, attrs)
		return nil
	}, instruments...)

	return err
}

// toInt64 - Converts uint64 to int64 without overflow
func toInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// Get - Gets value from cache
func (r *Ristretto) Get(key string) (interface{}, bool) {
	return r.c.Get(key)
}

// Set - Sets value to cache
func (r *Ristretto) Set(key string, value any, cost int64) bool {
	return r.c.Set(key, value, cost)
}

// Wait - Waits for cache to be ready
func (r *Ristretto) Wait() {
	r.c.Wait()
}

// Close - Closes cache
func (r *Ristretto) Close() {
	if r.registration != nil {
		_ = r.registration.Unregister()
	}
	r.c.Close()
}
