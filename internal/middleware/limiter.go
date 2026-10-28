package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/juju/ratelimit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/pkg/telemetry"
)

var (
	rateLimitAllowedAttrs  = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "allowed")))
	rateLimitExceededAttrs = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "exceeded")))
)

// RateLimiterOption - Option type for RateLimiter
type RateLimiterOption func(*rateLimiterOptions)

type rateLimiterOptions struct {
	meter metric.Meter
}

// WithLimiterMeter - Sets the meter used to create the RateLimiter metrics. The default is internal.Meter.
func WithLimiterMeter(meter metric.Meter) RateLimiterOption {
	return func(o *rateLimiterOptions) {
		o.meter = meter
	}
}

// RateLimiter struct is a wrapper around the juju Bucket struct
type RateLimiter struct {
	bucket          *ratelimit.Bucket // bucket is the token bucket that forms the core of the rate limiter
	requestsCounter metric.Int64Counter
}

// NewRateLimiter is a constructor function for RateLimiter.
// It creates a new RateLimiter that allows reqPerSec requests per second.
func NewRateLimiter(reqPerSec int64, opts ...RateLimiterOption) *RateLimiter {
	o := &rateLimiterOptions{meter: internal.Meter}
	for _, opt := range opts {
		opt(o)
	}

	// fillInterval is the amount of time between adding new tokens to the bucket.
	// We want to add a new token reqPerSec times per second, so fillInterval is the inverse of reqPerSec.
	fillInterval := time.Second / time.Duration(reqPerSec)

	// Create a new token bucket with a rate of reqPerSec tokens per second and a capacity of reqPerSec.
	bucket := ratelimit.NewBucket(fillInterval, reqPerSec)

	return &RateLimiter{
		bucket:          bucket,
		requestsCounter: telemetry.NewCounter(o.meter, "rate_limit_requests", "Number of requests checked by the server rate limiter, by result (allowed or exceeded)"),
	}
}

// Limit checks if a request should be allowed based on the current state of the bucket.
// If no tokens are available (i.e., if TakeAvailable(1) returns 0), it means the rate limit has been hit,
// so it returns true. If a token is available, it returns false, meaning the request can proceed.
func (l *RateLimiter) Limit(ctx context.Context) error {
	tokenRes := l.bucket.TakeAvailable(1)

	// When rate limit reached, return specific error for the clients.
	if tokenRes == 0 {
		l.requestsCounter.Add(ctx, 1, rateLimitExceededAttrs)
		return fmt.Errorf("reached Rate-Limiting %d", l.bucket.Available())
	}

	l.requestsCounter.Add(ctx, 1, rateLimitAllowedAttrs)
	// Rate limit isn't reached.
	return nil
}
