package preshared

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/pkg/errors"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/config"
	base "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
)

var (
	presharedOkAttrs                 = presharedAuthAttrs("ok", "ok")
	presharedMissingBearerTokenAttrs = presharedAuthAttrs("error", "missing_bearer_token")
	presharedInvalidKeyAttrs         = presharedAuthAttrs("error", "invalid_key")
)

func presharedAuthAttrs(result, reason string) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(
		attribute.String("method", "preshared"),
		attribute.String("result", result),
		attribute.String("reason", reason),
	))
}

// KeyAuthnOption - Option type for KeyAuthn
type KeyAuthnOption func(*KeyAuthn)

// WithMeter - Sets the meter used to create KeyAuthn metrics.
func WithMeter(meter metric.Meter) KeyAuthnOption {
	return func(a *KeyAuthn) {
		a.meter = meter
	}
}

// KeyAuthn - Authentication Keys Structure
type KeyAuthn struct {
	keys            map[string]struct{}
	meter           metric.Meter
	requestsCounter metric.Int64Counter
}

// NewKeyAuthn - Create New Authenticated Keys
func NewKeyAuthn(_ context.Context, cfg config.Preshared, opts ...KeyAuthnOption) (*KeyAuthn, error) {
	if len(cfg.Keys) < 1 {
		return nil, errors.New("pre shared key authn must have at least one key")
	}
	mapKeys := make(map[string]struct{})
	for _, k := range cfg.Keys {
		mapKeys[k] = struct{}{}
	}
	a := &KeyAuthn{
		keys:  mapKeys,
		meter: internal.Meter,
	}
	for _, opt := range opts {
		opt(a)
	}
	a.requestsCounter = telemetry.NewCounter(a.meter, "authn_requests", "Number of authentication requests, by method, result and reason")
	return a, nil
}

// Authenticate - Checking whether any API request contain keys
func (a *KeyAuthn) Authenticate(ctx context.Context) error {
	key, err := grpcAuth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		a.requestsCounter.Add(ctx, 1, presharedMissingBearerTokenAttrs)
		return errors.New(base.ErrorCode_ERROR_CODE_MISSING_BEARER_TOKEN.String())
	}
	if _, found := a.keys[key]; found {
		a.requestsCounter.Add(ctx, 1, presharedOkAttrs)
		return nil
	}
	a.requestsCounter.Add(ctx, 1, presharedInvalidKeyAttrs)
	return status.Error(codes.Unauthenticated, base.ErrorCode_ERROR_CODE_INVALID_KEY.String())
}
