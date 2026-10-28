package openid

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Permify/permify/internal/config"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestJWKSHTTPRequestsMetricCountsTransportAttempts(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	counter, err := meter.Int64Counter("oidc_jwks_http_requests")
	require.NoError(t, err)

	statuses := []int{http.StatusOK, http.StatusServiceUnavailable}
	transport := &jwksCountingRoundTripper{
		next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			status := statuses[0]
			statuses = statuses[1:]
			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
				Request:    request,
			}, nil
		}),
		counter: counter,
	}
	for range 2 {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://issuer.example/jwks", nil)
		require.NoError(t, err)
		_, err = transport.RoundTrip(request)
		require.NoError(t, err)
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	results := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, metric := range sm.Metrics {
			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				results[point.Attributes.ToSlice()[0].Value.AsString()] = point.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"ok": 1, "error": 1}, results)
}

func TestOidcAuthnRecordsCacheHTTPAndRetryMetrics(t *testing.T) {
	var srv *httptest.Server
	var provider *fakeOidcProvider
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provider.ServeHTTP(w, r)
	}))
	defer srv.Close()

	var err error
	provider, err = newFakeOidcProvider(ProviderConfig{
		IssuerURL:    srv.URL,
		AuthPath:     "/auth",
		TokenPath:    "/token",
		UserInfoPath: "/userInfo",
		JWKSPath:     "/jwks",
		Algorithms:   []string{"RS256"},
	})
	require.NoError(t, err)

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	ctx := context.Background()
	auth, err := NewOidcAuthn(ctx, config.Oidc{
		Audience:          "aud",
		Issuer:            srv.URL,
		RefreshInterval:   5 * time.Minute,
		BackoffInterval:   5 * time.Second,
		BackoffMaxRetries: 1,
		BackoffFrequency:  time.Millisecond,
	}, WithMeter(meter))
	require.NoError(t, err)

	now := time.Now()
	claims := jwt.RegisteredClaims{
		Issuer:    srv.URL,
		Subject:   "user",
		Audience:  []string{"aud"},
		ExpiresAt: &jwt.NumericDate{Time: now.Add(time.Hour)},
		IssuedAt:  &jwt.NumericDate{Time: now},
	}

	// Authenticate twice with a valid token: AutoRefresh.Fetch should serve from cache without an extra HTTP call.
	validToken := createUnsignedToken(claims, jwt.SigningMethodRS256)
	validToken.Header["kid"] = provider.signingKeyMap[jwt.SigningMethodRS256]
	signedValid, err := provider.SignIDToken(validToken)
	require.NoError(t, err)
	for range 2 {
		md := make(metautils.NiceMD)
		md.Set("authorization", "Bearer "+signedValid)
		require.NoError(t, auth.Authenticate(md.ToIncoming(ctx)))
	}

	// Authenticate with an unknown kid to trigger JWKS refresh and retry exhaustion.
	unknownKidToken := createUnsignedToken(claims, jwt.SigningMethodRS256)
	unknownKidToken.Header["kid"] = "missing-kid"
	signedUnknown, err := provider.SignIDToken(unknownKidToken)
	require.NoError(t, err)
	md := make(metautils.NiceMD)
	md.Set("authorization", "Bearer "+signedUnknown)
	require.Error(t, auth.Authenticate(md.ToIncoming(ctx)))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	metrics := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metrics[m.Name] = m
		}
	}

	authnSum := metrics["authn_requests"].Data.(metricdata.Sum[int64])
	authnByResult := map[string]int64{}
	for _, dp := range authnSum.DataPoints {
		res, _ := dp.Attributes.Value(attribute.Key("result"))
		authnByResult[res.AsString()] += dp.Value
	}
	assert.Equal(t, map[string]int64{"ok": 2, "error": 1}, authnByResult)

	cacheSum := metrics["oidc_jwks_cache_operations"].Data.(metricdata.Sum[int64])
	cacheByOp := map[string]int64{}
	for _, dp := range cacheSum.DataPoints {
		op, _ := dp.Attributes.Value(attribute.Key("op"))
		cacheByOp[op.AsString()] += dp.Value
	}
	assert.Equal(t, int64(5), cacheByOp["fetch"])
	assert.Equal(t, int64(2), cacheByOp["refresh"])

	httpSum := metrics["oidc_jwks_http_requests"].Data.(metricdata.Sum[int64])
	require.Len(t, httpSum.DataPoints, 1)
	assert.Equal(t, int64(3), httpSum.DataPoints[0].Value)

	retriesSum := metrics["oidc_jwks_retries"].Data.(metricdata.Sum[int64])
	retriesByReason := map[string]int64{}
	for _, dp := range retriesSum.DataPoints {
		reason, _ := dp.Attributes.Value(attribute.Key("reason"))
		retriesByReason[reason.AsString()] = dp.Value
	}
	assert.Equal(t, int64(2), retriesByReason["retry"])
	assert.Equal(t, int64(1), retriesByReason["max_retries_exceeded"])
}
