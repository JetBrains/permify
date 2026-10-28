package preshared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/metadata"

	"github.com/Permify/permify/internal/config"
)

func TestPresharedAuthnMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	auth, err := NewKeyAuthn(context.Background(), config.Preshared{Keys: []string{"secret"}}, WithMeter(meter))
	require.NoError(t, err)

	// Missing bearer token
	require.Error(t, auth.Authenticate(context.Background()))

	// Invalid key
	badCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong"))
	require.Error(t, auth.Authenticate(badCtx))

	// Valid key
	okCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	require.NoError(t, auth.Authenticate(okCtx))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "authn_requests" {
				continue
			}
			assert.Empty(t, m.Unit)
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				method, _ := dp.Attributes.Value(attribute.Key("method"))
				result, _ := dp.Attributes.Value(attribute.Key("result"))
				reason, _ := dp.Attributes.Value(attribute.Key("reason"))
				got[method.AsString()+"/"+result.AsString()+"/"+reason.AsString()] = dp.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{
		"preshared/error/missing_bearer_token": 1,
		"preshared/error/invalid_key":          1,
		"preshared/ok/ok":                      1,
	}, got)
}
