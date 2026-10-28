package balancer

import (
	"context"
	"errors"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"

	"github.com/Permify/permify/pkg/consistent"
	"github.com/Permify/permify/pkg/telemetry"
)

func TestConsistentBalancerMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")

	subConn := &mockSubConnWrapper{}
	clientConn := &mockClientConn{newSubConn: subConn}
	b := &Balancer{
		clientConn:            clientConn,
		subConnStates:         make(map[balancer.SubConn]connectivity.State),
		connectivityEvaluator: &balancer.ConnectivityStateEvaluator{},
		addressSubConns:       resolver.NewAddressMap(),
		config: &Config{
			PartitionCount:    100,
			ReplicationFactor: 3,
			Load:              1.25,
			PickerWidth:       2,
		},
		consistent: consistent.New(consistent.Config{
			PartitionCount:    100,
			ReplicationFactor: 3,
			Load:              1.25,
			Hasher:            xxhash.Sum64,
		}),
		resolverUpdatesCounter: telemetry.NewCounter(meter, "balancer_resolver_updates", "test"),
		subConnEventsCounter:   telemetry.NewCounter(meter, "balancer_subconn_events", "test"),
	}

	err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: resolver.State{
			Addresses: []resolver.Address{{Addr: "127.0.0.1:50051", ServerName: "s1"}},
		},
		BalancerConfig: b.config,
	})
	require.NoError(t, err)

	b.UpdateSubConnState(subConn, balancer.SubConnState{ConnectivityState: connectivity.Ready})
	b.UpdateSubConnState(subConn, balancer.SubConnState{
		ConnectivityState: connectivity.TransientFailure,
		ConnectionError:   errors.New("conn err"),
	})
	b.UpdateSubConnState(subConn, balancer.SubConnState{ConnectivityState: connectivity.Shutdown})
	b.UpdateSubConnState(&mockSubConnWrapper{}, balancer.SubConnState{ConnectivityState: connectivity.Ready})

	err = b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState:  resolver.State{Addresses: nil},
		BalancerConfig: b.config,
	})
	require.ErrorIs(t, err, balancer.ErrBadResolverState)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	resolverResults := map[string]int64{}
	subConnEvents := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			switch m.Name {
			case "balancer_resolver_updates":
				for _, dp := range sum.DataPoints {
					res, _ := dp.Attributes.Value(attribute.Key("result"))
					resolverResults[res.AsString()] = dp.Value
				}
			case "balancer_subconn_events":
				for _, dp := range sum.DataPoints {
					ev, _ := dp.Attributes.Value(attribute.Key("event"))
					subConnEvents[ev.AsString()] = dp.Value
				}
			}
		}
	}

	assert.Equal(t, map[string]int64{
		"ok":             1,
		"resolver_error": 1,
		"zero_addresses": 1,
	}, resolverResults)
	assert.Equal(t, map[string]int64{
		"created":           1,
		"ready":             1,
		"transient_failure": 1,
		"shutdown":          1,
		"unknown":           1,
	}, subConnEvents)
}
