package servers

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/storage"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
)

var (
	watchServerOkAttrs        = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "ok")))
	watchServerErrorAttrs     = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "error")))
	watchServerCancelledAttrs = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "cancelled")))
)

type WatchServer struct {
	v1.UnimplementedWatchServer

	dr          storage.DataReader
	w           storage.Watcher
	runsCounter metric.Int64Counter
}

func NewWatchServer(
	w storage.Watcher,
	dr storage.DataReader,
) *WatchServer {
	return &WatchServer{
		w:           w,
		dr:          dr,
		runsCounter: telemetry.NewCounter(internal.Meter, "watch_server_runs", "Number of Watch RPC streams completed, by result"),
	}
}

// Watch function sets up a stream for the client to receive changes.
func (r *WatchServer) Watch(request *v1.WatchRequest, server v1.Watch_WatchServer) (resultErr error) {
	// Start a new context and span for tracing.
	ctx, span := internal.Tracer.Start(server.Context(), "watch.watch")
	defer span.End() // Ensure the span ends when the function returns.
	defer func() {
		attrs := watchServerOkAttrs
		if resultErr != nil {
			attrs = watchServerErrorAttrs
			if status.Code(resultErr) == codes.Canceled || ctx.Err() != nil {
				attrs = watchServerCancelledAttrs
			}
		}
		r.runsCounter.Add(ctx, 1, attrs)
	}()

	// Validate the incoming request.
	v := request.Validate()
	if v != nil {
		slog.WarnContext(ctx, "watch request validation failed", "tenant_id", request.GetTenantId(), "error", v.Error())
		return status.Error(GetStatus(v), v.Error()) // Return validation error, if any.
	}

	// Extract the snapshot token from the request.
	snap := request.GetSnapToken()
	if snap == "" {
		// If the snapshot token is not provided, get the head snapshot from the database.
		st, err := r.dr.HeadSnapshot(ctx, request.GetTenantId())
		if err != nil {
			slog.ErrorContext(ctx, "failed to resolve watch snapshot", "tenant_id", request.GetTenantId(), "error", err.Error())
			return status.Error(GetStatus(err), err.Error()) // If there's an error retrieving the snapshot, return it.
		}
		// Encode the snapshot to a string.
		snap = st.Encode().String()
	}

	// Call the Watch function on the watcher, which returns two channels.
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	changes, errs := r.w.Watch(watchCtx, request.GetTenantId(), snap)

	// Create a separate goroutine to handle sending changes to the server.
	sendErrCh := make(chan error, 1)
	sendDone := make(chan struct{})
	stopSender := func() {
		cancel()
		if sendDone != nil {
			<-sendDone
			sendDone = nil
		}
	}
	defer stopSender()

	go func() {
		defer close(sendDone)
		for {
			select {
			case <-watchCtx.Done():
				return
			case change, ok := <-changes:
				if !ok {
					return
				}
				if watchCtx.Err() != nil {
					return
				}
				// For each change, send it to the client.
				if err := server.Send(&v1.WatchResponse{Changes: change}); err != nil {
					slog.ErrorContext(ctx, "failed to send watch response", "tenant_id", request.GetTenantId(), "error", err.Error())
					sendErrCh <- err
					return
				}
			}
		}
	}()

	// Main loop for handling errors.
	for {
		select {
		case err, ok := <-errs:
			if !ok {
				errs = nil
				if sendDone == nil {
					if ctx.Err() != nil {
						return status.Error(codes.Canceled, ctx.Err().Error())
					}
					return nil
				}
				continue
			}
			if err != nil {
				return status.Error(GetStatus(err), err.Error())
			}
		case err := <-sendErrCh:
			return status.Error(GetStatus(err), err.Error())
		case <-sendDone:
			sendDone = nil
			select {
			case err := <-sendErrCh:
				return status.Error(GetStatus(err), err.Error())
			default:
				if errs == nil && ctx.Err() != nil {
					return status.Error(codes.Canceled, ctx.Err().Error())
				}
				if errs == nil {
					return nil
				}
			}
		case <-ctx.Done():
			return status.Error(codes.Canceled, ctx.Err().Error())
		}
	}
}
