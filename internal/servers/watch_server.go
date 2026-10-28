package servers

import (
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
		return v // Return validation error, if any.
	}

	// Extract the snapshot token from the request.
	snap := request.GetSnapToken()
	if snap == "" {
		// If the snapshot token is not provided, get the head snapshot from the database.
		st, err := r.dr.HeadSnapshot(ctx, request.GetTenantId())
		if err != nil {
			slog.ErrorContext(ctx, "failed to resolve watch snapshot", "tenant_id", request.GetTenantId(), "error", err.Error())
			return err // If there's an error retrieving the snapshot, return it.
		}
		// Encode the snapshot to a string.
		snap = st.Encode().String()
	}

	// Call the Watch function on the watcher, which returns two channels.
	changes, errs := r.w.Watch(ctx, request.GetTenantId(), snap)

	// Create a separate goroutine to handle sending changes to the server.
	go func() {
		for change := range changes {
			// For each change, send it to the client.
			if err := server.Send(&v1.WatchResponse{Changes: change}); err != nil {
				// If an error occurs while sending, exit the goroutine.
				// The error is not handled here because the context will be cancelled if an error is detected.
				return
			}
		}
	}()

	// Main loop for handling errors.
	for err := range errs {
		// If an error occurs, convert it to a status error and return it.
		// This ends the Watch function, which in turn closes the changes channel and ends the above goroutine.
		return status.Error(GetStatus(err), err.Error())
	}

	// At this point, the errs channel has been closed, indicating that no more errors will be coming in.
	// Therefore, it's safe to return nil indicating that the operation was successful.
	return nil
}
