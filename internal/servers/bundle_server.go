package servers

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	otelCodes "go.opentelemetry.io/otel/codes"
	api "go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/status"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/internal/validation"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
)

var (
	writeBundleErrAttrs  = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "write_bundle")))
	readBundleErrAttrs   = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "read_bundle")))
	deleteBundleErrAttrs = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "delete_bundle")))
)

// BundleServer handles bundle operations.
type BundleServer struct {
	v1.UnimplementedBundleServer

	br                  storage.BundleReader
	bw                  storage.BundleWriter
	writeBundleCounter  api.Int64Counter
	readBundleCounter   api.Int64Counter
	deleteBundleCounter api.Int64Counter
	errorsCounter       api.Int64Counter
}

func NewBundleServer(
	br storage.BundleReader,
	bw storage.BundleWriter,
) *BundleServer {
	return &BundleServer{
		br:                  br,
		bw:                  bw,
		writeBundleCounter:  telemetry.NewCounter(internal.Meter, "write_bundle", "Number of writing bundles"),
		readBundleCounter:   telemetry.NewCounter(internal.Meter, "read_bundle", "Number of reading bundles"),
		deleteBundleCounter: telemetry.NewCounter(internal.Meter, "delete_bundle", "Number of deleting bundles"),
		errorsCounter:       telemetry.NewCounter(internal.Meter, "bundle_server_errors", "Number of failed BundleServer RPC calls, by operation"),
	}
}

// Write handles the writing of bundles.
func (r *BundleServer) Write(ctx context.Context, request *v1.BundleWriteRequest) (*v1.BundleWriteResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "bundle.write")
	defer span.End()

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, writeBundleErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	for _, bundle := range request.GetBundles() {
		for _, operation := range bundle.GetOperations() {
			err := validation.ValidateBundleOperation(operation)
			if err != nil {
				r.errorsCounter.Add(ctx, 1, writeBundleErrAttrs)
				return nil, status.Error(GetStatus(err), err.Error()) // Return operation validation error
			}
		}
	}

	var bundles []storage.Bundle
	for _, b := range request.GetBundles() {
		bundles = append(bundles, storage.Bundle{
			Name:       b.GetName(),
			DataBundle: b,
			TenantID:   request.GetTenantId(),
		})
	}

	names, err := r.bw.Write(ctx, bundles)
	if err != nil {
		r.errorsCounter.Add(ctx, 1, writeBundleErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.writeBundleCounter.Add(ctx, 1)

	return &v1.BundleWriteResponse{
		Names: names,
	}, nil
}

// Read handles the reading of bundles.
func (r *BundleServer) Read(ctx context.Context, request *v1.BundleReadRequest) (*v1.BundleReadResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "bundle.read")
	defer span.End()

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, readBundleErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	bundle, err := r.br.Read(ctx, request.GetTenantId(), request.GetName())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, readBundleErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.readBundleCounter.Add(ctx, 1)

	return &v1.BundleReadResponse{
		Bundle: bundle,
	}, nil
}

// Delete handles the deletion of bundles.
func (r *BundleServer) Delete(ctx context.Context, request *v1.BundleDeleteRequest) (*v1.BundleDeleteResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "bundle.delete")
	defer span.End()

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, deleteBundleErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	err := r.bw.Delete(ctx, request.GetTenantId(), request.GetName())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, deleteBundleErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.deleteBundleCounter.Add(ctx, 1)

	return &v1.BundleDeleteResponse{
		Name: request.GetName(),
	}, nil
}
