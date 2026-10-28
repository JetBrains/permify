package servers

import (
	"context"
	"log/slog"

	otelAttribute "go.opentelemetry.io/otel/attribute"
	otelCodes "go.opentelemetry.io/otel/codes"
	api "go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/status"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/internal/validation"
	"github.com/Permify/permify/pkg/attribute"
	"github.com/Permify/permify/pkg/database"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
	"github.com/Permify/permify/pkg/tuple"
)

var (
	writeDataErrAttrs           = api.WithAttributeSet(otelAttribute.NewSet(otelAttribute.String("op", "write_data")))
	deleteDataErrAttrs          = api.WithAttributeSet(otelAttribute.NewSet(otelAttribute.String("op", "delete_data")))
	readAttributesErrAttrs      = api.WithAttributeSet(otelAttribute.NewSet(otelAttribute.String("op", "read_attributes")))
	readRelationshipsErrAttrs   = api.WithAttributeSet(otelAttribute.NewSet(otelAttribute.String("op", "read_relationships")))
	writeRelationshipsErrAttrs  = api.WithAttributeSet(otelAttribute.NewSet(otelAttribute.String("op", "write_relationships")))
	deleteRelationshipsErrAttrs = api.WithAttributeSet(otelAttribute.NewSet(otelAttribute.String("op", "delete_relationships")))
	runBundleErrAttrs           = api.WithAttributeSet(otelAttribute.NewSet(otelAttribute.String("op", "run_bundle")))
)

// DataServer - Structure for Data Server
type DataServer struct {
	v1.UnimplementedDataServer

	sr                         storage.SchemaReader
	dr                         storage.DataReader
	br                         storage.BundleReader
	dw                         storage.DataWriter
	writeDataCounter           api.Int64Counter
	deleteDataCounter          api.Int64Counter
	readAttributesCounter      api.Int64Counter
	readRelationshipsCounter   api.Int64Counter
	writeRelationshipsCounter  api.Int64Counter
	deleteRelationshipsCounter api.Int64Counter
	runBundleCounter           api.Int64Counter
	errorsCounter              api.Int64Counter
}

// NewDataServer - Creates new Data Server
func NewDataServer(
	dr storage.DataReader,
	dw storage.DataWriter,
	br storage.BundleReader,
	sr storage.SchemaReader,
) *DataServer {
	return &DataServer{
		dr:                         dr,
		dw:                         dw,
		br:                         br,
		sr:                         sr,
		writeDataCounter:           telemetry.NewCounter(internal.Meter, "write_data", "Number of writing data"),
		deleteDataCounter:          telemetry.NewCounter(internal.Meter, "delete_data", "Number of deleting data"),
		readAttributesCounter:      telemetry.NewCounter(internal.Meter, "read_attributes", "Number of reading attributes"),
		readRelationshipsCounter:   telemetry.NewCounter(internal.Meter, "read_relationships", "Number of reading relationships"),
		writeRelationshipsCounter:  telemetry.NewCounter(internal.Meter, "write_relationships", "Number of writing relationships"),
		deleteRelationshipsCounter: telemetry.NewCounter(internal.Meter, "delete_relationships", "Number of deleting relationships"),
		runBundleCounter:           telemetry.NewCounter(internal.Meter, "run_bundle", "Number of running bundle"),
		errorsCounter:              telemetry.NewCounter(internal.Meter, "data_server_errors", "Number of failed DataServer RPC calls, by operation"),
	}
}

// ReadRelationships - Allows directly querying the stored engines data to display and filter stored relational tuples
func (r *DataServer) ReadRelationships(ctx context.Context, request *v1.RelationshipReadRequest) (*v1.RelationshipReadResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "data.read.relationships")
	defer span.End()

	size := request.GetPageSize()
	if size == 0 {
		size = 50
	}

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, readRelationshipsErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	snap := request.GetMetadata().GetSnapToken()
	if snap == "" {
		st, err := r.dr.HeadSnapshot(ctx, request.GetTenantId())
		if err != nil {
			r.errorsCounter.Add(ctx, 1, readRelationshipsErrAttrs)
			return nil, status.Error(GetStatus(err), err.Error()) // Return snapshot error
		}
		snap = st.Encode().String()
	}

	collection, ct, err := r.dr.ReadRelationships(
		ctx,
		request.GetTenantId(),
		request.GetFilter(),
		snap,
		database.NewPagination(
			database.Size(size),
			database.Token(request.GetContinuousToken()),
		),
	)
	if err != nil {
		r.errorsCounter.Add(ctx, 1, readRelationshipsErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.readRelationshipsCounter.Add(ctx, 1)

	return &v1.RelationshipReadResponse{
		Tuples:          collection.GetTuples(),
		ContinuousToken: ct.String(),
	}, nil
}

// ReadAttributes - Allows directly querying the stored engines data to display and filter stored attribute tuples
func (r *DataServer) ReadAttributes(ctx context.Context, request *v1.AttributeReadRequest) (*v1.AttributeReadResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "data.read.attributes")
	defer span.End()

	size := request.GetPageSize()
	if size == 0 {
		size = 50
	}

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, readAttributesErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	snap := request.GetMetadata().GetSnapToken()
	if snap == "" {
		st, err := r.dr.HeadSnapshot(ctx, request.GetTenantId())
		if err != nil {
			r.errorsCounter.Add(ctx, 1, readAttributesErrAttrs)
			return nil, status.Error(GetStatus(err), err.Error()) // Return snapshot error
		}
		snap = st.Encode().String()
	}

	collection, ct, err := r.dr.ReadAttributes(
		ctx,
		request.GetTenantId(),
		request.GetFilter(),
		snap,
		database.NewPagination(
			database.Size(size),
			database.Token(request.GetContinuousToken()),
		),
	)
	if err != nil {
		r.errorsCounter.Add(ctx, 1, readAttributesErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.readAttributesCounter.Add(ctx, 1)

	return &v1.AttributeReadResponse{
		Attributes:      collection.GetAttributes(),
		ContinuousToken: ct.String(),
	}, nil
}

// Write - Write relationships and attributes to writeDB
func (r *DataServer) Write(ctx context.Context, request *v1.DataWriteRequest) (*v1.DataWriteResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "data.write")
	defer span.End()

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, writeDataErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	version := request.GetMetadata().GetSchemaVersion()
	sharedSchemaID := request.GetMetadata().GetSharedSchemaId()
	if version == "" {
		var err error
		sharedSchemaID, version, err = r.sr.HeadVersion(ctx, request.GetTenantId())
		if err != nil {
			r.errorsCounter.Add(ctx, 1, writeDataErrAttrs)
			span.RecordError(err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, status.Error(GetStatus(err), err.Error()) // Return version error
		}
	}

	relationships := make([]*v1.Tuple, 0, len(request.GetTuples()))

	relationshipsMap := map[string]struct{}{}

	for _, tup := range request.GetTuples() {
		key := tuple.ToString(tup)

		if _, ok := relationshipsMap[key]; ok {
			continue
		}

		relationshipsMap[key] = struct{}{}

		definition, _, err := r.sr.ReadEntityDefinition(ctx, request.GetTenantId(), sharedSchemaID, tup.GetEntity().GetType(), version)
		if err != nil {
			r.errorsCounter.Add(ctx, 1, writeDataErrAttrs)
			span.RecordError(err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, status.Error(GetStatus(err), err.Error()) // Return entity definition error
		}

		err = validation.ValidateTuple(definition, tup)
		if err != nil {
			r.errorsCounter.Add(ctx, 1, writeDataErrAttrs)
			span.RecordError(err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, status.Error(GetStatus(err), err.Error()) // Return tuple validation error
		}

		relationships = append(relationships, tup)
	}

	attrs := make([]*v1.Attribute, 0, len(request.GetAttributes()))

	attributesMap := map[string]struct{}{}

	for _, attr := range request.GetAttributes() {
		key := attribute.EntityAndAttributeToString(attr.GetEntity(), attr.GetAttribute())

		if _, ok := attributesMap[key]; ok {
			continue
		}

		attributesMap[key] = struct{}{}

		definition, _, err := r.sr.ReadEntityDefinition(ctx, request.GetTenantId(), sharedSchemaID, attr.GetEntity().GetType(), version)
		if err != nil {
			r.errorsCounter.Add(ctx, 1, writeDataErrAttrs)
			span.RecordError(err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, status.Error(GetStatus(err), err.Error()) // Return entity definition error
		}

		err = validation.ValidateAttribute(definition, attr)
		if err != nil {
			r.errorsCounter.Add(ctx, 1, writeDataErrAttrs)
			span.RecordError(err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, status.Error(GetStatus(err), err.Error()) // Return attribute validation error
		}

		attrs = append(attrs, attr)
	}

	snap, err := r.dw.Write(ctx, request.GetTenantId(), database.NewTupleCollection(relationships...), database.NewAttributeCollection(attrs...))
	if err != nil {
		r.errorsCounter.Add(ctx, 1, writeDataErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.writeDataCounter.Add(ctx, 1)

	return &v1.DataWriteResponse{
		SnapToken: snap.String(),
	}, nil
}

// WriteRelationships - Write relation tuples to writeDB
func (r *DataServer) WriteRelationships(ctx context.Context, request *v1.RelationshipWriteRequest) (*v1.RelationshipWriteResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "relationships.write")
	defer span.End()

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, writeRelationshipsErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	version := request.GetMetadata().GetSchemaVersion()
	sharedSchemaID := request.GetMetadata().GetSharedSchemaId()
	if version == "" {
		var err error
		sharedSchemaID, version, err = r.sr.HeadVersion(ctx, request.GetTenantId())
		if err != nil {
			r.errorsCounter.Add(ctx, 1, writeRelationshipsErrAttrs)
			span.RecordError(err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, status.Error(GetStatus(err), err.Error()) // Return version error
		}
	}

	relationships := make([]*v1.Tuple, 0, len(request.GetTuples()))

	relationshipsMap := map[string]struct{}{}

	for _, tup := range request.GetTuples() {
		key := tuple.ToString(tup)

		if _, ok := relationshipsMap[key]; ok {
			continue
		}

		relationshipsMap[key] = struct{}{}

		definition, _, err := r.sr.ReadEntityDefinition(ctx, request.GetTenantId(), sharedSchemaID, tup.GetEntity().GetType(), version)
		if err != nil {
			r.errorsCounter.Add(ctx, 1, writeRelationshipsErrAttrs)
			span.RecordError(err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, status.Error(GetStatus(err), err.Error()) // Return entity definition error
		}

		err = validation.ValidateTuple(definition, tup)
		if err != nil {
			r.errorsCounter.Add(ctx, 1, writeRelationshipsErrAttrs)
			span.RecordError(err)
			span.SetStatus(otelCodes.Error, err.Error())
			return nil, status.Error(GetStatus(err), err.Error()) // Return tuple validation error
		}

		relationships = append(relationships, tup)
	}

	snap, err := r.dw.Write(ctx, request.GetTenantId(), database.NewTupleCollection(relationships...), database.NewAttributeCollection())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, writeRelationshipsErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.writeRelationshipsCounter.Add(ctx, 1)

	return &v1.RelationshipWriteResponse{
		SnapToken: snap.String(),
	}, nil
}

// Delete - Delete relationships and attributes from writeDB
func (r *DataServer) Delete(ctx context.Context, request *v1.DataDeleteRequest) (*v1.DataDeleteResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "data.delete")
	defer span.End()

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, deleteDataErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	err := validation.ValidateFilters(request.GetTupleFilter(), request.GetAttributeFilter())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, deleteDataErrAttrs)
		return nil, status.Error(GetStatus(err), err.Error())
	}

	snap, err := r.dw.Delete(ctx, request.GetTenantId(), request.GetTupleFilter(), request.GetAttributeFilter())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, deleteDataErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.deleteDataCounter.Add(ctx, 1)

	return &v1.DataDeleteResponse{
		SnapToken: snap.String(),
	}, nil
}

// DeleteRelationships - Delete relationships from writeDB
func (r *DataServer) DeleteRelationships(ctx context.Context, request *v1.RelationshipDeleteRequest) (*v1.RelationshipDeleteResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "relationships.delete")
	defer span.End()

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, deleteRelationshipsErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	err := validation.ValidateTupleFilter(request.GetFilter())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, deleteRelationshipsErrAttrs)
		return nil, status.Error(GetStatus(err), err.Error())
	}

	snap, err := r.dw.Delete(ctx, request.GetTenantId(), request.GetFilter(), &v1.AttributeFilter{})
	if err != nil {
		r.errorsCounter.Add(ctx, 1, deleteRelationshipsErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.deleteRelationshipsCounter.Add(ctx, 1)

	return &v1.RelationshipDeleteResponse{
		SnapToken: snap.String(),
	}, nil
}

// RunBundle executes a bundle and returns its snapshot token.
func (r *DataServer) RunBundle(ctx context.Context, request *v1.BundleRunRequest) (*v1.BundleRunResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "bundle.run")
	defer span.End()

	v := request.Validate()
	if v != nil {
		r.errorsCounter.Add(ctx, 1, runBundleErrAttrs)
		return nil, status.Error(GetStatus(v), v.Error()) // Return validation error
	}

	bundle, err := r.br.Read(ctx, request.GetTenantId(), request.GetName())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, runBundleErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	err = validation.ValidateBundleArguments(bundle.GetArguments(), request.GetArguments())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, runBundleErrAttrs)
		return nil, status.Error(GetStatus(err), err.Error()) // Return bundle argument validation error
	}

	snap, err := r.dw.RunBundle(ctx, request.GetTenantId(), request.GetArguments(), bundle)
	if err != nil {
		r.errorsCounter.Add(ctx, 1, runBundleErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.runBundleCounter.Add(ctx, 1)

	return &v1.BundleRunResponse{
		SnapToken: snap.String(),
	}, nil
}
