package servers

import (
	"context"
	"log/slog"

	"github.com/rs/xid"
	"go.opentelemetry.io/otel/attribute"
	api "go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/status"

	otelCodes "go.opentelemetry.io/otel/codes"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/storage"
	"github.com/Permify/permify/pkg/database"
	"github.com/Permify/permify/pkg/dsl/compiler"
	"github.com/Permify/permify/pkg/dsl/parser"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
)

var (
	writeSharedSchemaErrAttrs  = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "write_shared_schema")))
	readSharedSchemaErrAttrs   = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "read_shared_schema")))
	assignSharedSchemaErrAttrs = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "assign_shared_schema")))
	listSharedSchemaErrAttrs   = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "list_shared_schema")))
)

// SharedSchemaServer - Structure for Shared Schema Server
type SharedSchemaServer struct {
	v1.UnimplementedSharedSchemaServer

	ssw                       storage.SharedSchemaWriter
	ssr                       storage.SharedSchemaReader
	writeSharedSchemaCounter  api.Int64Counter
	readSharedSchemaCounter   api.Int64Counter
	listSharedSchemaCounter   api.Int64Counter
	assignSharedSchemaCounter api.Int64Counter
	errorsCounter             api.Int64Counter
}

// NewSharedSchemaServer - Creates new Shared Schema Server
func NewSharedSchemaServer(ssw storage.SharedSchemaWriter, ssr storage.SharedSchemaReader) *SharedSchemaServer {
	return &SharedSchemaServer{
		ssw:                       ssw,
		ssr:                       ssr,
		writeSharedSchemaCounter:  telemetry.NewCounter(internal.Meter, "write_shared_schema", "Number of writing shared schema"),
		readSharedSchemaCounter:   telemetry.NewCounter(internal.Meter, "read_shared_schema", "Number of reading shared schema"),
		listSharedSchemaCounter:   telemetry.NewCounter(internal.Meter, "list_shared_schema", "Number of listing shared schema"),
		assignSharedSchemaCounter: telemetry.NewCounter(internal.Meter, "assign_shared_schema", "Number of assigning shared schema"),
		errorsCounter:             telemetry.NewCounter(internal.Meter, "shared_schema_server_errors", "Number of failed SharedSchemaServer RPC calls, by operation"),
	}
}

// Write - Create or update a shared schema
func (r *SharedSchemaServer) Write(ctx context.Context, request *v1.SharedSchemaWriteRequest) (*v1.SharedSchemaWriteResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "shared-schemas.write")
	defer span.End()

	sch, err := parser.NewParser(request.GetSchema()).Parse()
	if err != nil {
		r.errorsCounter.Add(ctx, 1, writeSharedSchemaErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	_, _, err = compiler.NewCompiler(true, sch).Compile()
	if err != nil {
		r.errorsCounter.Add(ctx, 1, writeSharedSchemaErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	version := xid.New().String()

	cnf := make([]storage.SharedSchemaDefinition, 0, len(sch.Statements))
	for _, st := range sch.Statements {
		cnf = append(cnf, storage.SharedSchemaDefinition{
			SharedSchemaID:       request.GetSharedSchemaId(),
			Version:              version,
			Name:                 st.GetName(),
			SerializedDefinition: []byte(st.String()),
		})
	}

	err = r.ssw.WriteSharedSchema(ctx, cnf)
	if err != nil {
		r.errorsCounter.Add(ctx, 1, writeSharedSchemaErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.writeSharedSchemaCounter.Add(ctx, 1)

	return &v1.SharedSchemaWriteResponse{
		SchemaVersion: version,
	}, nil
}

// Read - Read a shared schema
func (r *SharedSchemaServer) Read(ctx context.Context, request *v1.SharedSchemaReadRequest) (*v1.SharedSchemaReadResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "shared-schemas.read")
	defer span.End()

	version := request.GetMetadata().GetSchemaVersion()
	if version == "" {
		ver, err := r.ssr.SharedHeadVersion(ctx, request.GetSharedSchemaId())
		if err != nil {
			r.errorsCounter.Add(ctx, 1, readSharedSchemaErrAttrs)
			return nil, status.Error(GetStatus(err), err.Error())
		}
		version = ver
	}

	response, err := r.ssr.ReadSharedSchema(ctx, request.GetSharedSchemaId(), version)
	if err != nil {
		r.errorsCounter.Add(ctx, 1, readSharedSchemaErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.readSharedSchemaCounter.Add(ctx, 1)

	return &v1.SharedSchemaReadResponse{
		Schema: response,
	}, nil
}

// Assign - Assign a shared schema to tenants
func (r *SharedSchemaServer) Assign(ctx context.Context, request *v1.SharedSchemaAssignRequest) (*v1.SharedSchemaAssignResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "shared-schemas.assign")
	defer span.End()

	// Validate shared schema exists
	_, err := r.ssr.SharedHeadVersion(ctx, request.GetSharedSchemaId())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, assignSharedSchemaErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	err = r.ssw.AssignSharedSchema(ctx, request.GetSharedSchemaId(), request.GetTenantIds())
	if err != nil {
		r.errorsCounter.Add(ctx, 1, assignSharedSchemaErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.assignSharedSchemaCounter.Add(ctx, 1)

	return &v1.SharedSchemaAssignResponse{
		TenantIds: request.GetTenantIds(),
	}, nil
}

// List - List all shared schemas
func (r *SharedSchemaServer) List(ctx context.Context, request *v1.SharedSchemaListRequest) (*v1.SharedSchemaListResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "shared-schemas.list")
	defer span.End()

	schemas, ct, err := r.ssr.ListSharedSchemas(ctx, database.NewPagination(database.Size(request.GetPageSize()), database.Token(request.GetContinuousToken())))
	if err != nil {
		r.errorsCounter.Add(ctx, 1, listSharedSchemaErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	r.listSharedSchemaCounter.Add(ctx, 1)

	return &v1.SharedSchemaListResponse{
		Schemas:         schemas,
		ContinuousToken: ct.String(),
	}, nil
}
