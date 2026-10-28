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
	"github.com/Permify/permify/pkg/database"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
)

var (
	createTenantErrAttrs = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "create_tenant")))
	deleteTenantErrAttrs = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "delete_tenant")))
	listTenantErrAttrs   = api.WithAttributeSet(attribute.NewSet(attribute.String("op", "list_tenant")))
)

// TenancyServer - Structure for Tenancy Server
type TenancyServer struct {
	v1.UnimplementedTenancyServer

	tr                  storage.TenantReader
	tw                  storage.TenantWriter
	createTenantCounter api.Int64Counter
	deleteTenantCounter api.Int64Counter
	listTenantCounter   api.Int64Counter
	errorsCounter       api.Int64Counter
}

// NewTenancyServer - Creates new Tenancy Server
func NewTenancyServer(tr storage.TenantReader, tw storage.TenantWriter) *TenancyServer {
	return &TenancyServer{
		tr:                  tr,
		tw:                  tw,
		createTenantCounter: telemetry.NewCounter(internal.Meter, "create_tenant", "Number of creating tenants"),
		deleteTenantCounter: telemetry.NewCounter(internal.Meter, "delete_tenant", "Number of deleting tenants"),
		listTenantCounter:   telemetry.NewCounter(internal.Meter, "list_tenant", "Number of listing tenants"),
		errorsCounter:       telemetry.NewCounter(internal.Meter, "tenancy_server_errors", "Number of failed TenancyServer RPC calls, by operation"),
	}
}

// Create - Create new Tenant
func (t *TenancyServer) Create(ctx context.Context, request *v1.TenantCreateRequest) (*v1.TenantCreateResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "tenant.create")
	defer span.End()

	tenant, err := t.tw.CreateTenant(ctx, request.GetId(), request.GetName())
	if err != nil {
		t.errorsCounter.Add(ctx, 1, createTenantErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	t.createTenantCounter.Add(ctx, 1)

	return &v1.TenantCreateResponse{
		Tenant: tenant,
	}, nil
}

// Delete - Delete a Tenant
func (t *TenancyServer) Delete(ctx context.Context, request *v1.TenantDeleteRequest) (*v1.TenantDeleteResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "tenant.delete")
	defer span.End()

	id := request.GetId()
	err := t.tw.DeleteTenant(ctx, id)
	if err != nil {
		t.errorsCounter.Add(ctx, 1, deleteTenantErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	t.deleteTenantCounter.Add(ctx, 1)

	return &v1.TenantDeleteResponse{
		TenantId: id,
	}, nil
}

// List - List Tenants
func (t *TenancyServer) List(ctx context.Context, request *v1.TenantListRequest) (*v1.TenantListResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "tenant.list")
	defer span.End()

	tenants, ct, err := t.tr.ListTenants(ctx, database.NewPagination(database.Size(request.GetPageSize()), database.Token(request.GetContinuousToken())))
	if err != nil {
		t.errorsCounter.Add(ctx, 1, listTenantErrAttrs)
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, err.Error())
		return nil, status.Error(GetStatus(err), err.Error())
	}

	t.listTenantCounter.Add(ctx, 1)

	return &v1.TenantListResponse{
		Tenants:         tenants,
		ContinuousToken: ct.String(),
	}, nil
}
