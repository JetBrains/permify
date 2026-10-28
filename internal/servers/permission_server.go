package servers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	otelCodes "go.opentelemetry.io/otel/codes"
	api "go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/invoke"
	v1 "github.com/Permify/permify/pkg/pb/base/v1"
	"github.com/Permify/permify/pkg/telemetry"
)

var (
	bulkCheckItemValidationAttrs        = api.WithAttributeSet(attribute.NewSet(attribute.String("reason", "validation")))
	permissionErrorAttrsByMethodAndCode = func() map[string]map[codes.Code]api.MeasurementOption {
		methods := []string{
			"check",
			"bulk_check",
			"expand",
			"lookup_entity",
			"lookup_entity_stream",
			"lookup_subject",
			"subject_permission",
		}
		out := make(map[string]map[codes.Code]api.MeasurementOption, len(methods))
		for _, m := range methods {
			byCode := make(map[codes.Code]api.MeasurementOption, 17)
			for c := codes.OK; c <= codes.Unauthenticated; c++ {
				byCode[c] = api.WithAttributeSet(attribute.NewSet(
					attribute.String("method", m),
					attribute.String("code", c.String()),
				))
			}
			out[m] = byCode
		}
		return out
	}()
)

func permissionErrorAttrs(method string, err error) api.MeasurementOption {
	code := status.Code(err)
	if byCode, ok := permissionErrorAttrsByMethodAndCode[method]; ok {
		if opt, ok := byCode[code]; ok {
			return opt
		}
	}
	return api.WithAttributeSet(attribute.NewSet(
		attribute.String("method", method),
		attribute.String("code", code.String()),
	))
}

// PermissionServer - Structure for Permission Server
type PermissionServer struct {
	v1.UnimplementedPermissionServer

	invoker                  invoke.Invoker
	concurrencyLimit         int
	bulkLimit                int
	bulkLimitExceededCounter api.Int64Counter
	bulkBatchErrorsCounter   api.Int64Counter
	bulkItemErrorsCounter    api.Int64Counter
	errorsCounter            api.Int64Counter
}

// NewPermissionServer - Creates new Permission Server
func NewPermissionServer(i invoke.Invoker, concurrencyLimit, bulkLimit int) *PermissionServer {
	if concurrencyLimit <= 0 {
		concurrencyLimit = invoke.DefaultConcurrencyLimit
	}
	if bulkLimit <= 0 {
		bulkLimit = 100
	}
	return &PermissionServer{
		invoker:                  i,
		concurrencyLimit:         concurrencyLimit,
		bulkLimit:                bulkLimit,
		bulkLimitExceededCounter: telemetry.NewCounter(internal.Meter, "bulk_check_limit_exceeded", "Number of BulkCheck requests rejected because the number of items exceeded bulk_limit"),
		bulkBatchErrorsCounter:   telemetry.NewCounter(internal.Meter, "bulk_check_batch_errors", "Number of batch check groups that failed inside a BulkCheck request"),
		bulkItemErrorsCounter:    telemetry.NewCounter(internal.Meter, "bulk_check_item_errors", "Number of invalid items returned as denied results in BulkCheck responses, by reason"),
		errorsCounter:            telemetry.NewCounter(internal.Meter, "permission_server_errors", "Number of PermissionServer method calls that returned a gRPC error, by method"),
	}
}

// Check - Performs Authorization Check
func (r *PermissionServer) Check(ctx context.Context, request *v1.PermissionCheckRequest) (*v1.PermissionCheckResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "permissions.check")
	defer span.End()
	ctx = invoke.WithConcurrencySemaphore(ctx, semaphore.NewWeighted(int64(r.concurrencyLimit)))

	v := request.Validate()
	if v != nil {
		rpcErr := status.Error(GetStatus(v), v.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("check", rpcErr))
		return nil, rpcErr // Return validation error
	}

	response, err := r.invoker.Check(ctx, invoke.NewBatchCheckRequest(request))
	if err != nil {
		rpcErr := status.Error(GetStatus(err), err.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("check", rpcErr))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, "permission check failed",
			"tenant_id", request.GetTenantId(),
			"entity_type", request.GetEntity().GetType(),
			"permission", request.GetPermission(),
			"error", err.Error(),
		)
		return nil, rpcErr
	}

	return response.ToPermissionCheckResponse(), nil
}

// BulkCheck - Performs multiple authorization checks in a single request
func (r *PermissionServer) BulkCheck(ctx context.Context, request *v1.PermissionBulkCheckRequest) (*v1.PermissionBulkCheckResponse, error) {
	// emptyResp is a default, empty response that we will return in case of an error or when the context is cancelled.
	emptyResp := &v1.PermissionBulkCheckResponse{
		Results: make([]*v1.PermissionCheckResponse, 0),
	}

	ctx, span := internal.Tracer.Start(ctx, "permissions.bulk-check")
	defer span.End()
	ctx = invoke.WithConcurrencySemaphore(ctx, semaphore.NewWeighted(int64(r.concurrencyLimit)))

	// Validate tenant_id
	if request.GetTenantId() == "" {
		err := status.Error(GetStatus(nil), "tenant_id is required")
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("bulk_check", err))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	checkItems := request.GetItems()

	// Validate number of requests
	if len(checkItems) == 0 {
		err := status.Error(GetStatus(nil), "at least one item is required")
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("bulk_check", err))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	if len(checkItems) > r.bulkLimit {
		r.bulkLimitExceededCounter.Add(ctx, 1)
		err := status.Error(GetStatus(nil), fmt.Sprintf("maximum %d items allowed", r.bulkLimit))
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("bulk_check", err))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		return nil, err
	}

	// Group items by (entityType, permission, subject) for batch processing.
	// Items in the same group share a single BatchCheckRequest.
	type groupKey struct {
		entityType, permission, subjectType, subjectID, subjectRelation string
	}
	type groupEntry struct {
		entityIDs []string
		indices   []int
		subject   *v1.Subject
	}

	results := make([]*v1.PermissionCheckResponse, len(checkItems))
	groups := map[groupKey]*groupEntry{}

	for i, item := range checkItems {
		if v := item.Validate(); v != nil {
			r.bulkItemErrorsCounter.Add(ctx, 1, bulkCheckItemValidationAttrs)
			slog.WarnContext(ctx, "invalid item in bulk check", "tenant_id", request.GetTenantId(), "item_index", i, "error", v.Error())
			results[i] = &v1.PermissionCheckResponse{
				Can:      v1.CheckResult_CHECK_RESULT_DENIED,
				Metadata: &v1.PermissionCheckResponseMetadata{},
			}
			continue
		}

		subj := item.GetSubject()
		key := groupKey{
			entityType:      item.GetEntity().GetType(),
			permission:      item.GetPermission(),
			subjectType:     subj.GetType(),
			subjectID:       subj.GetId(),
			subjectRelation: subj.GetRelation(),
		}
		g, ok := groups[key]
		if !ok {
			g = &groupEntry{subject: subj}
			groups[key] = g
		}
		g.entityIDs = append(g.entityIDs, item.GetEntity().GetId())
		g.indices = append(g.indices, i)
	}

	// Execute each group as a batch check, concurrently.
	var wg sync.WaitGroup
	for key, g := range groups {
		wg.Add(1)
		go func(key groupKey, g *groupEntry) {
			defer wg.Done()

			batchReq := &invoke.BatchCheckRequest{
				TenantID:   request.GetTenantId(),
				EntityType: key.entityType,
				EntityIDs:  g.entityIDs,
				Permission: key.permission,
				Subject:    g.subject,
				Metadata:   request.GetMetadata(),
				Context:    request.GetContext(),
				Arguments:  request.GetArguments(),
			}

			resp, err := r.invoker.Check(ctx, batchReq)
			if err != nil {
				r.bulkBatchErrorsCounter.Add(ctx, 1)
				slog.ErrorContext(ctx, "batch check failed in bulk operation", "tenant_id", request.GetTenantId(), "entity_type", key.entityType, "permission", key.permission, "error", err.Error())
				for _, idx := range g.indices {
					results[idx] = &v1.PermissionCheckResponse{
						Can:      v1.CheckResult_CHECK_RESULT_DENIED,
						Metadata: &v1.PermissionCheckResponseMetadata{CheckCount: 1},
					}
				}
				return
			}

			// Map batch results back to original indices.
			for j, entityID := range g.entityIDs {
				result := v1.CheckResult_CHECK_RESULT_DENIED
				if r, ok := resp.Results[entityID]; ok {
					result = r
				}
				results[g.indices[j]] = &v1.PermissionCheckResponse{
					Can:      result,
					Metadata: resp.Metadata,
				}
			}
		}(key, g)
	}
	wg.Wait()

	// Check for context cancellation.
	if ctx.Err() != nil {
		err := status.Error(codes.Canceled, v1.ErrorCode_ERROR_CODE_CANCELLED.String())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("bulk_check", err))
		return emptyResp, err
	}
	return &v1.PermissionBulkCheckResponse{
		Results: results,
	}, nil
}

// Expand - Get schema actions in a tree structure
func (r *PermissionServer) Expand(ctx context.Context, request *v1.PermissionExpandRequest) (*v1.PermissionExpandResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "permissions.expand")
	defer span.End()
	ctx = invoke.WithConcurrencySemaphore(ctx, semaphore.NewWeighted(int64(r.concurrencyLimit)))

	v := request.Validate()
	if v != nil {
		rpcErr := status.Error(GetStatus(v), v.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("expand", rpcErr))
		return nil, rpcErr // Return validation error
	}

	response, err := r.invoker.Expand(ctx, request)
	if err != nil {
		rpcErr := status.Error(GetStatus(err), err.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("expand", rpcErr))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, "permission expand failed",
			"tenant_id", request.GetTenantId(),
			"entity_type", request.GetEntity().GetType(),
			"permission", request.GetPermission(),
			"error", err.Error(),
		)
		return nil, rpcErr
	}

	return response, nil
}

// LookupEntity -
func (r *PermissionServer) LookupEntity(ctx context.Context, request *v1.PermissionLookupEntityRequest) (*v1.PermissionLookupEntityResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "permissions.lookup-entity")
	defer span.End()
	ctx = invoke.WithConcurrencySemaphore(ctx, semaphore.NewWeighted(int64(r.concurrencyLimit)))

	v := request.Validate()
	if v != nil {
		rpcErr := status.Error(GetStatus(v), v.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("lookup_entity", rpcErr))
		return nil, rpcErr // Return validation error
	}

	response, err := r.invoker.LookupEntity(ctx, request)
	if err != nil {
		rpcErr := status.Error(GetStatus(err), err.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("lookup_entity", rpcErr))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, "permission lookup entity failed",
			"tenant_id", request.GetTenantId(),
			"entity_type", request.GetEntityType(),
			"permission", request.GetPermission(),
			"error", err.Error(),
		)
		return nil, rpcErr
	}

	return response, nil
}

// LookupEntityStream -
func (r *PermissionServer) LookupEntityStream(request *v1.PermissionLookupEntityRequest, server v1.Permission_LookupEntityStreamServer) error {
	ctx, span := internal.Tracer.Start(server.Context(), "permissions.lookup-entity-stream")
	defer span.End()
	ctx = invoke.WithConcurrencySemaphore(ctx, semaphore.NewWeighted(int64(r.concurrencyLimit)))

	v := request.Validate()
	if v != nil {
		rpcErr := status.Error(GetStatus(v), v.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("lookup_entity_stream", rpcErr))
		return rpcErr
	}

	err := r.invoker.LookupEntityStream(ctx, request, server)
	if err != nil {
		rpcErr := status.Error(GetStatus(err), err.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("lookup_entity_stream", rpcErr))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, "permission lookup entity stream failed",
			"tenant_id", request.GetTenantId(),
			"entity_type", request.GetEntityType(),
			"permission", request.GetPermission(),
			"error", err.Error(),
		)
		return rpcErr
	}

	return nil
}

// LookupSubject -
func (r *PermissionServer) LookupSubject(ctx context.Context, request *v1.PermissionLookupSubjectRequest) (*v1.PermissionLookupSubjectResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "permissions.lookup-subject")
	defer span.End()
	ctx = invoke.WithConcurrencySemaphore(ctx, semaphore.NewWeighted(int64(r.concurrencyLimit)))

	v := request.Validate()
	if v != nil {
		rpcErr := status.Error(GetStatus(v), v.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("lookup_subject", rpcErr))
		return nil, rpcErr // Return validation error
	}

	response, err := r.invoker.LookupSubject(ctx, request)
	if err != nil {
		rpcErr := status.Error(GetStatus(err), err.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("lookup_subject", rpcErr))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, "permission lookup subject failed",
			"tenant_id", request.GetTenantId(),
			"entity_type", request.GetEntity().GetType(),
			"permission", request.GetPermission(),
			"error", err.Error(),
		)
		return nil, rpcErr
	}

	return response, nil
}

// SubjectPermission -
func (r *PermissionServer) SubjectPermission(ctx context.Context, request *v1.PermissionSubjectPermissionRequest) (*v1.PermissionSubjectPermissionResponse, error) {
	ctx, span := internal.Tracer.Start(ctx, "permissions.subject-permission")
	defer span.End()
	ctx = invoke.WithConcurrencySemaphore(ctx, semaphore.NewWeighted(int64(r.concurrencyLimit)))

	v := request.Validate()
	if v != nil {
		rpcErr := status.Error(GetStatus(v), v.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("subject_permission", rpcErr))
		return nil, rpcErr // Return validation error
	}

	response, err := r.invoker.SubjectPermission(ctx, request)
	if err != nil {
		rpcErr := status.Error(GetStatus(err), err.Error())
		r.errorsCounter.Add(ctx, 1, permissionErrorAttrs("subject_permission", rpcErr))
		span.RecordError(err)
		span.SetStatus(otelCodes.Error, err.Error())
		slog.ErrorContext(ctx, "subject permission failed",
			"tenant_id", request.GetTenantId(),
			"entity_type", request.GetEntity().GetType(),
			"error", err.Error(),
		)
		return nil, rpcErr
	}

	return response, nil
}
