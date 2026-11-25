-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS attributes_tenant_entity_ids_type_idx
ON public.attributes (tenant_id, entity_id, entity_type, attribute);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS public.attributes_tenant_entity_ids_type_idx;
