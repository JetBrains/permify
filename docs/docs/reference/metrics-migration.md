# Metrics migration

Permify records request event counts as OpenTelemetry counters rather than histograms. The instrument names are unchanged, but Prometheus series names and the cache lookup metrics have changed. Update dashboards and alerts before removing queries for the old series.

## Request counters

The 18 request instruments are now unitless monotonic counters:

| Instrument | Prometheus counter series |
| --- | --- |
| `check` | `check_total` |
| `expand` | `expand_total` |
| `lookup_entity` | `lookup_entity_total` |
| `lookup_subject` | `lookup_subject_total` |
| `subject_permission` | `subject_permission_total` |
| `write_data` | `write_data_total` |
| `delete_data` | `delete_data_total` |
| `read_attributes` | `read_attributes_total` |
| `read_relationships` | `read_relationships_total` |
| `write_relationships` | `write_relationships_total` |
| `delete_relationships` | `delete_relationships_total` |
| `run_bundle` | `run_bundle_total` |
| `write_bundle` | `write_bundle_total` |
| `read_bundle` | `read_bundle_total` |
| `delete_bundle` | `delete_bundle_total` |
| `write_schema` | `write_schema_total` |
| `read_schema` | `read_schema_total` |
| `list_schema` | `list_schema_total` |
| `write_shared_schema` | `write_shared_schema_total` |
| `read_shared_schema` | `read_shared_schema_total` |
| `list_shared_schema` | `list_shared_schema_total` |
| `assign_shared_schema` | `assign_shared_schema_total` |
| `create_tenant` | `create_tenant_total` |
| `delete_tenant` | `delete_tenant_total` |
| `list_tenant` | `list_tenant_total` |

Replace queries such as:

```promql
sum(rate(check_amount_count[5m]))
```

with:

```promql
sum(rate(check_total[5m]))
```

Use `rate()` or `increase()` for counters; histogram bucket and sum series are no longer emitted for these event counts. The final Prometheus spelling depends on the configured OpenTelemetry exporter and Collector pipeline, so verify the series names in the deployed scrape endpoint before rolling out dashboard changes.

`check_total` counts calls to `DirectInvoker.Check`, including nested checks and failed checks. It is not a one-to-one count of external `Permission/Check` RPCs or entity IDs in a batch. The `subject_type` label is retained, while subject IDs remain in traces and are not metric labels.

Each `DirectInvoker` caches up to 256 distinct `subject_type` values. Further unseen values are recorded as `subject_type="__other__"`, bounding metric cardinality and avoiding per-call attribute-set allocation after the cache fills. The first 256 values are selected by arrival order, not schema validation; an instance that receives arbitrary values early may aggregate later valid values into `__other__`. This limit is per invoker, not per process; account for every invoker instance when estimating the process-wide series count.

## Cache and coordination metrics

- `permission_cache_lookups_total{result="hit"}` and `permission_cache_lookups_total{result="miss"}` count permission-cache lookups per entity ID, including lookups from nested checks.
- `schema_cache_lookups_total{kind=~"entity|rule",result=~"hit|miss|bypass"}` records schema cache reads. `bypass` means no schema version was supplied and the cache was not consulted.
- `lookup_schema_cache_lookups_total{result=~"hit|miss"}` records full-schema cache lookups inside `LookupEngine`, while `lookup_entity_candidates_total` and `lookup_entity_matched_total` record candidate vs matched entity counts.
- `cache_keys_added_total`, `cache_keys_evicted_total`, `cache_cost_evicted_total`, and `cache_sets_dropped_total` / `cache_sets_rejected_total` describe Ristretto admission and eviction. `cache_cost_used` is a gauge calculated as accumulated cost added minus accumulated cost evicted; `cache_max_cost`, `cache_num_counters`, and `cache_buffer_items` expose the configured cache limits.
- Configured operational limits are exported as observable gauges when `meter.enabled: true`: `permission_bulk_limit`, `permission_concurrency_limit`, `server_rate_limit`, `database_max_connections`, `database_min_connections`, `database_max_data_per_write`, `database_max_retries`, `database_watch_buffer_size`, and `authn_oidc_backoff_max_retries`.
- Saturation and error counters (`check_errors_total`, `permission_server_errors_total`, `data_server_errors_total`, `schema_server_errors_total`, `shared_schema_server_errors_total`, `bundle_server_errors_total`, `tenancy_server_errors_total`, `bulk_check_limit_exceeded_total`, `bulk_check_batch_errors_total`, `engine_batch_splits_total`, `concurrency_limit_waits_total`, `cel_evaluations_total`, `rate_limit_requests_total`, `authn_requests_total`, `oidc_jwks_cache_operations_total`, `oidc_jwks_retries_total`, `database_write_tx_total`, `database_tx_retries_total`, `database_max_retries_exceeded_total`, `database_max_data_per_write_exceeded_total`, `database_error_handler_calls_total`, `watch_events_total`, `watch_buffer_full_total`, `gc_runs_total`, `gc_tenant_runs_total`, `balancer_resolver_updates_total`, and `balancer_subconn_events_total`) track limit saturation and logged error/warning events for SLO calculation.
- Ristretto's own hit/miss atomic accounting remains enabled when its metrics are enabled, but `cache_hits` and `cache_misses` are not exported. Use the domain lookup counters above for hit ratios.
- `singleflight_calls_total{op=...}` counts calls entering a deduplication group; `singleflight_executions_total{op=...}` counts delegate executions. Subtracting their `increase()` values over the same interval estimates calls not separately executed by the delegate.
- `balancer_fallbacks_total{reason=...}`, `balancer_remote_calls_total{rpc=...,result=...}`, and `circuit_breaker_state_changes_total{name=...,from=...,to=...}` expose distributed-check fallback, remote RPC, and breaker-transition events.

For example, calculate the permission-cache hit ratio over five minutes with:

```promql
sum(rate(permission_cache_lookups_total{result="hit"}[5m]))
/
sum(rate(permission_cache_lookups_total[5m]))
```

The equivalent schema-cache ratio, excluding bypasses, is:

```promql
sum(rate(schema_cache_lookups_total{result="hit"}[5m]))
/
sum(rate(schema_cache_lookups_total{result=~"hit|miss"}[5m]))
```

## Rollout checklist

1. Confirm the emitted names and labels in the production Collector or Prometheus scrape endpoint.
2. Update dashboards, alerts, recording rules, and any external queries to use the counter series and new cache lookup metrics.
3. Deploy the updated queries alongside the Permify release and check that request rates and cache ratios are populated.
4. Remove obsolete histogram queries after the old series have aged out of the retention window.

The change does not require Prometheus packages in Permify; metric translation is performed by the configured OpenTelemetry exporter and Collector.