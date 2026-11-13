package gc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Masterminds/squirrel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Permify/permify/internal"
	"github.com/Permify/permify/internal/storage/postgres"
	"github.com/Permify/permify/internal/storage/postgres/utils"
	db "github.com/Permify/permify/pkg/database/postgres"
	"github.com/Permify/permify/pkg/telemetry"
)

var (
	gcOkAttrs                 = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "ok")))
	gcErrorAttrs              = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "error")))
	gcRelationTuplesTableAttr = metric.WithAttributeSet(attribute.NewSet(attribute.String("table", postgres.RelationTuplesTable)))
	gcAttributesTableAttr     = metric.WithAttributeSet(attribute.NewSet(attribute.String("table", postgres.AttributesTable)))
	gcTransactionsTableAttr   = metric.WithAttributeSet(attribute.NewSet(attribute.String("table", postgres.TransactionsTable)))
)

// GC represents a Garbage Collector configuration for database cleanup.
type GC struct {
	// database is the database instance used for garbage collection.
	database *db.Postgres
	// interval is the duration between garbage collection runs.
	interval time.Duration
	// window is the time window for data considered for cleanup.
	window time.Duration
	// timeout is the maximum time allowed for a single GC run.
	timeout time.Duration

	meter             metric.Meter
	runsCounter       metric.Int64Counter
	tenantRunsCounter metric.Int64Counter
	runDuration       metric.Float64Histogram
	deletedRows       metric.Int64Counter
	lastSuccess       metric.Int64Gauge
}

// NewGC creates a new GC instance with the provided configuration.
func NewGC(db *db.Postgres, opts ...Option) *GC {
	gc := &GC{
		interval: _defaultInterval,
		window:   _defaultWindow,
		timeout:  _defaultTimeout,
		database: db,
		meter:    internal.Meter,
	}

	// Custom options
	for _, opt := range opts {
		opt(gc)
	}

	gc.runsCounter = telemetry.NewCounter(gc.meter, "gc_runs", "Number of database garbage collection cycles, by result (ok or error)")
	gc.tenantRunsCounter = telemetry.NewCounter(gc.meter, "gc_tenant_runs", "Number of per-tenant database garbage collection executions, by result (ok or error)")
	var err error
	gc.runDuration, err = gc.meter.Float64Histogram(
		"gc_run_duration_seconds",
		metric.WithDescription("Duration of database garbage collection cycles in seconds, by result"),
	)
	if err != nil {
		panic(err)
	}
	gc.deletedRows = telemetry.NewCounter(gc.meter, "gc_deleted_rows", "Number of rows deleted by database garbage collection, by table")
	gc.lastSuccess, err = gc.meter.Int64Gauge(
		"gc_last_success_timestamp_seconds",
		metric.WithDescription("Unix timestamp in seconds of the most recent successful database garbage collection cycle"),
	)
	if err != nil {
		panic(err)
	}

	return gc
}

// Start initiates the garbage collection process periodically.
func (gc *GC) Start(ctx context.Context) error {
	ticker := time.NewTicker(gc.interval)
	defer ticker.Stop() // Ensure the ticker is stopped when the function exits.

	for {
		select {
		case <-ticker.C: // Periodically trigger garbage collection.
			if err := gc.Run(); err != nil {
				slog.Error("Garbage collection failed with error", slog.Any("error", err))
				continue
			} else {
				slog.Info("Garbage collection completed successfully")
			}
		case <-ctx.Done():
			return ctx.Err() // Return context error if cancellation is requested.
		}
	}
}

// Run performs the garbage collection process.
func (gc *GC) Run() error {
	ctx, cancel := context.WithTimeout(context.Background(), gc.timeout)
	defer cancel()
	return gc.runWithMetrics(ctx, func() error {
		// Get the current time from the database timezone.
		var dbNow time.Time
		err := gc.database.WritePool.QueryRow(ctx, "SELECT NOW() AT TIME ZONE 'UTC'").Scan(&dbNow)
		if err != nil {
			slog.Error("Failed to get current time from database", slog.Any("error", err))
			return err
		}

		// Calculate the cutoff timestamp based on the window duration.
		cutoffTime := dbNow.Add(-gc.window)

		// Get all tenants for tenant-specific garbage collection
		tenants, err := gc.getAllTenants(ctx)
		if err != nil {
			slog.Error("Failed to retrieve tenants:", slog.Any("error", err))
			return err
		}

		// Process garbage collection for each tenant individually
		return gc.runTenants(ctx, tenants, cutoffTime, gc.runForTenant)
	})
}

func (gc *GC) runWithMetrics(ctx context.Context, run func() error) (runErr error) {
	started := time.Now()
	defer func() {
		attrs := gcOkAttrs
		if runErr != nil {
			attrs = gcErrorAttrs
		}
		gc.runsCounter.Add(ctx, 1, attrs)
		gc.runDuration.Record(ctx, time.Since(started).Seconds(), attrs)
		if runErr == nil {
			gc.lastSuccess.Record(ctx, time.Now().Unix())
		}
	}()
	return run()
}

func (gc *GC) runTenants(ctx context.Context, tenants []string, cutoffTime time.Time, runTenant func(context.Context, string, time.Time) error) error {
	var tenantErrors []error
	for _, tenantID := range tenants {
		if err := runTenant(ctx, tenantID, cutoffTime); err != nil {
			gc.tenantRunsCounter.Add(ctx, 1, gcErrorAttrs)
			slog.Error("Garbage collection failed for tenant:", slog.String("tenant_id", tenantID), slog.Any("error", err))
			// Continue with other tenants even if one fails
			tenantErrors = append(tenantErrors, fmt.Errorf("garbage collection failed for tenant %s: %w", tenantID, err))
			continue
		}
		gc.tenantRunsCounter.Add(ctx, 1, gcOkAttrs)
	}

	return errors.Join(tenantErrors...)
}

// getAllTenants retrieves all tenant IDs from the tenants table.
func (gc *GC) getAllTenants(ctx context.Context) ([]string, error) {
	builder := gc.database.Builder.
		Select("id").
		From("tenants").
		OrderBy("id")

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := gc.database.WritePool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tenants []string
	for rows.Next() {
		var tenantID string
		if err := rows.Scan(&tenantID); err != nil {
			return nil, err
		}
		tenants = append(tenants, tenantID)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return tenants, nil
}

// runForTenant performs garbage collection for a specific tenant.
func (gc *GC) runForTenant(ctx context.Context, tenantID string, cutoffTime time.Time) error {
	// Retrieve the last transaction ID for this specific tenant that occurred before the cutoff time.
	lastTransactionID, err := gc.getLastTransactionIDForTenant(ctx, tenantID, cutoffTime)
	if err != nil {
		slog.Error("Failed to retrieve last transaction ID for tenant:", slog.String("tenant_id", tenantID), slog.Any("error", err))
		return err
	}

	if lastTransactionID == 0 {
		// No transactions to clean up for this tenant
		return nil
	}

	// Delete records in relation_tuples, attributes, and transactions tables for this specific tenant.
	if err := gc.deleteRecordsForTenant(ctx, postgres.RelationTuplesTable, tenantID, lastTransactionID); err != nil {
		slog.Error("Failed to delete records in relation_tuples for tenant:", slog.String("tenant_id", tenantID), slog.Any("error", err))
		return err
	}
	if err := gc.deleteRecordsForTenant(ctx, postgres.AttributesTable, tenantID, lastTransactionID); err != nil {
		slog.Error("Failed to delete records in attributes for tenant:", slog.String("tenant_id", tenantID), slog.Any("error", err))
		return err
	}
	if err := gc.deleteTransactionsForTenant(ctx, tenantID, lastTransactionID); err != nil {
		slog.Error("Failed to delete transactions for tenant:", slog.String("tenant_id", tenantID), slog.Any("error", err))
		return err
	}

	slog.Debug("Garbage collection completed for tenant", slog.String("tenant_id", tenantID), slog.Uint64("last_transaction_id", lastTransactionID))
	return nil
}

// getLastTransactionIDForTenant retrieves the last transaction ID for a specific tenant that occurred before the provided timestamp.
func (gc *GC) getLastTransactionIDForTenant(ctx context.Context, tenantID string, before time.Time) (uint64, error) {
	builder := gc.database.Builder.
		Select("id").
		From(postgres.TransactionsTable).
		Where(squirrel.Eq{"tenant_id": tenantID}).
		Where(squirrel.Lt{"timestamp": before}).
		OrderBy("id DESC").
		Limit(1)

	tquery, targs, terr := builder.ToSql()
	if terr != nil {
		return 0, terr
	}

	var lastTransactionID uint64
	row := gc.database.WritePool.QueryRow(ctx, tquery, targs...)
	err := row.Scan(&lastTransactionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return lastTransactionID, nil
}

// deleteRecordsForTenant generates and executes DELETE queries for relation_tuples and attributes tables for a specific tenant.
func (gc *GC) deleteRecordsForTenant(ctx context.Context, table, tenantID string, lastTransactionID uint64) error {
	queryBuilder := utils.GenerateGCQueryForTenant(gc.database.Builder, table, tenantID, lastTransactionID)
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return err
	}

	commandTag, err := gc.database.WritePool.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	gc.recordDeletedRows(ctx, table, commandTag.RowsAffected())
	return nil
}

// deleteTransactionsForTenant deletes transactions for a specific tenant older than the provided lastTransactionID.
func (gc *GC) deleteTransactionsForTenant(ctx context.Context, tenantID string, lastTransactionID uint64) error {
	// Convert the provided lastTransactionID into a string format suitable for SQL queries.
	valStr := fmt.Sprintf("'%v'::xid8", lastTransactionID)

	// Create a Squirrel DELETE query builder for the 'transactions' table.
	queryBuilder := gc.database.Builder.Delete(postgres.TransactionsTable)

	// Create an expression to compare the 'id' column with the lastTransactionID using Lt.
	idExpr := squirrel.Expr(fmt.Sprintf("id < %s", valStr))

	// Add the WHERE clauses to filter transactions for the specific tenant and before the cutoff.
	queryBuilder = queryBuilder.Where(squirrel.Eq{"tenant_id": tenantID}).Where(idExpr)

	// Generate the SQL query and its arguments from the query builder.
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return err
	}

	// Execute the DELETE query with the provided context.
	commandTag, err := gc.database.WritePool.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	gc.recordDeletedRows(ctx, postgres.TransactionsTable, commandTag.RowsAffected())
	return nil
}

func (gc *GC) recordDeletedRows(ctx context.Context, table string, rows int64) {
	switch table {
	case postgres.RelationTuplesTable:
		gc.deletedRows.Add(ctx, rows, gcRelationTuplesTableAttr)
	case postgres.AttributesTable:
		gc.deletedRows.Add(ctx, rows, gcAttributesTableAttr)
	case postgres.TransactionsTable:
		gc.deletedRows.Add(ctx, rows, gcTransactionsTableAttr)
	default:
		gc.deletedRows.Add(ctx, rows, metric.WithAttributes(attribute.String("table", table)))
	}
}
