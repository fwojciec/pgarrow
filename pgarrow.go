package pgarrow

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps pgxpool.Pool and provides Arrow query capabilities.
// It enables executing PostgreSQL queries and receiving results directly
// in Apache Arrow format without CGO dependencies.
type Pool struct {
	pool      *pgxpool.Pool
	isOwner   bool // tracks whether this Pool should close the underlying pgxpool
	allocator memory.Allocator
}

// NewPool creates a new PGArrow pool from a PostgreSQL connection string.
// The pool provides instant connections without metadata preloading,
// making it significantly faster than ADBC for databases with many tables.
//
// Example connection strings:
//   - "postgres://user:pass@localhost/dbname"
//   - "postgres://user:pass@localhost/dbname?sslmode=require"
//   - "postgres://user:pass@localhost/dbname?pool_max_conns=10"
//
// The pool uses pgx internally for connection management and supports
// all pgx connection parameters. This is a convenience wrapper that uses the default
// Arrow allocator - for custom memory management, use NewPoolWithAllocator instead.
func NewPool(ctx context.Context, connString string) (*Pool, error) {
	return NewPoolWithAllocator(ctx, connString, memory.DefaultAllocator)
}

// NewPoolWithAllocator creates a new PGArrow pool with a custom memory allocator.
// This allows advanced users to implement custom memory management strategies,
// such as tracking allocations, using different memory pools, or implementing
// memory limits for Arrow operations.
//
// The allocator will be used for all Arrow record construction and must remain
// valid for the lifetime of the pool.
func NewPoolWithAllocator(ctx context.Context, connString string, alloc memory.Allocator) (*Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &Pool{pool: pool, isOwner: true, allocator: alloc}, nil
}

// NewPoolFromExisting creates a new PGArrow pool from an existing pgxpool.Pool.
// This allows sharing a single connection pool between transactional (pgx) and
// analytical (pgarrow) operations, reducing resource overhead and enabling
// consistent connection configuration.
//
// The provided pool remains under the caller's management - calling Close() on
// the PGArrow pool will NOT close the underlying pgx pool. The caller is
// responsible for closing the original pgx pool when appropriate.
//
// Memory allocation uses the default Arrow allocator.
//
// This is particularly useful for services with mixed workloads:
//
//	// Create shared pgx pool
//	pgxPool, err := pgxpool.New(ctx, connString)
//	if err != nil {
//	    return err
//	}
//	defer pgxPool.Close()
//
//	// Use for transactional operations
//	conn, err := pgxPool.Acquire(ctx)
//	// ... transactional work
//
//	// Use for analytical operations via pgarrow
//	arrowPool := pgarrow.NewPoolFromExisting(pgxPool)
//	reader, err := arrowPool.QueryArrow(ctx, "SELECT * FROM analytics_view")
//	// ... analytical work
func NewPoolFromExisting(pool *pgxpool.Pool) *Pool {
	return NewPoolFromExistingWithAllocator(pool, memory.DefaultAllocator)
}

// NewPoolFromExistingWithAllocator creates a new PGArrow pool from an existing
// pgxpool.Pool with a custom memory allocator. This combines the benefits of
// shared connection pools with custom memory management strategies.
func NewPoolFromExistingWithAllocator(pool *pgxpool.Pool, alloc memory.Allocator) *Pool {
	return &Pool{pool: pool, isOwner: false, allocator: alloc}
}

// QueryArrow executes a PostgreSQL query and returns results as an Apache Arrow RecordReader.
// This is the primary method for converting PostgreSQL data to Arrow format using
// the binary COPY protocol for optimal performance.
//
// IMPORTANT: This method does NOT support parameterized queries ($1, $2, etc.) due to
// limitations of PostgreSQL's COPY TO BINARY protocol. Use literal values in your SQL
// or consider alternative approaches for dynamic queries.
//
// The method handles all 7 supported data types:
//   - bool (PostgreSQL OID 16)
//   - int2/smallint (PostgreSQL OID 21)
//   - int4/integer (PostgreSQL OID 23)
//   - int8/bigint (PostgreSQL OID 20)
//   - float4/real (PostgreSQL OID 700)
//   - float8/double precision (PostgreSQL OID 701)
//   - text (PostgreSQL OID 25)
//
// The returned RecordReader streams data in batches and must be released by calling
// reader.Release() to prevent memory leaks.
//
// Example usage:
//
//	reader, err := pool.QueryArrow(ctx, "SELECT id, name FROM users")
//	if err != nil {
//	    return err
//	}
//	defer reader.Release()
//
//	for reader.Next() {
//	    record := reader.Record()
//	    // Access data using Arrow arrays
//	    idCol := record.Column(0).(*array.Int32)
//	    nameCol := record.Column(1).(*array.String)
//	    // Process record...
//	}
//	if err := reader.Err(); err != nil {
//	    return err
//	}
func (p *Pool) QueryArrow(ctx context.Context, sql string, args ...any) (array.RecordReader, error) {
	// Check for parameterized queries - COPY TO BINARY doesn't support parameters
	if len(args) > 0 {
		return nil, fmt.Errorf("parameterized queries are not supported with COPY TO BINARY protocol - use literal values in SQL instead")
	}

	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, &ConnectionError{
			ConnectionStr: maskConnectionString(""),
			Err:           err,
		}
	}

	// Get metadata and create schema
	schema, fieldOIDs, err := p.getQueryMetadata(ctx, conn, sql)
	if err != nil {
		conn.Release()
		return nil, &QueryError{
			SQL:       sql,
			Operation: "metadata_discovery",
			Err:       err,
		}
	}

	// Execute COPY and parse binary data - connection lifecycle is now managed by the reader
	reader, err := p.executeCopyAndParse(ctx, conn, sql, schema, fieldOIDs)
	if err != nil {
		conn.Release()
		return nil, &QueryError{
			SQL:       sql,
			Operation: "copy_execution",
			Err:       err,
		}
	}

	return reader, nil
}

// getQueryMetadata uses PREPARE to extract column metadata without executing the query
func (p *Pool) getQueryMetadata(ctx context.Context, conn *pgxpool.Conn, sql string) (*arrow.Schema, []uint32, error) {
	// Use PREPARE to get metadata without executing the full query - much more efficient!
	// Generate a unique statement name to avoid collisions in concurrent usage
	stmtName := fmt.Sprintf("pgarrow_meta_%p", conn)

	sd, err := conn.Conn().Prepare(ctx, stmtName, sql)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare statement for metadata discovery: %w", err)
	}

	// Clean up the prepared statement when done
	defer func() {
		// Ignore errors during cleanup as the connection might be in an error state
		_, _ = conn.Conn().Exec(ctx, "DEALLOCATE "+stmtName)
	}()

	if len(sd.Fields) == 0 {
		return nil, nil, fmt.Errorf("query returned no columns - Arrow conversion requires queries that return at least one column")
	}

	// Create column info and OID list from prepared statement description
	columns := make([]ColumnInfo, len(sd.Fields))
	fieldOIDs := make([]uint32, len(sd.Fields))
	for i, field := range sd.Fields {
		columns[i] = ColumnInfo{Name: field.Name, OID: field.DataTypeOID}
		fieldOIDs[i] = field.DataTypeOID
	}

	schema, err := CreateSchema(columns)
	if err != nil {
		return nil, nil, &SchemaError{
			Columns: columns,
			Err:     err,
		}
	}

	return schema, fieldOIDs, nil
}

// executeCopyAndParse runs COPY TO BINARY and parses the result into Arrow RecordReader
func (p *Pool) executeCopyAndParse(ctx context.Context, conn *pgxpool.Conn, sql string, schema *arrow.Schema, fieldOIDs []uint32) (array.RecordReader, error) {
	copySQL := fmt.Sprintf("COPY (%s) TO STDOUT (FORMAT BINARY)", sql)

	// Set up pipe for COPY data
	pipeReader, pipeWriter := io.Pipe()
	copyDone := make(chan struct{})

	go func() {
		defer pipeWriter.Close()
		defer close(copyDone) // Signal completion
		_, err := conn.Conn().PgConn().CopyTo(ctx, pipeWriter, copySQL)
		if err != nil {
			// Close pipe writer with error to propagate error to reader
			pipeWriter.CloseWithError(err)
		}
	}()

	// Create RecordReader with connection lifecycle management
	reader, err := newRecordReader(schema, fieldOIDs, p.allocator, conn, pipeReader, copyDone)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// Close closes the pool and all its connections if this Pool owns the underlying pgxpool.
// After calling Close, the pool cannot be used for further queries.
// It's safe to call Close multiple times.
//
// If this Pool was created using NewPool(), it owns the underlying pgxpool and will
// close it when Close() is called.
//
// If this Pool was created using NewPoolFromExisting(), it does NOT own the underlying
// pgxpool and calling Close() will be a no-op, leaving the original pgxpool unchanged.
// The caller remains responsible for managing the original pgxpool lifecycle.
//
// This method should be called when the pool is no longer needed,
// typically using defer after pool creation:
//
//	pool, err := pgarrow.NewPool(ctx, connString)
//	if err != nil {
//	    return err
//	}
//	defer pool.Close()
func (p *Pool) Close() {
	if p.isOwner && p.pool != nil {
		p.pool.Close()
	}
}
