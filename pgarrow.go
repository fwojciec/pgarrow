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
	pool    *pgxpool.Pool
	isOwner bool // tracks whether this Pool should close the underlying pgxpool
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
// all pgx connection parameters.
func NewPool(ctx context.Context, connString string) (*Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &Pool{pool: pool, isOwner: true}, nil
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
	return &Pool{pool: pool, isOwner: false}
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
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	// Get metadata and create schema
	schema, fieldOIDs, err := p.getQueryMetadata(ctx, conn, sql, args...)
	if err != nil {
		conn.Release()
		return nil, err
	}

	// Execute COPY and parse binary data - connection lifecycle is now managed by the reader
	reader, err := p.executeCopyAndParse(ctx, conn, sql, schema, fieldOIDs)
	if err != nil {
		conn.Release()
		return nil, err
	}

	return reader, nil
}

// getQueryMetadata executes query to extract column metadata and create Arrow schema
func (p *Pool) getQueryMetadata(ctx context.Context, conn *pgxpool.Conn, sql string, args ...any) (*arrow.Schema, []uint32, error) {
	rows, err := conn.Conn().Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare query: %w", err)
	}
	rows.Close() // We only need field descriptions

	fieldDescriptions := rows.FieldDescriptions()
	if len(fieldDescriptions) == 0 {
		return nil, nil, fmt.Errorf("query returned no columns")
	}

	// Create column info and OID list
	columns := make([]ColumnInfo, len(fieldDescriptions))
	fieldOIDs := make([]uint32, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = ColumnInfo{Name: fd.Name, OID: fd.DataTypeOID}
		fieldOIDs[i] = fd.DataTypeOID
	}

	schema, err := CreateSchema(columns)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Arrow schema: %w", err)
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
	reader, err := newRecordReader(schema, fieldOIDs, memory.DefaultAllocator, conn, pipeReader, copyDone)
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
	if p.isOwner {
		p.pool.Close()
	}
}
