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

// Option configures a Pool during creation.
type Option func(*poolConfig)

// poolConfig holds configuration options for Pool creation.
type poolConfig struct {
	allocator    memory.Allocator
	existingPool *pgxpool.Pool
}

// WithAllocator sets a custom memory allocator for Arrow operations.
// If not specified, memory.DefaultAllocator is used.
func WithAllocator(alloc memory.Allocator) Option {
	return func(c *poolConfig) {
		c.allocator = alloc
	}
}

// WithExistingPool uses an existing pgxpool.Pool instead of creating a new one.
// When this option is used, the Pool will not own the underlying pgxpool and
// calling Close() will be a no-op. The caller remains responsible for managing
// the original pgxpool lifecycle.
func WithExistingPool(pool *pgxpool.Pool) Option {
	return func(c *poolConfig) {
		c.existingPool = pool
	}
}

// NewPool creates a new PGArrow pool with configurable options.
// The pool provides instant connections without metadata preloading,
// making it significantly faster than ADBC for databases with many tables.
//
// By default, creates a new pgxpool.Pool from the connection string and uses
// memory.DefaultAllocator. Use options to customize behavior:
//
//   - WithAllocator(alloc): Use custom memory allocator
//   - WithExistingPool(pool): Use existing pgxpool.Pool instead of creating new one
//
// Example connection strings:
//   - "postgres://user:pass@localhost/dbname"
//   - "postgres://user:pass@localhost/dbname?sslmode=require"
//   - "postgres://user:pass@localhost/dbname?pool_max_conns=10"
//
// Examples:
//
//	// Basic usage with new pool
//	pool, err := pgarrow.NewPool(ctx, connString)
//
//	// With custom allocator
//	pool, err := pgarrow.NewPool(ctx, connString, pgarrow.WithAllocator(customAlloc))
//
//	// With existing pgx pool
//	pool, err := pgarrow.NewPool(ctx, "", pgarrow.WithExistingPool(pgxPool))
//
//	// Both options combined
//	pool, err := pgarrow.NewPool(ctx, "",
//		pgarrow.WithExistingPool(pgxPool),
//		pgarrow.WithAllocator(customAlloc))
func NewPool(ctx context.Context, connString string, opts ...Option) (*Pool, error) {
	// Set defaults
	config := &poolConfig{
		allocator: memory.DefaultAllocator,
	}

	// Apply options
	for _, opt := range opts {
		opt(config)
	}

	// Use existing pool if provided
	if config.existingPool != nil {
		return &Pool{
			pool:      config.existingPool,
			isOwner:   false,
			allocator: config.allocator,
		}, nil
	}

	// Create new pool from connection string
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}

	return &Pool{
		pool:      pool,
		isOwner:   true,
		allocator: config.allocator,
	}, nil
}

// QueryArrow executes a PostgreSQL query and returns results as an Apache Arrow RecordReader.
// This is the primary method for converting PostgreSQL data to Arrow format using
// the binary COPY protocol for optimal performance.
//
// DESIGN: This method requires literal SQL values and does not accept parameters.
// This design choice aligns with the COPY BINARY protocol's requirements and provides
// maximum performance. For dynamic queries, build SQL strings with literal values
// using proper quoting/escaping or use pgx directly for parameterized queries.
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
func (p *Pool) QueryArrow(ctx context.Context, sql string) (array.RecordReader, error) {

	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, &ConnectionError{
			ConnectionStr: maskConnectionString(""),
			Err:           err,
		}
	}

	// Get metadata and create compiled schema
	schema, fieldOIDs, err := p.getQueryMetadata(ctx, conn, sql)
	if err != nil {
		conn.Release()
		return nil, &QueryError{
			SQL:       sql,
			Operation: "metadata_discovery",
			Err:       err,
		}
	}

	// Create CompiledSchema for optimal performance
	compiledSchema, err := CompileSchema(fieldOIDs, schema, p.allocator)
	if err != nil {
		conn.Release()
		return nil, &QueryError{
			SQL:       sql,
			Operation: "schema_compilation",
			Err:       err,
		}
	}

	// Execute COPY and parse binary data using CompiledSchema.
	// Ownership of compiledSchema is transferred to the returned reader.
	// The reader is responsible for releasing compiledSchema when it is closed.
	reader, err := p.executeCopyAndParseCompiled(ctx, conn, sql, compiledSchema)
	if err != nil {
		compiledSchema.Release() // Clean up compiled schema on error
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
		columns[i] = ColumnInfo{
			Name: field.Name,
			OID:  field.DataTypeOID,
		}
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

// executeCopyAndParseCompiled runs COPY TO BINARY and parses the result using CompiledSchema
// This is the preferred method for optimal performance with pre-compiled schemas
func (p *Pool) executeCopyAndParseCompiled(ctx context.Context, conn *pgxpool.Conn, sql string, compiledSchema *CompiledSchema) (array.RecordReader, error) {
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

	// Create RecordReader using CompiledSchema for optimal performance
	reader, err := newCompiledRecordReader(compiledSchema, conn, pipeReader, copyDone)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// Close closes the pool and all its connections if this Pool owns the underlying pgxpool.
// After calling Close, the pool cannot be used for further queries.
// It's safe to call Close multiple times.
//
// If this Pool was created without WithExistingPool(), it owns the underlying pgxpool
// and will close it when Close() is called.
//
// If this Pool was created with WithExistingPool(), it does NOT own the underlying
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
