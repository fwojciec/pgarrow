package pgarrow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectParser_BasicTypes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Use checked allocator for memory leak detection
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	pool := getTestPool(t)
	t.Cleanup(pool.Close)

	testCases := []struct {
		name     string
		query    string
		schema   *arrow.Schema
		validate func(t *testing.T, record arrow.Record)
	}{
		{
			name:  "int64_values",
			query: "SELECT 1::int8, 2::int8, 3::int8",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "int8", Type: arrow.PrimitiveTypes.Int64},
				{Name: "int8", Type: arrow.PrimitiveTypes.Int64},
				{Name: "int8", Type: arrow.PrimitiveTypes.Int64},
			}, nil),
			validate: func(t *testing.T, record arrow.Record) {
				t.Helper()
				require.EqualValues(t, 1, record.NumRows())
				require.EqualValues(t, 3, record.NumCols())

				col0, ok := record.Column(0).(*array.Int64)
				require.True(t, ok, "column 0 should be Int64")
				assert.Equal(t, int64(1), col0.Value(0))

				col1, ok := record.Column(1).(*array.Int64)
				require.True(t, ok, "column 1 should be Int64")
				assert.Equal(t, int64(2), col1.Value(0))

				col2, ok := record.Column(2).(*array.Int64)
				require.True(t, ok, "column 2 should be Int64")
				assert.Equal(t, int64(3), col2.Value(0))
			},
		},
		{
			name:  "float64_values",
			query: "SELECT 1.5::float8, 2.5::float8, 3.5::float8",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "float8", Type: arrow.PrimitiveTypes.Float64},
				{Name: "float8", Type: arrow.PrimitiveTypes.Float64},
				{Name: "float8", Type: arrow.PrimitiveTypes.Float64},
			}, nil),
			validate: func(t *testing.T, record arrow.Record) {
				t.Helper()
				require.EqualValues(t, 1, record.NumRows())
				require.EqualValues(t, 3, record.NumCols())

				col0, ok := record.Column(0).(*array.Float64)
				require.True(t, ok, "column 0 should be Float64")
				assert.InEpsilon(t, 1.5, col0.Value(0), 0.001)

				col1, ok := record.Column(1).(*array.Float64)
				require.True(t, ok, "column 1 should be Float64")
				assert.InEpsilon(t, 2.5, col1.Value(0), 0.001)

				col2, ok := record.Column(2).(*array.Float64)
				require.True(t, ok, "column 2 should be Float64")
				assert.InEpsilon(t, 3.5, col2.Value(0), 0.001)
			},
		},
		{
			name:  "boolean_values",
			query: "SELECT true::bool, false::bool, true::bool",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
				{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
				{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			}, nil),
			validate: func(t *testing.T, record arrow.Record) {
				t.Helper()
				require.EqualValues(t, 1, record.NumRows())
				require.EqualValues(t, 3, record.NumCols())

				col0, ok := record.Column(0).(*array.Boolean)
				require.True(t, ok, "column 0 should be Boolean")
				assert.True(t, col0.Value(0))

				col1, ok := record.Column(1).(*array.Boolean)
				require.True(t, ok, "column 1 should be Boolean")
				assert.False(t, col1.Value(0))

				col2, ok := record.Column(2).(*array.Boolean)
				require.True(t, ok, "column 2 should be Boolean")
				assert.True(t, col2.Value(0))
			},
		},
		{
			name:  "text_values",
			query: "SELECT 'hello'::text, 'world'::text, 'test'::text",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "text", Type: arrow.BinaryTypes.String},
				{Name: "text", Type: arrow.BinaryTypes.String},
				{Name: "text", Type: arrow.BinaryTypes.String},
			}, nil),
			validate: func(t *testing.T, record arrow.Record) {
				t.Helper()
				require.EqualValues(t, 1, record.NumRows())
				require.EqualValues(t, 3, record.NumCols())

				col0, ok := record.Column(0).(*array.String)
				require.True(t, ok, "column 0 should be String")
				assert.Equal(t, "hello", col0.Value(0))

				col1, ok := record.Column(1).(*array.String)
				require.True(t, ok, "column 1 should be String")
				assert.Equal(t, "world", col1.Value(0))

				col2, ok := record.Column(2).(*array.String)
				require.True(t, ok, "column 2 should be String")
				assert.Equal(t, "test", col2.Value(0))
			},
		},
		{
			name:  "null_values",
			query: "SELECT NULL::int8, NULL::float8, NULL::text",
			schema: arrow.NewSchema([]arrow.Field{
				{Name: "int8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "float8", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
			}, nil),
			validate: func(t *testing.T, record arrow.Record) {
				t.Helper()
				require.EqualValues(t, 1, record.NumRows())
				require.EqualValues(t, 3, record.NumCols())

				col0, ok := record.Column(0).(*array.Int64)
				require.True(t, ok, "column 0 should be Int64")
				assert.True(t, col0.IsNull(0))

				col1, ok := record.Column(1).(*array.Float64)
				require.True(t, ok, "column 1 should be Float64")
				assert.True(t, col1.IsNull(0))

				col2, ok := record.Column(2).(*array.String)
				require.True(t, ok, "column 2 should be String")
				assert.True(t, col2.IsNull(0))
			},
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Get a fresh connection for each parallel test
			subConn, err := pool.Acquire(ctx)
			require.NoError(t, err)
			defer subConn.Release()

			parser, err := pgarrow.NewSelectParser(subConn.Conn(), tc.schema, alloc)
			require.NoError(t, err)
			defer parser.Release()

			record, err := parser.ParseAll(ctx, tc.query)
			require.NoError(t, err)
			defer record.Release()

			tc.validate(t, record)
		})
	}
}

func TestSelectParser_MultipleRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Use checked allocator for memory leak detection
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	pool := getTestPool(t)
	t.Cleanup(pool.Close)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "doubled", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String},
	}, nil)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	parser, err := pgarrow.NewSelectParser(conn.Conn(), schema, alloc)
	require.NoError(t, err)
	defer parser.Release()

	query := `
		SELECT 
			i::int8 as i,
			(i * 2)::int8 as doubled,
			'row_' || i::text as label
		FROM generate_series(1, 100) i
	`

	record, err := parser.ParseAll(ctx, query)
	require.NoError(t, err)
	defer record.Release()

	require.EqualValues(t, 100, record.NumRows())
	require.EqualValues(t, 3, record.NumCols())

	col0, ok := record.Column(0).(*array.Int64)
	require.True(t, ok, "column 0 should be Int64")
	col1, ok := record.Column(1).(*array.Int64)
	require.True(t, ok, "column 1 should be Int64")
	col2, ok := record.Column(2).(*array.String)
	require.True(t, ok, "column 2 should be String")

	for i := 0; i < 100; i++ {
		assert.Equal(t, int64(i+1), col0.Value(i))
		assert.Equal(t, int64((i+1)*2), col1.Value(i))
		assert.Equal(t, fmt.Sprintf("row_%d", i+1), col2.Value(i))
	}
}

func TestSelectParser_BatchProcessing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Use checked allocator for memory leak detection
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	pool := getTestPool(t)
	t.Cleanup(pool.Close)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	parser, err := pgarrow.NewSelectParser(conn.Conn(), schema, alloc)
	require.NoError(t, err)
	defer parser.Release()

	// Test with a large dataset to trigger batching
	query := `
		SELECT 
			i::int8 as id,
			random()::float8 as value
		FROM generate_series(1, 500000) i
	`

	err = parser.StartParsing(ctx, query)
	require.NoError(t, err)

	totalRows := int64(0)
	batchCount := 0

	for {
		record, done, err := parser.ParseNextBatch(ctx, pgarrow.DefaultBatchSizeBytes)
		require.NoError(t, err)

		if record != nil {
			batchRows := record.NumRows()
			totalRows += batchRows
			batchCount++
			t.Logf("Batch %d: %d rows, total so far: %d", batchCount, batchRows, totalRows)
			record.Release()
		}

		if done {
			t.Logf("Done flag set, total rows: %d", totalRows)
			break
		}
	}

	assert.Equal(t, int64(500000), totalRows, "Expected 500000 rows, got %d", totalRows)
	assert.Greater(t, batchCount, 1, "Should have multiple batches for 500K rows")
}

func TestSelectParser_QueryExecMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Use checked allocator for memory leak detection
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	// Create a pool with CacheDescribe mode for optimal performance
	config, err := pgxpool.ParseConfig(getTestDatabaseURL(t))
	require.NoError(t, err)

	// Set the query exec mode for binary protocol
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "num", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	parser, err := pgarrow.NewSelectParser(conn.Conn(), schema, alloc)
	require.NoError(t, err)
	defer parser.Release()

	// Query with explicit cast to ensure int64
	query := "SELECT 42::int8 as num"

	record, err := parser.ParseAll(ctx, query)
	require.NoError(t, err)
	defer record.Release()

	col, ok := record.Column(0).(*array.Int64)
	require.True(t, ok, "column should be Int64")
	assert.Equal(t, int64(42), col.Value(0))
}

func TestSelectParser_BuilderReuse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Use checked allocator for memory leak detection
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	pool := getTestPool(t)
	t.Cleanup(pool.Close)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	parser, err := pgarrow.NewSelectParser(conn.Conn(), schema, alloc)
	require.NoError(t, err)
	defer parser.Release()

	// Parse multiple batches and ensure builders are reused
	for i := 0; i < 3; i++ {
		query := fmt.Sprintf("SELECT %d::int8 as id", i)
		record, err := parser.ParseAll(ctx, query)
		require.NoError(t, err)

		col, ok := record.Column(0).(*array.Int64)
		require.True(t, ok, "column should be Int64")
		assert.Equal(t, int64(i), col.Value(0))

		record.Release()
	}
}

func TestSelectParser_ErrorHandling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Use checked allocator for memory leak detection
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	pool := getTestPool(t)
	t.Cleanup(pool.Close)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	parser, err := pgarrow.NewSelectParser(conn.Conn(), schema, alloc)
	require.NoError(t, err)
	defer parser.Release()

	// Invalid SQL
	_, err = parser.ParseAll(ctx, "SELECT invalid syntax")
	require.Error(t, err)

	// Column count mismatch
	_, err = parser.ParseAll(ctx, "SELECT 1::int8, 2::int8")
	require.Error(t, err)

	// Type mismatch (expecting int64, getting text)
	_, err = parser.ParseAll(ctx, "SELECT 'not a number'::text as id")
	require.Error(t, err)
}

// Helper functions for tests
func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	connStr := getTestDatabaseURL(t)
	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	return pool
}
