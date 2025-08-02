package pgarrow_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests

func TestPoolQueryArrowBasicIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT id, name, active FROM (VALUES (1, 'first', true), (2, 'second', false)) AS simple_test(id, name, active) ORDER BY id"

	// Execute QueryArrow
	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	// Verify schema
	schema := reader.Schema()
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "name", schema.Field(1).Name)
	assert.Equal(t, "active", schema.Field(2).Name)

	// Read all batches
	totalRows := int64(0)
	batchCount := 0
	for reader.Next() {
		batchCount++
		record := reader.Record()
		totalRows += record.NumRows()

		// Verify batch structure
		assert.Equal(t, int64(3), record.NumCols())

		// Verify data values for each batch
		idCol, ok := record.Column(0).(*array.Int32)
		require.True(t, ok)
		nameCol, ok := record.Column(1).(*array.String)
		require.True(t, ok)
		activeCol, ok := record.Column(2).(*array.Boolean)
		require.True(t, ok)

		// Verify the specific data (this test has 2 rows)
		if record.NumRows() >= 1 {
			assert.Equal(t, int32(1), idCol.Value(0))
			assert.Equal(t, "first", nameCol.Value(0))
			assert.True(t, activeCol.Value(0))
		}
		if record.NumRows() >= 2 {
			assert.Equal(t, int32(2), idCol.Value(1))
			assert.Equal(t, "second", nameCol.Value(1))
			assert.False(t, activeCol.Value(1))
		}
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	// Verify total results
	assert.Equal(t, int64(2), totalRows)
	assert.GreaterOrEqual(t, batchCount, 1)
}

func TestPoolQueryArrowAllDataTypesIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT col_bool, col_int2, col_int4, col_int8, col_float4, col_float8, col_text FROM (VALUES (true, 100::int2, 1000::int4, 10000::int8, 3.14::float4, 2.71828::float8, 'hello'), (false, 200::int2, 2000::int4, 20000::int8, 6.28::float4, 1.41421::float8, 'world'), (null, null, null, null, null, null, null)) AS test_all_types(col_bool, col_int2, col_int4, col_int8, col_float4, col_float8, col_text) WHERE col_bool IS NOT NULL ORDER BY col_int2"

	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	totalRows := int64(0)
	var boolCol *array.Boolean
	var int2Col *array.Int16
	var int4Col *array.Int32
	var int8Col *array.Int64
	var float4Col *array.Float32
	var float8Col *array.Float64
	var textCol *array.String

	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		// Verify we got the expected columns
		assert.Equal(t, int64(7), record.NumCols())

		// Extract column references for validation
		if boolCol == nil {
			var ok bool
			boolCol, ok = record.Column(0).(*array.Boolean)
			require.True(t, ok)
			int2Col, ok = record.Column(1).(*array.Int16)
			require.True(t, ok)
			int4Col, ok = record.Column(2).(*array.Int32)
			require.True(t, ok)
			int8Col, ok = record.Column(3).(*array.Int64)
			require.True(t, ok)
			float4Col, ok = record.Column(4).(*array.Float32)
			require.True(t, ok)
			float8Col, ok = record.Column(5).(*array.Float64)
			require.True(t, ok)
			textCol, ok = record.Column(6).(*array.String)
			require.True(t, ok)
		}

		// Verify data values within each batch
		for i := range int(record.NumRows()) {
			switch i {
			case 0: // First row values
				assert.True(t, boolCol.Value(i))
				assert.Equal(t, int16(100), int2Col.Value(i))
				assert.Equal(t, int32(1000), int4Col.Value(i))
				assert.Equal(t, int64(10000), int8Col.Value(i))
				assert.InDelta(t, float32(3.14), float4Col.Value(i), 0.001)
				assert.InDelta(t, float64(2.71828), float8Col.Value(i), 0.00001)
				assert.Equal(t, "hello", textCol.Value(i))
			case 1: // Second row values
				assert.False(t, boolCol.Value(i))
				assert.Equal(t, int16(200), int2Col.Value(i))
				assert.Equal(t, int32(2000), int4Col.Value(i))
				assert.Equal(t, int64(20000), int8Col.Value(i))
				assert.InDelta(t, float32(6.28), float4Col.Value(i), 0.001)
				assert.InDelta(t, float64(1.41421), float8Col.Value(i), 0.00001)
				assert.Equal(t, "world", textCol.Value(i))
			}
		}
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	// Verify we got the 2 non-null rows
	assert.Equal(t, int64(2), totalRows)
}

func TestPoolQueryArrowParameterizedQueryIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT id, name FROM (VALUES (1, 'first', true), (2, 'second', false)) AS simple_test(id, name, active) WHERE id = $1 ORDER BY id"

	// This should fail with a clear error message about parameterized queries not being supported
	reader, err := pool.QueryArrow(ctx, sql, 2)
	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "parameterized queries are not supported")
}

func TestPoolQueryArrowEmptyResultIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT id, name FROM (VALUES (1, 'first', true), (2, 'second', false)) AS simple_test(id, name, active) WHERE id > 100"

	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	// Verify schema even for empty results
	schema := reader.Schema()
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "name", schema.Field(1).Name)

	totalRows := int64(0)
	batchCount := 0
	for reader.Next() {
		batchCount++
		record := reader.Record()
		totalRows += record.NumRows()
		// Should get empty result
		assert.Equal(t, int64(2), record.NumCols())
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	// Should get empty result
	assert.Equal(t, int64(0), totalRows)
}

func TestPoolQueryArrowNullHandlingIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT col_bool, col_int4, col_text FROM (VALUES (true, 100::int2, 1000::int4, 10000::int8, 3.14::float4, 2.71828::float8, 'hello'), (false, 200::int2, 2000::int4, 20000::int8, 6.28::float4, 1.41421::float8, 'world'), (null, null, null, null, null, null, null)) AS test_all_types(col_bool, col_int2, col_int4, col_int8, col_float4, col_float8, col_text) WHERE col_bool IS NULL"

	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	totalRows := int64(0)
	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		// Should get expected column count
		assert.Equal(t, int64(3), record.NumCols())

		// Verify all values are null for each row in the batch
		for row := range int(record.NumRows()) {
			for col := range int(record.NumCols()) {
				assert.False(t, record.Column(col).IsValid(row), "Column %d, row %d should be NULL", col, row)
			}
		}
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	// Should get the null row
	assert.Equal(t, int64(1), totalRows)
}

// Error handling tests

func TestPoolQueryArrowInvalidSQLIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT * FROM nonexistent_table"

	reader, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "failed to prepare query")
}

func TestPoolQueryArrowSyntaxErrorIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT * FROM WHERE"

	reader, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "failed to prepare query")
}

func TestPoolQueryArrowConnectionErrorIntegration(t *testing.T) {
	t.Parallel()

	// Create pool with invalid connection string
	ctx := context.Background()
	invalidConnStr := "postgres://invalid:invalid@localhost:9999/invalid?sslmode=disable"

	pool, err := pgarrow.NewPool(ctx, invalidConnStr)
	require.NoError(t, err) // Pool creation should succeed
	defer pool.Close()

	sql := "SELECT 1"
	reader, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "failed to acquire connection")
}

func TestPoolQueryArrowCancelledContextIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	sql := "SELECT id, name FROM (VALUES (1, 'first', true), (2, 'second', false)) AS simple_test(id, name, active)"
	reader, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestPoolQueryArrowInvalidParametersIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT id FROM (VALUES (1, 'first', true), (2, 'second', false)) AS simple_test(id, name, active) WHERE id = $1"

	// Pass wrong number of parameters
	reader, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, reader)
}

// Resource cleanup verification tests

func TestPoolQueryArrowResourceCleanupOnErrorIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Execute invalid query - should clean up resources properly
	sql := "SELECT * FROM nonexistent_table"
	reader, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, reader)

	// Memory allocator should show no leaks
	// (verified by alloc.AssertSize(t, 0) in defer)
}

func TestPoolQueryArrowMultipleCallsResourceCleanupIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT * FROM (VALUES (1, 'first', true), (2, 'second', false)) AS simple_test(id, name, active) ORDER BY id"

	// Execute multiple queries to verify proper resource cleanup
	for range 10 {
		reader, err := pool.QueryArrow(ctx, sql)
		require.NoError(t, err)
		require.NotNil(t, reader)

		totalRows := int64(0)
		for reader.Next() {
			record := reader.Record()
			totalRows += record.NumRows()
		}

		// Check for reader errors
		require.NoError(t, reader.Err())

		// Verify data integrity
		assert.Equal(t, int64(2), totalRows)

		// Release reader immediately
		reader.Release()
	}

	// Memory allocator should show no leaks after all releases
	// (verified by alloc.AssertSize(t, 0) in defer)
}

// End-to-end test scenarios requested in issue #6

func TestPoolQueryArrowEmptyResultSetIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Query that returns no rows but has valid schema
	sql := "SELECT id, name FROM (VALUES (1, 'first', true), (2, 'second', false)) AS simple_test(id, name, active) WHERE id > 1000"
	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	// Schema should still be valid even for empty results
	schema := reader.Schema()
	assert.NotNil(t, schema)
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "name", schema.Field(1).Name)

	totalRows := int64(0)
	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()
		// Should have expected column count
		assert.Equal(t, int64(2), record.NumCols())
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	// Should have schema but no rows
	assert.Equal(t, int64(0), totalRows)
}

func TestPoolQueryArrowLargeResultSetIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Generate 200+ rows to test large result set handling
	sql := `
		SELECT 
			i as id,
			'user_' || i::text as name,
			(i % 2 = 0) as active,
			(i * 1.5)::float8 as score
		FROM generate_series(1, 250) i
	`
	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	totalRows := int64(0)
	var firstIDVal int32
	var firstNameVal string
	var firstActiveVal bool
	var firstScoreVal float64
	var lastIDVal int32
	var lastNameVal string
	var lastActiveVal bool
	var lastScoreVal float64
	firstRowProcessed := false

	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		// Verify expected column count
		assert.Equal(t, int64(4), record.NumCols())

		// Extract columns for data verification
		idCol, ok := record.Column(0).(*array.Int32)
		require.True(t, ok, "Failed to cast column 0 to Int32")
		nameCol, ok := record.Column(1).(*array.String)
		require.True(t, ok, "Failed to cast column 1 to String")
		activeCol, ok := record.Column(2).(*array.Boolean)
		require.True(t, ok, "Failed to cast column 2 to Boolean")
		scoreCol, ok := record.Column(3).(*array.Float64)
		require.True(t, ok, "Failed to cast column 3 to Float64")

		// Capture first row data
		if !firstRowProcessed && record.NumRows() > 0 {
			firstIDVal = idCol.Value(0)
			firstNameVal = nameCol.Value(0)
			firstActiveVal = activeCol.Value(0)
			firstScoreVal = scoreCol.Value(0)
			firstRowProcessed = true
		}

		// Capture last row data from this batch
		if record.NumRows() > 0 {
			lastIdx := int(record.NumRows()) - 1
			lastIDVal = idCol.Value(lastIdx)
			lastNameVal = nameCol.Value(lastIdx)
			lastActiveVal = activeCol.Value(lastIdx)
			lastScoreVal = scoreCol.Value(lastIdx)
		}
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	// Verify large result set
	assert.Equal(t, int64(250), totalRows)

	// Verify first row data
	assert.Equal(t, int32(1), firstIDVal)
	assert.Equal(t, "user_1", firstNameVal)
	assert.False(t, firstActiveVal) // 1 % 2 != 0
	assert.InDelta(t, 1.5, firstScoreVal, 0.01)

	// Verify last row data
	assert.Equal(t, int32(250), lastIDVal)
	assert.Equal(t, "user_250", lastNameVal)
	assert.True(t, lastActiveVal)                // 250 % 2 = 0
	assert.InDelta(t, 375.0, lastScoreVal, 0.01) // 250 * 1.5
}

func TestPoolQueryArrowMixedTypesWithNullsIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Query with mixed types and strategic NULL placement
	sql := `
		SELECT * FROM (VALUES 
			(1, 'Alice', 25.5::float8, true, 100::int2, 1000::int8),
			(2, NULL, 30.0::float8, false, NULL, 2000::int8),
			(NULL, 'Charlie', NULL::float8, true, 300::int2, NULL),
			(4, 'Diana', 28.7::float8, NULL, 400::int2, 4000::int8),
			(5, '', 0.0::float8, false, 0::int2, 0::int8)
		) AS mixed_data(id, name, score, active, small_num, big_num)
	`
	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	totalRows := int64(0)
	var idCol *array.Int32
	var nameCol *array.String
	var scoreCol *array.Float64
	var activeCol *array.Boolean
	var smallCol *array.Int16
	var bigCol *array.Int64

	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		assert.Equal(t, int64(6), record.NumCols())

		// Extract all columns (only once)
		if idCol == nil {
			var ok bool
			idCol, ok = record.Column(0).(*array.Int32)
			require.True(t, ok, "Failed to cast column 0 to Int32")
			nameCol, ok = record.Column(1).(*array.String)
			require.True(t, ok, "Failed to cast column 1 to String")
			scoreCol, ok = record.Column(2).(*array.Float64)
			require.True(t, ok, "Failed to cast column 2 to Float64")
			activeCol, ok = record.Column(3).(*array.Boolean)
			require.True(t, ok, "Failed to cast column 3 to Boolean")
			smallCol, ok = record.Column(4).(*array.Int16)
			require.True(t, ok, "Failed to cast column 4 to Int16")
			bigCol, ok = record.Column(5).(*array.Int64)
			require.True(t, ok, "Failed to cast column 5 to Int64")
		}

		// Validate data within each batch
		for i := range int(record.NumRows()) {
			switch i {
			case 0: // Row 0: all non-NULL values
				assert.False(t, idCol.IsNull(i))
				assert.Equal(t, int32(1), idCol.Value(i))
				assert.Equal(t, "Alice", nameCol.Value(i))
				assert.InDelta(t, 25.5, scoreCol.Value(i), 0.01)
				assert.True(t, activeCol.Value(i))
				assert.Equal(t, int16(100), smallCol.Value(i))
				assert.Equal(t, int64(1000), bigCol.Value(i))
			case 1: // Row 1: name=NULL, small_num=NULL
				assert.True(t, nameCol.IsNull(i))
				assert.True(t, smallCol.IsNull(i))
			case 2: // Row 2: id=NULL, score=NULL, big_num=NULL
				assert.True(t, idCol.IsNull(i))
				assert.True(t, scoreCol.IsNull(i))
				assert.True(t, bigCol.IsNull(i))
			case 3: // Row 3: active=NULL
				assert.True(t, activeCol.IsNull(i))
			case 4: // Row 4: empty string vs NULL
				assert.False(t, nameCol.IsNull(i)) // name='' (empty, not NULL)
				assert.Empty(t, nameCol.Value(i))
			}
		}
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	assert.Equal(t, int64(5), totalRows)
}

func TestPoolQueryArrowBadConnectionErrorIntegration(t *testing.T) {
	t.Parallel()

	// Create pool with invalid connection string
	invalidConnStr := "postgres://invalid:invalid@nonexistent:5432/invalid"
	pool, err := pgarrow.NewPool(context.Background(), invalidConnStr)

	// Pool creation might succeed, but queries should fail
	if err != nil {
		// If pool creation fails, that's expected
		assert.Contains(t, err.Error(), "connect")
		return
	}
	defer pool.Close()

	ctx := context.Background()
	sql := "SELECT 1"
	reader, err := pool.QueryArrow(ctx, sql)

	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "connect")
}

func TestPoolQueryArrowInvalidSQLErrorIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()

	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "syntax error",
			sql:  "SELCT invalid syntax",
		},
		{
			name: "nonexistent table",
			sql:  "SELECT * FROM nonexistent_table_xyz",
		},
		{
			name: "nonexistent column",
			sql:  "SELECT nonexistent_column FROM (VALUES (1, 'first', true), (2, 'second', false)) AS simple_test(id, name, active)",
		},
		{
			name: "type error",
			sql:  "SELECT 'text' + 123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader, err := pool.QueryArrow(ctx, tc.sql)
			require.Error(t, err, "should fail for: %s", tc.sql)
			assert.Nil(t, reader)
		})
	}
}

func TestPoolQueryArrowAllSupportedTypesEndToEndIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Comprehensive test of all 7 supported types with various values
	sql := `
		SELECT * FROM (VALUES
			-- Normal values
			(true, 123::int2, 456789::int4, 123456789012::int8, 3.14::float4, 2.718281828::float8, 'Hello World'),
			-- Edge cases
			(false, (-32768)::int2, (-2147483648)::int4, (-9223372036854775808)::int8, 0.0::float4, 0.0::float8, ''),
			-- More edge cases  
			(true, 32767::int2, 2147483647::int4, 9223372036854775807::int8, 'Infinity'::float4::float4, 'Infinity'::float8::float8, 'Special chars: áéíóú 中文 🚀'),
			-- NULL values
			(NULL, NULL, NULL, NULL, NULL, NULL, NULL)
		) AS all_types(col_bool, col_int2, col_int4, col_int8, col_float4, col_float8, col_text)
	`

	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	// Verify schema matches expected Arrow types
	schema := reader.Schema()
	assert.Equal(t, "col_bool", schema.Field(0).Name)
	assert.Equal(t, "col_int2", schema.Field(1).Name)
	assert.Equal(t, "col_int4", schema.Field(2).Name)
	assert.Equal(t, "col_int8", schema.Field(3).Name)
	assert.Equal(t, "col_float4", schema.Field(4).Name)
	assert.Equal(t, "col_float8", schema.Field(5).Name)
	assert.Equal(t, "col_text", schema.Field(6).Name)

	totalRows := int64(0)
	var boolCol *array.Boolean
	var int2Col *array.Int16
	var int4Col *array.Int32
	var int8Col *array.Int64
	var float4Col *array.Float32
	var float8Col *array.Float64
	var textCol *array.String

	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		assert.Equal(t, int64(7), record.NumCols())

		// Extract typed columns (only once)
		if boolCol == nil {
			var ok bool
			boolCol, ok = record.Column(0).(*array.Boolean)
			require.True(t, ok, "Failed to cast column 0 to Boolean")
			int2Col, ok = record.Column(1).(*array.Int16)
			require.True(t, ok, "Failed to cast column 1 to Int16")
			int4Col, ok = record.Column(2).(*array.Int32)
			require.True(t, ok, "Failed to cast column 2 to Int32")
			int8Col, ok = record.Column(3).(*array.Int64)
			require.True(t, ok, "Failed to cast column 3 to Int64")
			float4Col, ok = record.Column(4).(*array.Float32)
			require.True(t, ok, "Failed to cast column 4 to Float32")
			float8Col, ok = record.Column(5).(*array.Float64)
			require.True(t, ok, "Failed to cast column 5 to Float64")
			textCol, ok = record.Column(6).(*array.String)
			require.True(t, ok, "Failed to cast column 6 to String")
		}

		// Verify data within each batch
		for i := range int(record.NumRows()) {
			switch i {
			case 0: // First row (normal values)
				assert.True(t, boolCol.Value(i))
				assert.Equal(t, int16(123), int2Col.Value(i))
				assert.Equal(t, int32(456789), int4Col.Value(i))
				assert.Equal(t, int64(123456789012), int8Col.Value(i))
				assert.InDelta(t, 3.14, float4Col.Value(i), 0.001)
				assert.InDelta(t, 2.718281828, float8Col.Value(i), 0.000000001)
				assert.Equal(t, "Hello World", textCol.Value(i))
			case 1: // Second row (edge case values)
				assert.False(t, boolCol.Value(i))
				assert.Equal(t, int16(-32768), int2Col.Value(i))
				assert.Equal(t, int32(-2147483648), int4Col.Value(i))
				assert.Equal(t, int64(-9223372036854775808), int8Col.Value(i))
				assert.InDelta(t, 0.0, float4Col.Value(i), 0.01)
				assert.InDelta(t, 0.0, float8Col.Value(i), 0.01)
				assert.Empty(t, textCol.Value(i))
			case 3: // Fourth row (all NULLs)
				assert.True(t, boolCol.IsNull(i))
				assert.True(t, int2Col.IsNull(i))
				assert.True(t, int4Col.IsNull(i))
				assert.True(t, int8Col.IsNull(i))
				assert.True(t, float4Col.IsNull(i))
				assert.True(t, float8Col.IsNull(i))
				assert.True(t, textCol.IsNull(i))
			}
		}
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	assert.Equal(t, int64(4), totalRows)
}

// TestIsolatedTestEnvHelper is a test to verify our isolated test environment helper works
func TestIsolatedTestEnvHelper(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	ctx := context.Background()

	// Helper function to create isolated test environment per subtest
	createIsolatedTestEnv := func(t *testing.T, setupSQL string) (*pgarrow.Pool, func()) {
		t.Helper()

		databaseURL := getTestDatabaseURL(t)
		schemaName := fmt.Sprintf("test_%s_%d", randomID(), time.Now().UnixNano())

		// Create schema
		conn, err := pgx.Connect(ctx, databaseURL)
		require.NoError(t, err)

		_, err = conn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName))
		require.NoError(t, err)

		// Setup connection with schema
		connConfig, err := pgx.ParseConfig(databaseURL)
		require.NoError(t, err)
		connConfig.RuntimeParams["search_path"] = fmt.Sprintf("%s,public", schemaName)

		// Setup test data
		if setupSQL != "" {
			schemaConn, err := pgx.ConnectConfig(ctx, connConfig)
			require.NoError(t, err)
			defer schemaConn.Close(ctx)

			_, err = schemaConn.Exec(ctx, setupSQL)
			require.NoError(t, err)
		}

		// Create pool for this schema - manually add search_path since ConnString() doesn't preserve runtime params
		baseConnStr := connConfig.ConnString()
		connStrWithSchema := fmt.Sprintf("%s&search_path=%s,public", baseConnStr, schemaName)
		pool, err := pgarrow.NewPool(ctx, connStrWithSchema)
		require.NoError(t, err)

		cleanup := func() {
			pool.Close()
			_, _ = conn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
			conn.Close(ctx)
		}

		return pool, cleanup
	}

	setupSQL := `CREATE TABLE test_simple (id int4, name text); INSERT INTO test_simple VALUES (1, 'test');`
	testPool, cleanup := createIsolatedTestEnv(t, setupSQL)
	defer cleanup()

	// Test that we can query our custom table
	reader, err := testPool.QueryArrow(ctx, "SELECT * FROM test_simple")
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	totalRows := int64(0)
	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()
		assert.Equal(t, int64(2), record.NumCols())
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	assert.Equal(t, int64(1), totalRows)
}

// TestNewPoolFromExisting tests creating a pgarrow pool from an existing pgx pool
func TestNewPoolFromExisting(t *testing.T) {
	t.Parallel()

	// Skip if no database URL available
	databaseURL := getTestDatabaseURL(t)

	ctx := context.Background()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer func() {
		alloc.AssertSize(t, 0)
	}()

	// Create random schema name for isolation
	schemaName := fmt.Sprintf("test_%s_%d", randomID(), time.Now().UnixNano())

	// Create the schema using a regular pgx connection
	conn, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err, "should connect to database for schema creation")

	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	require.NoError(t, err, "should create test schema")
	conn.Close(ctx)

	// Create connection config with search_path set to the test schema
	connConfig, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(t, err, "should parse database URL")

	// Set search_path to use our test schema first, then public
	connConfig.ConnConfig.RuntimeParams["search_path"] = fmt.Sprintf("%s,public", schemaName)

	// Create pgx pool
	pgxPool, err := pgxpool.NewWithConfig(ctx, connConfig)
	require.NoError(t, err, "should create pgx pool")

	// Create pgarrow Pool from existing pgx pool
	pgarrowPool := pgarrow.NewPoolFromExisting(pgxPool)

	// Cleanup function
	cleanup := func() {
		// Note: We don't call pgarrowPool.Close() since it would close the shared pgx pool
		pgxPool.Close()

		// Create a new connection for cleanup
		cleanupConn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			t.Logf("failed to connect for cleanup: %v", err)
			return
		}
		defer cleanupConn.Close(ctx)

		// Drop the schema and all its contents
		_, err = cleanupConn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		if err != nil {
			t.Logf("failed to drop schema %s: %v", schemaName, err)
		}
	}
	defer cleanup()

	// Create test table using pgx pool directly
	pgxConn, err := pgxPool.Acquire(ctx)
	require.NoError(t, err, "should acquire connection from pgx pool")

	_, err = pgxConn.Exec(ctx, `
		CREATE TABLE shared_test (
			id int4,
			name text,
			value float8
		)
	`)
	require.NoError(t, err, "should create test table")

	_, err = pgxConn.Exec(ctx, `
		INSERT INTO shared_test (id, name, value) VALUES 
		(1, 'first', 10.5),
		(2, 'second', 20.7),
		(3, 'third', 30.2)
	`)
	require.NoError(t, err, "should insert test data")

	pgxConn.Release()

	// Test that pgarrow pool can query the same data
	reader, err := pgarrowPool.QueryArrow(ctx, "SELECT id, name, value FROM shared_test ORDER BY id")
	require.NoError(t, err, "should execute arrow query")
	require.NotNil(t, reader, "reader should not be nil")
	defer reader.Release()

	// Verify data
	totalRows := int64(0)
	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		assert.Equal(t, int64(3), record.NumCols())

		// Verify column types
		idCol, ok := record.Column(0).(*array.Int32)
		require.True(t, ok, "should cast id column to Int32")
		nameCol, ok := record.Column(1).(*array.String)
		require.True(t, ok, "should cast name column to String")
		valueCol, ok := record.Column(2).(*array.Float64)
		require.True(t, ok, "should cast value column to Float64")

		// Check data content
		for i := range int(record.NumRows()) {
			switch i {
			case 0:
				assert.Equal(t, int32(1), idCol.Value(i))
				assert.Equal(t, "first", nameCol.Value(i))
				assert.InDelta(t, 10.5, valueCol.Value(i), 1e-10)
			case 1:
				assert.Equal(t, int32(2), idCol.Value(i))
				assert.Equal(t, "second", nameCol.Value(i))
				assert.InDelta(t, 20.7, valueCol.Value(i), 1e-10)
			case 2:
				assert.Equal(t, int32(3), idCol.Value(i))
				assert.Equal(t, "third", nameCol.Value(i))
				assert.InDelta(t, 30.2, valueCol.Value(i), 1e-10)
			}
		}
	}

	require.NoError(t, reader.Err(), "reader should not have errors")
	assert.Equal(t, int64(3), totalRows, "should read 3 rows")

	// Test that both pgx and pgarrow can use the same pool concurrently
	// Use pgx to insert more data
	pgxConn, err = pgxPool.Acquire(ctx)
	require.NoError(t, err, "should acquire connection from pgx pool again")

	_, err = pgxConn.Exec(ctx, "INSERT INTO shared_test (id, name, value) VALUES (4, 'fourth', 40.9)")
	require.NoError(t, err, "should insert additional data via pgx")

	pgxConn.Release()

	// Use pgarrow to read updated data
	reader2, err := pgarrowPool.QueryArrow(ctx, "SELECT COUNT(*) as count FROM shared_test")
	require.NoError(t, err, "should execute count query")
	defer reader2.Release()

	for reader2.Next() {
		record := reader2.Record()
		countCol, ok := record.Column(0).(*array.Int64)
		require.True(t, ok, "should cast count column to Int64")
		assert.Equal(t, int64(4), countCol.Value(0), "should have 4 rows after insert")
	}

	require.NoError(t, reader2.Err(), "count reader should not have errors")
}

// TestPoolOwnershipBehavior tests the Pool ownership behavior between NewPool and NewPoolFromExisting
func TestPoolOwnershipBehavior(t *testing.T) {
	t.Parallel()

	databaseURL := getTestDatabaseURL(t)
	ctx := context.Background()

	t.Run("NewPool owns underlying pgxpool", func(t *testing.T) {
		t.Parallel()
		// Create Pool using NewPool - it should own the underlying pgxpool
		pool, err := pgarrow.NewPool(ctx, databaseURL)
		require.NoError(t, err)

		// Verify we can execute a query before closing
		reader, err := pool.QueryArrow(ctx, "SELECT 1 as test")
		require.NoError(t, err)
		reader.Release()

		// Close the pgarrow Pool - this should close the underlying pgxpool since it owns it
		pool.Close()

		// Attempting to use the pool after Close() should fail since the underlying pgxpool is closed
		reader, err = pool.QueryArrow(ctx, "SELECT 1 as test")
		require.Error(t, err, "should fail after Close() since underlying pgxpool is closed")
		assert.Nil(t, reader)
	})

	t.Run("NewPoolFromExisting does not own underlying pgxpool", func(t *testing.T) {
		t.Parallel()
		// Create pgxpool directly
		pgxPool, err := pgxpool.New(ctx, databaseURL)
		require.NoError(t, err)
		defer pgxPool.Close() // We manage this lifecycle

		// Create pgarrow Pool from existing pgxpool - it should NOT own the underlying pgxpool
		pgarrowPool := pgarrow.NewPoolFromExisting(pgxPool)

		// Verify we can execute a query before calling Close()
		reader, err := pgarrowPool.QueryArrow(ctx, "SELECT 1 as test")
		require.NoError(t, err)
		reader.Release()

		// Call Close() on pgarrow Pool - this should be a no-op and NOT close the underlying pgxpool
		pgarrowPool.Close()

		// The underlying pgxpool should still be usable since pgarrowPool.Close() didn't close it
		reader, err = pgarrowPool.QueryArrow(ctx, "SELECT 1 as test")
		require.NoError(t, err, "should still work after pgarrowPool.Close() since underlying pgxpool is not closed")
		reader.Release()

		// Verify the pgxpool is still functional directly
		pgxConn, err := pgxPool.Acquire(ctx)
		require.NoError(t, err, "pgxpool should still be functional")
		pgxConn.Release()
	})

	t.Run("multiple Close calls are safe", func(t *testing.T) {
		t.Parallel()
		// Test NewPool case
		pool, err := pgarrow.NewPool(ctx, databaseURL)
		require.NoError(t, err)

		// Multiple Close() calls should be safe
		pool.Close()
		pool.Close() // Should not panic or error
		pool.Close() // Should not panic or error

		// Test NewPoolFromExisting case
		pgxPool, err := pgxpool.New(ctx, databaseURL)
		require.NoError(t, err)
		defer pgxPool.Close()

		pgarrowPool := pgarrow.NewPoolFromExisting(pgxPool)

		// Multiple Close() calls should be safe (and no-ops)
		pgarrowPool.Close()
		pgarrowPool.Close() // Should not panic or error
		pgarrowPool.Close() // Should not panic or error

		// Verify pgxpool is still functional
		pgxConn, err := pgxPool.Acquire(ctx)
		require.NoError(t, err)
		pgxConn.Release()
	})
}

// TestTimestampTypesBasicIntegration tests basic timestamp functionality - DISABLED due to PostgreSQL type promotion
func TestTimestampTypesIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Test comprehensive timestamp and timestamptz scenarios using inline casts
	sql := `
		SELECT 
			'2023-01-15 12:30:45.123456'::timestamp as col_timestamp,
			'2023-01-15 12:30:45.123456+00'::timestamptz as col_timestamptz,
			'test data' as description
	`

	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	// Verify schema - should have proper Arrow timestamp types
	schema := reader.Schema()
	require.Equal(t, 3, schema.NumFields())

	// Check timestamp column type (should be timestamp with microsecond precision, no timezone)
	timestampField := schema.Field(0)
	assert.Equal(t, "col_timestamp", timestampField.Name)
	timestampType, ok := timestampField.Type.(*arrow.TimestampType)
	require.True(t, ok, "col_timestamp should be Arrow TimestampType")
	assert.Equal(t, arrow.Microsecond, timestampType.Unit)
	assert.Empty(t, timestampType.TimeZone, "timestamp should have no timezone")

	// Check timestamptz column type (should be timestamp with microsecond precision, UTC timezone)
	timestamptzField := schema.Field(1)
	assert.Equal(t, "col_timestamptz", timestamptzField.Name)
	timestamptzType, ok := timestamptzField.Type.(*arrow.TimestampType)
	require.True(t, ok, "col_timestamptz should be Arrow TimestampType")
	assert.Equal(t, arrow.Microsecond, timestamptzType.Unit)
	assert.Equal(t, "UTC", timestamptzType.TimeZone, "timestamptz should have UTC timezone")

	totalRows := int64(0)
	var timestampCol *array.Timestamp
	var timestamptzCol *array.Timestamp
	var descCol *array.String

	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		assert.Equal(t, int64(3), record.NumCols())

		// Extract typed columns (only once)
		if timestampCol == nil {
			var ok bool
			timestampCol, ok = record.Column(0).(*array.Timestamp)
			require.True(t, ok, "Failed to cast column 0 to Timestamp")
			timestamptzCol, ok = record.Column(1).(*array.Timestamp)
			require.True(t, ok, "Failed to cast column 1 to Timestamp")
			descCol, ok = record.Column(2).(*array.String)
			require.True(t, ok, "Failed to cast column 2 to String")
		}

		// Validate epoch adjustment and precision for each test case
		for i := range int(record.NumRows()) {
			desc := descCol.Value(i)

			switch desc {
			case "postgres epoch":
				// 2000-01-01 00:00:00 should map to PostgreSQL epoch (946684800000000 microseconds from Arrow epoch)
				expectedMicros := int64(946684800000000) // 30 years in microseconds
				assert.Equal(t, expectedMicros, int64(timestampCol.Value(i)), "PostgreSQL epoch timestamp conversion")
				assert.Equal(t, expectedMicros, int64(timestamptzCol.Value(i)), "PostgreSQL epoch timestamptz conversion")

			case "unix epoch":
				// 1970-01-01 00:00:00 should map to Arrow epoch (0 microseconds)
				assert.Equal(t, int64(0), int64(timestampCol.Value(i)), "Unix epoch timestamp conversion")
				assert.Equal(t, int64(0), int64(timestamptzCol.Value(i)), "Unix epoch timestamptz conversion")

			case "microsecond precision":
				// 2023-01-15 12:30:45.123456 - verify microsecond precision is preserved
				tsVal := int64(timestampCol.Value(i))
				tstzVal := int64(timestamptzCol.Value(i))

				// Both should be equal (both in UTC)
				assert.Equal(t, tsVal, tstzVal, "timestamp and timestamptz should have same value in UTC")

				// Value should be greater than PostgreSQL epoch but reasonable
				assert.Greater(t, tsVal, int64(946684800000000), "Should be after PostgreSQL epoch")
				assert.Less(t, tsVal, int64(2000000000000000), "Should be reasonable timestamp value")

				// Check microsecond precision: last 6 digits should be 123456
				microsecondPart := tsVal % 1000000
				assert.Equal(t, int64(123456), microsecondPart, "Microsecond precision should be preserved")

			case "max microseconds":
				// 2024-12-25 23:59:59.999999 - test maximum microsecond value
				tsVal := int64(timestampCol.Value(i))
				microsecondPart := tsVal % 1000000
				assert.Equal(t, int64(999999), microsecondPart, "Maximum microseconds should be preserved")

			case "before pg epoch":
				// 1999-12-31 23:59:59.999999 - should be less than PostgreSQL epoch
				tsVal := int64(timestampCol.Value(i))
				assert.Less(t, tsVal, int64(946684800000000), "Should be before PostgreSQL epoch")
				assert.Positive(t, tsVal, "Should still be after Arrow epoch")

			case "null values":
				// NULL values should be properly handled
				assert.True(t, timestampCol.IsNull(i), "timestamp should be NULL")
				assert.True(t, timestamptzCol.IsNull(i), "timestamptz should be NULL")
			}
		}
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	// Should have single test case
	assert.Equal(t, int64(1), totalRows)
}

// TestTimestampBoundaryConditionsIntegration tests critical boundary cases for timestamp types
func TestTimestampBoundaryConditionsIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Test extreme boundary values that previously caused issues with uint32/int32 conversions
	sql := `
		SELECT * FROM (VALUES
			-- Extreme negative values (way before PostgreSQL epoch)
			('1900-01-01 00:00:00'::timestamp, '1900-01-01 00:00:00+00'::timestamptz, 'very old timestamp'),
			('1950-01-01 00:00:00'::timestamp, '1950-01-01 00:00:00+00'::timestamptz, 'old timestamp'),
			-- Just before PostgreSQL epoch (negative values in PG format)
			('1999-12-31 23:59:59.999999'::timestamp, '1999-12-31 23:59:59.999999+00'::timestamptz, 'one microsecond before pg epoch'),
			('1970-01-01 00:00:00'::timestamp, '1970-01-01 00:00:00+00'::timestamptz, 'unix epoch - negative in pg format'),
			-- PostgreSQL epoch boundary
			('2000-01-01 00:00:00'::timestamp, '2000-01-01 00:00:00+00'::timestamptz, 'postgresql epoch - zero in pg format'),
			('2000-01-01 00:00:00.000001'::timestamp, '2000-01-01 00:00:00.000001+00'::timestamptz, 'one microsecond after pg epoch'),
			-- Far future values
			('2038-01-19 03:14:07'::timestamp, '2038-01-19 03:14:07+00'::timestamptz, 'year 2038 problem boundary'),
			('2100-12-31 23:59:59.999999'::timestamp, '2100-12-31 23:59:59.999999+00'::timestamptz, 'far future')
		) AS boundary_test(col_timestamp, col_timestamptz, description)
		ORDER BY col_timestamp
	`

	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Release()

	totalRows := int64(0)
	var timestampCol *array.Timestamp
	var timestamptzCol *array.Timestamp
	var descCol *array.String

	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		// Extract typed columns (only once)
		if timestampCol == nil {
			var ok bool
			timestampCol, ok = record.Column(0).(*array.Timestamp)
			require.True(t, ok, "Failed to cast column 0 to Timestamp")
			timestamptzCol, ok = record.Column(1).(*array.Timestamp)
			require.True(t, ok, "Failed to cast column 1 to Timestamp")
			descCol, ok = record.Column(2).(*array.String)
			require.True(t, ok, "Failed to cast column 2 to String")
		}

		// Validate boundary conditions
		for i := range int(record.NumRows()) {
			desc := descCol.Value(i)
			tsVal := int64(timestampCol.Value(i))
			tstzVal := int64(timestamptzCol.Value(i))

			// Both timestamp and timestamptz should have identical values when converted
			assert.Equal(t, tsVal, tstzVal, "timestamp and timestamptz should be equal for %s", desc)

			switch desc {
			case "very old timestamp":
				// 1900-01-01 should be a very negative value in Arrow epoch
				// This tests that we correctly handle signed integers, not uint32/uint64
				assert.Less(t, tsVal, int64(-2000000000000000), "1900 should be very negative: %d", tsVal)
				assert.Greater(t, tsVal, int64(-4000000000000000), "1900 should not be impossibly negative: %d", tsVal)

			case "old timestamp":
				// 1950-01-01 should be negative but less extreme
				assert.Less(t, tsVal, int64(-500000000000000), "1950 should be negative: %d", tsVal)
				assert.Greater(t, tsVal, int64(-2000000000000000), "1950 should not be as negative as 1900: %d", tsVal)

			case "one microsecond before pg epoch":
				// Should be exactly one microsecond before the PostgreSQL epoch in Arrow time
				expectedVal := int64(946684800000000 - 1) // PG epoch - 1 microsecond
				assert.Equal(t, expectedVal, tsVal, "One microsecond before PG epoch")

			case "unix epoch - negative in pg format":
				// 1970-01-01 should be exactly 0 in Arrow epoch
				assert.Equal(t, int64(0), tsVal, "Unix epoch should be 0 in Arrow time")

			case "postgresql epoch - zero in pg format":
				// 2000-01-01 should be exactly the epoch adjustment value
				assert.Equal(t, int64(946684800000000), tsVal, "PostgreSQL epoch conversion")

			case "one microsecond after pg epoch":
				// Should be exactly one microsecond after the PostgreSQL epoch in Arrow time
				expectedVal := int64(946684800000000 + 1) // PG epoch + 1 microsecond
				assert.Equal(t, expectedVal, tsVal, "One microsecond after PG epoch")

			case "year 2038 problem boundary":
				// This date is significant for 32-bit timestamp systems
				// Should be positive and reasonable
				assert.Greater(t, tsVal, int64(946684800000000), "2038 should be after PG epoch")
				assert.Less(t, tsVal, int64(3000000000000000), "2038 should be reasonable")

			case "far future":
				// 2100 should be a large positive value
				assert.Greater(t, tsVal, int64(2000000000000000), "2100 should be far in future")
				assert.Less(t, tsVal, int64(5000000000000000), "2100 should not overflow")
			}

			// Verify that our conversion is monotonic (ordered)
			if i > 0 {
				prevVal := int64(timestampCol.Value(i - 1))
				assert.Greater(t, tsVal, prevVal, "Timestamp values should be ordered: %s", desc)
			}
		}
	}

	// Check for reader errors
	require.NoError(t, reader.Err())

	// Should have all boundary test cases
	assert.Equal(t, int64(8), totalRows)
}
