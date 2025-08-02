package pgarrow_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"os"
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

// setupTestDB creates a test database with an isolated schema for each test.
// It returns a pgarrow Pool and a cleanup function.
func setupTestDB(t *testing.T) (*pgarrow.Pool, func()) {
	t.Helper()

	// Get database URL from environment
	databaseURL := getTestDatabaseURL(t)

	// Create random schema name for isolation
	schemaName := fmt.Sprintf("test_%s_%d", randomID(), time.Now().UnixNano())

	// Create the schema using a regular pgx connection
	conn, err := pgx.Connect(context.Background(), databaseURL)
	require.NoError(t, err, "should connect to database for schema creation")

	_, err = conn.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	require.NoError(t, err, "should create test schema")
	conn.Close(context.Background())

	// Create connection string with search_path set to the test schema
	connConfig, err := pgx.ParseConfig(databaseURL)
	require.NoError(t, err, "should parse database URL")

	// Set search_path to use our test schema first, then public
	connConfig.RuntimeParams["search_path"] = fmt.Sprintf("%s,public", schemaName)
	connStr := connConfig.ConnString()

	// Create pgarrow Pool with the schema-specific connection
	pool, err := pgarrow.NewPool(context.Background(), connStr)
	require.NoError(t, err, "should create pgarrow pool")

	// Create test tables in the schema
	setupTestTables(t, databaseURL, schemaName)

	// Return cleanup function
	cleanup := func() {
		// Close the pool first
		pool.Close()

		// Create a new connection for cleanup
		cleanupConn, err := pgx.Connect(context.Background(), databaseURL)
		if err != nil {
			t.Logf("failed to connect for cleanup: %v", err)
			return
		}
		defer cleanupConn.Close(context.Background())

		// Drop the schema and all its contents
		_, err = cleanupConn.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
		if err != nil {
			t.Logf("failed to drop schema %s: %v", schemaName, err)
		}
	}

	return pool, cleanup
}

// setupTestTables creates test tables in the current schema
func setupTestTables(t *testing.T, databaseURL, schemaName string) {
	t.Helper()

	ctx := context.Background()

	// Create a separate pgx connection for DDL operations
	// We'll use the same connection string that the pool uses
	connConfig, err := pgx.ParseConfig(databaseURL)
	require.NoError(t, err)

	// Set search_path to our test schema
	connConfig.RuntimeParams["search_path"] = fmt.Sprintf("%s,public", schemaName)

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Create test table with all supported data types
	createTableSQL := `
		CREATE TABLE test_all_types (
			id SERIAL PRIMARY KEY,
			col_bool BOOLEAN,
			col_int2 SMALLINT,
			col_int4 INTEGER,
			col_int8 BIGINT,
			col_float4 REAL,
			col_float8 DOUBLE PRECISION,
			col_text TEXT
		)
	`
	_, err = conn.Exec(ctx, createTableSQL)
	require.NoError(t, err, "should create test table")

	// Insert test data
	insertSQL := `
		INSERT INTO test_all_types (col_bool, col_int2, col_int4, col_int8, col_float4, col_float8, col_text)
		VALUES 
			(true, 100, 1000, 10000, 3.14, 2.71828, 'hello'),
			(false, 200, 2000, 20000, 6.28, 1.41421, 'world'),
			(null, null, null, null, null, null, null)
	`
	_, err = conn.Exec(ctx, insertSQL)
	require.NoError(t, err, "should insert test data")

	// Create simple test table
	simpleTableSQL := `
		CREATE TABLE simple_test (
			id INTEGER,
			name TEXT,
			active BOOLEAN
		)
	`
	_, err = conn.Exec(ctx, simpleTableSQL)
	require.NoError(t, err, "should create simple test table")

	insertSimpleSQL := `
		INSERT INTO simple_test (id, name, active)
		VALUES 
			(1, 'first', true),
			(2, 'second', false)
	`
	_, err = conn.Exec(ctx, insertSimpleSQL)
	require.NoError(t, err, "should insert simple test data")
}

// getTestDatabaseURL returns the test database URL from environment
func getTestDatabaseURL(t *testing.T) string {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping integration test")
	}
	return databaseURL
}

// randomID generates a random string for schema names
func randomID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

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

// TestQueryArrowDataTypes is a comprehensive table-based test covering all PostgreSQL data types
// This replaces multiple unit test files with a single comprehensive integration test
func TestQueryArrowDataTypes(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

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

	tests := []struct {
		name         string
		setupSQL     string
		querySQL     string
		args         []any
		expectedRows int64
		expectedCols int64
		validateFunc func(t *testing.T, record arrow.Record)
	}{
		{
			name:         "bool_all_values",
			setupSQL:     `CREATE TABLE test_bool (val bool); INSERT INTO test_bool VALUES (true), (false), (null);`,
			querySQL:     "SELECT * FROM test_bool ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 3,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				boolCol, ok := record.Column(0).(*array.Boolean)
				require.True(t, ok)

				assert.False(t, boolCol.Value(0)) // false comes first
				assert.True(t, boolCol.Value(1))  // then true
				assert.True(t, boolCol.IsNull(2)) // NULL comes last
			},
		},
		{
			name:         "int2_edge_cases",
			setupSQL:     `CREATE TABLE test_int2 (val int2); INSERT INTO test_int2 VALUES (32767), (-32768), (0), (null);`,
			querySQL:     "SELECT * FROM test_int2 ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				int2Col, ok := record.Column(0).(*array.Int16)
				require.True(t, ok)

				assert.Equal(t, int16(-32768), int2Col.Value(0)) // MIN_INT16
				assert.Equal(t, int16(0), int2Col.Value(1))
				assert.Equal(t, int16(32767), int2Col.Value(2)) // MAX_INT16
				assert.True(t, int2Col.IsNull(3))
			},
		},
		{
			name:         "int4_edge_cases",
			setupSQL:     `CREATE TABLE test_int4 (val int4); INSERT INTO test_int4 VALUES (2147483647), (-2147483648), (0), (null);`,
			querySQL:     "SELECT * FROM test_int4 ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				int4Col, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)

				assert.Equal(t, int32(-2147483648), int4Col.Value(0)) // MIN_INT32
				assert.Equal(t, int32(0), int4Col.Value(1))
				assert.Equal(t, int32(2147483647), int4Col.Value(2)) // MAX_INT32
				assert.True(t, int4Col.IsNull(3))
			},
		},
		{
			name:         "int8_edge_cases",
			setupSQL:     `CREATE TABLE test_int8 (val int8); INSERT INTO test_int8 VALUES (9223372036854775807), (-9223372036854775808), (0), (null);`,
			querySQL:     "SELECT * FROM test_int8 ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				int8Col, ok := record.Column(0).(*array.Int64)
				require.True(t, ok)

				assert.Equal(t, int64(-9223372036854775808), int8Col.Value(0)) // MIN_INT64
				assert.Equal(t, int64(0), int8Col.Value(1))
				assert.Equal(t, int64(9223372036854775807), int8Col.Value(2)) // MAX_INT64
				assert.True(t, int8Col.IsNull(3))
			},
		},
		{
			name:         "float4_precision",
			setupSQL:     `CREATE TABLE test_float4 (val float4); INSERT INTO test_float4 VALUES (3.14159), (-3.14159), (0.0), ('Infinity'::float4), ('-Infinity'::float4), ('NaN'::float4), (null);`,
			querySQL:     "SELECT * FROM test_float4 ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 7,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				float4Col, ok := record.Column(0).(*array.Float32)
				require.True(t, ok)

				// Check for NaN, Infinity values (order might vary due to special float handling)
				hasNaN := false
				hasInf := false
				hasNegInf := false
				regularValues := []float32{}

				for i := 0; i < int(record.NumRows()); i++ {
					if !float4Col.IsNull(i) {
						val := float4Col.Value(i)
						if val != val { // NaN check
							hasNaN = true
						} else if val == float32(math.Inf(1)) {
							hasInf = true
						} else if val == float32(math.Inf(-1)) {
							hasNegInf = true
						} else {
							regularValues = append(regularValues, val)
						}
					}
				}

				assert.True(t, hasNaN, "Should have NaN value")
				assert.True(t, hasInf, "Should have +Infinity")
				assert.True(t, hasNegInf, "Should have -Infinity")
				assert.Contains(t, regularValues, float32(3.14159))
				assert.Contains(t, regularValues, float32(-3.14159))
				assert.Contains(t, regularValues, float32(0.0))
				assert.True(t, float4Col.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "float8_precision",
			setupSQL:     `CREATE TABLE test_float8 (val float8); INSERT INTO test_float8 VALUES (2.718281828459045), (-2.718281828459045), (0.0), ('Infinity'::float8), ('-Infinity'::float8), ('NaN'::float8), (null);`,
			querySQL:     "SELECT * FROM test_float8 ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 7,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				float8Col, ok := record.Column(0).(*array.Float64)
				require.True(t, ok)

				// Similar validation as float4 but with higher precision
				hasNaN := false
				hasInf := false
				hasNegInf := false
				regularValues := []float64{}

				for i := 0; i < int(record.NumRows()); i++ {
					if !float8Col.IsNull(i) {
						val := float8Col.Value(i)
						if val != val { // NaN check
							hasNaN = true
						} else if val == math.Inf(1) {
							hasInf = true
						} else if val == math.Inf(-1) {
							hasNegInf = true
						} else {
							regularValues = append(regularValues, val)
						}
					}
				}

				assert.True(t, hasNaN, "Should have NaN value")
				assert.True(t, hasInf, "Should have +Infinity")
				assert.True(t, hasNegInf, "Should have -Infinity")
				// Find the e value (2.718...) in regularValues
				foundE := false
				for _, val := range regularValues {
					if val > 2.7 && val < 2.8 {
						assert.InDelta(t, 2.718281828459045, val, 0.000000000000001)
						foundE = true
						break
					}
				}
				assert.True(t, foundE, "Should find e value in regular values")
				assert.True(t, float8Col.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "text_encoding_cases",
			setupSQL:     `CREATE TABLE test_text (val text); INSERT INTO test_text VALUES ('Hello World'), (''), ('Unicode: 🚀 αβγ 中文'), ('Special' || chr(10) || 'Chars' || chr(9) || '"'), (null);`,
			querySQL:     "SELECT * FROM test_text ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 5,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				textCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0, record.NumRows()-1) // -1 for NULL
				for i := 0; i < int(record.NumRows()); i++ {
					if !textCol.IsNull(i) {
						values = append(values, textCol.Value(i))
					}
				}

				assert.Contains(t, values, "")
				assert.Contains(t, values, "Hello World")
				assert.Contains(t, values, "Unicode: 🚀 αβγ 中文")
				assert.Contains(t, values, "Special\nChars\t\"")
				assert.True(t, textCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name: "mixed_types_literal_filter",
			setupSQL: `CREATE TABLE test_mixed (id int4, name text, score float8, active bool); 
					   INSERT INTO test_mixed VALUES (1, 'Alice', 95.5, true), (2, 'Bob', 87.2, false), (3, 'Charlie', 92.1, true);`,
			querySQL:     "SELECT * FROM test_mixed WHERE score > 90.0 AND active = true ORDER BY score DESC",
			args:         nil,
			expectedRows: 2,
			expectedCols: 4,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "name", schema.Field(1).Name)
				assert.Equal(t, "score", schema.Field(2).Name)
				assert.Equal(t, "active", schema.Field(3).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				nameCol, ok := record.Column(1).(*array.String)
				require.True(t, ok)
				scoreCol, ok := record.Column(2).(*array.Float64)
				require.True(t, ok)
				activeCol, ok := record.Column(3).(*array.Boolean)
				require.True(t, ok)

				// First row should be Alice (highest score)
				assert.Equal(t, int32(1), idCol.Value(0))
				assert.Equal(t, "Alice", nameCol.Value(0))
				assert.InDelta(t, 95.5, scoreCol.Value(0), 0.01)
				assert.True(t, activeCol.Value(0))

				// Second row should be Charlie
				assert.Equal(t, int32(3), idCol.Value(1))
				assert.Equal(t, "Charlie", nameCol.Value(1))
				assert.InDelta(t, 92.1, scoreCol.Value(1), 0.01)
				assert.True(t, activeCol.Value(1))
			},
		},
		{
			name: "all_nulls_row",
			setupSQL: `CREATE TABLE test_nulls (col_bool bool, col_int2 int2, col_int4 int4, col_int8 int8, col_float4 float4, col_float8 float8, col_text text);
					   INSERT INTO test_nulls VALUES (null, null, null, null, null, null, null);`,
			querySQL:     "SELECT * FROM test_nulls",
			args:         nil,
			expectedRows: 1,
			expectedCols: 7,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// All columns should have NULL in the single row
				for i := range int(record.NumCols()) {
					col := record.Column(i)
					assert.True(t, col.IsNull(0), "Column %d should be NULL", i)
				}
			},
		},
		{
			name:         "varchar_all_values",
			setupSQL:     `CREATE TABLE test_varchar (val varchar(50)); INSERT INTO test_varchar VALUES ('Hello VARCHAR'), (''), ('Unicode: 🚀 αβγ 中文'), (null);`,
			querySQL:     "SELECT * FROM test_varchar ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				varcharCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !varcharCol.IsNull(i) {
						values = append(values, varcharCol.Value(i))
					}
				}

				assert.Contains(t, values, "")
				assert.Contains(t, values, "Hello VARCHAR")
				assert.Contains(t, values, "Unicode: 🚀 αβγ 中文")
				assert.True(t, varcharCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "bpchar_all_values",
			setupSQL:     `CREATE TABLE test_bpchar (val char(20)); INSERT INTO test_bpchar VALUES ('Hello BPCHAR'), (''), ('Unicode: 🚀'), (null);`,
			querySQL:     "SELECT * FROM test_bpchar ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				bpcharCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !bpcharCol.IsNull(i) {
						val := bpcharCol.Value(i)
						values = append(values, val)
					}
				}

				// Note: BPCHAR pads with spaces, so we check for trimmed values
				found := false
				for _, val := range values {
					if val == "Hello BPCHAR" || (len(val) >= len("Hello BPCHAR") && val[:len("Hello BPCHAR")] == "Hello BPCHAR") {
						found = true
						break
					}
				}
				assert.True(t, found, "Should find 'Hello BPCHAR' value")
				assert.True(t, bpcharCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "name_all_values",
			setupSQL:     `CREATE TABLE test_name (val name); INSERT INTO test_name VALUES ('table_name'), ('column_name'), (''), (null);`,
			querySQL:     "SELECT * FROM test_name ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				nameCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !nameCol.IsNull(i) {
						values = append(values, nameCol.Value(i))
					}
				}

				assert.Contains(t, values, "")
				assert.Contains(t, values, "table_name")
				assert.Contains(t, values, "column_name")
				assert.True(t, nameCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "char_all_values",
			setupSQL:     `CREATE TABLE test_char (val "char"); INSERT INTO test_char VALUES ('A'), ('Z'), ('1'), (null);`,
			querySQL:     "SELECT * FROM test_char ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				charCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !charCol.IsNull(i) {
						values = append(values, charCol.Value(i))
					}
				}

				// "char" type stores single bytes, so check for presence of expected values
				assert.Len(t, values, 3, "Should have 3 non-NULL values")
				assert.Contains(t, values, "A")
				assert.Contains(t, values, "Z")
				assert.Contains(t, values, "1")
				assert.True(t, charCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "bytea_basic_values",
			setupSQL:     `CREATE TABLE test_bytea (val bytea); INSERT INTO test_bytea VALUES (E'\\x48656c6c6f20576f726c64'), (E'\\x'), (E'\\x00FF00FF'), (null);`,
			querySQL:     "SELECT * FROM test_bytea ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				byteaCol, ok := record.Column(0).(*array.Binary)
				require.True(t, ok)

				values := make([][]byte, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !byteaCol.IsNull(i) {
						values = append(values, byteaCol.Value(i))
					}
				}

				// Check for expected values
				foundEmpty := false
				foundHello := false
				foundPattern := false
				for _, val := range values {
					if len(val) == 0 {
						foundEmpty = true
					} else if string(val) == "Hello World" {
						foundHello = true
					} else if len(val) == 4 && val[0] == 0x00 && val[1] == 0xFF && val[2] == 0x00 && val[3] == 0xFF {
						foundPattern = true
					}
				}

				assert.True(t, foundEmpty, "Should find empty bytea")
				assert.True(t, foundHello, "Should find 'Hello World' bytea")
				assert.True(t, foundPattern, "Should find 0x00FF00FF pattern")
				assert.True(t, byteaCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "bytea_special_bytes",
			setupSQL:     `CREATE TABLE test_bytea_special (val bytea); INSERT INTO test_bytea_special VALUES (E'\\x0001020304050607080910111213141516171819'), (E'\\xFFFFFFFF'), (E'\\x000000'), (null);`,
			querySQL:     "SELECT * FROM test_bytea_special ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				byteaCol, ok := record.Column(0).(*array.Binary)
				require.True(t, ok)

				values := make([][]byte, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !byteaCol.IsNull(i) {
						values = append(values, byteaCol.Value(i))
					}
				}

				// Check for expected patterns
				foundZeros := false
				foundFFs := false
				foundSequence := false
				for _, val := range values {
					if len(val) == 3 && val[0] == 0x00 && val[1] == 0x00 && val[2] == 0x00 {
						foundZeros = true
					} else if len(val) == 4 && val[0] == 0xFF && val[1] == 0xFF && val[2] == 0xFF && val[3] == 0xFF {
						foundFFs = true
					} else if len(val) == 20 && val[0] == 0x00 && val[1] == 0x01 && val[19] == 0x19 {
						// Check the sequential bytes pattern: 00 01 02 03 ... 19 (20 bytes total)
						foundSequence = true
					}
				}

				assert.True(t, foundZeros, "Should find zero bytes pattern")
				assert.True(t, foundFFs, "Should find 0xFF bytes pattern")
				assert.True(t, foundSequence, "Should find sequential bytes pattern")
				assert.True(t, byteaCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "date_edge_cases",
			setupSQL:     `CREATE TABLE test_date (val date); INSERT INTO test_date VALUES ('2000-01-01'::date), ('1970-01-01'::date), ('2024-12-31'::date), ('1999-12-31'::date), (null);`,
			querySQL:     "SELECT * FROM test_date ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 5,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				dateCol, ok := record.Column(0).(*array.Date32)
				require.True(t, ok)

				values := make([]int32, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !dateCol.IsNull(i) {
						values = append(values, int32(dateCol.Value(i)))
					}
				}

				// Check for expected values
				// 1970-01-01 should be 0 (Arrow epoch)
				// 2000-01-01 should be 10957 (PostgreSQL epoch + adjustment)
				foundArrowEpoch := false
				foundPgEpoch := false
				for _, val := range values {
					switch val {
					case 0:
						foundArrowEpoch = true // 1970-01-01
					case 10957:
						foundPgEpoch = true // 2000-01-01
					}
				}

				assert.True(t, foundArrowEpoch, "Should find Arrow epoch date (1970-01-01)")
				assert.True(t, foundPgEpoch, "Should find PostgreSQL epoch date (2000-01-01)")
				assert.Len(t, values, 4, "Should have 4 non-NULL date values")
				assert.True(t, dateCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "time_edge_cases",
			setupSQL:     `CREATE TABLE test_time (val time); INSERT INTO test_time VALUES ('00:00:00'::time), ('23:59:59.999999'::time), ('12:30:45.123456'::time), ('06:15:30'::time), (null);`,
			querySQL:     "SELECT * FROM test_time ORDER BY val NULLS LAST",
			args:         nil,
			expectedRows: 5,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				timeCol, ok := record.Column(0).(*array.Time64)
				require.True(t, ok)

				values := make([]int64, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !timeCol.IsNull(i) {
						values = append(values, int64(timeCol.Value(i)))
					}
				}

				// Check for expected values
				foundMidnight := false
				foundEndOfDay := false
				for _, val := range values {
					switch {
					case val == 0:
						foundMidnight = true // 00:00:00
					case val >= 86399999999: // Close to 23:59:59.999999
						foundEndOfDay = true
					}
				}

				assert.True(t, foundMidnight, "Should find midnight time (00:00:00)")
				assert.True(t, foundEndOfDay, "Should find end-of-day time")
				assert.Len(t, values, 4, "Should have 4 non-NULL time values")
				assert.True(t, timeCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name: "mixed_temporal_types",
			setupSQL: `CREATE TABLE test_mixed_temporal (
						id int4, 
						event_date date, 
						event_time time,
						description text
					   ); 
					   INSERT INTO test_mixed_temporal VALUES 
					   (1, '2000-01-01'::date, '00:00:00'::time, 'millennium'),
					   (2, '1970-01-01'::date, '12:30:45'::time, 'unix epoch'),
					   (3, '2024-12-31'::date, '23:59:59.999999'::time, 'year end'),
					   (4, null, null, 'nulls');`,
			querySQL:     "SELECT * FROM test_mixed_temporal ORDER BY id",
			args:         nil,
			expectedRows: 4,
			expectedCols: 4,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "event_date", schema.Field(1).Name)
				assert.Equal(t, "event_time", schema.Field(2).Name)
				assert.Equal(t, "description", schema.Field(3).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				dateCol, ok := record.Column(1).(*array.Date32)
				require.True(t, ok)
				timeCol, ok := record.Column(2).(*array.Time64)
				require.True(t, ok)
				descCol, ok := record.Column(3).(*array.String)
				require.True(t, ok)

				// Verify data for each row
				for i := range int(record.NumRows()) {
					switch idCol.Value(i) {
					case 1: // Millennium row
						assert.Equal(t, "millennium", descCol.Value(i))
						assert.Equal(t, int32(10957), int32(dateCol.Value(i))) // 2000-01-01
						assert.Equal(t, int64(0), int64(timeCol.Value(i)))     // 00:00:00
					case 2: // Unix epoch row
						assert.Equal(t, "unix epoch", descCol.Value(i))
						assert.Equal(t, int32(0), int32(dateCol.Value(i))) // 1970-01-01
						// 12:30:45 = 12*3600 + 30*60 + 45 = 45045 seconds = 45045000000 microseconds
						assert.Equal(t, int64(45045000000), int64(timeCol.Value(i)))
					case 3: // Year end row
						assert.Equal(t, "year end", descCol.Value(i))
						// 2024-12-31 is 20088 days from 1970-01-01
						assert.Greater(t, int32(dateCol.Value(i)), int32(19000))       // Should be around 20088
						assert.Greater(t, int64(timeCol.Value(i)), int64(86399000000)) // Near end of day
					case 4: // NULL row
						assert.Equal(t, "nulls", descCol.Value(i))
						assert.True(t, dateCol.IsNull(i))
						assert.True(t, timeCol.IsNull(i))
					}
				}
			},
		},
		{
			name: "mixed_types_with_bytea",
			setupSQL: `CREATE TABLE test_mixed_bytea (
						id int4, 
						name text, 
						data bytea, 
						active bool
					   ); 
					   INSERT INTO test_mixed_bytea VALUES 
					   (1, 'first', E'\\x48656c6c6f', true),
					   (2, 'second', E'\\x', false),
					   (3, 'third', null, true);`,
			querySQL:     "SELECT * FROM test_mixed_bytea ORDER BY id",
			args:         nil,
			expectedRows: 3,
			expectedCols: 4,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "name", schema.Field(1).Name)
				assert.Equal(t, "data", schema.Field(2).Name)
				assert.Equal(t, "active", schema.Field(3).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				nameCol, ok := record.Column(1).(*array.String)
				require.True(t, ok)
				dataCol, ok := record.Column(2).(*array.Binary)
				require.True(t, ok)
				activeCol, ok := record.Column(3).(*array.Boolean)
				require.True(t, ok)

				// Verify data for each row
				for i := range int(record.NumRows()) {
					switch idCol.Value(i) {
					case 1: // First row
						assert.Equal(t, "first", nameCol.Value(i))
						assert.Equal(t, "Hello", string(dataCol.Value(i)))
						assert.True(t, activeCol.Value(i))
					case 2: // Second row
						assert.Equal(t, "second", nameCol.Value(i))
						assert.Empty(t, dataCol.Value(i)) // Empty bytea
						assert.False(t, activeCol.Value(i))
					case 3: // Third row
						assert.Equal(t, "third", nameCol.Value(i))
						assert.True(t, dataCol.IsNull(i)) // NULL bytea
						assert.True(t, activeCol.Value(i))
					}
				}
			},
		},
		{
			name: "mixed_string_types",
			setupSQL: `CREATE TABLE test_mixed_strings (
						id int4, 
						text_col text, 
						varchar_col varchar(50), 
						bpchar_col char(10), 
						name_col name, 
						char_col "char"
					   ); 
					   INSERT INTO test_mixed_strings VALUES 
					   (1, 'text_value', 'varchar_value', 'bpchar', 'name_val', 'A'),
					   (2, '', '', '', '', '0'),
					   (3, null, null, null, null, null);`,
			querySQL:     "SELECT * FROM test_mixed_strings ORDER BY id",
			args:         nil,
			expectedRows: 3,
			expectedCols: 6,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "text_col", schema.Field(1).Name)
				assert.Equal(t, "varchar_col", schema.Field(2).Name)
				assert.Equal(t, "bpchar_col", schema.Field(3).Name)
				assert.Equal(t, "name_col", schema.Field(4).Name)
				assert.Equal(t, "char_col", schema.Field(5).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				textCol, ok := record.Column(1).(*array.String)
				require.True(t, ok)
				varcharCol, ok := record.Column(2).(*array.String)
				require.True(t, ok)
				bpcharCol, ok := record.Column(3).(*array.String)
				require.True(t, ok)
				nameCol, ok := record.Column(4).(*array.String)
				require.True(t, ok)
				charCol, ok := record.Column(5).(*array.String)
				require.True(t, ok)

				// Verify data for each row
				for i := range int(record.NumRows()) {
					switch idCol.Value(i) {
					case 1: // First row - all have values
						assert.Equal(t, "text_value", textCol.Value(i))
						assert.Equal(t, "varchar_value", varcharCol.Value(i))
						// bpchar may be padded, so check it starts with expected value
						bpcharVal := bpcharCol.Value(i)
						assert.True(t, len(bpcharVal) >= 6 && bpcharVal[:6] == "bpchar", "bpchar should start with 'bpchar'")
						assert.Equal(t, "name_val", nameCol.Value(i))
						// char_col should contain 'A'
						assert.Equal(t, "A", charCol.Value(i))
					case 2: // Second row - all empty strings
						assert.Empty(t, textCol.Value(i))
						assert.Empty(t, varcharCol.Value(i))
						// bpchar empty might be spaces, just check it's not null
						assert.False(t, bpcharCol.IsNull(i))
						assert.Empty(t, nameCol.Value(i))
						// char_col with '0' - just check it's not null
						assert.False(t, charCol.IsNull(i))
						assert.Equal(t, "0", charCol.Value(i))
					case 3: // Third row - all NULLs
						assert.True(t, textCol.IsNull(i))
						assert.True(t, varcharCol.IsNull(i))
						assert.True(t, bpcharCol.IsNull(i))
						assert.True(t, nameCol.IsNull(i))
						assert.True(t, charCol.IsNull(i))
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create isolated test environment with custom tables
			testPool, testCleanup := createIsolatedTestEnv(t, tt.setupSQL)
			defer testCleanup()

			// Execute query
			var reader array.RecordReader
			var err error
			if len(tt.args) > 0 {
				reader, err = testPool.QueryArrow(ctx, tt.querySQL, tt.args...)
			} else {
				reader, err = testPool.QueryArrow(ctx, tt.querySQL)
			}

			require.NoError(t, err, "Query failed for test %s", tt.name)
			require.NotNil(t, reader, "Reader should not be nil for test %s", tt.name)
			defer reader.Release()

			totalRows := int64(0)
			for reader.Next() {
				record := reader.Record()
				totalRows += record.NumRows()

				// Validate basic expectations for each batch
				assert.Equal(t, tt.expectedCols, record.NumCols(), "Column count mismatch for test %s", tt.name)

				// Run custom validation on each record batch
				if tt.validateFunc != nil {
					tt.validateFunc(t, record)
				}
			}

			// Check for reader errors
			require.NoError(t, reader.Err())

			// Validate total row count
			assert.Equal(t, tt.expectedRows, totalRows, "Row count mismatch for test %s", tt.name)
		})
	}
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
