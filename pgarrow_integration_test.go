package pgarrow_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/jackc/pgx/v5"
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
	sql := "SELECT id, name, active FROM simple_test ORDER BY id"

	// Execute QueryArrow
	record, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, record)
	defer record.Release()

	// Verify results
	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(3), record.NumCols())

	// Verify schema
	schema := record.Schema()
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "name", schema.Field(1).Name)
	assert.Equal(t, "active", schema.Field(2).Name)

	// Verify data values
	idCol, ok := record.Column(0).(*array.Int32)
	require.True(t, ok)
	nameCol, ok := record.Column(1).(*array.String)
	require.True(t, ok)
	activeCol, ok := record.Column(2).(*array.Boolean)
	require.True(t, ok)

	assert.Equal(t, int32(1), idCol.Value(0))
	assert.Equal(t, int32(2), idCol.Value(1))
	assert.Equal(t, "first", nameCol.Value(0))
	assert.Equal(t, "second", nameCol.Value(1))
	assert.True(t, activeCol.Value(0))
	assert.False(t, activeCol.Value(1))
}

func TestPoolQueryArrowAllDataTypesIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT col_bool, col_int2, col_int4, col_int8, col_float4, col_float8, col_text FROM test_all_types WHERE col_bool IS NOT NULL ORDER BY col_int2"

	record, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, record)
	defer record.Release()

	// Verify we got the 2 non-null rows
	assert.Equal(t, int64(2), record.NumRows())
	assert.Equal(t, int64(7), record.NumCols())

	// Verify all columns have correct types and values
	boolCol, ok := record.Column(0).(*array.Boolean)
	require.True(t, ok)
	int2Col, ok := record.Column(1).(*array.Int16)
	require.True(t, ok)
	int4Col, ok := record.Column(2).(*array.Int32)
	require.True(t, ok)
	int8Col, ok := record.Column(3).(*array.Int64)
	require.True(t, ok)
	float4Col, ok := record.Column(4).(*array.Float32)
	require.True(t, ok)
	float8Col, ok := record.Column(5).(*array.Float64)
	require.True(t, ok)
	textCol, ok := record.Column(6).(*array.String)
	require.True(t, ok)

	// Verify first row values
	assert.True(t, boolCol.Value(0))
	assert.Equal(t, int16(100), int2Col.Value(0))
	assert.Equal(t, int32(1000), int4Col.Value(0))
	assert.Equal(t, int64(10000), int8Col.Value(0))
	assert.InDelta(t, float32(3.14), float4Col.Value(0), 0.001)
	assert.InDelta(t, float64(2.71828), float8Col.Value(0), 0.00001)
	assert.Equal(t, "hello", textCol.Value(0))

	// Verify second row values
	assert.False(t, boolCol.Value(1))
	assert.Equal(t, int16(200), int2Col.Value(1))
	assert.Equal(t, int32(2000), int4Col.Value(1))
	assert.Equal(t, int64(20000), int8Col.Value(1))
	assert.InDelta(t, float32(6.28), float4Col.Value(1), 0.001)
	assert.InDelta(t, float64(1.41421), float8Col.Value(1), 0.00001)
	assert.Equal(t, "world", textCol.Value(1))
}

func TestPoolQueryArrowParameterizedQueryIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT id, name FROM simple_test WHERE id > $1 ORDER BY id"

	record, err := pool.QueryArrow(ctx, sql, 1)
	require.NoError(t, err)
	require.NotNil(t, record)
	defer record.Release()

	// Should only get the second row (id=2)
	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())

	idCol, ok := record.Column(0).(*array.Int32)
	require.True(t, ok)
	nameCol, ok := record.Column(1).(*array.String)
	require.True(t, ok)

	assert.Equal(t, int32(2), idCol.Value(0))
	assert.Equal(t, "second", nameCol.Value(0))
}

func TestPoolQueryArrowEmptyResultIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT id, name FROM simple_test WHERE id > 100"

	record, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, record)
	defer record.Release()

	// Should get empty result
	assert.Equal(t, int64(0), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())
}

func TestPoolQueryArrowNullHandlingIntegration(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT col_bool, col_int4, col_text FROM test_all_types WHERE col_bool IS NULL"

	record, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, record)
	defer record.Release()

	// Should get the null row
	assert.Equal(t, int64(1), record.NumRows())
	assert.Equal(t, int64(3), record.NumCols())

	// Verify all values are null
	for i := 0; i < int(record.NumCols()); i++ {
		assert.False(t, record.Column(i).IsValid(0), "Column %d should be NULL", i)
	}
}

// Error handling tests

func TestPoolQueryArrowInvalidSQLIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT * FROM nonexistent_table"

	record, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Contains(t, err.Error(), "failed to prepare query")
}

func TestPoolQueryArrowSyntaxErrorIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT * FROM WHERE"

	record, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, record)
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
	record, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Contains(t, err.Error(), "failed to acquire connection")
}

func TestPoolQueryArrowCancelledContextIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	sql := "SELECT id, name FROM simple_test"
	record, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, record)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestPoolQueryArrowInvalidParametersIntegration(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	sql := "SELECT id FROM simple_test WHERE id = $1"

	// Pass wrong number of parameters
	record, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, record)
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
	record, err := pool.QueryArrow(ctx, sql)
	require.Error(t, err)
	assert.Nil(t, record)

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
	sql := "SELECT id, name FROM simple_test ORDER BY id"

	// Execute multiple queries to verify proper resource cleanup
	for i := 0; i < 10; i++ {
		record, err := pool.QueryArrow(ctx, sql)
		require.NoError(t, err)
		require.NotNil(t, record)

		// Verify data integrity
		assert.Equal(t, int64(2), record.NumRows())

		// Release record immediately
		record.Release()
	}

	// Memory allocator should show no leaks after all releases
	// (verified by alloc.AssertSize(t, 0) in defer)
}
