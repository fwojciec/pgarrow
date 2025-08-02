package pgarrow_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fwojciec/pgarrow"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Shared benchmark constants
const (
	defaultBenchmarkRowCount = 1000
)

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

// createIsolatedTestEnv creates an isolated test environment with custom SQL setup
// This is the flexible version that allows custom table creation
func createIsolatedTestEnv(t *testing.T, setupSQL string) (*pgarrow.Pool, func()) {
	t.Helper()

	ctx := context.Background()
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

// setupTestDB creates a test database with predefined standard test tables
// This is the legacy version for tests that use the standard table structure
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

// setupTestTables creates predefined test tables in the current schema
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
			col_text TEXT,
			col_timestamp TIMESTAMP,
			col_timestamptz TIMESTAMPTZ
		)
	`
	_, err = conn.Exec(ctx, createTableSQL)
	require.NoError(t, err, "should create test table")

	// Insert test data
	insertSQL := `
		INSERT INTO test_all_types (col_bool, col_int2, col_int4, col_int8, col_float4, col_float8, col_text, col_timestamp, col_timestamptz)
		VALUES 
			(true, 100, 1000, 10000, 3.14, 2.71828, 'hello', '2023-01-15 12:30:45', '2023-01-15 12:30:45+00'),
			(false, 200, 2000, 20000, 6.28, 1.41421, 'world', '2024-12-25 00:00:00', '2024-12-25 00:00:00+00'),
			(null, null, null, null, null, null, null, null, null)
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
