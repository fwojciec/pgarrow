package pgarrow_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/fwojciec/pgarrow"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCase defines a declarative integration test
type testCase struct {
	name     string
	setup    string // Optional SQL to run before query
	query    string // The query to test
	args     []any  // Optional query arguments
	wantErr  string // Expected error substring (if any)
	validate func(t *testing.T, records []arrow.Record)
}

// runTests executes a slice of test cases
func runTests(t *testing.T, cases []testCase) {
	for _, tc := range cases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Setup isolated environment
			pool, cleanup := isolatedTest(t, tc.setup)
			defer cleanup()

			// Execute query
			ctx := context.Background()
			reader, err := pool.QueryArrow(ctx, tc.query, tc.args...)

			// Handle expected errors
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}

			// Ensure success
			require.NoError(t, err)
			defer reader.Release()

			// Collect and validate records if validator provided
			if tc.validate != nil {
				var records []arrow.Record
				for reader.Next() {
					rec := reader.Record()
					rec.Retain() // Retain for validation
					records = append(records, rec)
				}
				require.NoError(t, reader.Err())

				tc.validate(t, records)

				// Clean up retained records
				for _, rec := range records {
					rec.Release()
				}
			}
		})
	}
}

// isolatedTest creates a test environment with an isolated schema
func isolatedTest(t *testing.T, setupSQL string) (*pgarrow.Pool, func()) {
	t.Helper()

	// Get test database URL
	dbURL := getDatabaseURL(t)

	// Create admin connection
	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)

	// Create isolated schema with timestamp and random number
	schemaName := fmt.Sprintf("test_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
	_, err = adminPool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	require.NoError(t, err)

	// Create pool with schema search path
	poolConfig, err := pgxpool.ParseConfig(dbURL)
	require.NoError(t, err)
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schemaName

	pgxPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)

	// Run setup SQL if provided
	if setupSQL != "" {
		_, err = pgxPool.Exec(ctx, setupSQL)
		require.NoError(t, err)
	}

	// Create pgarrow pool
	pool, err := pgarrow.NewPool(ctx, "", pgarrow.WithExistingPool(pgxPool))
	require.NoError(t, err)

	// Return pool and cleanup function
	cleanup := func() {
		pool.Close()
		_, _ = adminPool.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
		adminPool.Close()
	}

	return pool, cleanup
}

// getDatabaseURL returns the test database connection string
func getDatabaseURL(t *testing.T) string {
	t.Helper()

	// Check for environment variable first
	if dbURL := os.Getenv("TEST_DATABASE_URL"); dbURL != "" {
		return dbURL
	}

	// Skip test if no database URL is available
	t.Skip("TEST_DATABASE_URL not set, skipping integration test")
	return ""
}

// Common validation helpers

// validateRowCount checks that the total number of rows matches expected
func validateRowCount(t *testing.T, records []arrow.Record, expected int64) {
	t.Helper()
	var total int64
	for _, rec := range records {
		total += rec.NumRows()
	}
	require.Equal(t, expected, total)
}

// validateSchema checks that the schema matches expected field names and types
func validateSchema(t *testing.T, records []arrow.Record, fieldNames []string) {
	t.Helper()
	require.NotEmpty(t, records)
	schema := records[0].Schema()
	require.Equal(t, len(fieldNames), schema.NumFields())
	for i, name := range fieldNames {
		assert.Equal(t, name, schema.Field(i).Name)
	}
}
