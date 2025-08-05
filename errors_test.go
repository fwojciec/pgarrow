package pgarrow_test

import (
	"context"
	"os"
	"testing"

	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/require"
)

func TestErrorsSimple(t *testing.T) {
	t.Parallel()
	runTests(t, []testCase{
		// SQL Syntax errors
		{
			name:    "syntax_error",
			query:   "INVALID SQL",
			wantErr: "syntax error",
		},
		{
			name:    "syntax_error_unmatched_paren",
			query:   "SELECT (1, 2",
			wantErr: "syntax error",
		},

		// Table/Column not found
		{
			name:    "table_not_found",
			query:   "SELECT * FROM non_existent_table",
			wantErr: "does not exist",
		},
		{
			name:    "column_not_found",
			setup:   "CREATE TABLE test_table (id int4)",
			query:   "SELECT unknown_column FROM test_table",
			wantErr: "does not exist",
		},

		// Type errors
		{
			name:    "type_mismatch_in_where",
			setup:   "CREATE TABLE test_table (id int4)",
			query:   "SELECT * FROM test_table WHERE id = 'not a number'",
			wantErr: "invalid input syntax for type integer",
		},
		{
			name:    "invalid_type_cast",
			query:   "SELECT 'hello'::int4",
			wantErr: "invalid input syntax for type integer",
		},

		// Parameter errors
		{
			name:    "missing_parameter",
			query:   "SELECT $1::int4",
			args:    []any{}, // No arguments provided
			wantErr: "expected 1 arguments, got 0",
		},
		{
			name:    "too_many_parameters",
			query:   "SELECT $1::int4",
			args:    []any{1, 2}, // Too many arguments
			wantErr: "expected 1 arguments, got 2",
		},

		// JSON errors
		{
			name:    "invalid_json",
			query:   "SELECT '{invalid json}'::json",
			wantErr: "invalid input syntax",
		},

		// Unsupported types - these are expected pgarrow limitations
		{
			name:    "unsupported_oid_type",
			query:   "SELECT oid FROM pg_class LIMIT 1",
			wantErr: "unsupported PostgreSQL type OID: 26",
		},
		{
			name:    "unsupported_numeric_type",
			query:   "SELECT 123.456::numeric",
			wantErr: "unsupported PostgreSQL type OID: 1700",
		},
		{
			name:    "unsupported_array_type",
			query:   "SELECT ARRAY[1,2,3]::int4[]",
			wantErr: "unsupported PostgreSQL type OID: 1007",
		},

		// Queries that return no columns
		{
			name:    "update_returns_no_columns",
			setup:   "CREATE TABLE test_table (id int4)",
			query:   "UPDATE test_table SET id = 1",
			wantErr: "query returned no columns",
		},
		// Additional unsupported type tests
		{
			name:    "unsupported_uuid_type",
			query:   "SELECT gen_random_uuid()",
			wantErr: "unsupported PostgreSQL type OID: 2950", // UUID OID
		},
	})
}

// Test pool lifecycle
func TestPoolLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("query_after_close", func(t *testing.T) {
		t.Parallel()
		dbURL := os.Getenv("TEST_DATABASE_URL")
		if dbURL == "" {
			t.Skip("TEST_DATABASE_URL not set")
		}

		ctx := context.Background()
		pool, err := pgarrow.NewPool(ctx, dbURL)
		require.NoError(t, err)

		// Close the pool
		pool.Close()

		// Try to query
		_, err = pool.QueryArrow(ctx, "SELECT 1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "closed")
	})

	t.Run("double_close", func(t *testing.T) {
		t.Parallel()
		dbURL := os.Getenv("TEST_DATABASE_URL")
		if dbURL == "" {
			t.Skip("TEST_DATABASE_URL not set")
		}

		ctx := context.Background()
		pool, err := pgarrow.NewPool(ctx, dbURL)
		require.NoError(t, err)

		// Close twice - should not panic
		pool.Close()
		pool.Close()
	})
}
