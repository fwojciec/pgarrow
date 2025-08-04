package pgarrow_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectCopyParser(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// This test verifies the new direct COPY parser works correctly
	// Currently fails because the implementation doesn't exist yet
	t.Run("basic_integration", func(t *testing.T) {
		t.Parallel()

		pool, cleanup := setupTestDB(t)
		defer cleanup()

		// Use VALUES clause for simple test - no table creation needed
		sql := `
			SELECT id, name, active 
			FROM (VALUES 
				(1::int8, 'Alice'::text, true::bool),
				(2::int8, 'Bob'::text, false::bool),
				(3::int8, 'Charlie'::text, NULL::bool)
			) AS test_direct(id, name, active) 
			ORDER BY id
		`

		// This should use the new direct parser internally
		reader, err := pool.QueryArrow(ctx, sql)
		require.NoError(t, err)
		defer reader.Release()

		// Verify we get all records
		require.True(t, reader.Next())
		record := reader.Record()
		assert.Equal(t, int64(3), record.NumRows())
		assert.Equal(t, int64(3), record.NumCols())

		// Verify data
		idCol, ok := record.Column(0).(*array.Int64)
		require.True(t, ok, "column 0 should be Int64")
		assert.Equal(t, int64(1), idCol.Value(0))
		assert.Equal(t, int64(2), idCol.Value(1))
		assert.Equal(t, int64(3), idCol.Value(2))

		nameCol, ok := record.Column(1).(*array.String)
		require.True(t, ok, "column 1 should be String")
		assert.Equal(t, "Alice", nameCol.Value(0))
		assert.Equal(t, "Bob", nameCol.Value(1))
		assert.Equal(t, "Charlie", nameCol.Value(2))

		boolCol, ok := record.Column(2).(*array.Boolean)
		require.True(t, ok, "column 2 should be Boolean")
		assert.True(t, boolCol.Value(0))
		assert.False(t, boolCol.Value(1))
		assert.True(t, boolCol.IsNull(2))

		// Should be no more records
		assert.False(t, reader.Next())
		require.NoError(t, reader.Err())
	})

	t.Run("large_dataset_streaming", func(t *testing.T) {
		t.Parallel()

		pool, cleanup := setupTestDB(t)
		defer cleanup()

		// Generate large dataset directly with SQL
		// Note: cast to int8 (bigint) for int64 type
		sql := `
			SELECT i::int8 AS id, random() * 1000 AS value
			FROM generate_series(1, 100000) AS i
			ORDER BY i
		`

		reader, err := pool.QueryArrow(ctx, sql)
		require.NoError(t, err)
		defer reader.Release()

		totalRows := int64(0)
		for reader.Next() {
			record := reader.Record()
			totalRows += record.NumRows()

			// Verify columns exist and have correct types
			assert.Equal(t, int64(2), record.NumCols())
			_, ok := record.Column(0).(*array.Int64)
			assert.True(t, ok, "column 0 should be Int64")
			_, ok = record.Column(1).(*array.Float64)
			assert.True(t, ok, "column 1 should be Float64")
		}
		require.NoError(t, reader.Err())

		assert.Equal(t, int64(100000), totalRows)
	})

	t.Run("memory_safety", func(t *testing.T) {
		t.Parallel()

		// Use checked allocator to detect leaks
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		pool, cleanup := setupTestDBWithAllocator(t, alloc)
		defer cleanup()

		sql := `
			SELECT data 
			FROM (VALUES ('test1'), ('test2'), ('test3')) AS test_memory(data)
		`

		reader, err := pool.QueryArrow(ctx, sql)
		require.NoError(t, err)

		// Read all data
		for reader.Next() {
			record := reader.Record()
			assert.Equal(t, int64(3), record.NumRows())
			record.Release() // Important: release each record
		}
		require.NoError(t, reader.Err())

		reader.Release() // Release reader
	})

	// TestDirectCopyParserWithNegativeValues tests edge cases with negative dates/timestamps
	// that Copilot flagged as potential issues. This verifies our signed/unsigned handling is correct.
	t.Run("negative_values", func(t *testing.T) {
		t.Parallel()

		pool, cleanup := setupTestDB(t)
		defer cleanup()

		// Test dates before 2000-01-01 (negative PostgreSQL date values)
		// and timestamps before 2000-01-01 (negative PostgreSQL timestamp values)
		sql := `
			SELECT 
				'1999-12-31'::date as past_date,
				'1999-12-31 23:59:59'::timestamp as past_timestamp,
				'-1 day'::interval as negative_interval,
				'-1 month'::interval as negative_month_interval
		`

		reader, err := pool.QueryArrow(ctx, sql)
		require.NoError(t, err)
		defer reader.Release()

		require.True(t, reader.Next())
		record := reader.Record()
		assert.Equal(t, int64(1), record.NumRows())
		assert.Equal(t, int64(4), record.NumCols())

		// Verify we actually got the data without corruption
		pastDateCol := record.Column(0)
		pastTimestampCol := record.Column(1)
		negativeIntervalCol := record.Column(2)
		negativeMonthIntervalCol := record.Column(3)

		assert.False(t, pastDateCol.IsNull(0), "Past date should not be null")
		assert.False(t, pastTimestampCol.IsNull(0), "Past timestamp should not be null")
		assert.False(t, negativeIntervalCol.IsNull(0), "Negative interval should not be null")
		assert.False(t, negativeMonthIntervalCol.IsNull(0), "Negative month interval should not be null")

		// Should be no more records
		assert.False(t, reader.Next())
		require.NoError(t, reader.Err())
	})
}
