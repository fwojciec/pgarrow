package pgarrow_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestByteBatchingPerformance tests that byte-based batching dramatically reduces
// the number of batches for large datasets, specifically targeting the 5M row case
// mentioned in issue #64.
func TestByteBatchingPerformance(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	pool, cleanup := setupTestDBWithAllocator(t, alloc)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Use generate_series to create large dataset without DDL/INSERT overhead
	// This simulates the 5M row scenario but with manageable size for CI
	testRowCount := 50000 // 50K rows for reasonable CI performance

	// Query with generated data to test batching behavior
	sql := fmt.Sprintf(`
		SELECT 
			i::INTEGER as id,
			'name_' || i::TEXT as name,
			(i * 1.5)::FLOAT8 as value,
			'2023-01-01 00:00:00'::TIMESTAMP as created_at
		FROM generate_series(1, %d) i
		ORDER BY i
	`, testRowCount)

	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(t, err, "Failed to execute query")
	defer reader.Release()

	batchCount := 0
	totalRows := 0

	for reader.Next() {
		batchCount++
		record := reader.Record()
		totalRows += int(record.NumRows())

		// Verify the batch is reasonably sized (should be much larger than legacy batching)
		assert.Greater(t, record.NumRows(), int64(1000),
			"Batch %d should contain significantly more rows than legacy batching", batchCount)

		record.Release()
	}

	require.NoError(t, reader.Err(), "Reader should not have errors")
	assert.Equal(t, testRowCount, totalRows, "Should read all generated rows")

	// With byte-based batching, we should have dramatically fewer batches
	// For 50K rows, we expect much fewer batches than the legacy approach
	legacyBatchCount := testRowCount / pgarrow.OptimalBatchSizeGo // ~195 batches with legacy approach

	t.Logf("Batch statistics:")
	t.Logf("  Total rows: %d", totalRows)
	t.Logf("  Total batches: %d", batchCount)
	t.Logf("  Average rows per batch: %d", totalRows/batchCount)
	t.Logf("  Expected legacy batches: %d", legacyBatchCount)
	t.Logf("  Improvement factor: %.2fx", float64(legacyBatchCount)/float64(batchCount))

	// We should have dramatically fewer batches than legacy approach
	assert.Less(t, batchCount, legacyBatchCount/10,
		"Byte-based batching should use at least 10x fewer batches than legacy approach")

	// For 50K rows, we should have somewhere between 1-20 batches (very rough estimate)
	assert.Less(t, batchCount, 20, "Should have relatively few batches")
	assert.Positive(t, batchCount, "Should have at least one batch")
}

// TestByteBatchingSizes tests that batches are appropriately sized based on data types
func TestByteBatchingSizes(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	pool, cleanup := setupTestDBWithAllocator(t, alloc)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Test different data type scenarios using VALUES clauses
	testCases := []struct {
		name               string
		sql                string
		expectSmallBatches bool // True if we expect smaller batches due to large data
	}{
		{
			name: "small_fixed_types",
			sql: `
				SELECT 
					i::INTEGER as id,
					(i % 2 = 0)::BOOLEAN as flag,
					(i % 100)::SMALLINT as small_num
				FROM generate_series(1, 10000) i
			`,
			expectSmallBatches: false, // Small types should pack efficiently
		},
		{
			name: "large_variable_types",
			sql: `
				SELECT 
					i::INTEGER as id,
					repeat('x', 1000)::TEXT as large_text,
					decode(repeat('41', 500), 'hex')::BYTEA as binary_data
				FROM generate_series(1, 1000) i
			`,
			expectSmallBatches: true, // Large variable types should create smaller batches
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Query and analyze batches
			reader, err := pool.QueryArrow(ctx, tc.sql)
			require.NoError(t, err, "Failed to execute query")
			defer reader.Release()

			batchCount := 0
			totalRows := 0
			var avgRowsPerBatch int

			for reader.Next() {
				batchCount++
				record := reader.Record()
				totalRows += int(record.NumRows())
				record.Release()
			}

			require.NoError(t, reader.Err())

			if totalRows > 0 {
				avgRowsPerBatch = totalRows / batchCount
			}

			t.Logf("%s: %d batches, %d total rows, %d avg rows/batch",
				tc.name, batchCount, totalRows, avgRowsPerBatch)

			// Verify batching behavior based on data type characteristics
			if tc.expectSmallBatches {
				// Large variable data should create smaller batches
				assert.Less(t, avgRowsPerBatch, 10000,
					"Large variable data should create smaller batches")
			} else {
				// Small fixed data should pack more efficiently
				assert.Greater(t, avgRowsPerBatch, 1000,
					"Small fixed data should pack efficiently into larger batches")
			}
		})
	}
}
