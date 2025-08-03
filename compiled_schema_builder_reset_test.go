package pgarrow_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/require"
)

// TestBuildRecordResetsBuilders verifies that calling BuildRecord() multiple times
// produces separate records without data accumulation from previous batches.
// This test validates that Arrow builders are properly reset after NewArray() calls.
func TestBuildRecordResetsBuilders(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() {
		require.Zero(t, alloc.CurrentAlloc(), "Memory leak detected")
	}()

	// Create a simple schema with int32 and string columns
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	pgOIDs := []uint32{
		pgarrow.TypeOIDInt4, // 23 -> int32
		pgarrow.TypeOIDText, // 25 -> string
	}

	// Create CompiledSchema
	compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
	require.NoError(t, err)
	defer compiledSchema.Release()

	// Batch 1: Add some data
	batch1Data := [][]byte{
		{0, 0, 0, 1}, // int32: 1
		[]byte("first"),
	}
	batch1Nulls := []bool{false, false}

	err = compiledSchema.ProcessRow(batch1Data, batch1Nulls)
	require.NoError(t, err)

	// Add another row to batch 1
	batch1Data2 := [][]byte{
		{0, 0, 0, 2}, // int32: 2
		[]byte("second"),
	}
	err = compiledSchema.ProcessRow(batch1Data2, batch1Nulls)
	require.NoError(t, err)

	// Build first record
	record1, err := compiledSchema.BuildRecord(2)
	require.NoError(t, err)
	defer record1.Release()

	// Verify first record contents
	require.Equal(t, int64(2), record1.NumRows())
	require.Equal(t, int64(2), record1.NumCols())

	// Check first record data
	idArray1, ok := record1.Column(0).(*array.Int32)
	require.True(t, ok, "Expected Int32 array")
	nameArray1, ok := record1.Column(1).(*array.String)
	require.True(t, ok, "Expected String array")

	require.Equal(t, int32(1), idArray1.Value(0))
	require.Equal(t, int32(2), idArray1.Value(1))
	require.Equal(t, "first", nameArray1.Value(0))
	require.Equal(t, "second", nameArray1.Value(1))

	// Batch 2: Add different data to the SAME CompiledSchema
	// If builders aren't reset, this data would be appended to previous data
	batch2Data := [][]byte{
		{0, 0, 0, 100}, // int32: 100
		[]byte("third"),
	}
	err = compiledSchema.ProcessRow(batch2Data, batch1Nulls)
	require.NoError(t, err)

	// Build second record
	record2, err := compiledSchema.BuildRecord(1)
	require.NoError(t, err)
	defer record2.Release()

	// Critical test: If builders weren't reset, record2 would contain 3 rows (2 from batch1 + 1 from batch2)
	// But it should only contain 1 row (just the new data)
	require.Equal(t, int64(1), record2.NumRows(), "BuildRecord() should reset builders - found accumulated data from previous batch")
	require.Equal(t, int64(2), record2.NumCols())

	// Check second record data - should only contain the new data
	idArray2, ok := record2.Column(0).(*array.Int32)
	require.True(t, ok, "Expected Int32 array")
	nameArray2, ok := record2.Column(1).(*array.String)
	require.True(t, ok, "Expected String array")

	require.Equal(t, int32(100), idArray2.Value(0))
	require.Equal(t, "third", nameArray2.Value(0))

	// Batch 3: Verify it still works for subsequent calls
	batch3Data := [][]byte{
		{0, 0, 0, 200}, // int32: 200
		[]byte("fourth"),
	}
	err = compiledSchema.ProcessRow(batch3Data, batch1Nulls)
	require.NoError(t, err)

	batch3Data2 := [][]byte{
		{0, 0, 0, 201}, // int32: 201
		[]byte("fifth"),
	}
	err = compiledSchema.ProcessRow(batch3Data2, batch1Nulls)
	require.NoError(t, err)

	// Build third record
	record3, err := compiledSchema.BuildRecord(2)
	require.NoError(t, err)
	defer record3.Release()

	// Should contain exactly 2 rows from batch3, not accumulated data
	require.Equal(t, int64(2), record3.NumRows(), "BuildRecord() should reset builders - found accumulated data from previous batches")

	idArray3, ok := record3.Column(0).(*array.Int32)
	require.True(t, ok, "Expected Int32 array")
	nameArray3, ok := record3.Column(1).(*array.String)
	require.True(t, ok, "Expected String array")

	require.Equal(t, int32(200), idArray3.Value(0))
	require.Equal(t, int32(201), idArray3.Value(1))
	require.Equal(t, "fourth", nameArray3.Value(0))
	require.Equal(t, "fifth", nameArray3.Value(1))
}
