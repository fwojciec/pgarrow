package pgarrow_test

import (
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/require"
)

func TestNewSchemaMetadata(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	t.Run("ValidSchema", func(t *testing.T) {
		t.Parallel()

		// Create Arrow schema with basic types
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// PostgreSQL OIDs matching the Arrow types
		pgOIDs := []uint32{
			20, // TypeOIDInt8 -> int64
			25, // TypeOIDText -> string
			16, // TypeOIDBool -> boolean
		}

		// Create SchemaMetadata - this should work
		metadata, err := pgarrow.NewSchemaMetadata(pgOIDs, arrowSchema, alloc)
		require.NoError(t, err)
		require.NotNil(t, metadata)
		defer metadata.Release()

		// Verify schema properties
		require.Equal(t, arrowSchema, metadata.Schema())
		require.Equal(t, len(pgOIDs), metadata.NumFields())
		require.Equal(t, pgOIDs, metadata.FieldOIDs())

		// Verify field metadata
		fieldMetadata := metadata.Fields()
		require.Len(t, fieldMetadata, 3)
		require.Equal(t, "id", fieldMetadata[0].Name)
		require.Equal(t, uint32(20), fieldMetadata[0].OID)
		require.Equal(t, arrow.PrimitiveTypes.Int64, fieldMetadata[0].ArrowType)
	})

	t.Run("SchemaMismatch", func(t *testing.T) {
		t.Parallel()

		// Arrow schema with 2 fields
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// PostgreSQL OIDs with 3 fields - mismatch
		pgOIDs := []uint32{20, 25, 16}

		// Should fail with schema mismatch error
		metadata, err := pgarrow.NewSchemaMetadata(pgOIDs, arrowSchema, alloc)
		require.Error(t, err)
		require.Nil(t, metadata)
		require.Contains(t, err.Error(), "schema mismatch")
	})
}

func TestSchemaMetadata_ProcessRow(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	t.Run("ValidRow", func(t *testing.T) {
		t.Parallel()

		// Create schema with basic types
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		pgOIDs := []uint32{20, 25} // int8, text

		metadata, err := pgarrow.NewSchemaMetadata(pgOIDs, arrowSchema, alloc)
		require.NoError(t, err)
		defer metadata.Release()

		// Prepare field data (PostgreSQL binary format)
		int64Data := make([]byte, 8)
		binary.BigEndian.PutUint64(int64Data, uint64(123))
		textData := []byte("hello")

		fieldData := [][]byte{int64Data, textData}
		nulls := []bool{false, false}

		// Process row should not return error
		err = metadata.ProcessRow(fieldData, nulls)
		require.NoError(t, err)
	})

	t.Run("NullValues", func(t *testing.T) {
		t.Parallel()

		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer alloc.AssertSize(t, 0)

		// Create schema
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		pgOIDs := []uint32{20, 25}

		metadata, err := pgarrow.NewSchemaMetadata(pgOIDs, arrowSchema, alloc)
		require.NoError(t, err)
		defer metadata.Release()

		// Process row with null values
		fieldData := [][]byte{nil, nil}
		nulls := []bool{true, true}

		err = metadata.ProcessRow(fieldData, nulls)
		require.NoError(t, err)
	})
}

func TestSchemaMetadata_BuildRecord(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer func() {
		require.Zero(t, alloc.CurrentAlloc(), "Memory leak detected")
	}()

	// Create schema with basic types
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	pgOIDs := []uint32{
		pgarrow.TypeOIDInt4, // 23 -> int32
		pgarrow.TypeOIDText, // 25 -> string
	}

	// Create SchemaMetadata
	metadata, err := pgarrow.NewSchemaMetadata(pgOIDs, arrowSchema, alloc)
	require.NoError(t, err)
	defer metadata.Release()

	// Add some data
	batch1Data := [][]byte{
		{0, 0, 0, 1}, // int32: 1
		[]byte("first"),
	}
	batch1Nulls := []bool{false, false}

	err = metadata.ProcessRow(batch1Data, batch1Nulls)
	require.NoError(t, err)

	// Add another row
	batch1Data2 := [][]byte{
		{0, 0, 0, 2}, // int32: 2
		[]byte("second"),
	}
	err = metadata.ProcessRow(batch1Data2, batch1Nulls)
	require.NoError(t, err)

	// Build record
	record, err := metadata.BuildRecord(2)
	require.NoError(t, err)
	defer record.Release()

	// Verify record contents
	require.Equal(t, int64(2), record.NumRows())
	require.Equal(t, int64(2), record.NumCols())

	// Check data - this validates that the direct parsing worked
	idArray := record.Column(0)
	nameArray := record.Column(1)

	require.Equal(t, 2, idArray.Len())
	require.Equal(t, 2, nameArray.Len())
}

func TestSchemaMetadata_MemoryManagement(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0) // This will fail if there are memory leaks

	// Create schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	pgOIDs := []uint32{23} // TypeOIDInt4

	metadata, err := pgarrow.NewSchemaMetadata(pgOIDs, arrowSchema, alloc)
	require.NoError(t, err)

	// Release should clean up all resources
	metadata.Release()

	// CheckedAllocator assertion in defer will catch any leaks
}
