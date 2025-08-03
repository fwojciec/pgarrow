package pgarrow_test

import (
	"encoding/binary"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/require"
)

func TestCompileSchema(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) }) // Ensure no memory leaks

	t.Run("ValidSchema", func(t *testing.T) {
		t.Parallel()

		// Create a simple Arrow schema with basic types
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

		// Compile schema
		compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
		require.NoError(t, err)
		require.NotNil(t, compiledSchema)

		// Verify schema properties
		require.Equal(t, arrowSchema, compiledSchema.Schema())
		require.Equal(t, len(pgOIDs), compiledSchema.NumColumns())
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
		pgOIDs := []uint32{
			20, // TypeOIDInt8
			25, // TypeOIDText
			16, // TypeOIDBool
		}

		// Should fail with schema mismatch error
		compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
		require.Error(t, err)
		require.Nil(t, compiledSchema)
		require.Contains(t, err.Error(), "schema mismatch")
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		t.Parallel()

		// Arrow schema with supported type
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		// PostgreSQL OID with unsupported type (fake OID)
		pgOIDs := []uint32{999999}

		// Should fail with unsupported type error
		compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
		require.Error(t, err)
		require.Nil(t, compiledSchema)
		require.Contains(t, err.Error(), "unsupported type mapping")
	})
}

func TestCompiledSchema_ProcessRow(t *testing.T) {
	t.Parallel()

	t.Run("ValidRow", func(t *testing.T) {
		t.Parallel()

		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		t.Cleanup(func() { alloc.AssertSize(t, 0) })

		// Create schema with basic types for this test
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		pgOIDs := []uint32{
			20, // TypeOIDInt8
			25, // TypeOIDText
		}

		compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
		require.NoError(t, err)
		t.Cleanup(func() { compiledSchema.Release() })

		// Prepare field data (PostgreSQL binary format)
		int64Data := make([]byte, 8)
		binary.BigEndian.PutUint64(int64Data, uint64(123))
		textData := []byte("hello")

		fieldData := [][]byte{int64Data, textData}
		nulls := []bool{false, false}

		// Process row should not return error
		err = compiledSchema.ProcessRow(fieldData, nulls)
		require.NoError(t, err)
	})

	t.Run("NullValues", func(t *testing.T) {
		t.Parallel()

		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		t.Cleanup(func() { alloc.AssertSize(t, 0) })

		// Create schema with basic types for this test
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		pgOIDs := []uint32{
			20, // TypeOIDInt8
			25, // TypeOIDText
		}

		compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
		require.NoError(t, err)
		t.Cleanup(func() { compiledSchema.Release() })

		// Process row with null values
		fieldData := [][]byte{nil, nil}
		nulls := []bool{true, true}

		err = compiledSchema.ProcessRow(fieldData, nulls)
		require.NoError(t, err)
	})

	t.Run("FieldCountMismatch", func(t *testing.T) {
		t.Parallel()

		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		t.Cleanup(func() { alloc.AssertSize(t, 0) })

		// Create schema with basic types for this test
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		}
		arrowSchema := arrow.NewSchema(fields, nil)

		pgOIDs := []uint32{
			20, // TypeOIDInt8
			25, // TypeOIDText
		}

		compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
		require.NoError(t, err)
		t.Cleanup(func() { compiledSchema.Release() })

		// Wrong number of fields
		fieldData := [][]byte{make([]byte, 8)} // Only 1 field, expect 2
		nulls := []bool{false}

		err = compiledSchema.ProcessRow(fieldData, nulls)
		require.Error(t, err)
		require.Contains(t, err.Error(), "field count mismatch")
	})
}

func TestCompiledSchema_MemoryManagement(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0) // This will fail if there are memory leaks

	// Create schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	pgOIDs := []uint32{23} // TypeOIDInt4

	compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
	require.NoError(t, err)

	// Release should clean up all resources
	compiledSchema.Release()

	// CheckedAllocator assertion in defer will catch any leaks
}

func TestCompiledSchema_TimestampMemoryManagement(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) }) // This will fail if there are memory leaks

	// Create schema specifically with timestamp and timestamptz types
	fields := []arrow.Field{
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}, Nullable: true},
		{Name: "tstz", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	pgOIDs := []uint32{1114, 1184} // TypeOIDTimestamp, TypeOIDTimestamptz

	compiledSchema, err := pgarrow.CompileSchema(pgOIDs, arrowSchema, alloc)
	require.NoError(t, err)

	// Release should clean up all timestamp resources
	compiledSchema.Release()

	// CheckedAllocator assertion in defer will catch any timestamp-related leaks
}
