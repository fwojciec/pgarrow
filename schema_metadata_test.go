package pgarrow_test

import (
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

// TestSchemaMetadata_ProcessRow has been removed as ProcessRow method
// is no longer part of SchemaMetadata's simplified API

// TestSchemaMetadata_BuildRecord has been removed as BuildRecord method
// is no longer part of SchemaMetadata's simplified API

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
