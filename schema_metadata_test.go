package pgarrow_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/require"
)

// TestSchemaMetadata_PublicAPI tests essential public API functionality that users depend on
// This is minimal unit testing focused on edge cases not covered by integration tests
func TestSchemaMetadata_PublicAPI(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	// Create Arrow schema with basic types
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	pgOIDs := []uint32{pgarrow.TypeOIDInt8, pgarrow.TypeOIDText}

	metadata, err := pgarrow.NewSchemaMetadata(pgOIDs, arrowSchema, alloc)
	require.NoError(t, err)
	defer metadata.Release()

	// Test public API methods that users might call directly
	require.Equal(t, 2, metadata.NumFields())
	require.Equal(t, pgOIDs, metadata.FieldOIDs())

	fieldsMetadata := metadata.Fields()
	require.Len(t, fieldsMetadata, 2)
	require.Equal(t, "id", fieldsMetadata[0].Name)
	require.Equal(t, uint32(pgarrow.TypeOIDInt8), fieldsMetadata[0].OID)
}

// TestSchemaMetadata_ErrorCases tests error conditions not easily triggered through integration tests
func TestSchemaMetadata_ErrorCases(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	// Schema mismatch: different number of fields vs OIDs
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}
	arrowSchema := arrow.NewSchema(fields, nil)
	pgOIDs := []uint32{pgarrow.TypeOIDInt8, pgarrow.TypeOIDText} // 2 OIDs for 1 field

	metadata, err := pgarrow.NewSchemaMetadata(pgOIDs, arrowSchema, alloc)
	require.Error(t, err)
	require.Nil(t, metadata)
	require.Contains(t, err.Error(), "schema mismatch")
}

// TestSchemaMetadata_ProcessRow has been removed as ProcessRow method
// is no longer part of SchemaMetadata's simplified API

// TestSchemaMetadata_BuildRecord has been removed as BuildRecord method
// is no longer part of SchemaMetadata's simplified API
