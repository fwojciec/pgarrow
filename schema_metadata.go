package pgarrow

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FieldMetadata contains metadata for a single field in the schema
type FieldMetadata struct {
	Name      string         // Field name from Arrow schema
	OID       uint32         // PostgreSQL type OID
	ArrowType arrow.DataType // Corresponding Arrow data type
}

// SchemaMetadata represents a lightweight PostgreSQL→Arrow schema mapping
// for type validation and field metadata only. This simplified version focuses
// solely on type mapping without builder management.
//
// SchemaMetadata instances are thread-safe for read operations.
type SchemaMetadata struct {
	// Core metadata
	fields    []FieldMetadata // Field metadata for each column
	schema    *arrow.Schema   // Arrow schema reference
	fieldOIDs []uint32        // PostgreSQL type OIDs (cached for performance)

	// Memory management
	released bool
}

// NewSchemaMetadata creates a SchemaMetadata for type mapping validation.
// Validates schema compatibility upfront and fails fast on any errors.
//
// The returned SchemaMetadata is thread-safe for read operations.
func NewSchemaMetadata(pgOIDs []uint32, arrowSchema *arrow.Schema, alloc memory.Allocator) (*SchemaMetadata, error) {
	// Validate schema compatibility upfront
	if len(pgOIDs) != arrowSchema.NumFields() {
		return nil, fmt.Errorf("schema mismatch: %d PostgreSQL types, %d Arrow fields", len(pgOIDs), arrowSchema.NumFields())
	}

	// Create field metadata and validate type compatibility
	fields := make([]FieldMetadata, len(pgOIDs))

	for i, oid := range pgOIDs {
		arrowField := arrowSchema.Field(i)

		// Get expected Arrow type for this PostgreSQL OID
		expectedArrowType, err := getArrowTypeForOID(oid)
		if err != nil {
			return nil, fmt.Errorf("unsupported type mapping for field %d: %w", i, err)
		}

		// Validate that the Arrow field type matches the expected type for this OID
		if !arrow.TypeEqual(expectedArrowType, arrowField.Type) {
			return nil, fmt.Errorf("type mismatch for field %d (OID %d): expected %s, got %s",
				i, oid, expectedArrowType, arrowField.Type)
		}

		// Create field metadata
		fields[i] = FieldMetadata{
			Name:      arrowField.Name,
			OID:       oid,
			ArrowType: expectedArrowType,
		}
	}

	return &SchemaMetadata{
		fields:    fields,
		schema:    arrowSchema,
		fieldOIDs: pgOIDs,
		released:  false,
	}, nil
}

// Schema returns the Arrow schema for this SchemaMetadata
func (sm *SchemaMetadata) Schema() *arrow.Schema {
	return sm.schema
}

// NumFields returns the number of fields in the schema
func (sm *SchemaMetadata) NumFields() int {
	return len(sm.fields)
}

// Fields returns the field metadata
func (sm *SchemaMetadata) Fields() []FieldMetadata {
	return sm.fields
}

// FieldOIDs returns the PostgreSQL OIDs for each field in the schema
func (sm *SchemaMetadata) FieldOIDs() []uint32 {
	return sm.fieldOIDs
}

// Release releases all resources held by the SchemaMetadata
func (sm *SchemaMetadata) Release() {
	if sm.released {
		return
	}

	// Clear references
	// Note: This simplified release is intentional since SchemaMetadata no longer manages builders or an allocator.
	sm.fields = nil
	sm.schema = nil
	sm.released = true
}

// getArrowTypeForOID returns the expected Arrow type for a given PostgreSQL OID
func getArrowTypeForOID(oid uint32) (arrow.DataType, error) {
	switch oid {
	case TypeOIDBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case TypeOIDInt2:
		return arrow.PrimitiveTypes.Int16, nil
	case TypeOIDInt4:
		return arrow.PrimitiveTypes.Int32, nil
	case TypeOIDInt8:
		return arrow.PrimitiveTypes.Int64, nil
	case TypeOIDFloat4:
		return arrow.PrimitiveTypes.Float32, nil
	case TypeOIDFloat8:
		return arrow.PrimitiveTypes.Float64, nil
	case TypeOIDText, TypeOIDVarchar, TypeOIDBpchar, TypeOIDName, TypeOIDChar:
		return arrow.BinaryTypes.String, nil
	case TypeOIDBytea:
		return arrow.BinaryTypes.Binary, nil
	case TypeOIDDate:
		return arrow.PrimitiveTypes.Date32, nil
	case TypeOIDTime:
		return arrow.FixedWidthTypes.Time64us, nil
	case TypeOIDTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}, nil
	case TypeOIDTimestamptz:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case TypeOIDInterval:
		return arrow.FixedWidthTypes.MonthDayNanoInterval, nil
	default:
		return nil, fmt.Errorf("unsupported PostgreSQL type OID: %d", oid)
	}
}
