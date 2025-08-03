package pgarrow

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FieldParserFunc represents a direct field parsing function with no interface overhead.
// Each function is selected at schema creation time based on PostgreSQL OID.
type FieldParserFunc func(data []byte, isNull bool) error

// FieldMetadata contains metadata for a single field in the schema
type FieldMetadata struct {
	Name      string         // Field name from Arrow schema
	OID       uint32         // PostgreSQL type OID
	ArrowType arrow.DataType // Corresponding Arrow data type
}

// SchemaMetadata represents a lightweight PostgreSQL→Arrow schema mapping
// following ADBC's approach with direct parsing functions instead of complex
// object hierarchies and dynamic dispatch.
//
// IMPORTANT: SchemaMetadata instances are NOT thread-safe. Each instance should
// be used by a single goroutine only, as the underlying Arrow builders modify
// shared state without synchronization.
type SchemaMetadata struct {
	// Core metadata
	fields    []FieldMetadata  // Field metadata for each column
	schema    *arrow.Schema    // Arrow schema reference
	fieldOIDs []uint32         // PostgreSQL type OIDs (cached for performance)
	allocator memory.Allocator // Memory allocator

	// Direct parsing functions - no interface overhead
	parsers  []FieldParserFunc // One parser per field, selected at schema time
	builders []any             // Direct concrete builders, no interface

	// Memory management
	released bool
}

// NewSchemaMetadata creates a SchemaMetadata with direct parsing functions.
// Validates schema compatibility upfront and fails fast on any errors.
//
// The returned SchemaMetadata is NOT thread-safe and should be used by a single
// goroutine. For concurrent processing, create separate instances per goroutine.
func NewSchemaMetadata(pgOIDs []uint32, arrowSchema *arrow.Schema, alloc memory.Allocator) (*SchemaMetadata, error) {
	// Validate schema compatibility upfront
	if len(pgOIDs) != arrowSchema.NumFields() {
		return nil, fmt.Errorf("schema mismatch: %d PostgreSQL types, %d Arrow fields", len(pgOIDs), arrowSchema.NumFields())
	}

	// Create field metadata and validate type compatibility
	fields := make([]FieldMetadata, len(pgOIDs))
	parsers := make([]FieldParserFunc, len(pgOIDs))
	builders := make([]any, len(pgOIDs))

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

		// Create direct parser and builder for this field
		parser, builder, err := createDirectParser(oid, alloc)
		if err != nil {
			// Clean up any builders created so far
			for j := 0; j < i; j++ {
				releaseBuilder(builders[j])
			}
			return nil, fmt.Errorf("failed to create parser for field %d (OID %d): %w", i, oid, err)
		}

		parsers[i] = parser
		builders[i] = builder
	}

	return &SchemaMetadata{
		fields:    fields,
		schema:    arrowSchema,
		fieldOIDs: pgOIDs,
		allocator: alloc,
		parsers:   parsers,
		builders:  builders,
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

// ProcessRow processes a row of PostgreSQL binary data directly to Arrow columns
// using direct function calls instead of interface dispatch.
//
// IMPORTANT: This method is NOT thread-safe. Only one goroutine should call
// ProcessRow on a SchemaMetadata instance at a time.
func (sm *SchemaMetadata) ProcessRow(fieldData [][]byte, nulls []bool) error {
	if sm.released {
		return fmt.Errorf("SchemaMetadata has been released")
	}

	if len(fieldData) != len(sm.fields) {
		return fmt.Errorf("field count mismatch: expected %d, got %d", len(sm.fields), len(fieldData))
	}

	if len(nulls) != len(sm.fields) {
		return fmt.Errorf("null indicator count mismatch: expected %d, got %d", len(sm.fields), len(nulls))
	}

	// Direct function calls - no interface dispatch
	for i, parser := range sm.parsers {
		if err := parser(fieldData[i], nulls[i]); err != nil {
			return fmt.Errorf("failed to parse field %d: %w", i, err)
		}
	}
	return nil
}

// BuildRecord creates an Arrow record from the current state of all builders.
// This finalizes the current batch and resets the builders for the next batch.
//
// IMPORTANT: This method is NOT thread-safe.
func (sm *SchemaMetadata) BuildRecord(numRows int64) (arrow.Record, error) {
	if sm.released {
		return nil, fmt.Errorf("SchemaMetadata has been released")
	}

	// Build arrays from builders using direct type switching (done once per batch, not per row)
	arrays := make([]arrow.Array, len(sm.builders))
	for i, builder := range sm.builders {
		arr, err := sm.buildArrayFromBuilder(builder)
		if err != nil {
			// Clean up any arrays created so far
			for j := 0; j < i; j++ {
				if arrays[j] != nil {
					arrays[j].Release()
				}
			}
			return nil, fmt.Errorf("failed to build array from builder %d: %w", i, err)
		}
		arrays[i] = arr
	}

	// Create record
	record := array.NewRecord(sm.schema, arrays, numRows)

	// Release arrays (record has retained them)
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

// Release releases all resources held by the SchemaMetadata
func (sm *SchemaMetadata) Release() {
	if sm.released {
		return
	}

	// Release all builders
	for _, builder := range sm.builders {
		if builder != nil {
			releaseBuilder(builder)
		}
	}

	sm.builders = nil
	sm.parsers = nil
	sm.fields = nil
	sm.schema = nil
	sm.allocator = nil
	sm.released = true
}

// createDirectParser creates a direct parsing function and builder for the given PostgreSQL OID
func createDirectParser(oid uint32, alloc memory.Allocator) (FieldParserFunc, any, error) {
	switch oid {
	case TypeOIDBool:
		return createBoolParser(alloc)
	case TypeOIDInt2:
		return createInt16Parser(alloc)
	case TypeOIDInt4:
		return createInt32Parser(alloc)
	case TypeOIDInt8:
		return createInt64Parser(alloc)
	case TypeOIDFloat4:
		return createFloat32Parser(alloc)
	case TypeOIDFloat8:
		return createFloat64Parser(alloc)
	case TypeOIDText, TypeOIDVarchar, TypeOIDBpchar, TypeOIDName, TypeOIDChar:
		return createStringParser(alloc)
	case TypeOIDBytea:
		return createBinaryParser(alloc)
	case TypeOIDDate:
		return createDate32Parser(alloc)
	case TypeOIDTime:
		return createTime64Parser(alloc)
	case TypeOIDTimestamp:
		return createTimestampParser(alloc)
	case TypeOIDTimestamptz:
		return createTimestamptzParser(alloc)
	case TypeOIDInterval:
		return createIntervalParser(alloc)
	default:
		return nil, nil, fmt.Errorf("unsupported PostgreSQL type OID: %d", oid)
	}
}

func createBoolParser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewBooleanBuilder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 1 {
			return fmt.Errorf("invalid data length for bool: expected 1, got %d", len(data))
		}
		builder.Append(data[0] != 0)
		return nil
	}
	return parser, builder, nil
}

func createInt16Parser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewInt16Builder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 2 {
			return fmt.Errorf("invalid data length for int16: expected 2, got %d", len(data))
		}
		value := int16(binary.BigEndian.Uint16(data))
		builder.Append(value)
		return nil
	}
	return parser, builder, nil
}

func createInt32Parser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewInt32Builder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 4 {
			return fmt.Errorf("invalid data length for int32: expected 4, got %d", len(data))
		}
		value := int32(binary.BigEndian.Uint32(data))
		builder.Append(value)
		return nil
	}
	return parser, builder, nil
}

func createInt64Parser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewInt64Builder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 8 {
			return fmt.Errorf("invalid data length for int64: expected 8, got %d", len(data))
		}
		value := int64(binary.BigEndian.Uint64(data))
		builder.Append(value)
		return nil
	}
	return parser, builder, nil
}

func createFloat32Parser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewFloat32Builder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 4 {
			return fmt.Errorf("invalid data length for float32: expected 4, got %d", len(data))
		}
		value := math.Float32frombits(binary.BigEndian.Uint32(data))
		builder.Append(value)
		return nil
	}
	return parser, builder, nil
}

func createFloat64Parser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewFloat64Builder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 8 {
			return fmt.Errorf("invalid data length for float64: expected 8, got %d", len(data))
		}
		value := math.Float64frombits(binary.BigEndian.Uint64(data))
		builder.Append(value)
		return nil
	}
	return parser, builder, nil
}

func createStringParser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewStringBuilder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		builder.Append(string(data))
		return nil
	}
	return parser, builder, nil
}

func createBinaryParser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		builder.Append(data)
		return nil
	}
	return parser, builder, nil
}

func createDate32Parser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewDate32Builder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 4 {
			return fmt.Errorf("invalid data length for date: expected 4, got %d", len(data))
		}
		pgDays := int32(binary.BigEndian.Uint32(data))
		arrowDays := pgDays + PostgresDateEpochDays
		builder.Append(arrow.Date32(arrowDays))
		return nil
	}
	return parser, builder, nil
}

func createTime64Parser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	timeType, ok := arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type)
	if !ok {
		return nil, nil, fmt.Errorf("failed to cast Time64us type")
	}
	builder := array.NewTime64Builder(alloc, timeType)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 8 {
			return fmt.Errorf("invalid data length for time: expected 8, got %d", len(data))
		}
		timeMicros := int64(binary.BigEndian.Uint64(data))
		builder.Append(arrow.Time64(timeMicros))
		return nil
	}
	return parser, builder, nil
}

func createTimestampParser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
	builder := array.NewTimestampBuilder(alloc, timestampType)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 8 {
			return fmt.Errorf("invalid data length for timestamp: expected 8, got %d", len(data))
		}
		pgMicros := int64(binary.BigEndian.Uint64(data))
		arrowMicros := pgMicros + PostgresTimestampEpochMicros
		builder.Append(arrow.Timestamp(arrowMicros))
		return nil
	}
	return parser, builder, nil
}

func createTimestamptzParser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	builder := array.NewTimestampBuilder(alloc, timestampType)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 8 {
			return fmt.Errorf("invalid data length for timestamptz: expected 8, got %d", len(data))
		}
		pgMicros := int64(binary.BigEndian.Uint64(data))
		arrowMicros := pgMicros + PostgresTimestampEpochMicros
		builder.Append(arrow.Timestamp(arrowMicros))
		return nil
	}
	return parser, builder, nil
}

func createIntervalParser(alloc memory.Allocator) (FieldParserFunc, any, error) {
	builder := array.NewMonthDayNanoIntervalBuilder(alloc)
	parser := func(data []byte, isNull bool) error {
		if isNull {
			builder.AppendNull()
			return nil
		}
		if len(data) != 16 {
			return fmt.Errorf("invalid data length for interval: expected 16, got %d", len(data))
		}
		pgMicros := int64(binary.BigEndian.Uint64(data[0:8]))
		pgDays := int32(binary.BigEndian.Uint32(data[8:12]))
		pgMonths := int32(binary.BigEndian.Uint32(data[12:16]))

		const maxMicros = math.MaxInt64 / 1000
		const minMicros = math.MinInt64 / 1000
		if pgMicros > maxMicros || pgMicros < minMicros {
			return fmt.Errorf("interval microseconds overflow: %d", pgMicros)
		}
		arrowNanos := pgMicros * 1000

		interval := arrow.MonthDayNanoInterval{
			Months:      pgMonths,
			Days:        pgDays,
			Nanoseconds: arrowNanos,
		}
		builder.Append(interval)
		return nil
	}
	return parser, builder, nil
}

// buildArrayFromBuilder creates an array from a builder based on its concrete type
func (sm *SchemaMetadata) buildArrayFromBuilder(builder any) (arrow.Array, error) {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		return b.NewArray(), nil
	case *array.Int16Builder:
		return b.NewArray(), nil
	case *array.Int32Builder:
		return b.NewArray(), nil
	case *array.Int64Builder:
		return b.NewArray(), nil
	case *array.Float32Builder:
		return b.NewArray(), nil
	case *array.Float64Builder:
		return b.NewArray(), nil
	case *array.StringBuilder:
		return b.NewArray(), nil
	case *array.BinaryBuilder:
		return b.NewArray(), nil
	case *array.Date32Builder:
		return b.NewArray(), nil
	case *array.Time64Builder:
		return b.NewArray(), nil
	case *array.TimestampBuilder:
		return b.NewArray(), nil
	case *array.MonthDayNanoIntervalBuilder:
		return b.NewArray(), nil
	default:
		return nil, fmt.Errorf("unsupported builder type: %T", builder)
	}
}

// releaseBuilder releases resources held by a builder
func releaseBuilder(builder any) {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		b.Release()
	case *array.Int16Builder:
		b.Release()
	case *array.Int32Builder:
		b.Release()
	case *array.Int64Builder:
		b.Release()
	case *array.Float32Builder:
		b.Release()
	case *array.Float64Builder:
		b.Release()
	case *array.StringBuilder:
		b.Release()
	case *array.BinaryBuilder:
		b.Release()
	case *array.Date32Builder:
		b.Release()
	case *array.Time64Builder:
		b.Release()
	case *array.TimestampBuilder:
		b.Release()
	case *array.MonthDayNanoIntervalBuilder:
		b.Release()
	}
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
