package pgarrow

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// DefaultBatchSize is the default batch size for compiled schemas
	DefaultBatchSize = 1024
)

// CompiledSchema represents a pre-compiled PostgreSQL→Arrow schema mapping
// with pre-allocated type-specific column writers, eliminating runtime type dispatch.
//
// IMPORTANT: CompiledSchema instances are NOT thread-safe. Each instance should
// be used by a single goroutine only.
//
// Reason: The underlying Arrow builders (array.BooleanBuilder, array.Int64Builder, etc.)
// maintain internal mutable state (length, capacity, data buffers) without
// synchronization. Concurrent access to these builders causes data races in
// operations like Append(), AppendNull(), and Resize().
//
// For concurrent processing, create separate CompiledSchema instances per goroutine.
type CompiledSchema struct {
	columnWriters []ColumnWriter
	schema        *arrow.Schema
	allocator     memory.Allocator
	batchSize     int
	released      bool
	fieldOIDs     []uint32
}

// CompileSchema creates a CompiledSchema with pre-allocated column writers.
// Validates schema compatibility upfront and fails fast on any errors.
//
// The returned CompiledSchema is NOT thread-safe and should be used by a single
// goroutine. For concurrent processing, create separate instances per goroutine.
func CompileSchema(pgOIDs []uint32, arrowSchema *arrow.Schema, alloc memory.Allocator) (*CompiledSchema, error) {
	// Validate schema compatibility upfront
	if len(pgOIDs) != arrowSchema.NumFields() {
		return nil, fmt.Errorf("schema mismatch: %d PostgreSQL types, %d Arrow fields", len(pgOIDs), arrowSchema.NumFields())
	}

	columnWriters := make([]ColumnWriter, len(pgOIDs))
	for i, oid := range pgOIDs {
		// Create type-specific writer with fail-fast validation
		writer, err := createColumnWriter(oid, arrowSchema.Field(i), alloc)
		if err != nil {
			// Clean up any writers created so far
			for j := 0; j < i; j++ {
				if columnWriters[j] != nil {
					releaseColumnWriter(columnWriters[j])
				}
			}
			return nil, fmt.Errorf("unsupported type mapping for field %d: %w", i, err)
		}
		columnWriters[i] = writer
	}

	return &CompiledSchema{
		columnWriters: columnWriters,
		schema:        arrowSchema,
		allocator:     alloc,
		batchSize:     DefaultBatchSize,
		released:      false,
		fieldOIDs:     pgOIDs,
	}, nil
}

// Schema returns the Arrow schema for this CompiledSchema
func (cs *CompiledSchema) Schema() *arrow.Schema {
	return cs.schema
}

// NumColumns returns the number of columns in the schema
func (cs *CompiledSchema) NumColumns() int {
	return len(cs.columnWriters)
}

// FieldOIDs returns the PostgreSQL OIDs for each field in the schema
func (cs *CompiledSchema) FieldOIDs() []uint32 {
	return cs.fieldOIDs
}

// ProcessRow processes a row of PostgreSQL binary data directly to Arrow columns.
// Direct field → column writer mapping, no runtime dispatch.
//
// IMPORTANT: This method is NOT thread-safe. Only one goroutine should call
// ProcessRow on a CompiledSchema instance at a time, as the underlying Arrow
// builders modify shared state without synchronization.
func (cs *CompiledSchema) ProcessRow(fieldData [][]byte, nulls []bool) error {
	if cs.released {
		return fmt.Errorf("CompiledSchema has been released")
	}

	if len(fieldData) != len(cs.columnWriters) {
		return fmt.Errorf("field count mismatch: expected %d, got %d", len(cs.columnWriters), len(fieldData))
	}

	if len(nulls) != len(cs.columnWriters) {
		return fmt.Errorf("null indicator count mismatch: expected %d, got %d", len(cs.columnWriters), len(nulls))
	}

	// Direct field → column writer mapping, no runtime dispatch
	for i, data := range fieldData {
		if err := cs.columnWriters[i].WriteField(data, nulls[i]); err != nil {
			return fmt.Errorf("failed to write field %d: %w", i, err)
		}
	}
	return nil
}

// BuildRecord creates an Arrow record from the current state of all column writers.
// This finalizes the current batch and resets the column writers for the next batch.
//
// IMPORTANT: This method is NOT thread-safe. Only one goroutine should call
// BuildRecord on a CompiledSchema instance at a time.
func (cs *CompiledSchema) BuildRecord(numRows int64) (arrow.Record, error) {
	if cs.released {
		return nil, fmt.Errorf("CompiledSchema has been released")
	}

	// Build arrays from column writers
	arrays := make([]arrow.Array, len(cs.columnWriters))
	for i, writer := range cs.columnWriters {
		arrays[i] = cs.buildArrayFromWriter(writer)
	}

	// Create record
	record := array.NewRecord(cs.schema, arrays, numRows)

	// Release arrays (record has retained them)
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

// buildArrayFromWriter creates an array from a ColumnWriter based on its type
func (cs *CompiledSchema) buildArrayFromWriter(writer ColumnWriter) arrow.Array {
	switch w := writer.(type) {
	case *BoolColumnWriter:
		return w.Builder.NewArray()
	case *Int16ColumnWriter:
		return w.Builder.NewArray()
	case *Int32ColumnWriter:
		return w.Builder.NewArray()
	case *Int64ColumnWriter:
		return w.Builder.NewArray()
	case *Float32ColumnWriter:
		return w.Builder.NewArray()
	case *Float64ColumnWriter:
		return w.Builder.NewArray()
	case *StringColumnWriter:
		return w.Builder.NewArray()
	case *BinaryColumnWriter:
		return w.Builder.NewArray()
	case *Date32ColumnWriter:
		return w.Builder.NewArray()
	case *Time64ColumnWriter:
		return w.Builder.NewArray()
	case *TimestampColumnWriter:
		return w.Builder.NewArray()
	case *MonthDayNanoIntervalColumnWriter:
		return w.Builder.NewArray()
	default:
		// This should never happen if column writers are created correctly
		panic(fmt.Sprintf("unsupported column writer type: %T", writer))
	}
}

// Release releases all resources held by the CompiledSchema
func (cs *CompiledSchema) Release() {
	if cs.released {
		return
	}

	// Release all column writers
	for _, writer := range cs.columnWriters {
		if writer != nil {
			releaseColumnWriter(writer)
		}
	}

	cs.columnWriters = nil
	cs.schema = nil
	cs.allocator = nil
	cs.released = true
}

// createColumnWriter creates a type-specific ColumnWriter for the given PostgreSQL OID and Arrow field
func createColumnWriter(oid uint32, arrowField arrow.Field, alloc memory.Allocator) (ColumnWriter, error) {
	// Get the expected Arrow type for this PostgreSQL OID
	expectedArrowType, err := getArrowTypeForOID(oid)
	if err != nil {
		return nil, err
	}

	// Validate that the Arrow field type matches the expected type for this OID
	if !arrow.TypeEqual(expectedArrowType, arrowField.Type) {
		return nil, fmt.Errorf("type mismatch for OID %d: expected %s, got %s",
			oid, expectedArrowType, arrowField.Type)
	}

	return createColumnWriterForOID(oid, alloc)
}

// createColumnWriterForOID creates the appropriate column writer based on PostgreSQL OID
func createColumnWriterForOID(oid uint32, alloc memory.Allocator) (ColumnWriter, error) {
	switch oid {
	case TypeOIDBool: // 16
		builder := array.NewBooleanBuilder(alloc)
		return &BoolColumnWriter{Builder: builder}, nil
	case TypeOIDInt2: // 21
		builder := array.NewInt16Builder(alloc)
		return &Int16ColumnWriter{Builder: builder}, nil
	case TypeOIDInt4: // 23
		builder := array.NewInt32Builder(alloc)
		return &Int32ColumnWriter{Builder: builder}, nil
	case TypeOIDInt8: // 20
		builder := array.NewInt64Builder(alloc)
		return &Int64ColumnWriter{Builder: builder}, nil
	case TypeOIDFloat4: // 700
		builder := array.NewFloat32Builder(alloc)
		return &Float32ColumnWriter{Builder: builder}, nil
	case TypeOIDFloat8: // 701
		builder := array.NewFloat64Builder(alloc)
		return &Float64ColumnWriter{Builder: builder}, nil
	case TypeOIDText, TypeOIDVarchar, TypeOIDBpchar, TypeOIDName, TypeOIDChar: // 25, 1043, 1042, 19, 18
		builder := array.NewStringBuilder(alloc)
		return &StringColumnWriter{Builder: builder}, nil
	case TypeOIDBytea: // 17
		builder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		return &BinaryColumnWriter{Builder: builder}, nil
	case TypeOIDDate: // 1082
		builder := array.NewDate32Builder(alloc)
		return &Date32ColumnWriter{Builder: builder}, nil
	case TypeOIDTime: // 1083
		return createTimeColumnWriter(alloc)
	case TypeOIDTimestamp: // 1114
		return createTimestampColumnWriter(alloc)
	case TypeOIDTimestamptz: // 1184
		return createTimestamptzColumnWriter(alloc)
	case TypeOIDInterval: // 1186
		builder := array.NewMonthDayNanoIntervalBuilder(alloc)
		return &MonthDayNanoIntervalColumnWriter{Builder: builder}, nil
	default:
		return nil, fmt.Errorf("unsupported PostgreSQL type OID: %d", oid)
	}
}

// createTimeColumnWriter creates a Time64ColumnWriter with proper type casting
func createTimeColumnWriter(alloc memory.Allocator) (ColumnWriter, error) {
	timeType, ok := arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type)
	if !ok {
		return nil, fmt.Errorf("failed to cast Time64us type")
	}
	builder := array.NewTime64Builder(alloc, timeType)
	return &Time64ColumnWriter{Builder: builder}, nil
}

// createTimestampColumnWriter creates a TimestampColumnWriter (no timezone)
func createTimestampColumnWriter(alloc memory.Allocator) (ColumnWriter, error) {
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
	builder := array.NewTimestampBuilder(alloc, timestampType)
	return NewTimestampColumnWriter(builder), nil
}

// createTimestamptzColumnWriter creates a TimestampColumnWriter with UTC timezone
func createTimestamptzColumnWriter(alloc memory.Allocator) (ColumnWriter, error) {
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	builder := array.NewTimestampBuilder(alloc, timestampType)
	return NewTimestamptzColumnWriter(builder), nil
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

// releaseColumnWriter releases the resources held by a ColumnWriter
func releaseColumnWriter(writer ColumnWriter) {
	// Each ColumnWriter implementation contains an Arrow builder that needs to be released
	switch w := writer.(type) {
	case *BoolColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *Int16ColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *Int32ColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *Int64ColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *Float32ColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *Float64ColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *StringColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *BinaryColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *Date32ColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *Time64ColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *TimestampColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	case *MonthDayNanoIntervalColumnWriter:
		if w.Builder != nil {
			w.Builder.Release()
		}
	}
}
