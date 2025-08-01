package pgarrow

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ColumnInfo represents PostgreSQL column metadata for Arrow schema generation
type ColumnInfo struct {
	Name string
	OID  uint32
}

// CreateSchema creates an Arrow schema from PostgreSQL column metadata
func CreateSchema(columns []ColumnInfo) (*arrow.Schema, error) {
	registry := NewRegistry()
	fields := make([]arrow.Field, len(columns))

	for i, col := range columns {
		handler, err := registry.GetHandler(col.OID)
		if err != nil {
			return nil, err
		}

		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     handler.ArrowType(),
			Nullable: true, // PostgreSQL columns are nullable by default
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// RecordBuilder manages Arrow builders and schema for creating Arrow records
type RecordBuilder struct {
	schema   *arrow.Schema
	alloc    memory.Allocator
	builders []array.Builder
}

// NewRecordBuilder creates a new RecordBuilder with the given schema and allocator
func NewRecordBuilder(schema *arrow.Schema, alloc memory.Allocator) (*RecordBuilder, error) {

	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builder, err := createBuilderForType(field.Type, alloc)
		if err != nil {
			// Clean up any builders created so far
			for j := range i {
				builders[j].Release()
			}
			return nil, fmt.Errorf("failed to create builder for field %s: %w", field.Name, err)
		}
		builders[i] = builder
	}

	return &RecordBuilder{
		schema:   schema,
		alloc:    alloc,
		builders: builders,
	}, nil
}

// createBuilderForType creates an appropriate Arrow builder for the given data type
func createBuilderForType(dataType arrow.DataType, alloc memory.Allocator) (array.Builder, error) {
	switch dataType.ID() {
	case arrow.BOOL:
		return array.NewBooleanBuilder(alloc), nil
	case arrow.INT16:
		return array.NewInt16Builder(alloc), nil
	case arrow.INT32:
		return array.NewInt32Builder(alloc), nil
	case arrow.INT64:
		return array.NewInt64Builder(alloc), nil
	case arrow.FLOAT32:
		return array.NewFloat32Builder(alloc), nil
	case arrow.FLOAT64:
		return array.NewFloat64Builder(alloc), nil
	case arrow.STRING:
		return array.NewStringBuilder(alloc), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type: %s", dataType)
	}
}

// Schema returns the Arrow schema for this RecordBuilder
func (rb *RecordBuilder) Schema() *arrow.Schema {
	return rb.schema
}

// AppendRow appends a row of values to the builders with type-safe conversion
func (rb *RecordBuilder) AppendRow(values []any) error {
	if len(values) != len(rb.builders) {
		return fmt.Errorf("expected %d values, got %d", len(rb.builders), len(values))
	}

	for i, value := range values {
		if err := rb.appendValueToBuilder(rb.builders[i], value); err != nil {
			return fmt.Errorf("error appending value to column %d (%s): %w",
				i, rb.schema.Field(i).Name, err)
		}
	}

	return nil
}

// appendValueToBuilder appends a single value to the appropriate builder
func (rb *RecordBuilder) appendValueToBuilder(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	return rb.appendNonNullValue(builder, value)
}

// appendNonNullValue appends a non-nil value to the appropriate builder
func (rb *RecordBuilder) appendNonNullValue(builder array.Builder, value any) error {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		return rb.appendBoolValue(b, value)
	case *array.Int16Builder:
		return rb.appendInt16Value(b, value)
	case *array.Int32Builder:
		return rb.appendInt32Value(b, value)
	case *array.Int64Builder:
		return rb.appendInt64Value(b, value)
	case *array.Float32Builder:
		return rb.appendFloat32Value(b, value)
	case *array.Float64Builder:
		return rb.appendFloat64Value(b, value)
	case *array.StringBuilder:
		return rb.appendStringValue(b, value)
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
}

func (rb *RecordBuilder) appendBoolValue(builder *array.BooleanBuilder, value any) error {
	v, ok := value.(bool)
	if !ok {
		return fmt.Errorf("type mismatch: expected bool, got %T", value)
	}
	builder.Append(v)
	return nil
}

func (rb *RecordBuilder) appendInt16Value(builder *array.Int16Builder, value any) error {
	v, ok := value.(int16)
	if !ok {
		return fmt.Errorf("type mismatch: expected int16, got %T", value)
	}
	builder.Append(v)
	return nil
}

func (rb *RecordBuilder) appendInt32Value(builder *array.Int32Builder, value any) error {
	v, ok := value.(int32)
	if !ok {
		return fmt.Errorf("type mismatch: expected int32, got %T", value)
	}
	builder.Append(v)
	return nil
}

func (rb *RecordBuilder) appendInt64Value(builder *array.Int64Builder, value any) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("type mismatch: expected int64, got %T", value)
	}
	builder.Append(v)
	return nil
}

func (rb *RecordBuilder) appendFloat32Value(builder *array.Float32Builder, value any) error {
	v, ok := value.(float32)
	if !ok {
		return fmt.Errorf("type mismatch: expected float32, got %T", value)
	}
	builder.Append(v)
	return nil
}

func (rb *RecordBuilder) appendFloat64Value(builder *array.Float64Builder, value any) error {
	v, ok := value.(float64)
	if !ok {
		return fmt.Errorf("type mismatch: expected float64, got %T", value)
	}
	builder.Append(v)
	return nil
}

func (rb *RecordBuilder) appendStringValue(builder *array.StringBuilder, value any) error {
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("type mismatch: expected string, got %T", value)
	}
	builder.Append(v)
	return nil
}

// NewRecord creates a new Arrow record from the current builders and resets them
func (rb *RecordBuilder) NewRecord() (arrow.Record, error) {
	// Build arrays from builders
	arrays := make([]arrow.Array, len(rb.builders))
	for i, builder := range rb.builders {
		arrays[i] = builder.NewArray()
	}

	// Handle empty arrays case
	var numRows int64
	if len(arrays) == 0 {
		numRows = 0
	} else {
		numRows = int64(arrays[0].Len())
	}

	// Create record
	record := array.NewRecord(rb.schema, arrays, numRows)

	// Release arrays (record has retained them)
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

// Release releases all resources held by the RecordBuilder
func (rb *RecordBuilder) Release() {
	rb.schema = nil

	for _, builder := range rb.builders {
		if builder != nil {
			builder.Release()
		}
	}
	rb.builders = nil
}
