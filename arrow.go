package pgarrow

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgxpool"
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
	case arrow.BINARY:
		return array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary), nil
	case arrow.DATE32:
		return array.NewDate32Builder(alloc), nil
	case arrow.TIME64:
		timeType, ok := dataType.(*arrow.Time64Type)
		if !ok {
			return nil, fmt.Errorf("expected Time64Type, got %T", dataType)
		}
		return array.NewTime64Builder(alloc, timeType), nil
	case arrow.TIMESTAMP:
		timestampType, ok := dataType.(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("expected TimestampType, got %T", dataType)
		}
		return array.NewTimestampBuilder(alloc, timestampType), nil
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return array.NewMonthDayNanoIntervalBuilder(alloc), nil
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
	case *array.BinaryBuilder:
		return rb.appendBinaryValue(b, value)
	case *array.Date32Builder:
		return rb.appendDate32Value(b, value)
	case *array.Time64Builder:
		return rb.appendTime64Value(b, value)
	case *array.TimestampBuilder:
		return rb.appendTimestampValue(b, value)
	case *array.MonthDayNanoIntervalBuilder:
		return rb.appendMonthDayNanoIntervalValue(b, value)
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

func (rb *RecordBuilder) appendBinaryValue(builder *array.BinaryBuilder, value any) error {
	v, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("type mismatch: expected []byte, got %T", value)
	}
	builder.Append(v)
	return nil
}

func (rb *RecordBuilder) appendDate32Value(builder *array.Date32Builder, value any) error {
	v, ok := value.(int32)
	if !ok {
		return fmt.Errorf("type mismatch: expected int32, got %T", value)
	}
	builder.Append(arrow.Date32(v))
	return nil
}

func (rb *RecordBuilder) appendTime64Value(builder *array.Time64Builder, value any) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("type mismatch: expected int64, got %T", value)
	}
	builder.Append(arrow.Time64(v))
	return nil
}

func (rb *RecordBuilder) appendTimestampValue(builder *array.TimestampBuilder, value any) error {
	v, ok := value.(int64)
	if !ok {
		return fmt.Errorf("type mismatch: expected int64, got %T", value)
	}
	builder.Append(arrow.Timestamp(v))
	return nil
}

func (rb *RecordBuilder) appendMonthDayNanoIntervalValue(builder *array.MonthDayNanoIntervalBuilder, value any) error {
	v, ok := value.(arrow.MonthDayNanoInterval)
	if !ok {
		return fmt.Errorf("type mismatch: expected arrow.MonthDayNanoInterval, got %T", value)
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

// PGArrowRecordReader implements array.RecordReader for streaming PostgreSQL data
type PGArrowRecordReader struct {
	refCount  int64
	schema    *arrow.Schema
	parser    *Parser
	fieldOIDs []uint32
	alloc     memory.Allocator

	// Connection lifecycle management
	conn       *pgxpool.Conn
	pipeReader *io.PipeReader
	copyDone   chan struct{} // Signal when COPY goroutine is done

	currentRecord arrow.Record
	err           error
	released      bool
	batchSize     int
}

// newRecordReader creates a new PGArrowRecordReader with connection lifecycle management
func newRecordReader(schema *arrow.Schema, fieldOIDs []uint32, alloc memory.Allocator, conn *pgxpool.Conn, pipeReader *io.PipeReader, copyDone chan struct{}) (*PGArrowRecordReader, error) {
	parser := NewParser(pipeReader, fieldOIDs)
	if err := parser.ParseHeader(); err != nil {
		return nil, fmt.Errorf("failed to parse COPY header: %w", err)
	}

	return &PGArrowRecordReader{
		refCount:   1,
		schema:     schema,
		parser:     parser,
		fieldOIDs:  fieldOIDs,
		alloc:      alloc,
		conn:       conn,
		pipeReader: pipeReader,
		copyDone:   copyDone,
		batchSize:  128800, // Default batch size optimized for DuckDB parallel processing
	}, nil
}

// Schema returns the Arrow schema
func (r *PGArrowRecordReader) Schema() *arrow.Schema {
	return r.schema
}

// Next advances to the next record batch
func (r *PGArrowRecordReader) Next() bool {
	if r.released || r.err != nil {
		return false
	}

	// Release previous record if exists
	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	// Build next batch
	recordBuilder, err := NewRecordBuilder(r.schema, r.alloc)
	if err != nil {
		r.err = fmt.Errorf("failed to create record builder: %w", err)
		return false
	}
	defer recordBuilder.Release()

	rowCount := r.parseRowsIntoBatch(recordBuilder)
	if rowCount == 0 || r.err != nil {
		return false
	}

	// Create record from batch
	record, err := recordBuilder.NewRecord()
	if err != nil {
		r.err = fmt.Errorf("failed to create Arrow record: %w", err)
		return false
	}

	r.currentRecord = record
	return true
}

// parseRowsIntoBatch parses rows from the parser into the record builder
func (r *PGArrowRecordReader) parseRowsIntoBatch(recordBuilder *RecordBuilder) int {
	rowCount := 0
	registry := NewRegistry() // Create type registry for conversions

	for rowCount < r.batchSize {
		fields, err := r.parser.ParseTuple()
		if err == io.EOF {
			break
		}
		if err != nil {
			r.err = fmt.Errorf("failed to parse tuple: %w", err)
			return rowCount
		}

		values, err := r.convertFieldsToValues(fields, registry)
		if err != nil {
			r.err = err
			return rowCount
		}

		if err := recordBuilder.AppendRow(values); err != nil {
			r.err = fmt.Errorf("failed to append row to builder: %w", err)
			return rowCount
		}

		rowCount++
	}
	return rowCount
}

// convertFieldsToValues converts parsed fields to typed values using TypeHandlers
func (r *PGArrowRecordReader) convertFieldsToValues(fields []Field, registry *TypeRegistry) ([]any, error) {
	values := make([]any, len(fields))
	for i, field := range fields {
		if field.Value == nil {
			values[i] = nil
		} else {
			// Get the appropriate type handler and convert
			handler, err := registry.GetHandler(r.fieldOIDs[i])
			if err != nil {
				return nil, fmt.Errorf("failed to get handler for OID %d: %w", r.fieldOIDs[i], err)
			}

			// Handle both []byte (for primitives/binary) and string (for text types)
			var convertedValue any
			switch fieldData := field.Value.(type) {
			case []byte:
				// Raw bytes from parser - let TypeHandler convert
				convertedValue, err = handler.Parse(fieldData)
			case string:
				// String from parser - convert back to []byte for TypeHandler
				convertedValue, err = handler.Parse([]byte(fieldData))
			default:
				return nil, fmt.Errorf("unexpected field data type from parser: %T", field.Value)
			}

			if err != nil {
				return nil, fmt.Errorf("failed to parse field %d: %w", i, err)
			}

			values[i] = convertedValue
		}
	}
	return values, nil
}

// Record returns the current record batch
func (r *PGArrowRecordReader) Record() arrow.Record {
	return r.currentRecord
}

// Err returns any error that occurred during reading
func (r *PGArrowRecordReader) Err() error {
	return r.err
}

// Release decreases the reference count and releases resources when it reaches 0
func (r *PGArrowRecordReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.currentRecord != nil {
			r.currentRecord.Release()
			r.currentRecord = nil
		}

		// Clean up connection resources
		if r.pipeReader != nil {
			r.pipeReader.Close()
			r.pipeReader = nil
		}

		// Wait for COPY goroutine to complete before releasing connection
		if r.copyDone != nil {
			<-r.copyDone
			r.copyDone = nil
		}

		if r.conn != nil {
			r.conn.Release()
			r.conn = nil
		}

		r.schema = nil
		r.parser = nil
		r.released = true
	}
}

// Retain increases the reference count
func (r *PGArrowRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}
