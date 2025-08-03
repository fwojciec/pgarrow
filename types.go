package pgarrow

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	// PostgreSQL epoch adjustment: days from 1970-01-01 to 2000-01-01
	PostgresDateEpochDays = 10957
	// PostgreSQL timestamp epoch adjustment: microseconds from 1970-01-01 to 2000-01-01
	PostgresTimestampEpochMicros = 946684800000000
)

// TypeHandler defines the interface for PostgreSQL type handlers
type TypeHandler interface {
	// OID returns the PostgreSQL type OID
	OID() uint32
	// Name returns the human-readable type name
	Name() string
	// ArrowType returns the corresponding Arrow data type
	ArrowType() arrow.DataType
	// Parse converts binary PostgreSQL data to Go value
	// Returns nil for NULL values (empty data)
	Parse(data []byte) (any, error)
}

// ColumnWriter defines the interface for direct binary data → Arrow column conversion
// without intermediate allocations. This replaces the TypeHandler.Parse pattern
// for performance-critical scenarios.
//
// IMPORTANT: ColumnWriter implementations are NOT thread-safe due to underlying
// Arrow builder state. Each ColumnWriter should be used by a single goroutine.
type ColumnWriter interface {
	// WriteField writes binary PostgreSQL data directly to Arrow column
	// data: binary PostgreSQL field data
	// isNull: true if field is NULL (data should be ignored)
	WriteField(data []byte, isNull bool) error

	// WriteFieldBatch writes multiple binary PostgreSQL fields directly to Arrow column
	// for improved cache efficiency and reduced function call overhead.
	// data: slice of binary PostgreSQL field data
	// nulls: slice of null indicators, must be same length as data
	WriteFieldBatch(data [][]byte, nulls []bool) error

	// ArrowType returns the corresponding Arrow data type
	ArrowType() arrow.DataType

	// BuilderStats returns current length and capacity for optimization insights
	BuilderStats() (length, capacity int)

	// SetBufferPool sets the buffer pool for memory management (NEW for GC optimization)
	SetBufferPool(pool *BufferPool)

	// PreAllocate pre-allocates capacity for the expected batch size (NEW for performance)
	PreAllocate(expectedBatchSize int)
}

// TypeRegistry manages type handlers by OID
type TypeRegistry struct {
	handlers map[uint32]TypeHandler
}

// NewRegistry creates a new type registry with all basic types registered
func NewRegistry() *TypeRegistry {
	registry := &TypeRegistry{
		handlers: make(map[uint32]TypeHandler),
	}

	// Register all basic types
	registry.register(&BoolType{})
	registry.register(&ByteaType{})
	registry.register(&Int2Type{})
	registry.register(&Int4Type{})
	registry.register(&Int8Type{})
	registry.register(&Float4Type{})
	registry.register(&Float8Type{})
	registry.register(&TextType{})
	registry.register(&VarcharType{})
	registry.register(&BpcharType{})
	registry.register(&NameType{})
	registry.register(&CharType{})
	registry.register(&DateType{})
	registry.register(&TimeType{})
	registry.register(&TimestampType{})
	registry.register(&TimestamptzType{})
	registry.register(&IntervalType{})

	return registry
}

// GetHandler returns the type handler for the given OID
func (r *TypeRegistry) GetHandler(oid uint32) (TypeHandler, error) {
	handler, exists := r.handlers[oid]
	if !exists {
		return nil, fmt.Errorf("unsupported type OID: %d", oid)
	}
	return handler, nil
}

// Register adds a new type handler to the registry
func (r *TypeRegistry) Register(handler TypeHandler) error {
	oid := handler.OID()
	if _, exists := r.handlers[oid]; exists {
		return fmt.Errorf("type OID already registered: %d", oid)
	}
	r.handlers[oid] = handler
	return nil
}

// register is internal method for initial setup
func (r *TypeRegistry) register(handler TypeHandler) {
	r.handlers[handler.OID()] = handler
}

// BoolType handles PostgreSQL bool type (OID 16)
type BoolType struct{}

func (t *BoolType) OID() uint32 {
	return TypeOIDBool
}

func (t *BoolType) Name() string {
	return "bool"
}

func (t *BoolType) ArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Boolean
}

func (t *BoolType) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 1 {
		return nil, fmt.Errorf("invalid data length for bool: expected 1, got %d", len(data))
	}

	return data[0] != 0, nil
}

// Int2Type handles PostgreSQL int2 type (OID 21)
type Int2Type struct{}

func (t *Int2Type) OID() uint32 {
	return TypeOIDInt2
}

func (t *Int2Type) Name() string {
	return "int2"
}

func (t *Int2Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int16
}

func (t *Int2Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 2 {
		return nil, fmt.Errorf("invalid data length for int2: expected 2, got %d", len(data))
	}

	return int16(binary.BigEndian.Uint16(data)), nil
}

// Int4Type handles PostgreSQL int4 type (OID 23)
type Int4Type struct{}

func (t *Int4Type) OID() uint32 {
	return TypeOIDInt4
}

func (t *Int4Type) Name() string {
	return "int4"
}

func (t *Int4Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int32
}

func (t *Int4Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 4 {
		return nil, fmt.Errorf("invalid data length for int4: expected 4, got %d", len(data))
	}

	return int32(binary.BigEndian.Uint32(data)), nil
}

// Int8Type handles PostgreSQL int8 type (OID 20)
type Int8Type struct{}

func (t *Int8Type) OID() uint32 {
	return TypeOIDInt8
}

func (t *Int8Type) Name() string {
	return "int8"
}

func (t *Int8Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int64
}

func (t *Int8Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 8 {
		return nil, fmt.Errorf("invalid data length for int8: expected 8, got %d", len(data))
	}

	return int64(binary.BigEndian.Uint64(data)), nil
}

// Float4Type handles PostgreSQL float4 type (OID 700)
type Float4Type struct{}

func (t *Float4Type) OID() uint32 {
	return TypeOIDFloat4
}

func (t *Float4Type) Name() string {
	return "float4"
}

func (t *Float4Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Float32
}

func (t *Float4Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 4 {
		return nil, fmt.Errorf("invalid data length for float4: expected 4, got %d", len(data))
	}

	return math.Float32frombits(binary.BigEndian.Uint32(data)), nil
}

// Float8Type handles PostgreSQL float8 type (OID 701)
type Float8Type struct{}

func (t *Float8Type) OID() uint32 {
	return TypeOIDFloat8
}

func (t *Float8Type) Name() string {
	return "float8"
}

func (t *Float8Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Float64
}

func (t *Float8Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 8 {
		return nil, fmt.Errorf("invalid data length for float8: expected 8, got %d", len(data))
	}

	return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
}

// TextType handles PostgreSQL text type (OID 25)
type TextType struct{}

func (t *TextType) OID() uint32 {
	return TypeOIDText
}

func (t *TextType) Name() string {
	return "text"
}

func (t *TextType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *TextType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// VarcharType handles PostgreSQL varchar type (OID 1043)
type VarcharType struct{}

func (t *VarcharType) OID() uint32 {
	return TypeOIDVarchar
}

func (t *VarcharType) Name() string {
	return "varchar"
}

func (t *VarcharType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *VarcharType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// BpcharType handles PostgreSQL bpchar type (OID 1042)
type BpcharType struct{}

func (t *BpcharType) OID() uint32 {
	return TypeOIDBpchar
}

func (t *BpcharType) Name() string {
	return "bpchar"
}

func (t *BpcharType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *BpcharType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// NameType handles PostgreSQL name type (OID 19)
type NameType struct{}

func (t *NameType) OID() uint32 {
	return TypeOIDName
}

func (t *NameType) Name() string {
	return "name"
}

func (t *NameType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *NameType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// CharType handles PostgreSQL char type (OID 18)
type CharType struct{}

func (t *CharType) OID() uint32 {
	return TypeOIDChar
}

func (t *CharType) Name() string {
	return "char"
}

func (t *CharType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *CharType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// ByteaType handles PostgreSQL bytea type (OID 17)
type ByteaType struct{}

func (t *ByteaType) OID() uint32 {
	return TypeOIDBytea
}

func (t *ByteaType) Name() string {
	return "bytea"
}

func (t *ByteaType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.Binary
}

func (t *ByteaType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	// Empty bytea (len(data) == 0) is valid - return the empty slice
	// Data is returned as a reference to the original buffer (zero-copy); no additional copying is performed
	return data, nil
}

// DateType handles PostgreSQL date type (OID 1082)
type DateType struct{}

func (t *DateType) OID() uint32 {
	return TypeOIDDate
}

func (t *DateType) Name() string {
	return "date"
}

func (t *DateType) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Date32
}

func (t *DateType) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 4 {
		return nil, fmt.Errorf("invalid data length for date: expected 4, got %d", len(data))
	}

	pgDays := int32(binary.BigEndian.Uint32(data))
	arrowDays := pgDays + PostgresDateEpochDays // Single addition operation
	return arrowDays, nil
}

// TimeType handles PostgreSQL time type (OID 1083)
type TimeType struct{}

func (t *TimeType) OID() uint32 {
	return TypeOIDTime
}

func (t *TimeType) Name() string {
	return "time"
}

func (t *TimeType) ArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Time64us
}

func (t *TimeType) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 8 {
		return nil, fmt.Errorf("invalid data length for time: expected 8, got %d", len(data))
	}

	// PostgreSQL time is stored as microseconds since midnight
	// Arrow Time64[microsecond] also uses microseconds - direct conversion
	timeMicros := int64(binary.BigEndian.Uint64(data))
	return timeMicros, nil
}

// TimestampType handles PostgreSQL timestamp type (OID 1114)
type TimestampType struct{}

func (t *TimestampType) OID() uint32 {
	return TypeOIDTimestamp
}

func (t *TimestampType) Name() string {
	return "timestamp"
}

func (t *TimestampType) ArrowType() arrow.DataType {
	return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
}

func (t *TimestampType) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 8 {
		return nil, fmt.Errorf("invalid data length for timestamp: expected 8, got %d", len(data))
	}

	// PostgreSQL timestamp is stored as microseconds since 2000-01-01
	// Arrow timestamp uses microseconds since 1970-01-01
	// Add epoch adjustment to convert from PostgreSQL epoch to Arrow epoch
	// IMPORTANT: Use signed int64 conversion, not uint64, since PG timestamps can be negative (before 2000-01-01)
	pgMicros := int64(binary.BigEndian.Uint64(data))
	arrowMicros := pgMicros + PostgresTimestampEpochMicros
	return arrowMicros, nil
}

// TimestamptzType handles PostgreSQL timestamptz type (OID 1184)
type TimestamptzType struct{}

func (t *TimestamptzType) OID() uint32 {
	return TypeOIDTimestamptz
}

func (t *TimestamptzType) Name() string {
	return "timestamptz"
}

func (t *TimestamptzType) ArrowType() arrow.DataType {
	return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
}

func (t *TimestamptzType) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 8 {
		return nil, fmt.Errorf("invalid data length for timestamptz: expected 8, got %d", len(data))
	}

	// PostgreSQL timestamptz is stored as microseconds since 2000-01-01 UTC
	// Arrow timestamp uses microseconds since 1970-01-01
	// Add epoch adjustment to convert from PostgreSQL epoch to Arrow epoch
	// IMPORTANT: Use signed int64 conversion, not uint64, since PG timestamps can be negative (before 2000-01-01)
	pgMicros := int64(binary.BigEndian.Uint64(data))
	arrowMicros := pgMicros + PostgresTimestampEpochMicros
	return arrowMicros, nil
}

// IntervalType handles PostgreSQL interval type (OID 1186)
type IntervalType struct{}

func (t *IntervalType) OID() uint32 {
	return TypeOIDInterval
}

func (t *IntervalType) Name() string {
	return "interval"
}

func (t *IntervalType) ArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.MonthDayNanoInterval
}

func (t *IntervalType) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 16 {
		return nil, fmt.Errorf("invalid data length for interval: expected 16, got %d", len(data))
	}

	// PostgreSQL interval binary format (16 bytes):
	// Bytes 0-7:   int64 microseconds (time field)
	// Bytes 8-11:  int32 days
	// Bytes 12-15: int32 months
	// IMPORTANT: Use signed int64 conversion, not uint64, since intervals can be negative
	pgMicros := int64(binary.BigEndian.Uint64(data[0:8]))
	pgDays := int32(binary.BigEndian.Uint32(data[8:12]))
	pgMonths := int32(binary.BigEndian.Uint32(data[12:16]))

	// Convert microseconds to nanoseconds with overflow check
	const maxMicros = math.MaxInt64 / 1000
	const minMicros = math.MinInt64 / 1000
	if pgMicros > maxMicros || pgMicros < minMicros {
		return nil, fmt.Errorf("interval microseconds overflow: %d", pgMicros)
	}
	arrowNanos := pgMicros * 1000

	// Return Arrow MonthDayNanoInterval with field reordering:
	// PostgreSQL: (microseconds, days, months)
	// Arrow: (months, days, nanoseconds)
	return arrow.MonthDayNanoInterval{
		Months:      pgMonths,
		Days:        pgDays,
		Nanoseconds: arrowNanos,
	}, nil
}

// ColumnWriter implementations for direct binary → Arrow conversion

// BoolColumnWriter writes PostgreSQL bool data directly to Arrow boolean arrays
type BoolColumnWriter struct {
	Builder *array.BooleanBuilder
}

func (w *BoolColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 1 {
		return fmt.Errorf("invalid data length for bool: expected 1, got %d", len(data))
	}

	w.Builder.Append(data[0] != 0)
	return nil
}

func (w *BoolColumnWriter) ArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Boolean
}

func (w *BoolColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 1 {
				return fmt.Errorf("invalid data length for bool: expected 1, got %d", len(data[i]))
			}
			w.Builder.Append(data[i][0] != 0)
		}
	}
	return nil
}

func (w *BoolColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *BoolColumnWriter) SetBufferPool(pool *BufferPool) {
	// Primitive types don't need buffer pools for temporary allocations
}

func (w *BoolColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// Int16ColumnWriter writes PostgreSQL int2 data directly to Arrow int16 arrays
type Int16ColumnWriter struct {
	Builder *array.Int16Builder
}

func (w *Int16ColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 2 {
		return fmt.Errorf("invalid data length for int16: expected 2, got %d", len(data))
	}

	value := int16(binary.BigEndian.Uint16(data))
	w.Builder.Append(value)
	return nil
}

func (w *Int16ColumnWriter) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int16
}

func (w *Int16ColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 2 {
				return fmt.Errorf("invalid data length for int16: expected 2, got %d", len(data[i]))
			}
			value := int16(binary.BigEndian.Uint16(data[i]))
			w.Builder.Append(value)
		}
	}
	return nil
}

func (w *Int16ColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *Int16ColumnWriter) SetBufferPool(pool *BufferPool) {
	// Primitive types don't need buffer pools for temporary allocations
}

func (w *Int16ColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// Int32ColumnWriter writes PostgreSQL int4 data directly to Arrow int32 arrays
type Int32ColumnWriter struct {
	Builder *array.Int32Builder
}

func (w *Int32ColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 4 {
		return fmt.Errorf("invalid data length for int32: expected 4, got %d", len(data))
	}

	value := int32(binary.BigEndian.Uint32(data))
	w.Builder.Append(value)
	return nil
}

func (w *Int32ColumnWriter) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int32
}

func (w *Int32ColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 4 {
				return fmt.Errorf("invalid data length for int32: expected 4, got %d", len(data[i]))
			}
			value := int32(binary.BigEndian.Uint32(data[i]))
			w.Builder.Append(value)
		}
	}
	return nil
}

func (w *Int32ColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *Int32ColumnWriter) SetBufferPool(pool *BufferPool) {
	// Primitive types don't need buffer pools for temporary allocations
}

func (w *Int32ColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// Int64ColumnWriter writes PostgreSQL int8 data directly to Arrow int64 arrays
type Int64ColumnWriter struct {
	Builder *array.Int64Builder
}

func (w *Int64ColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 8 {
		return fmt.Errorf("invalid data length for int64: expected 8, got %d", len(data))
	}

	value := int64(binary.BigEndian.Uint64(data))
	w.Builder.Append(value)
	return nil
}

func (w *Int64ColumnWriter) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int64
}

func (w *Int64ColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 8 {
				return fmt.Errorf("invalid data length for int64: expected 8, got %d", len(data[i]))
			}
			value := int64(binary.BigEndian.Uint64(data[i]))
			w.Builder.Append(value)
		}
	}
	return nil
}

func (w *Int64ColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *Int64ColumnWriter) SetBufferPool(pool *BufferPool) {
	// Primitive types don't need buffer pools for temporary allocations
}

func (w *Int64ColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// Float32ColumnWriter writes PostgreSQL float4 data directly to Arrow float32 arrays
type Float32ColumnWriter struct {
	Builder *array.Float32Builder
}

func (w *Float32ColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 4 {
		return fmt.Errorf("invalid data length for float32: expected 4, got %d", len(data))
	}

	value := math.Float32frombits(binary.BigEndian.Uint32(data))
	w.Builder.Append(value)
	return nil
}

func (w *Float32ColumnWriter) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Float32
}

func (w *Float32ColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 4 {
				return fmt.Errorf("invalid data length for float32: expected 4, got %d", len(data[i]))
			}
			value := math.Float32frombits(binary.BigEndian.Uint32(data[i]))
			w.Builder.Append(value)
		}
	}
	return nil
}

func (w *Float32ColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *Float32ColumnWriter) SetBufferPool(pool *BufferPool) {
	// Primitive types don't need buffer pools for temporary allocations
}

func (w *Float32ColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// Float64ColumnWriter writes PostgreSQL float8 data directly to Arrow float64 arrays
type Float64ColumnWriter struct {
	Builder *array.Float64Builder
}

func (w *Float64ColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 8 {
		return fmt.Errorf("invalid data length for float64: expected 8, got %d", len(data))
	}

	value := math.Float64frombits(binary.BigEndian.Uint64(data))
	w.Builder.Append(value)
	return nil
}

func (w *Float64ColumnWriter) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Float64
}

func (w *Float64ColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 8 {
				return fmt.Errorf("invalid data length for float64: expected 8, got %d", len(data[i]))
			}
			value := math.Float64frombits(binary.BigEndian.Uint64(data[i]))
			w.Builder.Append(value)
		}
	}
	return nil
}

func (w *Float64ColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *Float64ColumnWriter) SetBufferPool(pool *BufferPool) {
	// Primitive types don't need buffer pools for temporary allocations
}

func (w *Float64ColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// StringColumnWriter writes PostgreSQL text data directly to Arrow string arrays
// Achieves zero-copy by appending bytes directly to the binary builder
type StringColumnWriter struct {
	Builder    *array.StringBuilder
	bufferPool *BufferPool // NEW: Buffer pool for temporary allocations

	// Pre-allocation optimization
	expectedBatchSize int
}

func (w *StringColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	// Zero-copy: append bytes directly without string conversion
	w.Builder.BinaryBuilder.Append(data)
	return nil
}

func (w *StringColumnWriter) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (w *StringColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			// Zero-copy: append bytes directly without string conversion
			w.Builder.BinaryBuilder.Append(data[i])
		}
	}
	return nil
}

func (w *StringColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *StringColumnWriter) SetBufferPool(pool *BufferPool) {
	w.bufferPool = pool
}

func (w *StringColumnWriter) PreAllocate(expectedBatchSize int) {
	w.expectedBatchSize = expectedBatchSize
	// Pre-allocate string builder capacity
	w.Builder.Reserve(expectedBatchSize)
}

// BinaryColumnWriter writes PostgreSQL bytea data directly to Arrow binary arrays
type BinaryColumnWriter struct {
	Builder    *array.BinaryBuilder
	bufferPool *BufferPool // NEW: Buffer pool for temporary allocations

	// Pre-allocation optimization
	expectedBatchSize int
}

func (w *BinaryColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	// Zero-copy: append bytes directly
	w.Builder.Append(data)
	return nil
}

func (w *BinaryColumnWriter) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.Binary
}

func (w *BinaryColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			// Zero-copy: append bytes directly
			w.Builder.Append(data[i])
		}
	}
	return nil
}

func (w *BinaryColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *BinaryColumnWriter) SetBufferPool(pool *BufferPool) {
	w.bufferPool = pool
}

func (w *BinaryColumnWriter) PreAllocate(expectedBatchSize int) {
	w.expectedBatchSize = expectedBatchSize
	// Pre-allocate binary builder capacity
	w.Builder.Reserve(expectedBatchSize)
}

// Date32ColumnWriter writes PostgreSQL date data directly to Arrow date32 arrays
type Date32ColumnWriter struct {
	Builder *array.Date32Builder
}

func (w *Date32ColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 4 {
		return fmt.Errorf("invalid data length for date: expected 4, got %d", len(data))
	}

	pgDays := int32(binary.BigEndian.Uint32(data))
	arrowDays := pgDays + PostgresDateEpochDays // Single addition operation
	w.Builder.Append(arrow.Date32(arrowDays))
	return nil
}

func (w *Date32ColumnWriter) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Date32
}

func (w *Date32ColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 4 {
				return fmt.Errorf("invalid data length for date: expected 4, got %d", len(data[i]))
			}
			pgDays := int32(binary.BigEndian.Uint32(data[i]))
			arrowDays := pgDays + PostgresDateEpochDays
			w.Builder.Append(arrow.Date32(arrowDays))
		}
	}
	return nil
}

func (w *Date32ColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *Date32ColumnWriter) SetBufferPool(pool *BufferPool) {
	// Date types don't need buffer pools for temporary allocations
}

func (w *Date32ColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// Time64ColumnWriter writes PostgreSQL time data directly to Arrow time64 arrays
type Time64ColumnWriter struct {
	Builder *array.Time64Builder
}

func (w *Time64ColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 8 {
		return fmt.Errorf("invalid data length for time: expected 8, got %d", len(data))
	}

	// PostgreSQL time is stored as microseconds since midnight
	// Arrow Time64[microsecond] also uses microseconds - direct conversion
	timeMicros := int64(binary.BigEndian.Uint64(data))
	w.Builder.Append(arrow.Time64(timeMicros))
	return nil
}

func (w *Time64ColumnWriter) ArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Time64us
}

func (w *Time64ColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 8 {
				return fmt.Errorf("invalid data length for time: expected 8, got %d", len(data[i]))
			}
			// PostgreSQL time is stored as microseconds since midnight
			// Arrow Time64[microsecond] also uses microseconds - direct conversion
			timeMicros := int64(binary.BigEndian.Uint64(data[i]))
			w.Builder.Append(arrow.Time64(timeMicros))
		}
	}
	return nil
}

func (w *Time64ColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *Time64ColumnWriter) SetBufferPool(pool *BufferPool) {
	// Time types don't need buffer pools for temporary allocations
}

func (w *Time64ColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// TimestampColumnWriter writes PostgreSQL timestamp data directly to Arrow timestamp arrays
type TimestampColumnWriter struct {
	Builder       *array.TimestampBuilder
	timestampType *arrow.TimestampType
}

func (w *TimestampColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 8 {
		return fmt.Errorf("invalid data length for timestamp: expected 8, got %d", len(data))
	}

	// PostgreSQL timestamp is stored as microseconds since 2000-01-01
	// Arrow timestamp uses microseconds since 1970-01-01
	// Add epoch adjustment to convert from PostgreSQL epoch to Arrow epoch
	// IMPORTANT: Use signed int64 conversion, not uint64, since PG timestamps can be negative (before 2000-01-01)
	pgMicros := int64(binary.BigEndian.Uint64(data))
	arrowMicros := pgMicros + PostgresTimestampEpochMicros
	w.Builder.Append(arrow.Timestamp(arrowMicros))
	return nil
}

func (w *TimestampColumnWriter) ArrowType() arrow.DataType {
	return w.timestampType
}

func (w *TimestampColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 8 {
				return fmt.Errorf("invalid data length for timestamp: expected 8, got %d", len(data[i]))
			}
			// PostgreSQL timestamp is stored as microseconds since 2000-01-01
			// Arrow timestamp uses microseconds since 1970-01-01
			// Add epoch adjustment to convert from PostgreSQL epoch to Arrow epoch
			// IMPORTANT: Use signed int64 conversion, not uint64, since PG timestamps can be negative (before 2000-01-01)
			pgMicros := int64(binary.BigEndian.Uint64(data[i]))
			arrowMicros := pgMicros + PostgresTimestampEpochMicros
			w.Builder.Append(arrow.Timestamp(arrowMicros))
		}
	}
	return nil
}

func (w *TimestampColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *TimestampColumnWriter) SetBufferPool(pool *BufferPool) {
	// Timestamp types don't need buffer pools for temporary allocations
}

func (w *TimestampColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}

// NewTimestampColumnWriter creates a TimestampColumnWriter for timestamp (no timezone)
func NewTimestampColumnWriter(builder *array.TimestampBuilder) *TimestampColumnWriter {
	return &TimestampColumnWriter{
		Builder:       builder,
		timestampType: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""},
	}
}

// NewTimestamptzColumnWriter creates a TimestampColumnWriter for timestamptz (UTC timezone)
func NewTimestamptzColumnWriter(builder *array.TimestampBuilder) *TimestampColumnWriter {
	return &TimestampColumnWriter{
		Builder:       builder,
		timestampType: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
	}
}

// MonthDayNanoIntervalColumnWriter writes PostgreSQL interval data directly to Arrow interval arrays
type MonthDayNanoIntervalColumnWriter struct {
	Builder *array.MonthDayNanoIntervalBuilder
}

func (w *MonthDayNanoIntervalColumnWriter) WriteField(data []byte, isNull bool) error {
	if isNull {
		w.Builder.AppendNull()
		return nil
	}

	if len(data) != 16 {
		return fmt.Errorf("invalid data length for interval: expected 16, got %d", len(data))
	}

	// PostgreSQL interval binary format (16 bytes):
	// Bytes 0-7:   int64 microseconds (time field)
	// Bytes 8-11:  int32 days
	// Bytes 12-15: int32 months
	// IMPORTANT: Use signed int64 conversion, not uint64, since intervals can be negative
	pgMicros := int64(binary.BigEndian.Uint64(data[0:8]))
	pgDays := int32(binary.BigEndian.Uint32(data[8:12]))
	pgMonths := int32(binary.BigEndian.Uint32(data[12:16]))

	// Convert microseconds to nanoseconds with overflow check
	const maxMicros = math.MaxInt64 / 1000
	const minMicros = math.MinInt64 / 1000
	if pgMicros > maxMicros || pgMicros < minMicros {
		return fmt.Errorf("interval microseconds overflow: %d", pgMicros)
	}
	arrowNanos := pgMicros * 1000

	// Return Arrow MonthDayNanoInterval with field reordering:
	// PostgreSQL: (microseconds, days, months)
	// Arrow: (months, days, nanoseconds)
	interval := arrow.MonthDayNanoInterval{
		Months:      pgMonths,
		Days:        pgDays,
		Nanoseconds: arrowNanos,
	}
	w.Builder.Append(interval)
	return nil
}

func (w *MonthDayNanoIntervalColumnWriter) ArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.MonthDayNanoInterval
}

func (w *MonthDayNanoIntervalColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	for i := range data {
		if nulls[i] {
			w.Builder.AppendNull()
		} else {
			if len(data[i]) != 16 {
				return fmt.Errorf("invalid data length for interval: expected 16, got %d", len(data[i]))
			}

			// PostgreSQL interval binary format (16 bytes):
			// Bytes 0-7:   int64 microseconds (time field)
			// Bytes 8-11:  int32 days
			// Bytes 12-15: int32 months
			// IMPORTANT: Use signed int64 conversion, not uint64, since intervals can be negative
			pgMicros := int64(binary.BigEndian.Uint64(data[i][0:8]))
			pgDays := int32(binary.BigEndian.Uint32(data[i][8:12]))
			pgMonths := int32(binary.BigEndian.Uint32(data[i][12:16]))

			// Convert microseconds to nanoseconds with overflow check
			const maxMicros = math.MaxInt64 / 1000
			const minMicros = math.MinInt64 / 1000
			if pgMicros > maxMicros || pgMicros < minMicros {
				return fmt.Errorf("interval microseconds overflow: %d", pgMicros)
			}
			arrowNanos := pgMicros * 1000

			// Return Arrow MonthDayNanoInterval with field reordering:
			// PostgreSQL: (microseconds, days, months)
			// Arrow: (months, days, nanoseconds)
			interval := arrow.MonthDayNanoInterval{
				Months:      pgMonths,
				Days:        pgDays,
				Nanoseconds: arrowNanos,
			}
			w.Builder.Append(interval)
		}
	}
	return nil
}

func (w *MonthDayNanoIntervalColumnWriter) BuilderStats() (length, capacity int) {
	return w.Builder.Len(), w.Builder.Cap()
}

func (w *MonthDayNanoIntervalColumnWriter) SetBufferPool(pool *BufferPool) {
	// Interval types don't need buffer pools for temporary allocations
}

func (w *MonthDayNanoIntervalColumnWriter) PreAllocate(expectedBatchSize int) {
	w.Builder.Reserve(expectedBatchSize)
}
