package pgarrow

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// PostgreSQL type OIDs for supported data types
	TypeOIDBool        = 16
	TypeOIDBytea       = 17
	TypeOIDName        = 19
	TypeOIDInt8        = 20
	TypeOIDInt2        = 21
	TypeOIDInt4        = 23
	TypeOIDText        = 25
	TypeOIDFloat4      = 700
	TypeOIDFloat8      = 701
	TypeOIDBpchar      = 1042
	TypeOIDVarchar     = 1043
	TypeOIDChar        = 18
	TypeOIDDate        = 1082
	TypeOIDTime        = 1083
	TypeOIDTimestamp   = 1114
	TypeOIDTimestamptz = 1184
	TypeOIDInterval    = 1186

	// PostgreSQL epoch adjustment: days from 1970-01-01 to 2000-01-01
	PostgresDateEpochDays = 10957
	// PostgreSQL timestamp epoch adjustment: microseconds from 1970-01-01 to 2000-01-01
	PostgresTimestampEpochMicros = 946684800000000
)

// ScanPlan interface moved to direct_copy_parser.go where it belongs

// ColumnWriter defines the interface for direct binary data → Arrow column conversion
// without intermediate allocations, enabling high-performance PostgreSQL → Arrow streaming.
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

	// SetBufferPool sets the buffer pool for memory management
	SetBufferPool(pool *BufferPool)

	// PreAllocate pre-allocates capacity for the expected batch size
	PreAllocate(expectedBatchSize int)
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

// StringColumnWriter provides a high-performance string column writer
// that reduces allocations through batch-oriented buffer management and
// direct buffer manipulation
type StringColumnWriter struct {
	allocator         memory.Allocator
	bufferPool        *BufferPool
	expectedBatchSize int

	// Pre-allocated buffers for batch operations
	offsetBuffer   *memory.Buffer // int32 offsets
	dataBuffer     *memory.Buffer // raw string data
	validityBuffer *memory.Buffer // null bitmap

	// Current state
	length      int     // number of strings written
	dataSize    int     // total bytes in data buffer
	nullCount   int     // count of null values
	offsetSlice []int32 // direct access to offset buffer
	dataSlice   []byte  // direct access to data buffer
}

// NewStringColumnWriter creates a new optimized string column writer
func NewStringColumnWriter(alloc memory.Allocator) *StringColumnWriter {
	return &StringColumnWriter{
		allocator:         alloc,
		expectedBatchSize: 256, // default batch size
	}
}

func (w *StringColumnWriter) WriteField(data []byte, isNull bool) error {
	return w.WriteFieldBatch([][]byte{data}, []bool{isNull})
}

func (w *StringColumnWriter) WriteFieldBatch(data [][]byte, nulls []bool) error {
	if len(data) != len(nulls) {
		return fmt.Errorf("data and nulls length mismatch: %d vs %d", len(data), len(nulls))
	}

	batchSize := len(data)
	if batchSize == 0 {
		return nil
	}

	// Pre-calculate total data size for this batch
	totalDataSize := 0
	nullsInBatch := 0
	for i, isNull := range nulls {
		if !isNull {
			totalDataSize += len(data[i])
		} else {
			nullsInBatch++
		}
	}

	// Ensure buffers have sufficient capacity
	w.ensureCapacity(batchSize, totalDataSize)

	// Process the batch efficiently
	for i, isNull := range nulls {
		if isNull {
			// For nulls, repeat the current offset
			w.offsetSlice[w.length+i+1] = int32(w.dataSize)
			w.nullCount++
		} else {
			// Copy data directly to buffer
			dataLen := len(data[i])
			copy(w.dataSlice[w.dataSize:w.dataSize+dataLen], data[i])
			w.dataSize += dataLen
			w.offsetSlice[w.length+i+1] = int32(w.dataSize)
		}
	}

	// Update validity bitmap for this batch if there are nulls
	if nullsInBatch > 0 {
		w.updateValidityBitmap(w.length, nulls)
	}

	w.length += batchSize
	return nil
}

func (w *StringColumnWriter) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (w *StringColumnWriter) BuilderStats() (length, capacity int) {
	return w.length, w.expectedBatchSize // approximate capacity based on expected batch size
}

func (w *StringColumnWriter) SetBufferPool(pool *BufferPool) {
	w.bufferPool = pool
}

func (w *StringColumnWriter) PreAllocate(expectedBatchSize int) {
	w.expectedBatchSize = expectedBatchSize
	// Pre-allocate buffers based on expected usage
	w.ensureCapacity(expectedBatchSize, expectedBatchSize*32) // assume avg 32 bytes per string
}

// NewArray creates an Arrow array from the accumulated data
func (w *StringColumnWriter) NewArray() (arrow.Array, error) {
	if w.length == 0 {
		// Return empty array
		builder := array.NewStringBuilder(w.allocator)
		defer builder.Release()
		return builder.NewArray(), nil
	}

	// Create the final buffers - safe conversion from []int32 to []byte
	offsetCount := w.length + 1
	offsetBytes := make([]byte, offsetCount*4)
	for i := 0; i < offsetCount; i++ {
		offset := w.offsetSlice[i]
		if offset < 0 {
			return nil, fmt.Errorf("StringColumnWriter: invalid negative offset %d at position %d", offset, i)
		}
		binary.LittleEndian.PutUint32(offsetBytes[i*4:], uint32(offset))
	}
	offsetBuf := memory.NewBufferBytes(offsetBytes)

	dataBuf := memory.NewBufferBytes(w.dataSlice[:w.dataSize])

	var validityBuf *memory.Buffer
	if w.nullCount > 0 {
		validityBuf = w.validityBuffer
		validityBuf.Retain() // retain for the array
	}

	// Create array data
	arrayData := array.NewData(
		arrow.BinaryTypes.String,
		w.length,
		[]*memory.Buffer{validityBuf, offsetBuf, dataBuf},
		nil, // no child data
		w.nullCount,
		0, // offset
	)

	arr := array.MakeFromData(arrayData)

	// Reset the writer state (following Arrow's builder pattern)
	w.Reset()

	return arr, nil
}

// Reset resets the writer for reuse
func (w *StringColumnWriter) Reset() {
	w.length = 0
	w.dataSize = 0
	w.nullCount = 0
	// Keep buffers allocated for reuse
}

// Release releases all resources
func (w *StringColumnWriter) Release() {
	if w.offsetBuffer != nil {
		w.offsetBuffer.Release()
		w.offsetBuffer = nil
	}
	if w.dataBuffer != nil {
		w.dataBuffer.Release()
		w.dataBuffer = nil
	}
	if w.validityBuffer != nil {
		w.validityBuffer.Release()
		w.validityBuffer = nil
	}
}

// ensureCapacity ensures buffers have sufficient capacity for the new data
func (w *StringColumnWriter) ensureCapacity(additionalItems, additionalDataSize int) {
	newLength := w.length + additionalItems
	newDataSize := w.dataSize + additionalDataSize

	w.ensureOffsetBuffer(newLength)
	w.ensureDataBuffer(newDataSize)
	w.ensureValidityBuffer(newLength, additionalItems)
}

// ensureOffsetBuffer ensures offset buffer has sufficient capacity
func (w *StringColumnWriter) ensureOffsetBuffer(newLength int) {
	requiredOffsetBytes := (newLength + 1) * 4 // +1 for the final offset
	if w.offsetBuffer == nil || w.offsetBuffer.Len() < requiredOffsetBytes {
		currentLen := 0
		if w.offsetBuffer != nil {
			currentLen = w.offsetBuffer.Len()
		}
		newOffsetCapacity := max(requiredOffsetBytes, currentLen*3/2)
		newOffsetBuf := memory.NewResizableBuffer(w.allocator)
		newOffsetBuf.Resize(newOffsetCapacity)

		if w.offsetBuffer != nil {
			copy(newOffsetBuf.Bytes(), w.offsetBuffer.Bytes()[:w.offsetBuffer.Len()])
			w.offsetBuffer.Release()
		} else {
			// Initialize first offset to 0 using safe binary encoding
			binary.LittleEndian.PutUint32(newOffsetBuf.Bytes()[0:4], 0)
		}

		w.offsetBuffer = newOffsetBuf
		w.updateOffsetSlice(newOffsetCapacity)
	}
}

// ensureDataBuffer ensures data buffer has sufficient capacity
func (w *StringColumnWriter) ensureDataBuffer(newDataSize int) {
	if w.dataBuffer == nil || w.dataBuffer.Len() < newDataSize {
		currentDataLen := 0
		if w.dataBuffer != nil {
			currentDataLen = w.dataBuffer.Len()
		}
		newDataCapacity := max(newDataSize, currentDataLen*3/2)
		newDataBuf := memory.NewResizableBuffer(w.allocator)
		newDataBuf.Resize(newDataCapacity)

		if w.dataBuffer != nil {
			copy(newDataBuf.Bytes(), w.dataBuffer.Bytes()[:w.dataSize])
			w.dataBuffer.Release()
		}

		w.dataBuffer = newDataBuf
		w.dataSlice = newDataBuf.Bytes()
	}
}

// ensureValidityBuffer ensures validity buffer has sufficient capacity
func (w *StringColumnWriter) ensureValidityBuffer(newLength, additionalItems int) {
	if w.nullCount > 0 || additionalItems > 0 {
		requiredValidityBytes := (newLength + 7) / 8
		if w.validityBuffer == nil || w.validityBuffer.Len() < requiredValidityBytes {
			currentValidityLen := 0
			if w.validityBuffer != nil {
				currentValidityLen = w.validityBuffer.Len()
			}
			newValidityCapacity := max(requiredValidityBytes, currentValidityLen*3/2)
			newValidityBuf := memory.NewResizableBuffer(w.allocator)
			newValidityBuf.Resize(newValidityCapacity)

			if w.validityBuffer != nil {
				copy(newValidityBuf.Bytes(), w.validityBuffer.Bytes()[:w.validityBuffer.Len()])
				w.validityBuffer.Release()
			} else {
				// Efficiently fill the buffer with 0xFF using copy pattern
				buf := newValidityBuf.Bytes()
				if len(buf) > 0 {
					buf[0] = 0xFF
					for i := 1; i < len(buf); i *= 2 {
						copy(buf[i:], buf[:i])
					}
				}
			}

			w.validityBuffer = newValidityBuf
		}
	}
}

// updateOffsetSlice updates the offset slice after buffer reallocation
func (w *StringColumnWriter) updateOffsetSlice(newOffsetCapacity int) {
	// Safe conversion from buffer bytes back to []int32 slice
	offsetCount := newOffsetCapacity / 4
	if cap(w.offsetSlice) < offsetCount {
		w.offsetSlice = make([]int32, offsetCount)
	} else {
		w.offsetSlice = w.offsetSlice[:offsetCount]
	}

	// Copy existing offset data from buffer
	bufferBytes := w.offsetBuffer.Bytes()
	for i := 0; i < len(w.offsetSlice) && i*4 < len(bufferBytes); i++ {
		w.offsetSlice[i] = int32(binary.LittleEndian.Uint32(bufferBytes[i*4:]))
	}
}

// updateValidityBitmap updates the validity bitmap for a batch of items
func (w *StringColumnWriter) updateValidityBitmap(startIndex int, nulls []bool) {
	if w.validityBuffer == nil {
		return
	}

	validityBytes := w.validityBuffer.Bytes()
	for i, isNull := range nulls {
		if isNull {
			bitIndex := startIndex + i
			byteIndex := bitIndex / 8
			bitOffset := bitIndex % 8
			validityBytes[byteIndex] &= ^(1 << bitOffset) // clear bit
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// BinaryColumnWriter writes PostgreSQL bytea data directly to Arrow binary arrays
type BinaryColumnWriter struct {
	Builder    *array.BinaryBuilder
	bufferPool *BufferPool // Buffer pool for temporary allocations

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
