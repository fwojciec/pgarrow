package pgarrow

import (
	"sync"
)

const (
	// Go Runtime Memory Layout Optimization Constants

	// Optimize for Go page size (8KB) and cache lines (64 bytes)
	GoPageSizeBytes    = 8192 // Go runtime page size
	CacheLineSizeBytes = 64   // x86_64 cache line size

	// ADBC-style byte-based batching constants (preferred approach)
	DefaultBatchSizeBytes = 16777216 // 16MB - ADBC's kDefaultBatchSizeHintBytes
	MaxBatchSizeBytes     = 67108864 // 64MB - Upper limit for memory safety

	// Legacy row-based batch sizes (deprecated, use byte-based batching)
	OptimalBatchSizeGo = 256 // Sweet spot for Go GC and cache performance
	MaxBatchSizeGo     = 512 // Maximum before diminishing returns
	MinBatchSizeGo     = 64  // Minimum for cache efficiency

	// Type-specific optimizations based on data size and access patterns
	OptimalBatchSizeSmall  = 512 // For small types (bool, int16, int32)
	OptimalBatchSizeMedium = 256 // For medium types (int64, float64, date)
	OptimalBatchSizeLarge  = 128 // For large/variable types (string, binary)

	// Buffer pool size classes for different allocation patterns
	SmallBufferSizeBytes  = 256   // 0-256 bytes
	MediumBufferSizeBytes = 4096  // 256-4KB bytes
	LargeBufferSizeBytes  = 16384 // 4KB+ bytes
)

// BufferPool manages reusable byte slices to minimize GC pressure.
// Uses size-class based pools for optimal memory allocation patterns.
type BufferPool struct {
	// Different pools for different size classes to reduce fragmentation
	smallBytePool  *sync.Pool // 0-256 bytes
	mediumBytePool *sync.Pool // 256-4KB bytes
	largeBytePool  *sync.Pool // 4KB+ bytes

	// Field data pools for structured data
	fieldDataPool *sync.Pool // []FieldData slices
	rowBufferPool *sync.Pool // [][]FieldData slices
}

// FieldData represents a single field's binary data and null status.
// Cache-aligned and minimal for optimal memory access.
type FieldData struct {
	Data   []byte  // 24 bytes (slice header)
	IsNull bool    // 1 byte
	_      [7]byte // Pad to 32 bytes for cache alignment
}

// NewBufferPool creates a new buffer pool with Go-optimized allocation strategies.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		smallBytePool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate with capacity to reduce reallocations
				return make([]byte, 0, SmallBufferSizeBytes)
			},
		},
		mediumBytePool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, MediumBufferSizeBytes)
			},
		},
		largeBytePool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, LargeBufferSizeBytes)
			},
		},
		fieldDataPool: &sync.Pool{
			New: func() interface{} {
				// Typical column count for most queries
				return make([]FieldData, 0, 16)
			},
		},
		rowBufferPool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate for optimal batch size
				return make([][]FieldData, 0, OptimalBatchSizeGo)
			},
		},
	}
}

// GetByteSlice returns a byte slice from the appropriate size-class pool.
// The slice is reset to zero length but retains its capacity.
func (bp *BufferPool) GetByteSlice(sizeHint int) []byte {
	switch {
	case sizeHint <= SmallBufferSizeBytes:
		slice := bp.smallBytePool.Get().([]byte)
		return slice[:0]
	case sizeHint <= MediumBufferSizeBytes:
		slice := bp.mediumBytePool.Get().([]byte)
		return slice[:0]
	default:
		slice := bp.largeBytePool.Get().([]byte)
		return slice[:0]
	}
}

// PutByteSlice returns a byte slice to the appropriate size-class pool.
// The slice is reset to zero length to prevent data leakage.
func (bp *BufferPool) PutByteSlice(slice []byte) {
	if slice == nil {
		return
	}

	// Reset slice to zero length but keep capacity
	slice = slice[:0]

	// Return to appropriate pool based on capacity
	switch cap(slice) {
	case SmallBufferSizeBytes:
		bp.smallBytePool.Put(slice)
	case MediumBufferSizeBytes:
		bp.mediumBytePool.Put(slice)
	case LargeBufferSizeBytes:
		bp.largeBytePool.Put(slice)
		// For non-standard capacities, let GC handle them to avoid pool pollution
	}
}

// GetFieldDataSlice returns a reusable FieldData slice from the pool.
func (bp *BufferPool) GetFieldDataSlice() []FieldData {
	slice := bp.fieldDataPool.Get().([]FieldData)
	return slice[:0]
}

// PutFieldDataSlice returns a FieldData slice to the pool after resetting it.
func (bp *BufferPool) PutFieldDataSlice(slice []FieldData) {
	if slice == nil {
		return
	}

	// Clear the slice data to prevent memory leaks
	for i := range slice {
		slice[i] = FieldData{}
	}

	// Reset to zero length and return to pool
	bp.fieldDataPool.Put(slice[:0])
}

// GetRowBuffer returns a reusable row buffer from the pool.
func (bp *BufferPool) GetRowBuffer() [][]FieldData {
	buffer := bp.rowBufferPool.Get().([][]FieldData)
	return buffer[:0]
}

// PutRowBuffer returns a row buffer to the pool after resetting it.
func (bp *BufferPool) PutRowBuffer(buffer [][]FieldData) {
	if buffer == nil {
		return
	}

	// Clear the buffer data to prevent memory leaks
	for i := range buffer {
		buffer[i] = nil
	}

	// Reset to zero length and return to pool
	bp.rowBufferPool.Put(buffer[:0])
}

// BatchParserPool provides buffer pool management for batch parsing operations.
// This is the main interface for SchemaMetadata and Parser integration.
type BatchParserPool struct {
	bufferPool *BufferPool

	// Statistics for optimization insights
	allocations int64
	poolHits    int64
	poolMisses  int64
}

// NewBatchParserPool creates a new batch parser pool with integrated buffer management.
func NewBatchParserPool() *BatchParserPool {
	return &BatchParserPool{
		bufferPool: NewBufferPool(),
	}
}

// GetOptimalBatchSize returns the optimal batch size for a given data type.
// This considers Go GC patterns and cache efficiency.
func GetOptimalBatchSize(dataType uint32) int {
	switch dataType {
	case TypeOIDBool, TypeOIDInt2, TypeOIDInt4:
		// Small fixed-size types benefit from larger batches
		return OptimalBatchSizeSmall

	case TypeOIDInt8, TypeOIDFloat4, TypeOIDFloat8, TypeOIDDate, TypeOIDTime, TypeOIDTimestamp, TypeOIDTimestamptz:
		// Medium fixed-size types
		return OptimalBatchSizeMedium

	case TypeOIDText, TypeOIDVarchar, TypeOIDBpchar, TypeOIDName, TypeOIDChar, TypeOIDBytea, TypeOIDInterval:
		// Variable-size types benefit from smaller batches
		return OptimalBatchSizeLarge

	default:
		// Conservative default
		return OptimalBatchSizeGo
	}
}

// GetOptimalBatchSizeForSchema returns the optimal batch size for a mixed schema.
// This considers the mix of data types and chooses a balanced size.
func GetOptimalBatchSizeForSchema(fieldOIDs []uint32) int {
	if len(fieldOIDs) == 0 {
		return OptimalBatchSizeGo
	}

	smallTypeCount := 0
	mediumTypeCount := 0
	largeTypeCount := 0

	for _, oid := range fieldOIDs {
		switch oid {
		case TypeOIDBool, TypeOIDInt2, TypeOIDInt4:
			smallTypeCount++
		case TypeOIDInt8, TypeOIDFloat4, TypeOIDFloat8, TypeOIDDate, TypeOIDTime, TypeOIDTimestamp, TypeOIDTimestamptz:
			mediumTypeCount++
		case TypeOIDText, TypeOIDVarchar, TypeOIDBpchar, TypeOIDName, TypeOIDChar, TypeOIDBytea, TypeOIDInterval:
			largeTypeCount++
		}
	}

	totalFields := len(fieldOIDs)

	// If more than 50% are large/variable types, use smaller batches
	if largeTypeCount > totalFields/2 {
		return OptimalBatchSizeLarge
	}

	// If more than 50% are small types, use larger batches
	if smallTypeCount > totalFields/2 {
		return OptimalBatchSizeSmall
	}

	// Mixed or mostly medium types
	return OptimalBatchSizeMedium
}

// GetBufferPool returns the underlying buffer pool for direct access.
func (bpp *BatchParserPool) GetBufferPool() *BufferPool {
	return bpp.bufferPool
}

// RecordAllocation increments the allocation counter for statistics.
func (bpp *BatchParserPool) RecordAllocation() {
	bpp.allocations++
}

// RecordPoolHit increments the pool hit counter for statistics.
func (bpp *BatchParserPool) RecordPoolHit() {
	bpp.poolHits++
}

// RecordPoolMiss increments the pool miss counter for statistics.
func (bpp *BatchParserPool) RecordPoolMiss() {
	bpp.poolMisses++
}

// GetStats returns current pool statistics for optimization insights.
func (bpp *BatchParserPool) GetStats() (allocations, poolHits, poolMisses int64) {
	return bpp.allocations, bpp.poolHits, bpp.poolMisses
}

// GetPoolEfficiency returns the pool hit rate as a percentage.
func (bpp *BatchParserPool) GetPoolEfficiency() float64 {
	total := bpp.poolHits + bpp.poolMisses
	if total == 0 {
		return 0.0
	}
	return float64(bpp.poolHits) / float64(total) * 100.0
}
