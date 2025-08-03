package pgarrow_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
)

// Benchmark data for different types
var (
	boolData    = []byte{0x01}
	int16Data   = func() []byte { d := make([]byte, 2); binary.BigEndian.PutUint16(d, 12345); return d }()
	int32Data   = func() []byte { d := make([]byte, 4); binary.BigEndian.PutUint32(d, 1234567890); return d }()
	int64Data   = func() []byte { d := make([]byte, 8); binary.BigEndian.PutUint64(d, 1234567890123456789); return d }()
	float32Data = func() []byte {
		d := make([]byte, 4)
		binary.BigEndian.PutUint32(d, math.Float32bits(3.14159))
		return d
	}()
	float64Data = func() []byte {
		d := make([]byte, 8)
		binary.BigEndian.PutUint64(d, math.Float64bits(3.141592653589793))
		return d
	}()
	stringData    = []byte("The quick brown fox jumps over the lazy dog")
	dateData      = func() []byte { d := make([]byte, 4); binary.BigEndian.PutUint32(d, 0); return d }()                // 2000-01-01
	timeData      = func() []byte { d := make([]byte, 8); binary.BigEndian.PutUint64(d, 12*60*60*1000000); return d }() // 12:00:00
	timestampData = func() []byte { d := make([]byte, 8); binary.BigEndian.PutUint64(d, 0); return d }()                // 2000-01-01 00:00:00
	intervalData  = func() []byte {
		d := make([]byte, 16)
		binary.BigEndian.PutUint64(d[0:8], 3000000) // 3 seconds in microseconds
		binary.BigEndian.PutUint32(d[8:12], 2)      // 2 days
		binary.BigEndian.PutUint32(d[12:16], 1)     // 1 month
		return d
	}()
)

// BenchmarkColumnWriter_SingleVsBatch compares single field vs batch processing performance
func BenchmarkColumnWriter_SingleVsBatch(b *testing.B) {
	alloc := memory.NewGoAllocator()

	// Test with different batch sizes
	batchSizes := []int{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("Int64_Single_Batch%d", batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				builder := array.NewInt64Builder(alloc)
				writer := &pgarrow.Int64ColumnWriter{Builder: builder}

				// Process fields one by one
				for j := 0; j < batchSize; j++ {
					err := writer.WriteField(int64Data, false)
					if err != nil {
						b.Fatal(err)
					}
				}

				arr := builder.NewArray()
				arr.Release()
				builder.Release()
			}
		})

		b.Run(fmt.Sprintf("Int64_Batch_Batch%d", batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				builder := array.NewInt64Builder(alloc)
				writer := &pgarrow.Int64ColumnWriter{Builder: builder}

				// Prepare batch data
				data := make([][]byte, batchSize)
				nulls := make([]bool, batchSize)
				for j := 0; j < batchSize; j++ {
					data[j] = int64Data
					nulls[j] = false
				}

				// Process as batch
				err := writer.WriteFieldBatch(data, nulls)
				if err != nil {
					b.Fatal(err)
				}

				arr := builder.NewArray()
				arr.Release()
				builder.Release()
			}
		})
	}
}

// BenchmarkBoolColumnWriter benchmarks the new ColumnWriter approach for bool
func BenchmarkBoolColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	builder := array.NewBooleanBuilder(alloc)
	defer builder.Release()

	writer := &pgarrow.BoolColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(boolData, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkInt16ColumnWriter benchmarks the new ColumnWriter approach for int16
func BenchmarkInt16ColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt16Builder(alloc)
	defer builder.Release()

	writer := &pgarrow.Int16ColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(int16Data, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkInt32ColumnWriter benchmarks the new ColumnWriter approach for int32
func BenchmarkInt32ColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt32Builder(alloc)
	defer builder.Release()

	writer := &pgarrow.Int32ColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(int32Data, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkInt64ColumnWriter benchmarks the new ColumnWriter approach for int64
func BenchmarkInt64ColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt64Builder(alloc)
	defer builder.Release()

	writer := &pgarrow.Int64ColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(int64Data, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkFloat32ColumnWriter benchmarks the new ColumnWriter approach for float32
func BenchmarkFloat32ColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	builder := array.NewFloat32Builder(alloc)
	defer builder.Release()

	writer := &pgarrow.Float32ColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(float32Data, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkFloat64ColumnWriter benchmarks the new ColumnWriter approach for float64
func BenchmarkFloat64ColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(alloc)
	defer builder.Release()

	writer := &pgarrow.Float64ColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(float64Data, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkStringColumnWriter benchmarks the new ColumnWriter approach for strings
func BenchmarkStringColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	writer := pgarrow.NewStringColumnWriter(alloc)
	defer writer.Release()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(stringData, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr, err := writer.NewArray()
			if err != nil {
				b.Fatal(err)
			}
			arr.Release()
			writer.Reset()
		}
	}
}

// BenchmarkEndToEnd_ColumnWriter simulates the full conversion pipeline using ColumnWriter
func BenchmarkEndToEnd_ColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	writer := pgarrow.NewStringColumnWriter(alloc)
	defer writer.Release()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Direct binary → Arrow conversion
		err := writer.WriteField(stringData, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr, err := writer.NewArray()
			if err != nil {
				b.Fatal(err)
			}
			arr.Release()
			writer.Reset()
		}
	}
}

// BenchmarkBatch_ColumnWriter benchmarks batch processing with ColumnWriter approach
func BenchmarkBatch_ColumnWriter(b *testing.B) {
	const batchSize = 1000
	alloc := memory.NewGoAllocator()

	// Setup builders for different types
	boolBuilder := array.NewBooleanBuilder(alloc)
	defer boolBuilder.Release()
	int32Builder := array.NewInt32Builder(alloc)
	defer int32Builder.Release()
	// Setup writers
	boolWriter := &pgarrow.BoolColumnWriter{Builder: boolBuilder}
	int32Writer := &pgarrow.Int32ColumnWriter{Builder: int32Builder}
	stringWriter := pgarrow.NewStringColumnWriter(alloc)
	defer stringWriter.Release()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < batchSize; j++ {
			// Process bool field
			err := boolWriter.WriteField(boolData, false)
			if err != nil {
				b.Fatal(err)
			}

			// Process int32 field
			err = int32Writer.WriteField(int32Data, false)
			if err != nil {
				b.Fatal(err)
			}

			// Process string field
			err = stringWriter.WriteField(stringData, false)
			if err != nil {
				b.Fatal(err)
			}
		}

		// Build arrays and release
		arr1 := boolBuilder.NewArray()
		arr1.Release()
		arr2 := int32Builder.NewArray()
		arr2.Release()
		arr3, err := stringWriter.NewArray()
		if err != nil {
			b.Fatal(err)
		}
		arr3.Release()
		stringWriter.Reset()
	}
}

// Date/Time type benchmarks comparing legacy vs ColumnWriter approaches

// BenchmarkDate32ColumnWriter benchmarks the new ColumnWriter approach for date
func BenchmarkDate32ColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	builder := array.NewDate32Builder(alloc)
	defer builder.Release()

	writer := &pgarrow.Date32ColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(dateData, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkTimestampColumnWriter benchmarks the new ColumnWriter approach for timestamp
func BenchmarkTimestampColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
	builder := array.NewTimestampBuilder(alloc, timestampType)
	defer builder.Release()

	writer := pgarrow.NewTimestampColumnWriter(builder)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(timestampData, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkTime64ColumnWriter benchmarks the new ColumnWriter approach for time
func BenchmarkTime64ColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	timeType := &arrow.Time64Type{Unit: arrow.Microsecond}
	builder := array.NewTime64Builder(alloc, timeType)
	defer builder.Release()

	writer := &pgarrow.Time64ColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(timeData, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}

// BenchmarkMonthDayNanoIntervalColumnWriter benchmarks the new ColumnWriter approach for interval
func BenchmarkMonthDayNanoIntervalColumnWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	builder := array.NewMonthDayNanoIntervalBuilder(alloc)
	defer builder.Release()

	writer := &pgarrow.MonthDayNanoIntervalColumnWriter{Builder: builder}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := writer.WriteField(intervalData, false)
		if err != nil {
			b.Fatal(err)
		}

		// Reset writer periodically to avoid unbounded growth
		if i%1000 == 999 {
			arr := builder.NewArray()
			arr.Release()
		}
	}
}
