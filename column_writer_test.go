package pgarrow_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/require"
)

func TestColumnWriter_Interface(t *testing.T) {
	t.Parallel()

	// Test that ColumnWriter interface exists and has expected methods
	t.Run("BoolColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewBooleanBuilder(alloc)
		defer builder.Release()

		// This will fail until we implement ColumnWriter interface
		writer := &pgarrow.BoolColumnWriter{Builder: builder}

		// Test interface compliance
		var _ pgarrow.ColumnWriter = writer

		// Test ArrowType
		require.Equal(t, arrow.FixedWidthTypes.Boolean, writer.ArrowType())

		// Test WriteField with non-null value
		data := []byte{0x01} // true
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test WriteField with null value
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		// Test BuilderStats
		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("Int16ColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewInt16Builder(alloc)
		defer builder.Release()

		writer := &pgarrow.Int16ColumnWriter{Builder: builder}
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.PrimitiveTypes.Int16, writer.ArrowType())

		// Test with int16 value (123)
		data := make([]byte, 2)
		binary.BigEndian.PutUint16(data, 123)
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("Int32ColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewInt32Builder(alloc)
		defer builder.Release()

		writer := &pgarrow.Int32ColumnWriter{Builder: builder}
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.PrimitiveTypes.Int32, writer.ArrowType())

		// Test with int32 value (123456)
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, 123456)
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("Int64ColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewInt64Builder(alloc)
		defer builder.Release()

		writer := &pgarrow.Int64ColumnWriter{Builder: builder}
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.PrimitiveTypes.Int64, writer.ArrowType())

		// Test with int64 value (123456789)
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, 123456789)
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("Float32ColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewFloat32Builder(alloc)
		defer builder.Release()

		writer := &pgarrow.Float32ColumnWriter{Builder: builder}
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.PrimitiveTypes.Float32, writer.ArrowType())

		// Test with float32 value (3.14)
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, math.Float32bits(3.14))
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("Float64ColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewFloat64Builder(alloc)
		defer builder.Release()

		writer := &pgarrow.Float64ColumnWriter{Builder: builder}
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.PrimitiveTypes.Float64, writer.ArrowType())

		// Test with float64 value (3.14159)
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, math.Float64bits(3.14159))
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("StringColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		writer := pgarrow.NewStringColumnWriter(alloc)
		defer writer.Release()
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.BinaryTypes.String, writer.ArrowType())

		// Test with string data (zero-copy)
		data := []byte("hello world")
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})
}

func TestColumnWriter_ErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("BoolColumnWriter_InvalidDataLength", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewBooleanBuilder(alloc)
		defer builder.Release()

		writer := &pgarrow.BoolColumnWriter{Builder: builder}

		// Test with invalid data length
		data := []byte{0x01, 0x02} // bool should be 1 byte
		err := writer.WriteField(data, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid data length for bool")
	})

	t.Run("Int16ColumnWriter_InvalidDataLength", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewInt16Builder(alloc)
		defer builder.Release()

		writer := &pgarrow.Int16ColumnWriter{Builder: builder}

		// Test with invalid data length
		data := []byte{0x01} // int16 should be 2 bytes
		err := writer.WriteField(data, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid data length for int16")
	})

	t.Run("StringColumnWriter_EmptyData", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		writer := pgarrow.NewStringColumnWriter(alloc)
		defer writer.Release()

		// Test with empty data (should be valid for strings)
		data := []byte{}
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		length, _ := writer.BuilderStats()
		require.Equal(t, 1, length)
	})
}

func TestColumnWriter_ZeroCopyBehavior(t *testing.T) {
	t.Parallel()

	t.Run("StringColumnWriter_ZeroCopy", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		writer := pgarrow.NewStringColumnWriter(alloc)
		defer writer.Release()

		// Original data that we'll modify after writing
		data := []byte("original")
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Modify original data to test zero-copy behavior
		// Note: This test will help us verify zero-copy implementation
		copy(data, "modified")

		// Build array and check the stored value
		arr := writer.NewArray()
		defer arr.Release()

		stringArr, ok := arr.(*array.String)
		if !ok {
			t.Fatalf("expected *array.String, got %T", arr)
		}
		// The stored value should still be "original" if zero-copy is properly implemented
		// with internal copying as needed by Arrow builders
		require.Equal(t, "original", stringArr.Value(0))
	})
}

func TestColumnWriter_DateTimeTypes(t *testing.T) {
	t.Parallel()

	t.Run("Date32ColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewDate32Builder(alloc)
		defer builder.Release()

		writer := &pgarrow.Date32ColumnWriter{Builder: builder}
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.PrimitiveTypes.Date32, writer.ArrowType())

		// Test with date value (PostgreSQL epoch: 2000-01-01 = 0 days)
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, 0) // 2000-01-01
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("Time64ColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		timeType := &arrow.Time64Type{Unit: arrow.Microsecond}
		builder := array.NewTime64Builder(alloc, timeType)
		defer builder.Release()

		writer := &pgarrow.Time64ColumnWriter{Builder: builder}
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.FixedWidthTypes.Time64us, writer.ArrowType())

		// Test with time value (12:00:00 = 12 * 60 * 60 * 1000000 microseconds)
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, 12*60*60*1000000) // 12:00:00
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("TimestampColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
		builder := array.NewTimestampBuilder(alloc, timestampType)
		defer builder.Release()

		writer := pgarrow.NewTimestampColumnWriter(builder)
		var _ pgarrow.ColumnWriter = writer

		expectedType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
		require.Equal(t, expectedType, writer.ArrowType())

		// Test with timestamp value (PostgreSQL epoch: 2000-01-01 = 0 microseconds)
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, 0) // 2000-01-01 00:00:00
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("TimestamptzColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		builder := array.NewTimestampBuilder(alloc, timestampType)
		defer builder.Release()

		writer := pgarrow.NewTimestamptzColumnWriter(builder)
		var _ pgarrow.ColumnWriter = writer

		expectedType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		require.Equal(t, expectedType, writer.ArrowType())

		// Test with timestamptz value
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, 0) // 2000-01-01 00:00:00 UTC
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})

	t.Run("MonthDayNanoIntervalColumnWriter", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewMonthDayNanoIntervalBuilder(alloc)
		defer builder.Release()

		writer := &pgarrow.MonthDayNanoIntervalColumnWriter{Builder: builder}
		var _ pgarrow.ColumnWriter = writer

		require.Equal(t, arrow.FixedWidthTypes.MonthDayNanoInterval, writer.ArrowType())

		// Test with interval value (1 month, 2 days, 3 microseconds)
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], 3)   // 3 microseconds
		binary.BigEndian.PutUint32(data[8:12], 2)  // 2 days
		binary.BigEndian.PutUint32(data[12:16], 1) // 1 month
		err := writer.WriteField(data, false)
		require.NoError(t, err)

		// Test null
		err = writer.WriteField(nil, true)
		require.NoError(t, err)

		length, capacity := writer.BuilderStats()
		require.Equal(t, 2, length)
		require.GreaterOrEqual(t, capacity, length)
	})
}

func TestColumnWriter_BatchProcessing(t *testing.T) {
	t.Parallel()

	t.Run("BoolColumnWriter_Batch", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()
		builder := array.NewBooleanBuilder(alloc)
		defer builder.Release()

		writer := &pgarrow.BoolColumnWriter{Builder: builder}

		// Test batch data: [true, false, null, true]
		data := [][]byte{
			{0x01}, // true
			{0x00}, // false
			nil,    // will be ignored due to null
			{0x01}, // true
		}
		nulls := []bool{false, false, true, false}

		err := writer.WriteFieldBatch(data, nulls)
		require.NoError(t, err)

		// Verify length
		length, _ := writer.BuilderStats()
		require.Equal(t, 4, length)

		// Build array and verify values
		arr := builder.NewArray()
		defer arr.Release()
		boolArr, ok := arr.(*array.Boolean)
		require.True(t, ok)

		require.False(t, boolArr.IsNull(0))
		require.True(t, boolArr.Value(0))

		require.False(t, boolArr.IsNull(1))
		require.False(t, boolArr.Value(1))

		require.True(t, boolArr.IsNull(2))

		require.False(t, boolArr.IsNull(3))
		require.True(t, boolArr.Value(3))
	})

	t.Run("Batch_VsSingle_Equivalence", func(t *testing.T) {
		t.Parallel()
		alloc := memory.NewGoAllocator()

		// Test that batch processing produces same result as individual WriteField calls
		builderSingle := array.NewInt32Builder(alloc)
		defer builderSingle.Release()
		writerSingle := &pgarrow.Int32ColumnWriter{Builder: builderSingle}

		builderBatch := array.NewInt32Builder(alloc)
		defer builderBatch.Release()
		writerBatch := &pgarrow.Int32ColumnWriter{Builder: builderBatch}

		// Test data
		values := []int32{100, -200, 0, 999, -1}
		data := make([][]byte, len(values))
		nulls := make([]bool, len(values))

		for i, val := range values {
			data[i] = make([]byte, 4)
			binary.BigEndian.PutUint32(data[i], uint32(val))
			nulls[i] = (i == 2) // Make index 2 null
		}

		// Process with single WriteField calls
		for i := range values {
			err := writerSingle.WriteField(data[i], nulls[i])
			require.NoError(t, err)
		}

		// Process with batch
		err := writerBatch.WriteFieldBatch(data, nulls)
		require.NoError(t, err)

		// Build arrays and compare
		arrSingle := builderSingle.NewArray()
		defer arrSingle.Release()
		arrBatch := builderBatch.NewArray()
		defer arrBatch.Release()

		int32Single, ok := arrSingle.(*array.Int32)
		require.True(t, ok)
		int32Batch, ok := arrBatch.(*array.Int32)
		require.True(t, ok)

		require.Equal(t, int32Single.Len(), int32Batch.Len())

		for i := 0; i < int32Single.Len(); i++ {
			require.Equal(t, int32Single.IsNull(i), int32Batch.IsNull(i))
			if !int32Single.IsNull(i) {
				require.Equal(t, int32Single.Value(i), int32Batch.Value(i))
			}
		}
	})
}
