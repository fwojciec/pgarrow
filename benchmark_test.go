package pgarrow_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
)

const (
	// Default number of rows to use in benchmarks for consistent measurements
	defaultBenchmarkRowCount = 1000
)

// Benchmark binary parsing and Arrow conversion for different data types

func BenchmarkBinaryToArrow_Int4(b *testing.B) {
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDInt4,
		valueCount: defaultBenchmarkRowCount,
		dataGen:    generateInt4Data,
	})
}

func BenchmarkBinaryToArrow_Int8(b *testing.B) {
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDInt8,
		valueCount: defaultBenchmarkRowCount,
		dataGen:    generateInt8Data,
	})
}

func BenchmarkBinaryToArrow_Float8(b *testing.B) {
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDFloat8,
		valueCount: defaultBenchmarkRowCount,
		dataGen:    generateFloat8Data,
	})
}

func BenchmarkBinaryToArrow_Text(b *testing.B) {
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDText,
		valueCount: defaultBenchmarkRowCount,
		dataGen:    generateTextData,
	})
}

func BenchmarkBinaryToArrow_Bytea(b *testing.B) {
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDBytea,
		valueCount: defaultBenchmarkRowCount,
		dataGen:    generateByteaData,
	})
}

// Benchmark different batch sizes
func BenchmarkBinaryToArrow_SmallBatch(b *testing.B) {
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDInt4,
		valueCount: 100,
		dataGen:    generateInt4Data,
	})
}

func BenchmarkBinaryToArrow_MediumBatch(b *testing.B) {
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDInt4,
		valueCount: defaultBenchmarkRowCount,
		dataGen:    generateInt4Data,
	})
}

func BenchmarkBinaryToArrow_LargeBatch(b *testing.B) {
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDInt4,
		valueCount: 10000,
		dataGen:    generateInt4Data,
	})
}

// Benchmark mixed data types (realistic scenario)
func BenchmarkBinaryToArrow_MixedTypes(b *testing.B) {
	benchmarkMixedTypes(b, 1000)
}

// Memory allocation benchmarks
func BenchmarkBinaryToArrow_ByteaMemory(b *testing.B) {
	b.ReportAllocs()
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDBytea,
		valueCount: defaultBenchmarkRowCount,
		dataGen:    generateByteaData,
	})
}

func BenchmarkBinaryToArrow_TextMemory(b *testing.B) {
	b.ReportAllocs()
	benchmarkBinaryToArrow(b, benchmarkConfig{
		fieldOID:   pgarrow.TypeOIDText,
		valueCount: defaultBenchmarkRowCount,
		dataGen:    generateTextData,
	})
}

// Core benchmark infrastructure

type benchmarkConfig struct {
	fieldOID   uint32
	valueCount int
	dataGen    func(count int) []byte
}

func benchmarkBinaryToArrow(b *testing.B, config benchmarkConfig) {
	b.Helper()
	// Generate test data once
	binaryData := config.dataGen(config.valueCount)
	fieldOIDs := []uint32{config.fieldOID}

	// Create schema
	columns := []pgarrow.ColumnInfo{
		{Name: "test_col", OID: config.fieldOID},
	}
	schema, err := pgarrow.CreateSchema(columns)
	if err != nil {
		b.Fatalf("Failed to create schema: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		alloc := memory.NewGoAllocator()

		// Parse binary data
		reader := bytes.NewReader(binaryData)
		parser := pgarrow.NewParser(reader, fieldOIDs)
		if err := parser.ParseHeader(); err != nil {
			b.Fatalf("Failed to parse header: %v", err)
		}

		// Create record builder
		recordBuilder, err := pgarrow.NewRecordBuilder(schema, alloc)
		if err != nil {
			b.Fatalf("Failed to create record builder: %v", err)
		}

		// Parse and convert data
		registry := pgarrow.NewRegistry()
		rowCount := 0
		for {
			fields, err := parser.ParseTuple()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Failed to parse tuple: %v", err)
			}

			// Convert field to typed value
			field := fields[0]
			var value any
			if field.Value == nil {
				value = nil
			} else {
				handler, err := registry.GetHandler(config.fieldOID)
				if err != nil {
					b.Fatalf("Failed to get handler: %v", err)
				}

				switch fieldData := field.Value.(type) {
				case []byte:
					value, err = handler.Parse(fieldData)
				case string:
					value, err = handler.Parse([]byte(fieldData))
				default:
					b.Fatalf("Unexpected field data type: %T", field.Value)
				}
				if err != nil {
					b.Fatalf("Failed to parse field: %v", err)
				}
			}

			if err := recordBuilder.AppendRow([]any{value}); err != nil {
				b.Fatalf("Failed to append row: %v", err)
			}
			rowCount++
		}

		// Create Arrow record
		record, err := recordBuilder.NewRecord()
		if err != nil {
			b.Fatalf("Failed to create record: %v", err)
		}

		// Clean up
		record.Release()
		recordBuilder.Release()
	}
}

func benchmarkMixedTypes(b *testing.B, rowCount int) {
	b.Helper()
	// Generate mixed type data
	binaryData := generateMixedTypeData(rowCount)
	fieldOIDs := []uint32{
		pgarrow.TypeOIDInt4,
		pgarrow.TypeOIDText,
		pgarrow.TypeOIDFloat8,
		pgarrow.TypeOIDBytea,
		pgarrow.TypeOIDBool,
	}

	// Create schema
	columns := []pgarrow.ColumnInfo{
		{Name: "id", OID: pgarrow.TypeOIDInt4},
		{Name: "name", OID: pgarrow.TypeOIDText},
		{Name: "score", OID: pgarrow.TypeOIDFloat8},
		{Name: "data", OID: pgarrow.TypeOIDBytea},
		{Name: "active", OID: pgarrow.TypeOIDBool},
	}
	schema, err := pgarrow.CreateSchema(columns)
	if err != nil {
		b.Fatalf("Failed to create schema: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		alloc := memory.NewGoAllocator()

		// Parse binary data
		reader := bytes.NewReader(binaryData)
		parser := pgarrow.NewParser(reader, fieldOIDs)
		if err := parser.ParseHeader(); err != nil {
			b.Fatalf("Failed to parse header: %v", err)
		}

		// Create record builder
		recordBuilder, err := pgarrow.NewRecordBuilder(schema, alloc)
		if err != nil {
			b.Fatalf("Failed to create record builder: %v", err)
		}

		// Parse and convert data
		registry := pgarrow.NewRegistry()
		processedRows := 0
		for {
			fields, err := parser.ParseTuple()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Failed to parse tuple: %v", err)
			}

			// Convert all fields
			values := make([]any, len(fields))
			for j, field := range fields {
				if field.Value == nil {
					values[j] = nil
				} else {
					handler, err := registry.GetHandler(fieldOIDs[j])
					if err != nil {
						b.Fatalf("Failed to get handler: %v", err)
					}

					switch fieldData := field.Value.(type) {
					case []byte:
						values[j], err = handler.Parse(fieldData)
					case string:
						values[j], err = handler.Parse([]byte(fieldData))
					default:
						b.Fatalf("Unexpected field data type: %T", field.Value)
					}
					if err != nil {
						b.Fatalf("Failed to parse field: %v", err)
					}
				}
			}

			if err := recordBuilder.AppendRow(values); err != nil {
				b.Fatalf("Failed to append row: %v", err)
			}
			processedRows++
		}

		// Create Arrow record
		record, err := recordBuilder.NewRecord()
		if err != nil {
			b.Fatalf("Failed to create record: %v", err)
		}

		// Clean up
		record.Release()
		recordBuilder.Release()
	}
}

// Data generators for benchmarks

// mustWrite panics if binary.Write fails (for benchmark data generation)
func mustWrite(buf *bytes.Buffer, order binary.ByteOrder, data any) {
	if err := binary.Write(buf, order, data); err != nil {
		panic(err)
	}
}

func generateInt4Data(count int) []byte {
	var buf bytes.Buffer

	// Write PGCOPY header
	writePGCopyHeader(&buf)

	// Write rows
	for i := 0; i < count; i++ {
		// Field count (1)
		mustWrite(&buf, binary.BigEndian, int16(1))
		// Field length (4 bytes for int4)
		mustWrite(&buf, binary.BigEndian, int32(4))
		// Field data
		mustWrite(&buf, binary.BigEndian, int32(i))
	}

	// Write trailer
	mustWrite(&buf, binary.BigEndian, int16(-1))

	return buf.Bytes()
}

func generateInt8Data(count int) []byte {
	var buf bytes.Buffer
	writePGCopyHeader(&buf)

	for i := 0; i < count; i++ {
		mustWrite(&buf, binary.BigEndian, int16(1))
		mustWrite(&buf, binary.BigEndian, int32(8))
		mustWrite(&buf, binary.BigEndian, int64(i*1000000))
	}

	mustWrite(&buf, binary.BigEndian, int16(-1))
	return buf.Bytes()
}

func generateFloat8Data(count int) []byte {
	var buf bytes.Buffer
	writePGCopyHeader(&buf)

	for i := 0; i < count; i++ {
		mustWrite(&buf, binary.BigEndian, int16(1))
		mustWrite(&buf, binary.BigEndian, int32(8))
		mustWrite(&buf, binary.BigEndian, math.Float64bits(float64(i)*3.14159))
	}

	mustWrite(&buf, binary.BigEndian, int16(-1))
	return buf.Bytes()
}

func generateTextData(count int) []byte {
	var buf bytes.Buffer
	writePGCopyHeader(&buf)

	for i := 0; i < count; i++ {
		text := []byte("benchmark_text_" + string(rune('0'+i%10)))
		mustWrite(&buf, binary.BigEndian, int16(1))
		mustWrite(&buf, binary.BigEndian, int32(len(text)))
		buf.Write(text)
	}

	mustWrite(&buf, binary.BigEndian, int16(-1))
	return buf.Bytes()
}

func generateByteaData(count int) []byte {
	var buf bytes.Buffer
	writePGCopyHeader(&buf)

	for i := 0; i < count; i++ {
		// Generate binary data of varying sizes
		dataSize := 16 + (i % 64) // 16-80 bytes
		data := make([]byte, dataSize)
		for j := range data {
			data[j] = byte(i + j)
		}

		mustWrite(&buf, binary.BigEndian, int16(1))
		mustWrite(&buf, binary.BigEndian, int32(len(data)))
		buf.Write(data)
	}

	mustWrite(&buf, binary.BigEndian, int16(-1))
	return buf.Bytes()
}

func generateMixedTypeData(count int) []byte {
	var buf bytes.Buffer
	writePGCopyHeader(&buf)

	for i := 0; i < count; i++ {
		mustWrite(&buf, binary.BigEndian, int16(5)) // 5 fields

		// int4 field
		mustWrite(&buf, binary.BigEndian, int32(4))
		mustWrite(&buf, binary.BigEndian, int32(i))

		// text field
		text := []byte("row_" + string(rune('0'+i%10)))
		mustWrite(&buf, binary.BigEndian, int32(len(text)))
		buf.Write(text)

		// float8 field
		mustWrite(&buf, binary.BigEndian, int32(8))
		mustWrite(&buf, binary.BigEndian, math.Float64bits(float64(i)*1.5))

		// bytea field
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		mustWrite(&buf, binary.BigEndian, int32(len(data)))
		buf.Write(data)

		// bool field
		mustWrite(&buf, binary.BigEndian, int32(1))
		if i%2 == 0 {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}

	mustWrite(&buf, binary.BigEndian, int16(-1))
	return buf.Bytes()
}

func writePGCopyHeader(buf *bytes.Buffer) {
	// PGCOPY signature
	buf.Write([]byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', '\377', '\r', '\n', '\000'})
	// Flags (0)
	mustWrite(buf, binary.BigEndian, uint32(0))
	// Header extension length (0)
	mustWrite(buf, binary.BigEndian, uint32(0))
}
