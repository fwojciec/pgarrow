package pgarrow_test

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/fwojciec/pgarrow"
)

// buildMockBinaryData generates valid PostgreSQL COPY binary format data for testing
func buildMockBinaryData(rows [][]interface{}) []byte {
	buf := &bytes.Buffer{}

	// Write PGCOPY header
	buf.WriteString("PGCOPY\n\377\r\n\000")

	// Write flags (4 bytes, network byte order)
	_ = binary.Write(buf, binary.BigEndian, uint32(0))

	// Write header extension area length (4 bytes, network byte order)
	_ = binary.Write(buf, binary.BigEndian, uint32(0))

	// Write each row
	for _, row := range rows {
		writeRowBinary(buf, row)
	}

	// Write trailer (-1 as 16-bit integer)
	_ = binary.Write(buf, binary.BigEndian, int16(-1))

	return buf.Bytes()
}

// writeRowBinary writes a single row in PostgreSQL binary format
func writeRowBinary(buf *bytes.Buffer, row []interface{}) {
	// Write field count (2 bytes, network byte order)
	_ = binary.Write(buf, binary.BigEndian, int16(len(row)))

	// Write each field
	for _, field := range row {
		writeFieldBinary(buf, field)
	}
}

// writeFieldBinary writes a single field in PostgreSQL binary format
func writeFieldBinary(buf *bytes.Buffer, field interface{}) {
	if field == nil {
		// NULL value: length = -1
		_ = binary.Write(buf, binary.BigEndian, int32(-1))
		return
	}

	switch v := field.(type) {
	case bool:
		// Boolean: 1 byte
		_ = binary.Write(buf, binary.BigEndian, int32(1))
		if v {
			buf.WriteByte(0x01)
		} else {
			buf.WriteByte(0x00)
		}

	case int16:
		// int2: 2 bytes
		_ = binary.Write(buf, binary.BigEndian, int32(2))
		_ = binary.Write(buf, binary.BigEndian, v)

	case int32:
		// int4: 4 bytes
		_ = binary.Write(buf, binary.BigEndian, int32(4))
		_ = binary.Write(buf, binary.BigEndian, v)

	case int64:
		// int8: 8 bytes
		_ = binary.Write(buf, binary.BigEndian, int32(8))
		_ = binary.Write(buf, binary.BigEndian, v)

	case float32:
		// float4: 4 bytes
		_ = binary.Write(buf, binary.BigEndian, int32(4))
		_ = binary.Write(buf, binary.BigEndian, math.Float32bits(v))

	case float64:
		// float8: 8 bytes
		_ = binary.Write(buf, binary.BigEndian, int32(8))
		_ = binary.Write(buf, binary.BigEndian, math.Float64bits(v))

	case string:
		// text: variable length UTF-8
		data := []byte(v)
		_ = binary.Write(buf, binary.BigEndian, int32(len(data)))
		buf.Write(data)

	default:
		// Unsupported type, write as NULL
		_ = binary.Write(buf, binary.BigEndian, int32(-1))
	}
}

// generateTestRows creates various test data scenarios for benchmarking and testing
func generateTestRows(rowCount int, fieldsPerRow int) [][]interface{} {
	rows := make([][]interface{}, rowCount)

	for i := 0; i < rowCount; i++ {
		row := make([]interface{}, fieldsPerRow)

		for j := 0; j < fieldsPerRow; j++ {
			// Cycle through different data types
			switch j % 7 {
			case 0:
				row[j] = int32(i + j)
			case 1:
				row[j] = int64(i+j) * 1000000
			case 2:
				row[j] = float64(i+j) * 3.14159
			case 3:
				row[j] = i%2 == 0
			case 4:
				if i%5 == 0 {
					row[j] = nil // Include some NULL values
				} else {
					row[j] = "test_string_" + string(rune('A'+i%26))
				}
			case 5:
				row[j] = int16(i + j)
			case 6:
				row[j] = float32(i+j) * 2.71828
			}
		}

		rows[i] = row
	}

	return rows
}

// generateLargeDataset creates a large dataset for performance benchmarking
func generateLargeDataset() []byte {
	// Generate realistic dataset: 10000 rows with 8 fields each
	rows := generateTestRows(10000, 8)
	return buildMockBinaryData(rows)
}

// generateSimpleRow creates a simple test row for basic functionality testing
func generateSimpleRow() []byte {
	rows := [][]interface{}{
		{int32(42), "Hello", true, nil, int64(1000000), float64(3.14159)},
	}
	return buildMockBinaryData(rows)
}

// generateMultiTypeRow creates a row with all supported data types
func generateMultiTypeRow() []byte {
	rows := [][]interface{}{
		{
			true,                       // bool
			int16(1000),                // int2
			int32(1000000),             // int4
			int64(1000000000000),       // int8
			float32(3.14159),           // float4
			float64(3.141592653589793), // float8
			"Hello, World!",            // text
			nil,                        // NULL value
		},
	}
	return buildMockBinaryData(rows)
}

// generateEmptyDataset creates minimal valid binary data with no rows
func generateEmptyDataset() []byte {
	return buildMockBinaryData([][]interface{}{})
}

// Helper functions that provide OID information for tests

// getSimpleRowOIDs returns the OIDs for generateSimpleRow data
func getSimpleRowOIDs() []uint32 {
	return []uint32{
		pgarrow.TypeOIDInt4,   // int32(42)
		pgarrow.TypeOIDText,   // "Hello"
		pgarrow.TypeOIDBool,   // true
		pgarrow.TypeOIDInt4,   // nil (NULL int4)
		pgarrow.TypeOIDInt8,   // int64(1000000)
		pgarrow.TypeOIDFloat8, // float64(3.14159)
	}
}

// getMultiTypeRowOIDs returns the OIDs for generateMultiTypeRow data
func getMultiTypeRowOIDs() []uint32 {
	return []uint32{
		pgarrow.TypeOIDBool,   // true
		pgarrow.TypeOIDInt2,   // int16(1000)
		pgarrow.TypeOIDInt4,   // int32(1000000)
		pgarrow.TypeOIDInt8,   // int64(1000000000000)
		pgarrow.TypeOIDFloat4, // float32(3.14159)
		pgarrow.TypeOIDFloat8, // float64(3.141592653589793)
		pgarrow.TypeOIDText,   // "Hello, World!"
		pgarrow.TypeOIDText,   // nil (NULL text)
	}
}
