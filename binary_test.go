package pgarrow_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_ParseHeader(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		data        []byte
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid header",
			data:        generateSimpleRow(),
			expectError: false,
		},
		{
			name:        "invalid signature",
			data:        []byte("INVALID\n\377\r\n\000\x00\x00\x00\x00\x00\x00\x00\x00"),
			expectError: true,
			errorMsg:    "invalid PGCOPY signature",
		},
		{
			name:        "truncated header",
			data:        []byte("PGCOPY\n\377\r\n"),
			expectError: true,
			errorMsg:    "unexpected EOF reading header",
		},
		{
			name:        "empty data",
			data:        []byte{},
			expectError: true,
			errorMsg:    "unexpected EOF reading header",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Use empty OID slice for header tests since we only test header parsing
			parser := pgarrow.NewParser(bytes.NewReader(tt.data), []uint32{})
			err := parser.ParseHeader()

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParser_ParseTuple(t *testing.T) {
	t.Parallel()
	t.Run("simple tuple parsing", func(t *testing.T) {
		t.Parallel()
		data := generateSimpleRow()
		oids := getSimpleRowOIDs()
		parser := pgarrow.NewParser(bytes.NewReader(data), oids)

		// Parse header first
		require.NoError(t, parser.ParseHeader())

		// Parse the tuple
		fields, err := parser.ParseTuple()
		require.NoError(t, err)
		require.Len(t, fields, 6)

		// Verify field values
		assert.Equal(t, int32(42), fields[0].Value)
		assert.Equal(t, "Hello", fields[1].Value)
		assert.Equal(t, true, fields[2].Value)
		assert.Nil(t, fields[3].Value) // NULL field
		assert.Equal(t, int64(1000000), fields[4].Value)
		assert.InEpsilon(t, float64(3.14159), fields[5].Value, 0.00001)
	})

	t.Run("multi-type tuple parsing", func(t *testing.T) {
		t.Parallel()
		data := generateMultiTypeRow()
		oids := getMultiTypeRowOIDs()
		parser := pgarrow.NewParser(bytes.NewReader(data), oids)

		require.NoError(t, parser.ParseHeader())

		fields, err := parser.ParseTuple()
		require.NoError(t, err)
		require.Len(t, fields, 8)

		// Verify all data types
		assert.Equal(t, true, fields[0].Value)
		assert.Equal(t, int16(1000), fields[1].Value)
		assert.Equal(t, int32(1000000), fields[2].Value)
		assert.Equal(t, int64(1000000000000), fields[3].Value)
		assert.InEpsilon(t, float32(3.14159), fields[4].Value, 0.00001)
		assert.InEpsilon(t, float64(3.141592653589793), fields[5].Value, 0.00001)
		assert.Equal(t, "Hello, World!", fields[6].Value)
		assert.Nil(t, fields[7].Value) // NULL
	})

	t.Run("EOF detection", func(t *testing.T) {
		t.Parallel()
		data := generateEmptyDataset()
		// Empty OIDs for empty dataset
		parser := pgarrow.NewParser(bytes.NewReader(data), []uint32{})

		require.NoError(t, parser.ParseHeader())

		// Should return EOF immediately since no rows
		fields, err := parser.ParseTuple()
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, fields)
	})

	t.Run("multiple tuples", func(t *testing.T) {
		t.Parallel()
		rows := [][]interface{}{
			{int32(1), "first"},
			{int32(2), "second"},
			{int32(3), "third"},
		}
		data := buildMockBinaryData(rows)
		oids := []uint32{pgarrow.TypeOIDInt4, pgarrow.TypeOIDText}
		parser := pgarrow.NewParser(bytes.NewReader(data), oids)

		require.NoError(t, parser.ParseHeader())

		// Parse all three rows
		for i := 0; i < 3; i++ {
			fields, err := parser.ParseTuple()
			require.NoError(t, err)
			require.Len(t, fields, 2)

			assert.Equal(t, int32(i+1), fields[0].Value)
			expectedText := []string{"first", "second", "third"}[i]
			assert.Equal(t, expectedText, fields[1].Value)
		}

		// Next call should return EOF
		fields, err := parser.ParseTuple()
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, fields)
	})
}

func TestParser_ParseField(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		fieldData   []byte
		oid         uint32
		expectedVal interface{}
		expectError bool
	}{
		{
			name:        "NULL field",
			fieldData:   []byte{0xFF, 0xFF, 0xFF, 0xFF}, // -1 length
			oid:         pgarrow.TypeOIDInt4,
			expectedVal: nil,
			expectError: false,
		},
		{
			name:        "boolean true",
			fieldData:   []byte{0x00, 0x00, 0x00, 0x01, 0x01}, // length=1, value=true
			oid:         pgarrow.TypeOIDBool,
			expectedVal: true,
			expectError: false,
		},
		{
			name:        "boolean false",
			fieldData:   []byte{0x00, 0x00, 0x00, 0x01, 0x00}, // length=1, value=false
			oid:         pgarrow.TypeOIDBool,
			expectedVal: false,
			expectError: false,
		},
		{
			name:        "int32 value",
			fieldData:   []byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A}, // length=4, value=42
			oid:         pgarrow.TypeOIDInt4,
			expectedVal: int32(42),
			expectError: false,
		},
		{
			name:        "float32 value",
			fieldData:   []byte{0x00, 0x00, 0x00, 0x04, 0x40, 0x49, 0x0F, 0xD0}, // length=4, value≈3.14159
			oid:         pgarrow.TypeOIDFloat4,
			expectedVal: float32(3.14159),
			expectError: false,
		},
		{
			name:        "text value",
			fieldData:   append([]byte{0x00, 0x00, 0x00, 0x05}, []byte("Hello")...), // length=5, value="Hello"
			oid:         pgarrow.TypeOIDText,
			expectedVal: "Hello",
			expectError: false,
		},
		{
			name:        "text empty data (NULL)",
			fieldData:   []byte{0x00, 0x00, 0x00, 0x00}, // length=0, empty data
			oid:         pgarrow.TypeOIDText,
			expectedVal: nil,
			expectError: false,
		},
		{
			name:        "truncated field",
			fieldData:   []byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00}, // length=4 but only 2 data bytes
			oid:         pgarrow.TypeOIDInt4,
			expectedVal: nil,
			expectError: true,
		},
		{
			name:        "wrong length for type",
			fieldData:   []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x2A}, // length=2 for int4 type
			oid:         pgarrow.TypeOIDInt4,
			expectedVal: nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parser := pgarrow.NewParser(bytes.NewReader(tt.fieldData), []uint32{tt.oid})
			field, err := parser.ParseField(0)

			if tt.expectError {
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.expectedVal == nil {
					assert.Nil(t, field.Value)
				} else {
					switch expected := tt.expectedVal.(type) {
					case float32:
						assert.InEpsilon(t, expected, field.Value, 0.00001)
					default:
						assert.Equal(t, expected, field.Value)
					}
				}
			}
		})
	}
}

func TestParser_ErrorHandling(t *testing.T) {
	t.Parallel()
	t.Run("malformed tuple header", func(t *testing.T) {
		t.Parallel()
		// Valid header but malformed tuple (incomplete field count)
		data := []byte("PGCOPY\n\377\r\n\000\x00\x00\x00\x00\x00\x00\x00\x00\x00") // truncated
		parser := pgarrow.NewParser(bytes.NewReader(data), []uint32{})

		require.NoError(t, parser.ParseHeader())

		fields, err := parser.ParseTuple()
		require.Error(t, err)
		assert.Nil(t, fields)
	})

	t.Run("invalid field length", func(t *testing.T) {
		t.Parallel()
		// Create data with invalid field length (positive but no data)
		header := []byte("PGCOPY\n\377\r\n\000\x00\x00\x00\x00\x00\x00\x00\x00")
		tupleHeader := []byte{0x00, 0x01}             // 1 field
		fieldLength := []byte{0x00, 0x00, 0x00, 0x04} // length=4 but no data follows
		trailer := []byte{0xFF, 0xFF}

		data := append(header, tupleHeader...)
		data = append(data, fieldLength...)
		data = append(data, trailer...)

		parser := pgarrow.NewParser(bytes.NewReader(data), []uint32{pgarrow.TypeOIDInt4})
		require.NoError(t, parser.ParseHeader())

		fields, err := parser.ParseTuple()
		require.Error(t, err)
		assert.Nil(t, fields)
	})
}


// Benchmark tests for performance validation
func BenchmarkParser_ParseHeader(b *testing.B) {
	data := generateSimpleRow()
	oids := getSimpleRowOIDs()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		parser := pgarrow.NewParser(bytes.NewReader(data), oids)
		err := parser.ParseHeader()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParser_ParseTuple(b *testing.B) {
	data := generateMultiTypeRow()
	oids := getMultiTypeRowOIDs()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		parser := pgarrow.NewParser(bytes.NewReader(data), oids)
		err := parser.ParseHeader()
		if err != nil {
			b.Fatal(err)
		}

		fields, err := parser.ParseTuple()
		if err != nil {
			b.Fatal(err)
		}
		if len(fields) != 8 {
			b.Fatalf("expected 8 fields, got %d", len(fields))
		}
	}
}

func BenchmarkParser_ParseField(b *testing.B) {
	// Create field data for different types
	benchmarks := []struct {
		name string
		data []byte
		oid  uint32
	}{
		{
			name: "int32",
			data: []byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A}, // length=4, value=42
			oid:  pgarrow.TypeOIDInt4,
		},
		{
			name: "text",
			data: append([]byte{0x00, 0x00, 0x00, 0x05}, []byte("Hello")...), // length=5, value="Hello"
			oid:  pgarrow.TypeOIDText,
		},
		{
			name: "bool",
			data: []byte{0x00, 0x00, 0x00, 0x01, 0x01}, // length=1, value=true
			oid:  pgarrow.TypeOIDBool,
		},
		{
			name: "NULL",
			data: []byte{0xFF, 0xFF, 0xFF, 0xFF}, // -1 length
			oid:  pgarrow.TypeOIDInt4,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				parser := pgarrow.NewParser(bytes.NewReader(bm.data), []uint32{bm.oid})
				field, err := parser.ParseField(0)
				if err != nil {
					b.Fatal(err)
				}
				_ = field
			}
		})
	}
}

func BenchmarkParser_LargeDataset(b *testing.B) {
	data := generateLargeDataset()
	// Large dataset has 8 fields per row matching generateTestRows pattern
	oids := []uint32{
		pgarrow.TypeOIDInt4,   // int32(i + j)
		pgarrow.TypeOIDInt8,   // int64(i+j) * 1000000
		pgarrow.TypeOIDFloat8, // float64(i+j) * 3.14159
		pgarrow.TypeOIDBool,   // i%2 == 0
		pgarrow.TypeOIDText,   // "test_string_" + ... (or NULL)
		pgarrow.TypeOIDInt2,   // int16(i + j)
		pgarrow.TypeOIDFloat4, // float32(i+j) * 2.71828
		pgarrow.TypeOIDInt4,   // Cycle back to start (j % 7 wraps around)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		parser := pgarrow.NewParser(bytes.NewReader(data), oids)
		err := parser.ParseHeader()
		if err != nil {
			b.Fatal(err)
		}

		rowCount := 0
		for {
			fields, err := parser.ParseTuple()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			rowCount++
			_ = fields
		}

		if rowCount != 10000 {
			b.Fatalf("expected 10000 rows, got %d", rowCount)
		}
	}
}
