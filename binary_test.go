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
			parser := pgarrow.NewParser(bytes.NewReader(tt.data))
			err := parser.ParseHeader()

			if tt.expectError {
				assert.Error(t, err)
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
	t.Run("simple tuple parsing", func(t *testing.T) {
		data := generateSimpleRow()
		parser := pgarrow.NewParser(bytes.NewReader(data))
		
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
		assert.Equal(t, float64(3.14159), fields[5].Value)
	})

	t.Run("multi-type tuple parsing", func(t *testing.T) {
		data := generateMultiTypeRow()
		parser := pgarrow.NewParser(bytes.NewReader(data))
		
		require.NoError(t, parser.ParseHeader())
		
		fields, err := parser.ParseTuple()
		require.NoError(t, err)
		require.Len(t, fields, 8)
		
		// Verify all data types
		assert.Equal(t, true, fields[0].Value)
		assert.Equal(t, int16(1000), fields[1].Value)
		assert.Equal(t, int32(1000000), fields[2].Value)
		assert.Equal(t, int64(1000000000000), fields[3].Value)
		assert.Equal(t, float32(3.14159), fields[4].Value)
		assert.Equal(t, float64(3.141592653589793), fields[5].Value)
		assert.Equal(t, "Hello, World!", fields[6].Value)
		assert.Nil(t, fields[7].Value) // NULL
	})

	t.Run("EOF detection", func(t *testing.T) {
		data := generateEmptyDataset()
		parser := pgarrow.NewParser(bytes.NewReader(data))
		
		require.NoError(t, parser.ParseHeader())
		
		// Should return EOF immediately since no rows
		fields, err := parser.ParseTuple()
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, fields)
	})

	t.Run("multiple tuples", func(t *testing.T) {
		rows := [][]interface{}{
			{int32(1), "first"},
			{int32(2), "second"},
			{int32(3), "third"},
		}
		data := buildMockBinaryData(rows)
		parser := pgarrow.NewParser(bytes.NewReader(data))
		
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

func TestParser_parseField(t *testing.T) {
	tests := []struct {
		name         string
		fieldData    []byte
		expectedType string
		expectedVal  interface{}
		expectError  bool
	}{
		{
			name:         "NULL field",
			fieldData:    []byte{0xFF, 0xFF, 0xFF, 0xFF}, // -1 length
			expectedType: "",
			expectedVal:  nil,
			expectError:  false,
		},
		{
			name:         "boolean true",
			fieldData:    []byte{0x00, 0x00, 0x00, 0x01, 0x01}, // length=1, value=true
			expectedType: "bool",
			expectedVal:  true,
			expectError:  false,
		},
		{
			name:         "boolean false",
			fieldData:    []byte{0x00, 0x00, 0x00, 0x01, 0x00}, // length=1, value=false
			expectedType: "bool",
			expectedVal:  false,
			expectError:  false,
		},
		{
			name:         "int32 value",
			fieldData:    []byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A}, // length=4, value=42
			expectedType: "int32",
			expectedVal:  int32(42),
			expectError:  false,
		},
		{
			name:         "text value",
			fieldData:    append([]byte{0x00, 0x00, 0x00, 0x05}, []byte("Hello")...), // length=5, value="Hello"
			expectedType: "string",
			expectedVal:  "Hello",
			expectError:  false,
		},
		{
			name:         "truncated field",
			fieldData:    []byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00}, // length=4 but only 2 data bytes
			expectedType: "",
			expectedVal:  nil,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := pgarrow.NewParser(bytes.NewReader(tt.fieldData))
			field, err := parser.ParseField()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedVal, field.Value)
			}
		})
	}
}

func TestParser_ErrorHandling(t *testing.T) {
	t.Run("malformed tuple header", func(t *testing.T) {
		// Valid header but malformed tuple (incomplete field count)
		data := []byte("PGCOPY\n\377\r\n\000\x00\x00\x00\x00\x00\x00\x00\x00\x00") // truncated
		parser := pgarrow.NewParser(bytes.NewReader(data))
		
		require.NoError(t, parser.ParseHeader())
		
		fields, err := parser.ParseTuple()
		assert.Error(t, err)
		assert.Nil(t, fields)
	})

	t.Run("invalid field length", func(t *testing.T) {
		// Create data with invalid field length (positive but no data)
		header := []byte("PGCOPY\n\377\r\n\000\x00\x00\x00\x00\x00\x00\x00\x00")
		tupleHeader := []byte{0x00, 0x01} // 1 field
		fieldLength := []byte{0x00, 0x00, 0x00, 0x04} // length=4 but no data follows
		trailer := []byte{0xFF, 0xFF}
		
		data := append(header, tupleHeader...)
		data = append(data, fieldLength...)
		data = append(data, trailer...)
		
		parser := pgarrow.NewParser(bytes.NewReader(data))
		require.NoError(t, parser.ParseHeader())
		
		fields, err := parser.ParseTuple()
		assert.Error(t, err)
		assert.Nil(t, fields)
	})
}

// Benchmark tests for performance validation
func BenchmarkParser_ParseHeader(b *testing.B) {
	data := generateSimpleRow()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		parser := pgarrow.NewParser(bytes.NewReader(data))
		err := parser.ParseHeader()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParser_ParseTuple(b *testing.B) {
	data := generateMultiTypeRow()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		parser := pgarrow.NewParser(bytes.NewReader(data))
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
	}{
		{
			name: "int32",
			data: []byte{0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A}, // length=4, value=42
		},
		{
			name: "text",
			data: append([]byte{0x00, 0x00, 0x00, 0x05}, []byte("Hello")...), // length=5, value="Hello"
		},
		{
			name: "bool",
			data: []byte{0x00, 0x00, 0x00, 0x01, 0x01}, // length=1, value=true
		},
		{
			name: "NULL",
			data: []byte{0xFF, 0xFF, 0xFF, 0xFF}, // -1 length
		},
	}
	
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				parser := pgarrow.NewParser(bytes.NewReader(bm.data))
				field, err := parser.ParseField()
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
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		parser := pgarrow.NewParser(bytes.NewReader(data))
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