package pgarrow_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/fwojciec/pgarrow"
)

// testCustomType is a test type handler for benchmarking
type testCustomType struct{}

func (t *testCustomType) OID() uint32                    { return 9999 }
func (t *testCustomType) Name() string                   { return "test" }
func (t *testCustomType) ArrowType() arrow.DataType      { return arrow.PrimitiveTypes.Int32 }
func (t *testCustomType) Parse(data []byte) (any, error) { return nil, nil }

// BenchmarkTypeHandlers_Parse benchmarks individual type handler parsing
func BenchmarkTypeHandlers_Parse(b *testing.B) {
	benchmarks := []struct {
		name    string
		handler pgarrow.TypeHandler
		data    []byte
	}{
		{
			name:    "BoolType",
			handler: &pgarrow.BoolType{},
			data:    []byte{0x01}, // true
		},
		{
			name:    "Int2Type",
			handler: &pgarrow.Int2Type{},
			data:    []byte{0x00, 0x7B}, // 123
		},
		{
			name:    "Int4Type",
			handler: &pgarrow.Int4Type{},
			data:    []byte{0x00, 0x00, 0x30, 0x39}, // 12345
		},
		{
			name:    "Int8Type",
			handler: &pgarrow.Int8Type{},
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xD2}, // 1234567890
		},
		{
			name:    "Float4Type",
			handler: &pgarrow.Float4Type{},
			data:    []byte{0x42, 0x28, 0x00, 0x00}, // 42.0
		},
		{
			name:    "Float8Type",
			handler: &pgarrow.Float8Type{},
			data:    []byte{0x40, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // 42.0
		},
		{
			name:    "TextType_Short",
			handler: &pgarrow.TextType{},
			data:    []byte("Hello"), // 5 bytes
		},
		{
			name:    "TextType_Long",
			handler: &pgarrow.TextType{},
			data:    []byte("This is a longer text string to test performance with larger text data that might be more representative of real-world usage patterns"), // ~130 bytes
		},
		{
			name:    "TextType_UTF8",
			handler: &pgarrow.TextType{},
			data:    []byte("Hello 世界 🌍 Здравствуй мир"), // UTF-8 with various scripts
		},
		{
			name:    "BoolType_NULL",
			handler: &pgarrow.BoolType{},
			data:    []byte{}, // NULL (empty data)
		},
		{
			name:    "TextType_NULL",
			handler: &pgarrow.TextType{},
			data:    []byte{}, // NULL (empty data)
		},
		{
			name:    "IntervalType",
			handler: &pgarrow.IntervalType{},
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x0D, 0xFB, 0x38, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x0E}, // 1 year 2 months 3 days 4 hours (14706000000 microseconds, 3 days, 14 months)
		},
		{
			name:    "IntervalType_Zero",
			handler: &pgarrow.IntervalType{},
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, // Zero interval
		},
		{
			name:    "IntervalType_NULL",
			handler: &pgarrow.IntervalType{},
			data:    []byte{}, // NULL (empty data)
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				result, err := bm.handler.Parse(bm.data)
				if err != nil {
					b.Fatal(err)
				}
				_ = result
			}
		})
	}
}

// BenchmarkTypeRegistry_Register benchmarks registry registration performance
func BenchmarkTypeRegistry_Register(b *testing.B) {

	b.ReportAllocs()

	for b.Loop() {
		registry := pgarrow.NewRegistry()

		// Register a custom type handler
		customHandler := &testCustomType{}
		err := registry.Register(customHandler)
		if err != nil {
			b.Fatal(err)
		}
	}
}
