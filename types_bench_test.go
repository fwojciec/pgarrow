package pgarrow_test

import (
	"testing"

	"github.com/fwojciec/pgarrow"
)

// BenchmarkTypeRegistry_GetHandler benchmarks registry lookup performance
func BenchmarkTypeRegistry_GetHandler(b *testing.B) {
	registry := pgarrow.NewRegistry()
	oids := []uint32{16, 21, 23, 20, 700, 701, 25} // All 7 basic types

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		oid := oids[i%len(oids)]
		handler, err := registry.GetHandler(oid)
		if err != nil {
			b.Fatal(err)
		}
		_ = handler
	}
}

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
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := bm.handler.Parse(bm.data)
				if err != nil {
					b.Fatal(err)
				}
				_ = result
			}
		})
	}
}

// BenchmarkTypeRegistry_Integration benchmarks registry + type handler integration
func BenchmarkTypeRegistry_Integration(b *testing.B) {
	registry := pgarrow.NewRegistry()

	testCases := []struct {
		oid  uint32
		data []byte
	}{
		{16, []byte{0x01}},                                            // bool
		{21, []byte{0x00, 0x7B}},                                      // int2
		{23, []byte{0x00, 0x00, 0x30, 0x39}},                          // int4
		{20, []byte{0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xD2}},  // int8
		{700, []byte{0x42, 0x28, 0x00, 0x00}},                         // float4
		{701, []byte{0x40, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}}, // float8
		{25, []byte("Hello World")},                                   // text
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tc := testCases[i%len(testCases)]

		handler, err := registry.GetHandler(tc.oid)
		if err != nil {
			b.Fatal(err)
		}

		result, err := handler.Parse(tc.data)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// BenchmarkTypeRegistry_AllTypes benchmarks parsing all 7 types in sequence
func BenchmarkTypeRegistry_AllTypes(b *testing.B) {
	registry := pgarrow.NewRegistry()

	allTypes := []struct {
		oid  uint32
		data []byte
	}{
		{16, []byte{0x01}},                                            // bool
		{21, []byte{0x00, 0x7B}},                                      // int2
		{23, []byte{0x00, 0x00, 0x30, 0x39}},                          // int4
		{20, []byte{0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xD2}},  // int8
		{700, []byte{0x42, 0x28, 0x00, 0x00}},                         // float4
		{701, []byte{0x40, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}}, // float8
		{25, []byte("Hello World")},                                   // text
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Parse all 7 types in each iteration
		for _, tc := range allTypes {
			handler, err := registry.GetHandler(tc.oid)
			if err != nil {
				b.Fatal(err)
			}

			result, err := handler.Parse(tc.data)
			if err != nil {
				b.Fatal(err)
			}
			_ = result
		}
	}
}

// BenchmarkTypeRegistry_Register benchmarks registry registration performance
func BenchmarkTypeRegistry_Register(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		registry := pgarrow.NewRegistry()

		// Register a custom type handler
		customHandler := &testCustomType{}
		err := registry.Register(customHandler)
		if err != nil {
			b.Fatal(err)
		}
	}
}
