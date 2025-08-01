package pgarrow_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCustomType is a test type handler with unique OID
type testCustomType struct{}

func (t *testCustomType) OID() uint32                            { return 9999 }
func (t *testCustomType) Name() string                           { return "test" }
func (t *testCustomType) ArrowType() arrow.DataType              { return arrow.PrimitiveTypes.Int32 }
func (t *testCustomType) Parse(data []byte) (interface{}, error) { return nil, nil }

func TestTypeHandler_BoolType(t *testing.T) {
	t.Parallel()
	handler := &pgarrow.BoolType{}

	t.Run("OID", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, uint32(16), handler.OID())
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "bool", handler.Name())
	})

	t.Run("ArrowType", func(t *testing.T) {
		t.Parallel()
		expected := arrow.FixedWidthTypes.Boolean
		assert.Equal(t, expected, handler.ArrowType())
	})

	t.Run("Parse valid true", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x01}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("Parse valid false", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x00}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	t.Run("Parse invalid length", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x01, 0x02}
		_, err := handler.Parse(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid data length for bool")
	})

	t.Run("Parse NULL (empty data)", func(t *testing.T) {
		t.Parallel()
		data := []byte{}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestTypeHandler_Int2Type(t *testing.T) {
	t.Parallel()
	handler := &pgarrow.Int2Type{}

	t.Run("OID", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, uint32(21), handler.OID())
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "int2", handler.Name())
	})

	t.Run("ArrowType", func(t *testing.T) {
		t.Parallel()
		expected := arrow.PrimitiveTypes.Int16
		assert.Equal(t, expected, handler.ArrowType())
	})

	t.Run("Parse valid positive", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x00, 0x7B} // 123 in network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, int16(123), result)
	})

	t.Run("Parse valid negative", func(t *testing.T) {
		t.Parallel()
		data := []byte{0xFF, 0x85} // -123 in network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, int16(-123), result)
	})

	t.Run("Parse invalid length", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x00}
		_, err := handler.Parse(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid data length for int2")
	})

	t.Run("Parse NULL (empty data)", func(t *testing.T) {
		t.Parallel()
		data := []byte{}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestTypeHandler_Int4Type(t *testing.T) {
	t.Parallel()
	handler := &pgarrow.Int4Type{}

	t.Run("OID", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, uint32(23), handler.OID())
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "int4", handler.Name())
	})

	t.Run("ArrowType", func(t *testing.T) {
		t.Parallel()
		expected := arrow.PrimitiveTypes.Int32
		assert.Equal(t, expected, handler.ArrowType())
	})

	t.Run("Parse valid positive", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x00, 0x00, 0x30, 0x39} // 12345 in network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, int32(12345), result)
	})

	t.Run("Parse valid negative", func(t *testing.T) {
		t.Parallel()
		data := []byte{0xFF, 0xFF, 0xCF, 0xC7} // -12345 in network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, int32(-12345), result)
	})

	t.Run("Parse invalid length", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x00, 0x00}
		_, err := handler.Parse(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid data length for int4")
	})

	t.Run("Parse NULL (empty data)", func(t *testing.T) {
		t.Parallel()
		data := []byte{}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestTypeHandler_Int8Type(t *testing.T) {
	t.Parallel()
	handler := &pgarrow.Int8Type{}

	t.Run("OID", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, uint32(20), handler.OID())
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "int8", handler.Name())
	})

	t.Run("ArrowType", func(t *testing.T) {
		t.Parallel()
		expected := arrow.PrimitiveTypes.Int64
		assert.Equal(t, expected, handler.ArrowType())
	})

	t.Run("Parse valid positive", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xD2} // 1234567890 in network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, int64(1234567890), result)
	})

	t.Run("Parse valid negative", func(t *testing.T) {
		t.Parallel()
		data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xB6, 0x69, 0xFD, 0x2E} // -1234567890 in network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, int64(-1234567890), result)
	})

	t.Run("Parse invalid length", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x00, 0x00, 0x00, 0x00}
		_, err := handler.Parse(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid data length for int8")
	})

	t.Run("Parse NULL (empty data)", func(t *testing.T) {
		t.Parallel()
		data := []byte{}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestTypeHandler_Float4Type(t *testing.T) {
	t.Parallel()
	handler := &pgarrow.Float4Type{}

	t.Run("OID", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, uint32(700), handler.OID())
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "float4", handler.Name())
	})

	t.Run("ArrowType", func(t *testing.T) {
		t.Parallel()
		expected := arrow.PrimitiveTypes.Float32
		assert.Equal(t, expected, handler.ArrowType())
	})

	t.Run("Parse valid positive", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x42, 0x28, 0x00, 0x00} // 42.0 in IEEE 754 network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.InDelta(t, float32(42.0), result, 0.001)
	})

	t.Run("Parse valid negative", func(t *testing.T) {
		t.Parallel()
		data := []byte{0xC2, 0x28, 0x00, 0x00} // -42.0 in IEEE 754 network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.InDelta(t, float32(-42.0), result, 0.001)
	})

	t.Run("Parse invalid length", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x42, 0x28}
		_, err := handler.Parse(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid data length for float4")
	})

	t.Run("Parse NULL (empty data)", func(t *testing.T) {
		t.Parallel()
		data := []byte{}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestTypeHandler_Float8Type(t *testing.T) {
	t.Parallel()
	handler := &pgarrow.Float8Type{}

	t.Run("OID", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, uint32(701), handler.OID())
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "float8", handler.Name())
	})

	t.Run("ArrowType", func(t *testing.T) {
		t.Parallel()
		expected := arrow.PrimitiveTypes.Float64
		assert.Equal(t, expected, handler.ArrowType())
	})

	t.Run("Parse valid positive", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x40, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00} // 42.0 in IEEE 754 network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.InDelta(t, float64(42.0), result, 0.001)
	})

	t.Run("Parse valid negative", func(t *testing.T) {
		t.Parallel()
		data := []byte{0xC0, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00} // -42.0 in IEEE 754 network byte order
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.InDelta(t, float64(-42.0), result, 0.001)
	})

	t.Run("Parse invalid length", func(t *testing.T) {
		t.Parallel()
		data := []byte{0x40, 0x45, 0x00, 0x00}
		_, err := handler.Parse(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid data length for float8")
	})

	t.Run("Parse NULL (empty data)", func(t *testing.T) {
		t.Parallel()
		data := []byte{}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestTypeHandler_TextType(t *testing.T) {
	t.Parallel()
	handler := &pgarrow.TextType{}

	t.Run("OID", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, uint32(25), handler.OID())
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "text", handler.Name())
	})

	t.Run("ArrowType", func(t *testing.T) {
		t.Parallel()
		expected := arrow.BinaryTypes.String
		assert.Equal(t, expected, handler.ArrowType())
	})

	t.Run("Parse valid ASCII", func(t *testing.T) {
		t.Parallel()
		data := []byte("hello world")
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "hello world", result)
	})

	t.Run("Parse valid UTF-8", func(t *testing.T) {
		t.Parallel()
		data := []byte("héllo 世界")
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "héllo 世界", result)
	})

	t.Run("Parse empty string", func(t *testing.T) {
		t.Parallel()
		data := []byte("")
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("Parse NULL (empty data)", func(t *testing.T) {
		t.Parallel()
		data := []byte{}
		result, err := handler.Parse(data)
		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestTypeRegistry(t *testing.T) {
	t.Parallel()
	t.Run("NewRegistry", func(t *testing.T) {
		t.Parallel()
		registry := pgarrow.NewRegistry()
		assert.NotNil(t, registry)
	})

	t.Run("GetHandler by OID", func(t *testing.T) {
		t.Parallel()
		registry := pgarrow.NewRegistry()

		// Test all 7 basic types
		testCases := []struct {
			oid      uint32
			expected string
		}{
			{16, "bool"},
			{21, "int2"},
			{23, "int4"},
			{20, "int8"},
			{700, "float4"},
			{701, "float8"},
			{25, "text"},
		}

		for _, tc := range testCases {
			handler, err := registry.GetHandler(tc.oid)
			require.NoError(t, err, "Failed to get handler for OID %d", tc.oid)
			assert.Equal(t, tc.expected, handler.Name())
			assert.Equal(t, tc.oid, handler.OID())
		}
	})

	t.Run("GetHandler unknown OID", func(t *testing.T) {
		t.Parallel()
		registry := pgarrow.NewRegistry()
		_, err := registry.GetHandler(9999)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported type OID")
	})

	t.Run("Register new handler", func(t *testing.T) {
		t.Parallel()
		registry := pgarrow.NewRegistry()

		// Create a custom handler with unique OID
		customHandler := &testCustomType{}
		err := registry.Register(customHandler)
		assert.NoError(t, err)
	})

	t.Run("Register duplicate OID", func(t *testing.T) {
		t.Parallel()
		registry := pgarrow.NewRegistry()

		// Try to register a handler with OID that already exists
		duplicateHandler := &pgarrow.BoolType{}
		err := registry.Register(duplicateHandler)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "type OID already registered")
	})
}

func TestTypeRegistry_Integration(t *testing.T) {
	t.Parallel()
	registry := pgarrow.NewRegistry()

	t.Run("Parse through registry", func(t *testing.T) {
		t.Parallel()
		// Test parsing through the registry for each type
		testCases := []struct {
			oid      uint32
			data     []byte
			expected interface{}
		}{
			{16, []byte{0x01}, true},
			{21, []byte{0x00, 0x7B}, int16(123)},
			{23, []byte{0x00, 0x00, 0x30, 0x39}, int32(12345)},
			{20, []byte{0x00, 0x00, 0x00, 0x00, 0x49, 0x96, 0x02, 0xD2}, int64(1234567890)},
			{700, []byte{0x42, 0x28, 0x00, 0x00}, float32(42.0)},
			{701, []byte{0x40, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, float64(42.0)},
			{25, []byte("hello"), "hello"},
		}

		for _, tc := range testCases {
			handler, err := registry.GetHandler(tc.oid)
			require.NoError(t, err)

			result, err := handler.Parse(tc.data)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		}
	})
}
