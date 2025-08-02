package pgarrow

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
)

// TypeHandler defines the interface for PostgreSQL type handlers
type TypeHandler interface {
	// OID returns the PostgreSQL type OID
	OID() uint32
	// Name returns the human-readable type name
	Name() string
	// ArrowType returns the corresponding Arrow data type
	ArrowType() arrow.DataType
	// Parse converts binary PostgreSQL data to Go value
	// Returns nil for NULL values (empty data)
	Parse(data []byte) (any, error)
}

// TypeRegistry manages type handlers by OID
type TypeRegistry struct {
	handlers map[uint32]TypeHandler
}

// NewRegistry creates a new type registry with all basic types registered
func NewRegistry() *TypeRegistry {
	registry := &TypeRegistry{
		handlers: make(map[uint32]TypeHandler),
	}

	// Register all basic types
	registry.register(&BoolType{})
	registry.register(&ByteaType{})
	registry.register(&Int2Type{})
	registry.register(&Int4Type{})
	registry.register(&Int8Type{})
	registry.register(&Float4Type{})
	registry.register(&Float8Type{})
	registry.register(&TextType{})
	registry.register(&VarcharType{})
	registry.register(&BpcharType{})
	registry.register(&NameType{})
	registry.register(&CharType{})

	return registry
}

// GetHandler returns the type handler for the given OID
func (r *TypeRegistry) GetHandler(oid uint32) (TypeHandler, error) {
	handler, exists := r.handlers[oid]
	if !exists {
		return nil, fmt.Errorf("unsupported type OID: %d", oid)
	}
	return handler, nil
}

// Register adds a new type handler to the registry
func (r *TypeRegistry) Register(handler TypeHandler) error {
	oid := handler.OID()
	if _, exists := r.handlers[oid]; exists {
		return fmt.Errorf("type OID already registered: %d", oid)
	}
	r.handlers[oid] = handler
	return nil
}

// register is internal method for initial setup
func (r *TypeRegistry) register(handler TypeHandler) {
	r.handlers[handler.OID()] = handler
}

// BoolType handles PostgreSQL bool type (OID 16)
type BoolType struct{}

func (t *BoolType) OID() uint32 {
	return TypeOIDBool
}

func (t *BoolType) Name() string {
	return "bool"
}

func (t *BoolType) ArrowType() arrow.DataType {
	return arrow.FixedWidthTypes.Boolean
}

func (t *BoolType) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 1 {
		return nil, fmt.Errorf("invalid data length for bool: expected 1, got %d", len(data))
	}

	return data[0] != 0, nil
}

// Int2Type handles PostgreSQL int2 type (OID 21)
type Int2Type struct{}

func (t *Int2Type) OID() uint32 {
	return TypeOIDInt2
}

func (t *Int2Type) Name() string {
	return "int2"
}

func (t *Int2Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int16
}

func (t *Int2Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 2 {
		return nil, fmt.Errorf("invalid data length for int2: expected 2, got %d", len(data))
	}

	return int16(binary.BigEndian.Uint16(data)), nil
}

// Int4Type handles PostgreSQL int4 type (OID 23)
type Int4Type struct{}

func (t *Int4Type) OID() uint32 {
	return TypeOIDInt4
}

func (t *Int4Type) Name() string {
	return "int4"
}

func (t *Int4Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int32
}

func (t *Int4Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 4 {
		return nil, fmt.Errorf("invalid data length for int4: expected 4, got %d", len(data))
	}

	return int32(binary.BigEndian.Uint32(data)), nil
}

// Int8Type handles PostgreSQL int8 type (OID 20)
type Int8Type struct{}

func (t *Int8Type) OID() uint32 {
	return TypeOIDInt8
}

func (t *Int8Type) Name() string {
	return "int8"
}

func (t *Int8Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Int64
}

func (t *Int8Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 8 {
		return nil, fmt.Errorf("invalid data length for int8: expected 8, got %d", len(data))
	}

	return int64(binary.BigEndian.Uint64(data)), nil
}

// Float4Type handles PostgreSQL float4 type (OID 700)
type Float4Type struct{}

func (t *Float4Type) OID() uint32 {
	return TypeOIDFloat4
}

func (t *Float4Type) Name() string {
	return "float4"
}

func (t *Float4Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Float32
}

func (t *Float4Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 4 {
		return nil, fmt.Errorf("invalid data length for float4: expected 4, got %d", len(data))
	}

	return math.Float32frombits(binary.BigEndian.Uint32(data)), nil
}

// Float8Type handles PostgreSQL float8 type (OID 701)
type Float8Type struct{}

func (t *Float8Type) OID() uint32 {
	return TypeOIDFloat8
}

func (t *Float8Type) Name() string {
	return "float8"
}

func (t *Float8Type) ArrowType() arrow.DataType {
	return arrow.PrimitiveTypes.Float64
}

func (t *Float8Type) Parse(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil // NULL value
	}

	if len(data) != 8 {
		return nil, fmt.Errorf("invalid data length for float8: expected 8, got %d", len(data))
	}

	return math.Float64frombits(binary.BigEndian.Uint64(data)), nil
}

// TextType handles PostgreSQL text type (OID 25)
type TextType struct{}

func (t *TextType) OID() uint32 {
	return TypeOIDText
}

func (t *TextType) Name() string {
	return "text"
}

func (t *TextType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *TextType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// VarcharType handles PostgreSQL varchar type (OID 1043)
type VarcharType struct{}

func (t *VarcharType) OID() uint32 {
	return TypeOIDVarchar
}

func (t *VarcharType) Name() string {
	return "varchar"
}

func (t *VarcharType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *VarcharType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// BpcharType handles PostgreSQL bpchar type (OID 1042)
type BpcharType struct{}

func (t *BpcharType) OID() uint32 {
	return TypeOIDBpchar
}

func (t *BpcharType) Name() string {
	return "bpchar"
}

func (t *BpcharType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *BpcharType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// NameType handles PostgreSQL name type (OID 19)
type NameType struct{}

func (t *NameType) OID() uint32 {
	return TypeOIDName
}

func (t *NameType) Name() string {
	return "name"
}

func (t *NameType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *NameType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// CharType handles PostgreSQL char type (OID 18)
type CharType struct{}

func (t *CharType) OID() uint32 {
	return TypeOIDChar
}

func (t *CharType) Name() string {
	return "char"
}

func (t *CharType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.String
}

func (t *CharType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	return string(data), nil // Empty string (len(data) == 0) is valid
}

// ByteaType handles PostgreSQL bytea type (OID 17)
type ByteaType struct{}

func (t *ByteaType) OID() uint32 {
	return TypeOIDBytea
}

func (t *ByteaType) Name() string {
	return "bytea"
}

func (t *ByteaType) ArrowType() arrow.DataType {
	return arrow.BinaryTypes.Binary
}

func (t *ByteaType) Parse(data []byte) (any, error) {
	if data == nil {
		return nil, nil // NULL value
	}
	// Empty bytea (len(data) == 0) is valid - return the empty slice
	// Data is already safely copied in parseFieldData(), no additional copying needed
	return data, nil
}
