package pgarrow

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	// defaultBufferCapacity is the initial capacity for reusable field data buffers
	defaultBufferCapacity = 256

	// PostgreSQL type OIDs for Phase 1 support
	TypeOIDBool   = 16
	TypeOIDInt8   = 20
	TypeOIDInt2   = 21
	TypeOIDInt4   = 23
	TypeOIDText   = 25
	TypeOIDFloat4 = 700
	TypeOIDFloat8 = 701
)

// Parser parses PostgreSQL COPY binary format data
type Parser struct {
	reader    io.Reader
	buf       []byte   // Reusable buffer for field data
	fields    []Field  // Reusable slice for fields
	fieldOIDs []uint32 // Optional: PostgreSQL type OIDs for each field
}

// Field represents a parsed field from binary data
type Field struct {
	Value interface{}
}

// NewParser creates a new binary format parser with explicit field type information
// fieldOIDs must contain the PostgreSQL type OID for each field in order
func NewParser(reader io.Reader, fieldOIDs []uint32) *Parser {
	return &Parser{
		reader:    reader,
		buf:       make([]byte, 0, defaultBufferCapacity),
		fieldOIDs: fieldOIDs,
	}
}

// ParseHeader validates the PGCOPY signature and reads metadata
func (p *Parser) ParseHeader() error {
	// Read signature (11 bytes): "PGCOPY\n\377\r\n\0"
	signature := make([]byte, 11)
	if _, err := io.ReadFull(p.reader, signature); err != nil {
		return fmt.Errorf("unexpected EOF reading header: %w", err)
	}

	expectedSignature := []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', '\377', '\r', '\n', '\000'}
	for i, b := range signature {
		if b != expectedSignature[i] {
			return fmt.Errorf("invalid PGCOPY signature")
		}
	}

	// Read flags (4 bytes, network byte order)
	var flags uint32
	if err := binary.Read(p.reader, binary.BigEndian, &flags); err != nil {
		return fmt.Errorf("unexpected EOF reading header: %w", err)
	}

	// Read header extension area length (4 bytes, network byte order)
	var extLength uint32
	if err := binary.Read(p.reader, binary.BigEndian, &extLength); err != nil {
		return fmt.Errorf("unexpected EOF reading header: %w", err)
	}

	// Skip extension area if present
	if extLength > 0 {
		extData := make([]byte, extLength)
		if _, err := io.ReadFull(p.reader, extData); err != nil {
			return fmt.Errorf("unexpected EOF reading header extension: %w", err)
		}
	}

	return nil
}

// ParseTuple reads a single tuple (row) from the binary data
// Returns io.EOF when the trailer (-1) is encountered
func (p *Parser) ParseTuple() ([]Field, error) {
	// Read field count (2 bytes, network byte order)
	var fieldCount int16
	if err := binary.Read(p.reader, binary.BigEndian, &fieldCount); err != nil {
		return nil, err
	}

	// Check for trailer (-1 indicates end of data)
	if fieldCount == -1 {
		return nil, io.EOF
	}

	if fieldCount < 0 {
		return nil, fmt.Errorf("invalid field count: %d", fieldCount)
	}

	// Reuse fields slice if possible
	if cap(p.fields) < int(fieldCount) {
		p.fields = make([]Field, fieldCount)
	} else {
		p.fields = p.fields[:fieldCount]
	}

	// Validate field count matches expected types
	if len(p.fieldOIDs) != int(fieldCount) {
		return nil, fmt.Errorf("field count mismatch: got %d fields, expected %d types", fieldCount, len(p.fieldOIDs))
	}

	// Parse each field
	for i := int16(0); i < fieldCount; i++ {
		field, err := p.ParseField(int(i))
		if err != nil {
			return nil, fmt.Errorf("error parsing field %d: %w", i, err)
		}
		p.fields[i] = field
	}

	// Return a copy to prevent mutations
	result := make([]Field, fieldCount)
	copy(result, p.fields)
	return result, nil
}

// ParseField reads and parses a single field from the binary data using type information
func (p *Parser) ParseField(fieldIndex int) (Field, error) {
	// Read field length (4 bytes, network byte order)
	var length int32
	if err := binary.Read(p.reader, binary.BigEndian, &length); err != nil {
		return Field{}, err
	}

	// Handle NULL value
	if length == -1 {
		return Field{Value: nil}, nil
	}

	if length < 0 {
		return Field{}, fmt.Errorf("invalid field length: %d", length)
	}

	// Read field data using reusable buffer
	if cap(p.buf) < int(length) {
		p.buf = make([]byte, length)
	} else {
		p.buf = p.buf[:length]
	}

	if _, err := io.ReadFull(p.reader, p.buf); err != nil {
		return Field{}, fmt.Errorf("unexpected EOF reading field data: %w", err)
	}

	// Parse field data using explicit type information
	if fieldIndex < 0 || fieldIndex >= len(p.fieldOIDs) {
		return Field{}, fmt.Errorf("field index %d out of range for %d types", fieldIndex, len(p.fieldOIDs))
	}

	fieldOID := p.fieldOIDs[fieldIndex]
	value, err := p.parseFieldData(p.buf, fieldOID)
	if err != nil {
		return Field{}, fmt.Errorf("failed to parse field data for OID %d: %w", fieldOID, err)
	}

	return Field{Value: value}, nil
}

// parseFieldData interprets field data using explicit PostgreSQL type information
func (p *Parser) parseFieldData(data []byte, oid uint32) (interface{}, error) {
	switch oid {
	case TypeOIDBool:
		if len(data) != 1 {
			return nil, fmt.Errorf("invalid data length for bool: expected 1, got %d", len(data))
		}
		return data[0] != 0, nil

	case TypeOIDInt2:
		if len(data) != 2 {
			return nil, fmt.Errorf("invalid data length for int2: expected 2, got %d", len(data))
		}
		return int16(binary.BigEndian.Uint16(data)), nil

	case TypeOIDInt4:
		if len(data) != 4 {
			return nil, fmt.Errorf("invalid data length for int4: expected 4, got %d", len(data))
		}
		return int32(binary.BigEndian.Uint32(data)), nil

	case TypeOIDInt8:
		if len(data) != 8 {
			return nil, fmt.Errorf("invalid data length for int8: expected 8, got %d", len(data))
		}
		return int64(binary.BigEndian.Uint64(data)), nil

	case TypeOIDFloat4:
		if len(data) != 4 {
			return nil, fmt.Errorf("invalid data length for float4: expected 4, got %d", len(data))
		}
		return math.Float32frombits(binary.BigEndian.Uint32(data)), nil

	case TypeOIDFloat8:
		if len(data) != 8 {
			return nil, fmt.Errorf("invalid data length for float8: expected 8, got %d", len(data))
		}
		return math.Float64frombits(binary.BigEndian.Uint64(data)), nil

	case TypeOIDText:
		// Empty data represents an empty string, not NULL
		// NULL values are handled at the field level (length == -1)
		return string(data), nil

	default:
		return nil, fmt.Errorf("unsupported PostgreSQL type OID: %d", oid)
	}
}
