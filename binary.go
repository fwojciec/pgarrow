package pgarrow

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// PostgreSQL type OIDs for Phase 1 support
	TypeOIDBool        = 16
	TypeOIDBytea       = 17
	TypeOIDName        = 19
	TypeOIDInt8        = 20
	TypeOIDInt2        = 21
	TypeOIDInt4        = 23
	TypeOIDText        = 25
	TypeOIDFloat4      = 700
	TypeOIDFloat8      = 701
	TypeOIDBpchar      = 1042
	TypeOIDVarchar     = 1043
	TypeOIDChar        = 18
	TypeOIDDate        = 1082
	TypeOIDTime        = 1083
	TypeOIDTimestamp   = 1114
	TypeOIDTimestamptz = 1184
	TypeOIDInterval    = 1186
)

// Parser parses PostgreSQL COPY binary format data
type Parser struct {
	reader       io.Reader
	fieldBuffers [][]byte // One buffer per field to avoid copying binary data
	fields       []Field  // Reusable slice for fields
	fieldOIDs    []uint32 // Optional: PostgreSQL type OIDs for each field
}

// Field represents a parsed field from binary data
type Field struct {
	Value any
}

// NewParser creates a new binary format parser with explicit field type information
// fieldOIDs must contain the PostgreSQL type OID for each field in order
func NewParser(reader io.Reader, fieldOIDs []uint32) *Parser {
	return &Parser{
		reader:       reader,
		fieldBuffers: make([][]byte, 0, len(fieldOIDs)), // Pre-allocate for expected field count
		fieldOIDs:    fieldOIDs,
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

	// Ensure we have enough field buffers
	if len(p.fieldBuffers) <= fieldIndex {
		// Expand fieldBuffers slice to accommodate this field
		newBuffers := make([][]byte, fieldIndex+1)
		copy(newBuffers, p.fieldBuffers)
		p.fieldBuffers = newBuffers
	}

	// Read field data using field-specific buffer (avoids copying for binary data)
	if cap(p.fieldBuffers[fieldIndex]) < int(length) {
		p.fieldBuffers[fieldIndex] = make([]byte, length)
	} else {
		p.fieldBuffers[fieldIndex] = p.fieldBuffers[fieldIndex][:length]
	}

	// Ensure we have a valid empty slice for zero-length data (not nil)
	if p.fieldBuffers[fieldIndex] == nil && length == 0 {
		p.fieldBuffers[fieldIndex] = make([]byte, 0)
	}

	if _, err := io.ReadFull(p.reader, p.fieldBuffers[fieldIndex]); err != nil {
		return Field{}, fmt.Errorf("unexpected EOF reading field data: %w", err)
	}

	// Parse field data using explicit type information
	if fieldIndex < 0 || fieldIndex >= len(p.fieldOIDs) {
		return Field{}, fmt.Errorf("field index %d out of range for %d types", fieldIndex, len(p.fieldOIDs))
	}

	fieldOID := p.fieldOIDs[fieldIndex]
	value := p.parseFieldData(p.fieldBuffers[fieldIndex], fieldOID)

	return Field{Value: value}, nil
}

// parseFieldData does minimal necessary conversions - most work is handled by TypeHandlers
func (p *Parser) parseFieldData(data []byte, oid uint32) any {
	switch oid {
	case TypeOIDBytea:
		// Return raw bytes for binary data - no conversion needed
		return data

	case TypeOIDText, TypeOIDVarchar, TypeOIDBpchar, TypeOIDName, TypeOIDChar:
		// Return raw bytes for text types - zero-copy optimization
		return data

	default:
		// For primitive types (bool, int, float, date, time, timestamp, interval), return raw bytes
		// TypeHandler will do the conversion
		return data
	}
}
