package pgarrow

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// Parser parses PostgreSQL COPY binary format data
type Parser struct {
	reader io.Reader
	buf    []byte  // Reusable buffer for field data
	fields []Field // Reusable slice for fields
}

// Field represents a parsed field from binary data
type Field struct {
	Value interface{}
}

// NewParser creates a new binary format parser
func NewParser(reader io.Reader) *Parser {
	return &Parser{
		reader: reader,
		buf:    make([]byte, 0, 256), // Pre-allocate buffer with reasonable capacity
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
	
	// Parse each field
	for i := int16(0); i < fieldCount; i++ {
		field, err := p.ParseField()
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

// ParseField reads and parses a single field from the binary data
func (p *Parser) ParseField() (Field, error) {
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
	
	// Parse field data based on length and content
	value := p.parseFieldData(p.buf)
	
	return Field{Value: value}, nil
}

// parseFieldData interprets the field data based on its length and format
func (p *Parser) parseFieldData(data []byte) interface{} {
	switch len(data) {
	case 1:
		// Boolean (1 byte)
		return data[0] != 0
		
	case 2:
		// int2 (2 bytes, network byte order)
		return int16(binary.BigEndian.Uint16(data))
		
	case 4:
		// Could be int4 or float4 - we need to distinguish
		// For now, we'll try to detect if it's a valid float32
		bits := binary.BigEndian.Uint32(data)
		
		// Check if it looks like a float32 (has reasonable exponent)
		exponent := (bits >> 23) & 0xFF
		if exponent > 0 && exponent < 255 {
			// Likely a float32
			return math.Float32frombits(bits)
		}
		
		// Treat as int32
		return int32(bits)
		
	case 8:
		// Could be int8 or float8 - similar detection logic
		bits := binary.BigEndian.Uint64(data)
		
		// Check if it looks like a float64 (has reasonable exponent)
		exponent := (bits >> 52) & 0x7FF
		if exponent > 0 && exponent < 0x7FF {
			// Likely a float64
			return math.Float64frombits(bits)
		}
		
		// Treat as int64
		return int64(bits)
		
	default:
		// Variable length - treat as text (UTF-8 string)
		return string(data)
	}
}