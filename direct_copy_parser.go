package pgarrow

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// DirectCopyParser parses PostgreSQL COPY BINARY format directly to Arrow builders
// without goroutine overhead by directly processing COPY protocol messages.
// This is the high-performance implementation that eliminates io.Pipe overhead.
type DirectCopyParser struct {
	conn      *pgconn.PgConn
	alloc     memory.Allocator
	schema    *arrow.Schema
	builders  []array.Builder
	scanPlans []ScanPlan

	// Buffer for accumulating partial data across messages
	buffer *bytes.Buffer

	// Reusable buffers to minimize allocations
	fieldBuf []byte

	// State tracking
	rowsProcessed int64
	headerParsed  bool
	started       bool
	copyDone      bool
	consumedAll   bool // tracks if we've consumed to ReadyForQuery

	// Current batch tracking
	batchRows      int64
	batchSizeBytes int64
	lastRowSize    int64 // Actual size of last processed row
}

// NewDirectCopyParser creates a parser that reads COPY data directly from the connection.
// This eliminates the goroutine synchronization overhead of io.Pipe.
func NewDirectCopyParser(conn *pgconn.PgConn, schema *arrow.Schema, alloc memory.Allocator) (*DirectCopyParser, error) {
	p := &DirectCopyParser{
		conn:      conn,
		alloc:     alloc,
		schema:    schema,
		builders:  make([]array.Builder, len(schema.Fields())),
		scanPlans: make([]ScanPlan, len(schema.Fields())),
		buffer:    bytes.NewBuffer(make([]byte, 0, 65536)), // 64KB initial buffer
		fieldBuf:  make([]byte, 0, 4096),                   // 4KB field buffer
	}

	// Create builders and scan plans for each field
	for i, field := range schema.Fields() {
		builder := array.NewBuilder(alloc, field.Type)
		p.builders[i] = builder

		// Create scan plan for this field type
		scanPlan, err := createScanPlan(field.Type)
		if err != nil {
			// Clean up builders created so far
			for j := 0; j <= i; j++ {
				if p.builders[j] != nil {
					p.builders[j].Release()
				}
			}
			return nil, fmt.Errorf("unsupported type %s for field %s: %w", field.Type, field.Name, err)
		}
		p.scanPlans[i] = scanPlan
	}

	return p, nil
}

// ParseAll reads all COPY data directly from the connection without goroutines.
// This is the key innovation that eliminates thread synchronization overhead.
func (p *DirectCopyParser) ParseAll(ctx context.Context, copyQuery string) error {
	// Send COPY command
	p.conn.Frontend().SendQuery(&pgproto3.Query{String: copyQuery})
	if err := p.conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("sending COPY command: %w", err)
	}

	// Process COPY response messages
	for {
		msg, err := p.conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receiving message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyOutResponse:
			// COPY started - verify binary format
			if msg.OverallFormat != 1 { // 1 = binary
				return errors.New("expected binary COPY format")
			}

		case *pgproto3.CopyData:
			// Process the data directly - this is where the performance gain happens
			if err := p.processCopyData(msg.Data); err != nil {
				return fmt.Errorf("processing COPY data: %w", err)
			}

		case *pgproto3.CopyDone:
			// COPY completed successfully - but keep reading until ReadyForQuery
			continue

		case *pgproto3.CommandComplete:
			// Command completed - but keep reading until ReadyForQuery
			continue

		case *pgproto3.ReadyForQuery:
			// Transaction status - now we're really done
			p.consumedAll = true
			return nil

		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(msg)

		default:
			// Ignore other message types (e.g., NoticeResponse)
		}
	}
}

// processCopyData handles a chunk of COPY binary data
func (p *DirectCopyParser) processCopyData(data []byte) error {
	// Add new data to buffer
	p.buffer.Write(data)

	// Parse header if not done
	if !p.headerParsed {
		if err := p.parseHeader(); err != nil {
			return err
		}
		if !p.headerParsed {
			return nil // Need more data for header
		}
	}

	// Process rows from buffer
	for {
		processed, err := p.processNextRow()
		if err != nil {
			return err
		}
		if !processed {
			break // No more complete rows in buffer
		}
	}

	return nil
}

// parseHeader parses the COPY binary format header
func (p *DirectCopyParser) parseHeader() error {
	if p.buffer.Len() < 19 {
		return nil // Need more data for header
	}

	headerBytes := make([]byte, 19)
	if _, err := p.buffer.Read(headerBytes); err != nil {
		return err
	}

	// Verify magic bytes "PGCOPY\n\377\r\n\000"
	expected := []byte("PGCOPY\n\377\r\n\000")
	if !bytes.Equal(headerBytes[:11], expected) {
		return errors.New("invalid COPY binary format header")
	}

	// Skip flags (4 bytes) and header extension (4 bytes)
	// These are already consumed in headerBytes

	p.headerParsed = true
	return nil
}

// processNextRow attempts to process the next row from the buffer
func (p *DirectCopyParser) processNextRow() (bool, error) {
	// Check if we have enough data for field count
	if p.buffer.Len() < 2 {
		return false, nil
	}

	// Peek at field count without consuming
	peekBuf := p.buffer.Bytes()
	fieldCount := int16(binary.BigEndian.Uint16(peekBuf[:2]))

	// Check for end-of-data marker (-1)
	if fieldCount == -1 {
		p.buffer.Next(2) // Consume the marker
		return false, nil
	}

	// Validate field count
	if int(fieldCount) != len(p.builders) {
		return false, fmt.Errorf("field count mismatch: got %d, expected %d", fieldCount, len(p.builders))
	}

	// Check if we have complete row data and calculate its size
	rowSize, hasComplete := p.getCompleteRowSize(peekBuf, fieldCount)
	if !hasComplete {
		return false, nil // Wait for more data
	}

	// Track the size before processing
	p.lastRowSize = rowSize

	// We have a complete row, process it
	p.buffer.Next(2) // Consume field count

	// Parse each field
	for i := 0; i < int(fieldCount); i++ {
		if err := p.parseField(i); err != nil {
			return false, fmt.Errorf("parsing field %d: %w", i, err)
		}
	}

	p.rowsProcessed++
	return true, nil
}

// getCompleteRowSize checks if buffer contains a complete row and returns its total size
func (p *DirectCopyParser) getCompleteRowSize(data []byte, fieldCount int16) (int64, bool) {
	offset := 2           // Skip field count bytes (which we already counted)
	totalSize := int64(2) // Start with field count bytes

	for i := 0; i < int(fieldCount); i++ {
		if len(data) < offset+4 {
			return 0, false // Not enough data for field length
		}

		fieldLen := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		totalSize += 4 // Field length bytes

		if fieldLen > 0 {
			if len(data) < offset+int(fieldLen) {
				return 0, false // Not enough data for field value
			}
			offset += int(fieldLen)
			totalSize += int64(fieldLen) // Field data bytes
		}
		// fieldLen == -1 means NULL, no additional data bytes
	}

	return totalSize, true
}

// parseField parses a single field from the buffer
func (p *DirectCopyParser) parseField(fieldIndex int) error {
	// Read field length
	lengthBytes := make([]byte, 4)
	if _, err := p.buffer.Read(lengthBytes); err != nil {
		return err
	}

	fieldLen := int32(binary.BigEndian.Uint32(lengthBytes))

	// Handle NULL
	if fieldLen == -1 {
		p.builders[fieldIndex].AppendNull()
		return nil
	}

	// Read field data
	if cap(p.fieldBuf) < int(fieldLen) {
		p.fieldBuf = make([]byte, fieldLen)
	} else {
		p.fieldBuf = p.fieldBuf[:fieldLen]
	}

	if _, err := p.buffer.Read(p.fieldBuf); err != nil {
		return err
	}

	// Parse directly to builder using scan plan
	return p.scanPlans[fieldIndex].ScanToBuilder(p.fieldBuf, p.builders[fieldIndex])
}

// Finish builds the final Arrow record from accumulated data
func (p *DirectCopyParser) Finish() (arrow.Record, error) {
	arrays := make([]arrow.Array, len(p.builders))
	for i, builder := range p.builders {
		arrays[i] = builder.NewArray()
	}

	// Create the record - it now owns the arrays
	record := array.NewRecord(p.schema, arrays, p.rowsProcessed)

	// Release our references to the arrays since the record owns them now
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

// Release cleans up resources
func (p *DirectCopyParser) Release() {
	for _, builder := range p.builders {
		if builder != nil {
			builder.Release()
		}
	}
}

// StartParsing initiates the COPY command and prepares for streaming
func (p *DirectCopyParser) StartParsing(ctx context.Context, copyQuery string) error {
	if p.started {
		return errors.New("parsing already started")
	}

	// Send COPY command
	p.conn.Frontend().SendQuery(&pgproto3.Query{String: copyQuery})
	if err := p.conn.Frontend().Flush(); err != nil {
		return fmt.Errorf("sending COPY command: %w", err)
	}

	// Process initial response to get CopyOutResponse
	for {
		msg, err := p.conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receiving initial message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyOutResponse:
			// COPY started - verify binary format
			if msg.OverallFormat != 1 { // 1 = binary
				return errors.New("expected binary COPY format")
			}
			p.started = true
			return nil

		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(msg)

		default:
			// Keep reading until we get CopyOutResponse
		}
	}
}

// ParseNextBatch parses data until reaching the size limit or end of data
func (p *DirectCopyParser) ParseNextBatch(ctx context.Context, maxBatchBytes int) (arrow.Record, bool, error) {
	if !p.started {
		return nil, false, errors.New("parsing not started - call StartParsing first")
	}

	if p.copyDone {
		return nil, true, nil
	}

	// Reset batch tracking
	p.batchRows = 0
	p.batchSizeBytes = 0
	p.resetBuilders()

	// Process messages until we hit size limit or end of data
	for p.batchSizeBytes < int64(maxBatchBytes) {
		// Process buffered data first
		if processed := p.processBufferedData(); processed {
			continue
		}

		// Receive and handle next message
		done, err := p.receiveAndProcessMessage(ctx)
		if err != nil {
			return nil, false, err
		}
		if done {
			break
		}
	}

	// Return batch if we have data
	if p.batchRows > 0 {
		record := p.buildBatch()
		return record, p.copyDone && p.consumedAll, nil
	}

	return nil, p.copyDone && p.consumedAll, nil
}

// resetBuilders clears all builders for a new batch
func (p *DirectCopyParser) resetBuilders() {
	// Creating and releasing arrays clears the builder data
	// We don't re-reserve because that causes extra allocations
	// The builders will grow as needed for each batch
	for _, builder := range p.builders {
		arr := builder.NewArray()
		arr.Release()
	}
}

// processBufferedData tries to process data from the buffer
func (p *DirectCopyParser) processBufferedData() bool {
	if p.buffer.Len() == 0 {
		return false
	}

	if !p.headerParsed {
		if err := p.parseHeader(); err != nil {
			return false
		}
		return p.headerParsed
	}

	// Try to process a row from buffer
	processed, err := p.processNextRow()
	if err != nil || !processed {
		return false
	}

	p.batchRows++
	p.batchSizeBytes += p.lastRowSize
	return true
}

// receiveAndProcessMessage receives and processes a protocol message
func (p *DirectCopyParser) receiveAndProcessMessage(ctx context.Context) (bool, error) {
	msg, err := p.conn.ReceiveMessage(ctx)
	if err != nil {
		return false, fmt.Errorf("receiving message: %w", err)
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		p.buffer.Write(msg.Data)
		return false, nil

	case *pgproto3.CopyDone:
		p.copyDone = true
		// Process remaining buffer
		for p.buffer.Len() > 0 && p.headerParsed {
			if !p.processBufferedData() {
				break
			}
		}
		return false, nil

	case *pgproto3.CommandComplete:
		return false, nil

	case *pgproto3.ReadyForQuery:
		p.consumedAll = true
		return true, nil

	case *pgproto3.ErrorResponse:
		return false, pgconn.ErrorResponseToPgError(msg)

	default:
		return false, nil
	}
}

// buildBatch creates an Arrow record from the current batch
func (p *DirectCopyParser) buildBatch() arrow.Record {
	arrays := make([]arrow.Array, len(p.builders))
	for i, builder := range p.builders {
		arrays[i] = builder.NewArray()
	}

	// Create the record - it now owns the arrays
	record := array.NewRecord(p.schema, arrays, p.batchRows)

	// Release our references to the arrays since the record owns them now
	for _, arr := range arrays {
		arr.Release()
	}

	p.rowsProcessed += p.batchRows
	return record
}

// ConsumeTillReady consumes and discards remaining COPY messages until ReadyForQuery.
// This ensures the connection is in a clean state for reuse.
// This method is only called on early release paths, not in normal operation,
// so it doesn't impact the performance of the happy path.
func (p *DirectCopyParser) ConsumeTillReady(ctx context.Context) error {
	// If we've already consumed everything, nothing to do
	if p.consumedAll {
		return nil
	}

	// If parsing never started, nothing to consume
	if !p.started {
		return nil
	}

	// Consume and discard remaining messages
	for !p.consumedAll {
		msg, err := p.conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("consuming remaining messages: %w", err)
		}

		switch msg.(type) {
		case *pgproto3.CopyData:
			// Discard data
			continue

		case *pgproto3.CopyDone:
			// COPY completed
			continue

		case *pgproto3.CommandComplete:
			// Command completed
			continue

		case *pgproto3.ReadyForQuery:
			// Clean state reached
			p.consumedAll = true
			return nil

		case *pgproto3.ErrorResponse:
			// Even on error, keep consuming until ReadyForQuery
			continue

		default:
			// Ignore other message types
			continue
		}
	}

	return nil
}
