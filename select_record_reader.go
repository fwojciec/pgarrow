package pgarrow

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SelectRecordReader streams PostgreSQL SELECT query results as Arrow record batches.
// It uses the SELECT protocol with binary format for 2x performance over COPY BINARY.
// This implementation processes data in optimized batches to support larger-than-memory datasets.
type SelectRecordReader struct {
	ctx    context.Context
	conn   *pgxpool.Conn
	parser *SelectParser
	query  string

	// Batch management
	currentBatch arrow.Record
	err          error
	done         bool

	// Reference counting
	refCount int64
}

// NewSelectRecordReader creates a streaming record reader using SELECT protocol
func NewSelectRecordReader(ctx context.Context, conn *pgxpool.Conn, schema *arrow.Schema, query string, alloc memory.Allocator) (*SelectRecordReader, error) {
	// Ensure connection uses binary protocol for optimal performance
	pgxConn := conn.Conn()

	// Create parser with SELECT protocol optimizations
	parser, err := NewSelectParser(pgxConn, schema, alloc)
	if err != nil {
		return nil, err
	}

	// Start query execution eagerly to reduce latency on first Next()
	if err := parser.StartParsing(ctx, query); err != nil {
		parser.Release()
		return nil, fmt.Errorf("starting SELECT query: %w", err)
	}

	return &SelectRecordReader{
		ctx:      ctx,
		conn:     conn,
		parser:   parser,
		query:    query,
		refCount: 1,
	}, nil
}

// Schema returns the Arrow schema
func (r *SelectRecordReader) Schema() *arrow.Schema {
	return r.parser.schema
}

// Next advances to the next record batch
func (r *SelectRecordReader) Next() bool {
	if r.done || r.err != nil {
		return false
	}

	// Release previous batch if any
	if r.currentBatch != nil {
		r.currentBatch.Release()
		r.currentBatch = nil
	}

	// Parse next batch using optimized SELECT protocol
	record, done, err := r.parser.ParseNextBatch(r.ctx)
	if err != nil {
		r.err = err
		return false
	}

	if done && record == nil {
		r.done = true
		return false
	}

	r.currentBatch = record
	if done {
		r.done = true
	}

	return r.currentBatch != nil
}

// Record returns the current record
func (r *SelectRecordReader) Record() arrow.Record {
	return r.currentBatch
}

// Err returns any error that occurred during reading
func (r *SelectRecordReader) Err() error {
	return r.err
}

// Retain increases the reference count
func (r *SelectRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

// Release decreases the reference count and cleans up when it reaches 0
func (r *SelectRecordReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.currentBatch != nil {
			r.currentBatch.Release()
			r.currentBatch = nil
		}

		// Clean up parser
		if r.parser != nil {
			r.parser.Release()
			r.parser = nil
		}

		// Release connection back to pool
		if r.conn != nil {
			r.conn.Release()
			r.conn = nil
		}
	}
}

// RecordReader interface implementation for Arrow compatibility
var _ array.RecordReader = (*SelectRecordReader)(nil)
