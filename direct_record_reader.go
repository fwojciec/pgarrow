package pgarrow

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DirectRecordReader streams PostgreSQL COPY data directly as Arrow record batches.
// It processes data in chunks to support larger-than-memory datasets.
type DirectRecordReader struct {
	ctx     context.Context
	conn    *pgxpool.Conn
	parser  *DirectCopyParser
	copySQL string

	// Batch management
	batchSizeBytes int
	currentBatch   arrow.Record
	err            error
	done           bool

	// Reference counting
	refCount int64
}

// NewDirectRecordReader creates a streaming record reader using direct COPY parsing
func NewDirectRecordReader(ctx context.Context, conn *pgxpool.Conn, schema *arrow.Schema, copySQL string, alloc memory.Allocator) (*DirectRecordReader, error) {
	return NewDirectRecordReaderWithBatchSize(ctx, conn, schema, copySQL, alloc, DefaultBatchSizeBytes)
}

// NewDirectRecordReaderWithBatchSize creates a streaming record reader with custom batch size
func NewDirectRecordReaderWithBatchSize(ctx context.Context, conn *pgxpool.Conn, schema *arrow.Schema, copySQL string, alloc memory.Allocator, batchSizeBytes int) (*DirectRecordReader, error) {
	parser, err := NewDirectCopyParser(conn.Conn().PgConn(), schema, alloc)
	if err != nil {
		return nil, err
	}

	// Start COPY eagerly instead of lazily to reduce latency on first Next()
	if err := parser.StartParsing(ctx, copySQL); err != nil {
		parser.Release()
		return nil, fmt.Errorf("starting COPY: %w", err)
	}

	return &DirectRecordReader{
		ctx:            ctx,
		conn:           conn,
		parser:         parser,
		copySQL:        copySQL,
		batchSizeBytes: batchSizeBytes,
		refCount:       1,
	}, nil
}

// Schema returns the Arrow schema
func (r *DirectRecordReader) Schema() *arrow.Schema {
	return r.parser.schema
}

// Next advances to the next record batch
func (r *DirectRecordReader) Next() bool {
	if r.done || r.err != nil {
		return false
	}

	// Release previous batch if any
	if r.currentBatch != nil {
		r.currentBatch.Release()
		r.currentBatch = nil
	}

	// Parser already started in NewDirectRecordReader, so just continue parsing

	// Parse next batch up to size limit
	record, done, err := r.parser.ParseNextBatch(r.ctx, r.batchSizeBytes)
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
func (r *DirectRecordReader) Record() arrow.Record {
	return r.currentBatch
}

// Err returns any error that occurred during reading
func (r *DirectRecordReader) Err() error {
	return r.err
}

// Retain increases the reference count
func (r *DirectRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

// Release decreases the reference count and cleans up when it reaches 0
func (r *DirectRecordReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.currentBatch != nil {
			r.currentBatch.Release()
			r.currentBatch = nil
		}

		// Ensure connection is in clean state before releasing
		if r.parser != nil {
			// Use a short timeout context for cleanup
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Try to consume remaining messages to clean state
			if err := r.parser.ConsumeTillReady(cleanupCtx); err != nil {
				// If we can't clean the connection, close it instead of returning to pool
				// This prevents poisoning the pool with a bad connection
				if r.conn != nil {
					// Close the underlying connection, not just release to pool
					r.conn.Conn().Close(cleanupCtx)
					r.conn = nil
				}
			}

			r.parser.Release()
			r.parser = nil
		}

		// Only release to pool if connection is clean
		if r.conn != nil {
			r.conn.Release()
			r.conn = nil
		}
	}
}
