package pgarrow

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps pgxpool.Pool and provides Arrow query capabilities
type Pool struct {
	pool *pgxpool.Pool
}

// NewPool creates a new PGArrow pool
func NewPool(ctx context.Context, connString string) (*Pool, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &Pool{pool: pool}, nil
}

// QueryArrow executes a query and returns Arrow record
func (p *Pool) QueryArrow(ctx context.Context, sql string, args ...interface{}) (arrow.Record, error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Get metadata and create schema
	schema, fieldOIDs, err := p.getQueryMetadata(ctx, conn, sql, args...)
	if err != nil {
		return nil, err
	}

	// Execute COPY and parse binary data
	record, err := p.executeCopyAndParse(ctx, conn, sql, schema, fieldOIDs)
	if err != nil {
		return nil, err
	}

	return record, nil
}

// getQueryMetadata executes query to extract column metadata and create Arrow schema
func (p *Pool) getQueryMetadata(ctx context.Context, conn *pgxpool.Conn, sql string, args ...interface{}) (*arrow.Schema, []uint32, error) {
	rows, err := conn.Conn().Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare query: %w", err)
	}
	rows.Close() // We only need field descriptions

	fieldDescriptions := rows.FieldDescriptions()
	if len(fieldDescriptions) == 0 {
		return nil, nil, fmt.Errorf("query returned no columns")
	}

	// Create column info and OID list
	columns := make([]ColumnInfo, len(fieldDescriptions))
	fieldOIDs := make([]uint32, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = ColumnInfo{Name: fd.Name, OID: fd.DataTypeOID}
		fieldOIDs[i] = fd.DataTypeOID
	}

	schema, err := CreateSchema(columns)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Arrow schema: %w", err)
	}

	return schema, fieldOIDs, nil
}

// executeCopyAndParse runs COPY TO BINARY and parses the result into Arrow record
func (p *Pool) executeCopyAndParse(ctx context.Context, conn *pgxpool.Conn, sql string, schema *arrow.Schema, fieldOIDs []uint32) (arrow.Record, error) {
	copySQL := fmt.Sprintf("COPY (%s) TO STDOUT (FORMAT BINARY)", sql)

	// Set up pipe for COPY data
	pipeReader, pipeWriter := io.Pipe()
	copyErrChan := make(chan error, 1)
	
	go func() {
		defer pipeWriter.Close()
		_, err := conn.Conn().PgConn().CopyTo(ctx, pipeWriter, copySQL)
		copyErrChan <- err
	}()
	defer pipeReader.Close()

	// Parse and build record
	record, err := p.parseDataToRecord(pipeReader, schema, fieldOIDs)
	if err != nil {
		return nil, err
	}

	// Check COPY operation result
	if copyErr := <-copyErrChan; copyErr != nil {
		record.Release()
		return nil, fmt.Errorf("COPY operation failed: %w", copyErr)
	}

	return record, nil
}

// parseDataToRecord parses binary COPY data and builds Arrow record
func (p *Pool) parseDataToRecord(reader io.Reader, schema *arrow.Schema, fieldOIDs []uint32) (arrow.Record, error) {
	parser := NewParser(reader, fieldOIDs)
	if err := parser.ParseHeader(); err != nil {
		return nil, fmt.Errorf("failed to parse COPY header: %w", err)
	}

	recordBuilder, err := NewRecordBuilder(schema, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("failed to create record builder: %w", err)
	}
	defer recordBuilder.Release()

	// Parse all tuples
	for {
		fields, err := parser.ParseTuple()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse tuple: %w", err)
		}

		values := make([]interface{}, len(fields))
		for i, field := range fields {
			values[i] = field.Value
		}

		if err := recordBuilder.AppendRow(values); err != nil {
			return nil, fmt.Errorf("failed to append row to builder: %w", err)
		}
	}

	record, err := recordBuilder.NewRecord()
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow record: %w", err)
	}

	return record, nil
}

// Close closes the pool
func (p *Pool) Close() {
	p.pool.Close()
}
