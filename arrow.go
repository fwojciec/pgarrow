package pgarrow

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ColumnInfo represents PostgreSQL column metadata for Arrow schema generation
type ColumnInfo struct {
	Name string
	OID  uint32
}

// CreateSchema creates an Arrow schema from PostgreSQL column metadata
func CreateSchema(columns []ColumnInfo) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columns))

	for i, col := range columns {
		arrowType, err := oidToArrowType(col.OID)
		if err != nil {
			return nil, err
		}

		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: true, // PostgreSQL columns are nullable by default
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// oidToArrowType maps PostgreSQL OIDs directly to Arrow types
func oidToArrowType(oid uint32) (arrow.DataType, error) {
	switch oid {
	case TypeOIDBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case TypeOIDBytea:
		return arrow.BinaryTypes.Binary, nil
	case TypeOIDInt2:
		return arrow.PrimitiveTypes.Int16, nil
	case TypeOIDInt4:
		return arrow.PrimitiveTypes.Int32, nil
	case TypeOIDInt8:
		return arrow.PrimitiveTypes.Int64, nil
	case TypeOIDFloat4:
		return arrow.PrimitiveTypes.Float32, nil
	case TypeOIDFloat8:
		return arrow.PrimitiveTypes.Float64, nil
	case TypeOIDText, TypeOIDVarchar, TypeOIDBpchar, TypeOIDName, TypeOIDChar:
		return arrow.BinaryTypes.String, nil
	case TypeOIDDate:
		return arrow.PrimitiveTypes.Date32, nil
	case TypeOIDTime:
		return arrow.FixedWidthTypes.Time64us, nil
	case TypeOIDTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}, nil
	case TypeOIDTimestamptz:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case TypeOIDInterval:
		return arrow.FixedWidthTypes.MonthDayNanoInterval, nil
	default:
		return nil, fmt.Errorf("unsupported PostgreSQL type OID: %d", oid)
	}
}

// extractFieldData extracts raw binary data and null indicators from parsed fields
// This converts from the parser's Field format to the format expected by ColumnWriters
func extractFieldData(fields []Field) ([][]byte, []bool, error) {
	fieldData := make([][]byte, len(fields))
	nulls := make([]bool, len(fields))

	for i, field := range fields {
		if field.Value == nil {
			nulls[i] = true
			fieldData[i] = nil
		} else {
			nulls[i] = false
			// ColumnWriters expect []byte - the parser should provide this for binary types
			if data, ok := field.Value.([]byte); ok {
				fieldData[i] = data
			} else {
				// For non-binary types, this is an error in integration
				return nil, nil, fmt.Errorf("unexpected field value type for compiled schema: %T", field.Value)
			}
		}
	}
	return fieldData, nulls, nil
}

// parseRowsIntoBatchCompiled parses rows using CompiledSchema for direct column writing
// This replaces the old parseRowsIntoBatch method, eliminating TypeRegistry overhead
func (r *PGArrowRecordReader) parseRowsIntoBatchCompiled() int {
	rowCount := 0

	for rowCount < r.batchSize {
		fields, err := r.parser.ParseTuple()
		if err == io.EOF {
			break
		}
		if err != nil {
			r.err = fmt.Errorf("failed to parse tuple: %w", err)
			return rowCount
		}

		// Extract raw binary data and null indicators
		fieldData, nulls, err := extractFieldData(fields)
		if err != nil {
			r.err = fmt.Errorf("failed to extract field data: %w", err)
			return rowCount
		}

		// Direct writing to compiled column writers
		err = r.compiledSchema.ProcessRow(fieldData, nulls)
		if err != nil {
			r.err = err
			return rowCount
		}

		rowCount++
	}
	return rowCount
}

// PGArrowRecordReader implements array.RecordReader for streaming PostgreSQL data
type PGArrowRecordReader struct {
	refCount       int64
	compiledSchema *CompiledSchema
	parser         *Parser

	// Connection lifecycle management
	conn       *pgxpool.Conn
	pipeReader *io.PipeReader
	copyDone   chan struct{} // Signal when COPY goroutine is done

	currentRecord arrow.Record
	err           error
	released      bool
	batchSize     int
}

// newCompiledRecordReader creates a new PGArrowRecordReader using an existing CompiledSchema.
// This is the preferred constructor for optimal performance as it uses pre-compiled schema.
func newCompiledRecordReader(compiledSchema *CompiledSchema, conn *pgxpool.Conn, pipeReader *io.PipeReader, copyDone chan struct{}) (*PGArrowRecordReader, error) {
	parser := NewParser(pipeReader, compiledSchema.FieldOIDs())
	if err := parser.ParseHeader(); err != nil {
		return nil, fmt.Errorf("failed to parse COPY header: %w", err)
	}

	return &PGArrowRecordReader{
		refCount:       1,
		compiledSchema: compiledSchema,
		parser:         parser,
		conn:           conn,
		pipeReader:     pipeReader,
		copyDone:       copyDone,
		batchSize:      OptimalBatchSizeGo, // Go-optimized batch size for GC and cache efficiency
	}, nil
}

// Schema returns the Arrow schema
func (r *PGArrowRecordReader) Schema() *arrow.Schema {
	return r.compiledSchema.Schema()
}

// Next advances to the next record batch
func (r *PGArrowRecordReader) Next() bool {
	if r.released || r.err != nil {
		return false
	}

	// Release previous record if exists
	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}

	// Use compiled schema for direct column writing
	rowCount := r.parseRowsIntoBatchCompiled()
	if rowCount == 0 || r.err != nil {
		return false
	}

	// Create record from compiled schema
	record, err := r.compiledSchema.BuildRecord(int64(rowCount))
	if err != nil {
		r.err = fmt.Errorf("failed to create Arrow record: %w", err)
		return false
	}

	r.currentRecord = record
	return true
}

// Record returns the current record batch
func (r *PGArrowRecordReader) Record() arrow.Record {
	return r.currentRecord
}

// Err returns any error that occurred during reading
func (r *PGArrowRecordReader) Err() error {
	return r.err
}

// Release decreases the reference count and releases resources when it reaches 0
func (r *PGArrowRecordReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.currentRecord != nil {
			r.currentRecord.Release()
			r.currentRecord = nil
		}

		// Clean up connection resources
		if r.pipeReader != nil {
			r.pipeReader.Close()
			r.pipeReader = nil
		}

		// Wait for COPY goroutine to complete before releasing connection
		if r.copyDone != nil {
			<-r.copyDone
			r.copyDone = nil
		}

		if r.conn != nil {
			r.conn.Release()
			r.conn = nil
		}

		if r.compiledSchema != nil {
			r.compiledSchema.Release()
			r.compiledSchema = nil
		}
		r.parser = nil
		r.released = true
	}
}

// Retain increases the reference count
func (r *PGArrowRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}
