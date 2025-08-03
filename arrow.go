package pgarrow

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// ArrowFieldMetadataOverhead represents the estimated bytes of overhead per field in Arrow arrays
	// This includes null bitmap, validity buffer, and other Arrow-specific metadata
	ArrowFieldMetadataOverhead = 8
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

// parseRowsIntoBatchWithMetadata parses rows using SchemaMetadata for direct column writing
// Uses byte-based batching following ADBC approach for optimal batch sizing
func (r *PGArrowRecordReader) parseRowsIntoBatchWithMetadata() int {
	rowCount := 0
	accumulatedBytes := 0

	for accumulatedBytes < r.batchSizeBytes {
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

		// Calculate byte size of this row for batch size tracking
		rowBytes := calculateRowByteSize(fieldData, nulls)

		// Check if adding this row would exceed the maximum batch size limit
		if accumulatedBytes > 0 && accumulatedBytes+rowBytes > MaxBatchSizeBytes {
			// Since parser doesn't support putback, we must process this row first
			// then break to start a new batch for subsequent rows
			err = r.schemaMetadata.ProcessRow(fieldData, nulls)
			if err != nil {
				r.err = err
				return rowCount
			}
			rowCount++
			// accumulatedBytes not updated since we break immediately
			break
		}

		// Direct writing using schema metadata parsers
		err = r.schemaMetadata.ProcessRow(fieldData, nulls)
		if err != nil {
			r.err = err
			return rowCount
		}

		rowCount++
		accumulatedBytes += rowBytes
	}
	return rowCount
}

// calculateRowByteSize estimates the byte size of a row based on field data
// This provides the byte tracking needed for ADBC-style batch sizing
func calculateRowByteSize(fieldData [][]byte, nulls []bool) int {
	totalBytes := 0
	for i, data := range fieldData {
		if nulls[i] {
			// Null values take minimal space (just the null indicator)
			totalBytes += 1
		} else {
			// Data size plus some overhead for Arrow array storage
			totalBytes += len(data) + ArrowFieldMetadataOverhead
		}
	}
	return totalBytes
}

// PGArrowRecordReader implements array.RecordReader for streaming PostgreSQL data
type PGArrowRecordReader struct {
	refCount       int64
	schemaMetadata *SchemaMetadata
	parser         *Parser

	// Connection lifecycle management
	conn       *pgxpool.Conn
	pipeReader *io.PipeReader
	copyDone   chan struct{} // Signal when COPY goroutine is done

	currentRecord arrow.Record
	err           error
	released      bool

	// Byte-based batching (ADBC approach)
	batchSizeBytes int // Target batch size in bytes

	// Legacy row-based batching (deprecated)
	batchSize int // For compatibility with existing code
}

// newSchemaMetadataRecordReader creates a new PGArrowRecordReader using SchemaMetadata.
// This uses the lightweight ADBC-style approach with direct parsing functions.
func newSchemaMetadataRecordReader(schemaMetadata *SchemaMetadata, conn *pgxpool.Conn, pipeReader *io.PipeReader, copyDone chan struct{}) (*PGArrowRecordReader, error) {
	parser := NewParser(pipeReader, schemaMetadata.FieldOIDs())
	if err := parser.ParseHeader(); err != nil {
		return nil, fmt.Errorf("failed to parse COPY header: %w", err)
	}

	return &PGArrowRecordReader{
		refCount:       1,
		schemaMetadata: schemaMetadata,
		parser:         parser,
		conn:           conn,
		pipeReader:     pipeReader,
		copyDone:       copyDone,
		batchSizeBytes: DefaultBatchSizeBytes, // ADBC-style byte-based batching (16MB)
		batchSize:      OptimalBatchSizeGo,    // Legacy row-based fallback for compatibility
	}, nil
}

// Schema returns the Arrow schema
func (r *PGArrowRecordReader) Schema() *arrow.Schema {
	return r.schemaMetadata.Schema()
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

	// Use schema metadata for direct column writing
	rowCount := r.parseRowsIntoBatchWithMetadata()
	if rowCount == 0 || r.err != nil {
		return false
	}

	// Create record from schema metadata
	record, err := r.schemaMetadata.BuildRecord(int64(rowCount))
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

		if r.schemaMetadata != nil {
			r.schemaMetadata.Release()
			r.schemaMetadata = nil
		}
		r.parser = nil
		r.released = true
	}
}

// Retain increases the reference count
func (r *PGArrowRecordReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}
