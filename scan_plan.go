package pgarrow

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ScanPlan defines how to parse a specific PostgreSQL type directly to Arrow builder.
// Each implementation is optimized for a specific data type and performs zero-copy
// conversion where possible.
type ScanPlan interface {
	// ScanToBuilder parses binary data and appends to the Arrow builder
	ScanToBuilder(data []byte, builder array.Builder) error
	// RequiredBytes returns the fixed size in bytes for this type (-1 for variable length)
	RequiredBytes() int
}

// scanPlanInt64 handles PostgreSQL INT8/BIGINT
type scanPlanInt64 struct{}

func (scanPlanInt64) RequiredBytes() int { return 8 }

func (scanPlanInt64) ScanToBuilder(data []byte, builder array.Builder) error {
	value := int64(binary.BigEndian.Uint64(data))
	b, ok := builder.(*array.Int64Builder)
	if !ok {
		return fmt.Errorf("expected *array.Int64Builder, got %T", builder)
	}
	b.Append(value)
	return nil
}

// scanPlanInt32 handles PostgreSQL INT4/INTEGER
type scanPlanInt32 struct{}

func (scanPlanInt32) RequiredBytes() int { return 4 }

func (scanPlanInt32) ScanToBuilder(data []byte, builder array.Builder) error {
	value := int32(binary.BigEndian.Uint32(data))
	b, ok := builder.(*array.Int32Builder)
	if !ok {
		return fmt.Errorf("expected *array.Int32Builder, got %T", builder)
	}
	b.Append(value)
	return nil
}

// scanPlanInt16 handles PostgreSQL INT2/SMALLINT
type scanPlanInt16 struct{}

func (scanPlanInt16) RequiredBytes() int { return 2 }

func (scanPlanInt16) ScanToBuilder(data []byte, builder array.Builder) error {
	value := int16(binary.BigEndian.Uint16(data))
	b, ok := builder.(*array.Int16Builder)
	if !ok {
		return fmt.Errorf("expected *array.Int16Builder, got %T", builder)
	}
	b.Append(value)
	return nil
}

// scanPlanFloat64 handles PostgreSQL FLOAT8/DOUBLE PRECISION
type scanPlanFloat64 struct{}

func (scanPlanFloat64) RequiredBytes() int { return 8 }

func (scanPlanFloat64) ScanToBuilder(data []byte, builder array.Builder) error {
	bits := binary.BigEndian.Uint64(data)
	value := math.Float64frombits(bits)
	b, ok := builder.(*array.Float64Builder)
	if !ok {
		return fmt.Errorf("expected *array.Float64Builder, got %T", builder)
	}
	b.Append(value)
	return nil
}

// scanPlanFloat32 handles PostgreSQL FLOAT4/REAL
type scanPlanFloat32 struct{}

func (scanPlanFloat32) RequiredBytes() int { return 4 }

func (scanPlanFloat32) ScanToBuilder(data []byte, builder array.Builder) error {
	bits := binary.BigEndian.Uint32(data)
	value := math.Float32frombits(bits)
	b, ok := builder.(*array.Float32Builder)
	if !ok {
		return fmt.Errorf("expected *array.Float32Builder, got %T", builder)
	}
	b.Append(value)
	return nil
}

// scanPlanBool handles PostgreSQL BOOL
type scanPlanBool struct{}

func (scanPlanBool) RequiredBytes() int { return 1 }

func (scanPlanBool) ScanToBuilder(data []byte, builder array.Builder) error {
	value := data[0] != 0
	b, ok := builder.(*array.BooleanBuilder)
	if !ok {
		return fmt.Errorf("expected *array.BooleanBuilder, got %T", builder)
	}
	b.Append(value)
	return nil
}

// scanPlanString handles PostgreSQL TEXT/VARCHAR
type scanPlanString struct{}

func (scanPlanString) RequiredBytes() int { return -1 } // Variable length

func (scanPlanString) ScanToBuilder(data []byte, builder array.Builder) error {
	// PostgreSQL TEXT is already UTF-8 encoded
	b, ok := builder.(*array.StringBuilder)
	if !ok {
		return fmt.Errorf("expected *array.StringBuilder, got %T", builder)
	}
	b.Append(string(data))
	return nil
}

// scanPlanBinary handles PostgreSQL BYTEA
type scanPlanBinary struct{}

func (scanPlanBinary) RequiredBytes() int { return -1 } // Variable length

func (scanPlanBinary) ScanToBuilder(data []byte, builder array.Builder) error {
	b, ok := builder.(*array.BinaryBuilder)
	if !ok {
		return fmt.Errorf("expected *array.BinaryBuilder, got %T", builder)
	}
	b.Append(data)
	return nil
}

// scanPlanDate32 handles PostgreSQL DATE
type scanPlanDate32 struct{}

func (scanPlanDate32) RequiredBytes() int { return 4 }

func (scanPlanDate32) ScanToBuilder(data []byte, builder array.Builder) error {
	// PostgreSQL sends days since 2000-01-01
	// Arrow expects days since Unix epoch (1970-01-01)
	pgDays := int32(binary.BigEndian.Uint32(data))

	// PostgreSQL epoch: 2000-01-01
	// Unix epoch: 1970-01-01
	// Difference: 10957 days
	const pgEpochOffset = 10957
	arrowDays := pgDays + pgEpochOffset

	b, ok := builder.(*array.Date32Builder)
	if !ok {
		return fmt.Errorf("expected *array.Date32Builder, got %T", builder)
	}
	b.Append(arrow.Date32(arrowDays))
	return nil
}

// scanPlanDate64 handles PostgreSQL DATE when using Date64
type scanPlanDate64 struct{}

func (scanPlanDate64) RequiredBytes() int { return 4 }

func (scanPlanDate64) ScanToBuilder(data []byte, builder array.Builder) error {
	// PostgreSQL sends days since 2000-01-01
	pgDays := int32(binary.BigEndian.Uint32(data))

	// Convert to milliseconds since Unix epoch for Date64
	const pgEpochOffset = 10957
	arrowDays := pgDays + pgEpochOffset
	arrowMillis := int64(arrowDays) * 86400000 // days to milliseconds

	b, ok := builder.(*array.Date64Builder)
	if !ok {
		return fmt.Errorf("expected *array.Date64Builder, got %T", builder)
	}
	b.Append(arrow.Date64(arrowMillis))
	return nil
}

// scanPlanTimestamp handles PostgreSQL TIMESTAMP/TIMESTAMPTZ
type scanPlanTimestamp struct{}

func (scanPlanTimestamp) RequiredBytes() int { return 8 }

func (scanPlanTimestamp) ScanToBuilder(data []byte, builder array.Builder) error {
	// PostgreSQL sends microseconds since 2000-01-01
	pgMicros := int64(binary.BigEndian.Uint64(data))

	// Convert to Unix timestamp
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	unixMicros := pgEpoch.UnixMicro() + pgMicros

	b, ok := builder.(*array.TimestampBuilder)
	if !ok {
		return fmt.Errorf("expected *array.TimestampBuilder, got %T", builder)
	}
	b.Append(arrow.Timestamp(unixMicros))
	return nil
}

// scanPlanTime32 handles PostgreSQL TIME as seconds
type scanPlanTime32 struct{}

func (scanPlanTime32) RequiredBytes() int { return 8 }

func (scanPlanTime32) ScanToBuilder(data []byte, builder array.Builder) error {
	// PostgreSQL sends microseconds since midnight
	pgMicros := int64(binary.BigEndian.Uint64(data))
	seconds := int32(pgMicros / 1_000_000)

	b, ok := builder.(*array.Time32Builder)
	if !ok {
		return fmt.Errorf("expected *array.Time32Builder, got %T", builder)
	}
	b.Append(arrow.Time32(seconds))
	return nil
}

// scanPlanTime64 handles PostgreSQL TIME as microseconds
type scanPlanTime64 struct{}

func (scanPlanTime64) RequiredBytes() int { return 8 }

func (scanPlanTime64) ScanToBuilder(data []byte, builder array.Builder) error {
	// PostgreSQL sends microseconds since midnight
	pgMicros := int64(binary.BigEndian.Uint64(data))

	b, ok := builder.(*array.Time64Builder)
	if !ok {
		return fmt.Errorf("expected *array.Time64Builder, got %T", builder)
	}
	b.Append(arrow.Time64(pgMicros))
	return nil
}

// scanPlanInterval handles PostgreSQL INTERVAL
type scanPlanInterval struct{}

func (scanPlanInterval) RequiredBytes() int { return 16 } // 8 bytes microseconds + 4 bytes days + 4 bytes months

func (scanPlanInterval) ScanToBuilder(data []byte, builder array.Builder) error {
	// PostgreSQL binary format sends:
	// - 8 bytes: microseconds (int64)
	// - 4 bytes: days (int32)
	// - 4 bytes: months (int32)
	microseconds := int64(binary.BigEndian.Uint64(data[0:8]))
	days := int32(binary.BigEndian.Uint32(data[8:12]))
	months := int32(binary.BigEndian.Uint32(data[12:16]))

	// Convert microseconds to nanoseconds
	nanoseconds := microseconds * 1000

	// Create Arrow interval
	interval := arrow.MonthDayNanoInterval{
		Months:      months,
		Days:        days,
		Nanoseconds: nanoseconds,
	}

	b, ok := builder.(*array.MonthDayNanoIntervalBuilder)
	if !ok {
		return fmt.Errorf("expected *array.MonthDayNanoIntervalBuilder, got %T", builder)
	}
	b.Append(interval)
	return nil
}

// createScanPlan creates an optimized scan plan for a given Arrow type
func createScanPlan(dataType arrow.DataType) (ScanPlan, error) {
	// Try type assertion first for concrete types
	switch dataType.(type) {
	case *arrow.Int64Type:
		return scanPlanInt64{}, nil
	case *arrow.Int32Type:
		return scanPlanInt32{}, nil
	case *arrow.Int16Type:
		return scanPlanInt16{}, nil
	case *arrow.Float64Type:
		return scanPlanFloat64{}, nil
	case *arrow.Float32Type:
		return scanPlanFloat32{}, nil
	case *arrow.BooleanType:
		return scanPlanBool{}, nil
	case *arrow.StringType:
		return scanPlanString{}, nil
	case *arrow.BinaryType:
		return scanPlanBinary{}, nil
	case *arrow.Date32Type:
		return scanPlanDate32{}, nil
	case *arrow.Date64Type:
		return scanPlanDate64{}, nil
	case *arrow.TimestampType:
		return scanPlanTimestamp{}, nil
	case *arrow.Time32Type:
		return scanPlanTime32{}, nil
	case *arrow.Time64Type:
		return scanPlanTime64{}, nil
	case *arrow.MonthDayNanoIntervalType:
		return scanPlanInterval{}, nil
	default:
		// Fall back to ID-based matching
		return createScanPlanByID(dataType)
	}
}

// createScanPlanByID creates a scan plan using the type ID
func createScanPlanByID(dataType arrow.DataType) (ScanPlan, error) {
	switch dataType.ID() {
	case arrow.INT64:
		return scanPlanInt64{}, nil
	case arrow.INT32:
		return scanPlanInt32{}, nil
	case arrow.INT16:
		return scanPlanInt16{}, nil
	case arrow.FLOAT64:
		return scanPlanFloat64{}, nil
	case arrow.FLOAT32:
		return scanPlanFloat32{}, nil
	case arrow.BOOL:
		return scanPlanBool{}, nil
	case arrow.STRING:
		return scanPlanString{}, nil
	case arrow.BINARY:
		return scanPlanBinary{}, nil
	case arrow.DATE32:
		return scanPlanDate32{}, nil
	case arrow.DATE64:
		return scanPlanDate64{}, nil
	case arrow.TIMESTAMP:
		return scanPlanTimestamp{}, nil
	case arrow.TIME32:
		return scanPlanTime32{}, nil
	case arrow.TIME64:
		return scanPlanTime64{}, nil
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return scanPlanInterval{}, nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type: %s", dataType)
	}
}
