package pgarrow

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

const (
	// ArrowFieldMetadataOverhead represents the estimated bytes of overhead per field in Arrow arrays
	// This includes null bitmap, validity buffer, and other Arrow-specific metadata
	ArrowFieldMetadataOverhead = 8

	// PostgreSQL type OIDs for supported data types
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

	// PostgreSQL epoch adjustment: days from 1970-01-01 to 2000-01-01
	PostgresDateEpochDays = 10957
	// PostgreSQL timestamp epoch adjustment: microseconds from 1970-01-01 to 2000-01-01
	PostgresTimestampEpochMicros = 946684800000000
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
