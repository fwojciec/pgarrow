package pgarrow

const (
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

// This file contains PostgreSQL OID constants and epoch adjustments used by the current implementation.
