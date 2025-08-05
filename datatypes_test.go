package pgarrow_test

import (
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

func TestDataTypes(t *testing.T) {
	t.Parallel()
	runTests(t, []testCase{
		// Boolean
		{
			name:  "bool_all_values",
			query: "SELECT true, false, NULL::bool",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(3), rec.NumCols())

				// Column 0: true
				require.False(t, rec.Column(0).IsNull(0))
				require.True(t, rec.Column(0).(*array.Boolean).Value(0))

				// Column 1: false
				require.False(t, rec.Column(1).IsNull(0))
				require.False(t, rec.Column(1).(*array.Boolean).Value(0))

				// Column 2: NULL
				require.True(t, rec.Column(2).IsNull(0))
			},
		},

		// Integer types
		{
			name:  "int2_edge_cases",
			query: "SELECT (-32768)::int2, 32767::int2, 0::int2, NULL::int2",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				require.Equal(t, int16(-32768), rec.Column(0).(*array.Int16).Value(0))
				require.Equal(t, int16(32767), rec.Column(1).(*array.Int16).Value(0))
				require.Equal(t, int16(0), rec.Column(2).(*array.Int16).Value(0))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},
		{
			name:  "int4_edge_cases",
			query: "SELECT (-2147483648)::int4, 2147483647::int4, 0::int4, NULL::int4",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				require.Equal(t, int32(-2147483648), rec.Column(0).(*array.Int32).Value(0))
				require.Equal(t, int32(2147483647), rec.Column(1).(*array.Int32).Value(0))
				require.Equal(t, int32(0), rec.Column(2).(*array.Int32).Value(0))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},
		{
			name:  "int8_edge_cases",
			query: "SELECT (-9223372036854775808)::int8, 9223372036854775807::int8, 0::int8, NULL::int8",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				require.Equal(t, int64(-9223372036854775808), rec.Column(0).(*array.Int64).Value(0))
				require.Equal(t, int64(9223372036854775807), rec.Column(1).(*array.Int64).Value(0))
				require.Equal(t, int64(0), rec.Column(2).(*array.Int64).Value(0))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},

		// Float types
		{
			name:  "float4_special_values",
			query: "SELECT 'Infinity'::float4, '-Infinity'::float4, 'NaN'::float4, NULL::float4",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				require.True(t, math.IsInf(float64(rec.Column(0).(*array.Float32).Value(0)), 1))
				require.True(t, math.IsInf(float64(rec.Column(1).(*array.Float32).Value(0)), -1))
				require.True(t, math.IsNaN(float64(rec.Column(2).(*array.Float32).Value(0))))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},
		{
			name:  "float8_special_values",
			query: "SELECT 'Infinity'::float8, '-Infinity'::float8, 'NaN'::float8, NULL::float8",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				require.True(t, math.IsInf(rec.Column(0).(*array.Float64).Value(0), 1))
				require.True(t, math.IsInf(rec.Column(1).(*array.Float64).Value(0), -1))
				require.True(t, math.IsNaN(rec.Column(2).(*array.Float64).Value(0)))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},

		// String types
		{
			name:  "text_encoding",
			query: "SELECT 'Hello'::text, 'Héllo 世界'::text, ''::text, NULL::text",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				require.Equal(t, "Hello", rec.Column(0).(*array.String).Value(0))
				require.Equal(t, "Héllo 世界", rec.Column(1).(*array.String).Value(0))
				require.Empty(t, rec.Column(2).(*array.String).Value(0))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},
		{
			name:  "varchar_values",
			query: "SELECT 'Test'::varchar(10), NULL::varchar",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(2), rec.NumCols())

				require.Equal(t, "Test", rec.Column(0).(*array.String).Value(0))
				require.True(t, rec.Column(1).IsNull(0))
			},
		},
		{
			name:  "bpchar_padding",
			query: "SELECT 'abc'::bpchar(5), NULL::bpchar",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(2), rec.NumCols())

				// bpchar pads with spaces
				require.Equal(t, "abc  ", rec.Column(0).(*array.String).Value(0))
				require.True(t, rec.Column(1).IsNull(0))
			},
		},
		{
			name:  "name_type",
			query: "SELECT 'pg_class'::name, NULL::name",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(2), rec.NumCols())

				require.Equal(t, "pg_class", rec.Column(0).(*array.String).Value(0))
				require.True(t, rec.Column(1).IsNull(0))
			},
		},
		{
			name:  "char_single_byte",
			query: "SELECT 'a'::\"char\", NULL::\"char\"",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(2), rec.NumCols())

				require.Equal(t, "a", rec.Column(0).(*array.String).Value(0))
				require.True(t, rec.Column(1).IsNull(0))
			},
		},

		// Binary
		{
			name:  "bytea_values",
			query: "SELECT '\\x'::bytea, '\\xDEADBEEF'::bytea, NULL::bytea",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(3), rec.NumCols())

				// Empty bytea can be nil or empty slice
				emptyBytes := rec.Column(0).(*array.Binary).Value(0)
				require.Empty(t, emptyBytes)
				require.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, rec.Column(1).(*array.Binary).Value(0))
				require.True(t, rec.Column(2).IsNull(0))
			},
		},

		// Date and Time
		{
			name:  "date_values",
			query: "SELECT '1970-01-01'::date, '2000-01-01'::date, '2024-08-15'::date, NULL::date",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				// Dates are days since Arrow epoch (1970-01-01)
				require.Equal(t, arrow.Date32(0), rec.Column(0).(*array.Date32).Value(0))
				require.Equal(t, arrow.Date32(10957), rec.Column(1).(*array.Date32).Value(0))
				require.Equal(t, arrow.Date32(19950), rec.Column(2).(*array.Date32).Value(0))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},
		{
			name:  "date_before_pg_epoch",
			query: "SELECT '1999-12-31'::date",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				// This is the regression test - should be negative days
				require.Equal(t, arrow.Date32(10956), rec.Column(0).(*array.Date32).Value(0))
			},
		},
		{
			name:  "time_values",
			query: "SELECT '00:00:00'::time, '12:34:56.789'::time, '23:59:59.999999'::time, NULL::time",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				// Times are microseconds since midnight
				require.Equal(t, arrow.Time64(0), rec.Column(0).(*array.Time64).Value(0))
				require.Equal(t, arrow.Time64(45296789000), rec.Column(1).(*array.Time64).Value(0))
				require.Equal(t, arrow.Time64(86399999999), rec.Column(2).(*array.Time64).Value(0))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},

		// Timestamps
		{
			name:  "timestamp_values",
			query: "SELECT '2000-01-01 00:00:00'::timestamp, '2024-08-15 12:34:56.789'::timestamp, NULL::timestamp",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(3), rec.NumCols())

				// Timestamps are microseconds since Unix epoch
				ts1 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
				ts2 := time.Date(2024, 8, 15, 12, 34, 56, 789000000, time.UTC)

				require.Equal(t, arrow.Timestamp(ts1.UnixMicro()), rec.Column(0).(*array.Timestamp).Value(0))
				require.Equal(t, arrow.Timestamp(ts2.UnixMicro()), rec.Column(1).(*array.Timestamp).Value(0))
				require.True(t, rec.Column(2).IsNull(0))
			},
		},
		{
			name:  "timestamptz_values",
			query: "SELECT '2000-01-01 00:00:00+00'::timestamptz, '2024-08-15 12:34:56.789+00'::timestamptz, NULL::timestamptz",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(3), rec.NumCols())

				// Timestamptz are also microseconds since Unix epoch (UTC)
				ts1 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
				ts2 := time.Date(2024, 8, 15, 12, 34, 56, 789000000, time.UTC)

				require.Equal(t, arrow.Timestamp(ts1.UnixMicro()), rec.Column(0).(*array.Timestamp).Value(0))
				require.Equal(t, arrow.Timestamp(ts2.UnixMicro()), rec.Column(1).(*array.Timestamp).Value(0))
				require.True(t, rec.Column(2).IsNull(0))
			},
		},

		// Interval
		{
			name:  "interval_values",
			query: "SELECT '1 year 2 months 3 days 4 hours 5 minutes 6.789 seconds'::interval, '-1 day'::interval, NULL::interval",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(3), rec.NumCols())

				// Intervals are stored as MonthDayNanoInterval
				i1 := rec.Column(0).(*array.MonthDayNanoInterval).Value(0)
				require.Equal(t, int32(14), i1.Months) // 1 year + 2 months
				require.Equal(t, int32(3), i1.Days)
				require.Equal(t, int64(14706789000000), i1.Nanoseconds) // 4h5m6.789s

				i2 := rec.Column(1).(*array.MonthDayNanoInterval).Value(0)
				require.Equal(t, int32(0), i2.Months)
				require.Equal(t, int32(-1), i2.Days)
				require.Equal(t, int64(0), i2.Nanoseconds)

				require.True(t, rec.Column(2).IsNull(0))
			},
		},

		// Mixed types in single query
		{
			name:  "mixed_types",
			query: "SELECT 42::int4, 'hello'::text, true::bool, '2024-08-15'::date, '\\xDEAD'::bytea",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(5), rec.NumCols())

				require.Equal(t, int32(42), rec.Column(0).(*array.Int32).Value(0))
				require.Equal(t, "hello", rec.Column(1).(*array.String).Value(0))
				require.True(t, rec.Column(2).(*array.Boolean).Value(0))
				require.Equal(t, arrow.Date32(19950), rec.Column(3).(*array.Date32).Value(0))
				require.Equal(t, []byte{0xDE, 0xAD}, rec.Column(4).(*array.Binary).Value(0))
			},
		},

		// All nulls
		{
			name:  "all_nulls",
			query: "SELECT NULL::bool, NULL::int4, NULL::text, NULL::date, NULL::bytea",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(5), rec.NumCols())

				for i := 0; i < 5; i++ {
					require.True(t, rec.Column(i).IsNull(0))
				}
			},
		},
		// Numeric type tests
		{
			name:  "numeric_basic_values",
			query: "SELECT 123.456::numeric, 0::numeric, -999.99::numeric, NULL::numeric",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(4), rec.NumCols())

				require.Equal(t, "123.456", rec.Column(0).(*array.String).Value(0))
				require.Equal(t, "0", rec.Column(1).(*array.String).Value(0))
				require.Equal(t, "-999.99", rec.Column(2).(*array.String).Value(0))
				require.True(t, rec.Column(3).IsNull(0))
			},
		},
		{
			name:  "numeric_special_values",
			query: "SELECT 'NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(3), rec.NumCols())

				require.Equal(t, "NaN", rec.Column(0).(*array.String).Value(0))
				require.Equal(t, "Infinity", rec.Column(1).(*array.String).Value(0))
				require.Equal(t, "-Infinity", rec.Column(2).(*array.String).Value(0))
			},
		},
		{
			name:  "numeric_large_precision",
			query: "SELECT 1234567890123456789012345.123456789::numeric",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(1), rec.NumCols())

				require.Equal(t, "1234567890123456789012345.123456789", rec.Column(0).(*array.String).Value(0))
			},
		},
		{
			name:  "numeric_with_scale",
			query: "SELECT 123.456::numeric(10,3), 999999.999::numeric(10,3)",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int64(1), rec.NumRows())
				require.Equal(t, int64(2), rec.NumCols())

				require.Equal(t, "123.456", rec.Column(0).(*array.String).Value(0))
				require.Equal(t, "999999.999", rec.Column(1).(*array.String).Value(0))
			},
		},

		// Additional timestamp tests (microsecond is default, tested above)
		{
			name:  "timestamp_preserves_precision",
			query: "SELECT '2024-08-15 12:34:56.789123'::timestamp",
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]

				// Verify microsecond precision is preserved
				ts := rec.Column(0).(*array.Timestamp).Value(0)
				// Convert back to time to check
				tsTime := ts.ToTime(arrow.Microsecond)
				// Microseconds are in the nanosecond component
				require.Equal(t, 789123000, tsTime.Nanosecond())
			},
		},
	})
}
