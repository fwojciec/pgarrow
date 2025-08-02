package pgarrow_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueryArrowDataTypes is a comprehensive table-based test covering all PostgreSQL data types
// This replaces multiple unit test files with a single comprehensive integration test
func TestQueryArrowDataTypes(t *testing.T) {
	t.Parallel()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	ctx := context.Background()

	tests := []struct {
		name         string
		setupSQL     string
		querySQL     string
		expectedRows int64
		expectedCols int64
		validateFunc func(t *testing.T, record arrow.Record)
	}{
		{
			name:         "bool_all_values",
			setupSQL:     `CREATE TABLE test_bool (val bool); INSERT INTO test_bool VALUES (true), (false), (null);`,
			querySQL:     "SELECT * FROM test_bool ORDER BY val NULLS LAST",
			expectedRows: 3,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				boolCol, ok := record.Column(0).(*array.Boolean)
				require.True(t, ok)

				assert.False(t, boolCol.Value(0)) // false comes first
				assert.True(t, boolCol.Value(1))  // then true
				assert.True(t, boolCol.IsNull(2)) // NULL comes last
			},
		},
		{
			name:         "int2_edge_cases",
			setupSQL:     `CREATE TABLE test_int2 (val int2); INSERT INTO test_int2 VALUES (32767), (-32768), (0), (null);`,
			querySQL:     "SELECT * FROM test_int2 ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				int2Col, ok := record.Column(0).(*array.Int16)
				require.True(t, ok)

				assert.Equal(t, int16(-32768), int2Col.Value(0)) // MIN_INT16
				assert.Equal(t, int16(0), int2Col.Value(1))
				assert.Equal(t, int16(32767), int2Col.Value(2)) // MAX_INT16
				assert.True(t, int2Col.IsNull(3))
			},
		},
		{
			name:         "int4_edge_cases",
			setupSQL:     `CREATE TABLE test_int4 (val int4); INSERT INTO test_int4 VALUES (2147483647), (-2147483648), (0), (null);`,
			querySQL:     "SELECT * FROM test_int4 ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				int4Col, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)

				assert.Equal(t, int32(-2147483648), int4Col.Value(0)) // MIN_INT32
				assert.Equal(t, int32(0), int4Col.Value(1))
				assert.Equal(t, int32(2147483647), int4Col.Value(2)) // MAX_INT32
				assert.True(t, int4Col.IsNull(3))
			},
		},
		{
			name:         "int8_edge_cases",
			setupSQL:     `CREATE TABLE test_int8 (val int8); INSERT INTO test_int8 VALUES (9223372036854775807), (-9223372036854775808), (0), (null);`,
			querySQL:     "SELECT * FROM test_int8 ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				int8Col, ok := record.Column(0).(*array.Int64)
				require.True(t, ok)

				assert.Equal(t, int64(-9223372036854775808), int8Col.Value(0)) // MIN_INT64
				assert.Equal(t, int64(0), int8Col.Value(1))
				assert.Equal(t, int64(9223372036854775807), int8Col.Value(2)) // MAX_INT64
				assert.True(t, int8Col.IsNull(3))
			},
		},
		{
			name:         "float4_precision",
			setupSQL:     `CREATE TABLE test_float4 (val float4); INSERT INTO test_float4 VALUES (3.14159), (-3.14159), (0.0), ('Infinity'::float4), ('-Infinity'::float4), ('NaN'::float4), (null);`,
			querySQL:     "SELECT * FROM test_float4 ORDER BY val NULLS LAST",
			expectedRows: 7,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				float4Col, ok := record.Column(0).(*array.Float32)
				require.True(t, ok)

				// Check for NaN, Infinity values (order might vary due to special float handling)
				hasNaN := false
				hasInf := false
				hasNegInf := false
				regularValues := []float32{}

				for i := 0; i < int(record.NumRows()); i++ {
					if !float4Col.IsNull(i) {
						val := float4Col.Value(i)
						if val != val { // NaN check
							hasNaN = true
						} else if val == float32(math.Inf(1)) {
							hasInf = true
						} else if val == float32(math.Inf(-1)) {
							hasNegInf = true
						} else {
							regularValues = append(regularValues, val)
						}
					}
				}

				assert.True(t, hasNaN, "Should have NaN value")
				assert.True(t, hasInf, "Should have +Infinity")
				assert.True(t, hasNegInf, "Should have -Infinity")
				assert.Contains(t, regularValues, float32(3.14159))
				assert.Contains(t, regularValues, float32(-3.14159))
				assert.Contains(t, regularValues, float32(0.0))
				assert.True(t, float4Col.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "float8_precision",
			setupSQL:     `CREATE TABLE test_float8 (val float8); INSERT INTO test_float8 VALUES (2.718281828459045), (-2.718281828459045), (0.0), ('Infinity'::float8), ('-Infinity'::float8), ('NaN'::float8), (null);`,
			querySQL:     "SELECT * FROM test_float8 ORDER BY val NULLS LAST",
			expectedRows: 7,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				float8Col, ok := record.Column(0).(*array.Float64)
				require.True(t, ok)

				// Similar validation as float4 but with higher precision
				hasNaN := false
				hasInf := false
				hasNegInf := false
				regularValues := []float64{}

				for i := 0; i < int(record.NumRows()); i++ {
					if !float8Col.IsNull(i) {
						val := float8Col.Value(i)
						if val != val { // NaN check
							hasNaN = true
						} else if val == math.Inf(1) {
							hasInf = true
						} else if val == math.Inf(-1) {
							hasNegInf = true
						} else {
							regularValues = append(regularValues, val)
						}
					}
				}

				assert.True(t, hasNaN, "Should have NaN value")
				assert.True(t, hasInf, "Should have +Infinity")
				assert.True(t, hasNegInf, "Should have -Infinity")
				// Find the e value (2.718...) in regularValues
				foundE := false
				for _, val := range regularValues {
					if val > 2.7 && val < 2.8 {
						assert.InDelta(t, 2.718281828459045, val, 0.000000000000001)
						foundE = true
						break
					}
				}
				assert.True(t, foundE, "Should find e value in regular values")
				assert.True(t, float8Col.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "text_encoding_cases",
			setupSQL:     `CREATE TABLE test_text (val text); INSERT INTO test_text VALUES ('Hello World'), (''), ('Unicode: 🚀 αβγ 中文'), ('Special' || chr(10) || 'Chars' || chr(9) || '"'), (null);`,
			querySQL:     "SELECT * FROM test_text ORDER BY val NULLS LAST",
			expectedRows: 5,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				textCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0, record.NumRows()-1) // -1 for NULL
				for i := 0; i < int(record.NumRows()); i++ {
					if !textCol.IsNull(i) {
						values = append(values, textCol.Value(i))
					}
				}

				assert.Contains(t, values, "")
				assert.Contains(t, values, "Hello World")
				assert.Contains(t, values, "Unicode: 🚀 αβγ 中文")
				assert.Contains(t, values, "Special\nChars\t\"")
				assert.True(t, textCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name: "mixed_types_literal_filter",
			setupSQL: `CREATE TABLE test_mixed (id int4, name text, score float8, active bool); 
					   INSERT INTO test_mixed VALUES (1, 'Alice', 95.5, true), (2, 'Bob', 87.2, false), (3, 'Charlie', 92.1, true);`,
			querySQL:     "SELECT * FROM test_mixed WHERE score > 90.0 AND active = true ORDER BY score DESC",
			expectedRows: 2,
			expectedCols: 4,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "name", schema.Field(1).Name)
				assert.Equal(t, "score", schema.Field(2).Name)
				assert.Equal(t, "active", schema.Field(3).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				nameCol, ok := record.Column(1).(*array.String)
				require.True(t, ok)
				scoreCol, ok := record.Column(2).(*array.Float64)
				require.True(t, ok)
				activeCol, ok := record.Column(3).(*array.Boolean)
				require.True(t, ok)

				// First row should be Alice (highest score)
				assert.Equal(t, int32(1), idCol.Value(0))
				assert.Equal(t, "Alice", nameCol.Value(0))
				assert.InDelta(t, 95.5, scoreCol.Value(0), 0.01)
				assert.True(t, activeCol.Value(0))

				// Second row should be Charlie
				assert.Equal(t, int32(3), idCol.Value(1))
				assert.Equal(t, "Charlie", nameCol.Value(1))
				assert.InDelta(t, 92.1, scoreCol.Value(1), 0.01)
				assert.True(t, activeCol.Value(1))
			},
		},
		{
			name: "all_nulls_row",
			setupSQL: `CREATE TABLE test_nulls (col_bool bool, col_int2 int2, col_int4 int4, col_int8 int8, col_float4 float4, col_float8 float8, col_text text);
					   INSERT INTO test_nulls VALUES (null, null, null, null, null, null, null);`,
			querySQL:     "SELECT * FROM test_nulls",
			expectedRows: 1,
			expectedCols: 7,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// All columns should have NULL in the single row
				for i := range int(record.NumCols()) {
					col := record.Column(i)
					assert.True(t, col.IsNull(0), "Column %d should be NULL", i)
				}
			},
		},
		{
			name:         "varchar_all_values",
			setupSQL:     `CREATE TABLE test_varchar (val varchar(50)); INSERT INTO test_varchar VALUES ('Hello VARCHAR'), (''), ('Unicode: 🚀 αβγ 中文'), (null);`,
			querySQL:     "SELECT * FROM test_varchar ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				varcharCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !varcharCol.IsNull(i) {
						values = append(values, varcharCol.Value(i))
					}
				}

				assert.Contains(t, values, "")
				assert.Contains(t, values, "Hello VARCHAR")
				assert.Contains(t, values, "Unicode: 🚀 αβγ 中文")
				assert.True(t, varcharCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "bpchar_all_values",
			setupSQL:     `CREATE TABLE test_bpchar (val char(20)); INSERT INTO test_bpchar VALUES ('Hello BPCHAR'), (''), ('Unicode: 🚀'), (null);`,
			querySQL:     "SELECT * FROM test_bpchar ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				bpcharCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !bpcharCol.IsNull(i) {
						val := bpcharCol.Value(i)
						values = append(values, val)
					}
				}

				// Note: BPCHAR pads with spaces, so we check for trimmed values
				found := false
				for _, val := range values {
					if val == "Hello BPCHAR" || (len(val) >= len("Hello BPCHAR") && val[:len("Hello BPCHAR")] == "Hello BPCHAR") {
						found = true
						break
					}
				}
				assert.True(t, found, "Should find 'Hello BPCHAR' value")
				assert.True(t, bpcharCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "name_all_values",
			setupSQL:     `CREATE TABLE test_name (val name); INSERT INTO test_name VALUES ('table_name'), ('column_name'), (''), (null);`,
			querySQL:     "SELECT * FROM test_name ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				nameCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !nameCol.IsNull(i) {
						values = append(values, nameCol.Value(i))
					}
				}

				assert.Contains(t, values, "")
				assert.Contains(t, values, "table_name")
				assert.Contains(t, values, "column_name")
				assert.True(t, nameCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "char_all_values",
			setupSQL:     `CREATE TABLE test_char (val "char"); INSERT INTO test_char VALUES ('A'), ('Z'), ('1'), (null);`,
			querySQL:     "SELECT * FROM test_char ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				charCol, ok := record.Column(0).(*array.String)
				require.True(t, ok)

				values := make([]string, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !charCol.IsNull(i) {
						values = append(values, charCol.Value(i))
					}
				}

				// "char" type stores single bytes, so check for presence of expected values
				assert.Len(t, values, 3, "Should have 3 non-NULL values")
				assert.Contains(t, values, "A")
				assert.Contains(t, values, "Z")
				assert.Contains(t, values, "1")
				assert.True(t, charCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "bytea_basic_values",
			setupSQL:     `CREATE TABLE test_bytea (val bytea); INSERT INTO test_bytea VALUES (E'\\x48656c6c6f20576f726c64'), (E'\\x'), (E'\\x00FF00FF'), (null);`,
			querySQL:     "SELECT * FROM test_bytea ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				byteaCol, ok := record.Column(0).(*array.Binary)
				require.True(t, ok)

				values := make([][]byte, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !byteaCol.IsNull(i) {
						values = append(values, byteaCol.Value(i))
					}
				}

				// Check for expected values
				foundEmpty := false
				foundHello := false
				foundPattern := false
				for _, val := range values {
					if len(val) == 0 {
						foundEmpty = true
					} else if string(val) == "Hello World" {
						foundHello = true
					} else if len(val) == 4 && val[0] == 0x00 && val[1] == 0xFF && val[2] == 0x00 && val[3] == 0xFF {
						foundPattern = true
					}
				}

				assert.True(t, foundEmpty, "Should find empty bytea")
				assert.True(t, foundHello, "Should find 'Hello World' bytea")
				assert.True(t, foundPattern, "Should find 0x00FF00FF pattern")
				assert.True(t, byteaCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "bytea_special_bytes",
			setupSQL:     `CREATE TABLE test_bytea_special (val bytea); INSERT INTO test_bytea_special VALUES (E'\\x0001020304050607080910111213141516171819'), (E'\\xFFFFFFFF'), (E'\\x000000'), (null);`,
			querySQL:     "SELECT * FROM test_bytea_special ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				byteaCol, ok := record.Column(0).(*array.Binary)
				require.True(t, ok)

				values := make([][]byte, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !byteaCol.IsNull(i) {
						values = append(values, byteaCol.Value(i))
					}
				}

				// Check for expected patterns
				foundZeros := false
				foundFFs := false
				foundSequence := false
				for _, val := range values {
					if len(val) == 3 && val[0] == 0x00 && val[1] == 0x00 && val[2] == 0x00 {
						foundZeros = true
					} else if len(val) == 4 && val[0] == 0xFF && val[1] == 0xFF && val[2] == 0xFF && val[3] == 0xFF {
						foundFFs = true
					} else if len(val) == 20 && val[0] == 0x00 && val[1] == 0x01 && val[19] == 0x19 {
						// Check the sequential bytes pattern: 00 01 02 03 ... 19 (20 bytes total)
						foundSequence = true
					}
				}

				assert.True(t, foundZeros, "Should find zero bytes pattern")
				assert.True(t, foundFFs, "Should find 0xFF bytes pattern")
				assert.True(t, foundSequence, "Should find sequential bytes pattern")
				assert.True(t, byteaCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "date_edge_cases",
			setupSQL:     `CREATE TABLE test_date (val date); INSERT INTO test_date VALUES ('2000-01-01'::date), ('1970-01-01'::date), ('2024-12-31'::date), ('1999-12-31'::date), (null);`,
			querySQL:     "SELECT * FROM test_date ORDER BY val NULLS LAST",
			expectedRows: 5,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				dateCol, ok := record.Column(0).(*array.Date32)
				require.True(t, ok)

				values := make([]int32, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !dateCol.IsNull(i) {
						values = append(values, int32(dateCol.Value(i)))
					}
				}

				// Check for expected values
				// 1970-01-01 should be 0 (Arrow epoch)
				// 2000-01-01 should be 10957 (PostgreSQL epoch + adjustment)
				foundArrowEpoch := false
				foundPgEpoch := false
				for _, val := range values {
					switch val {
					case 0:
						foundArrowEpoch = true // 1970-01-01
					case 10957:
						foundPgEpoch = true // 2000-01-01
					}
				}

				assert.True(t, foundArrowEpoch, "Should find Arrow epoch date (1970-01-01)")
				assert.True(t, foundPgEpoch, "Should find PostgreSQL epoch date (2000-01-01)")
				assert.Len(t, values, 4, "Should have 4 non-NULL date values")
				assert.True(t, dateCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "time_edge_cases",
			setupSQL:     `CREATE TABLE test_time (val time); INSERT INTO test_time VALUES ('00:00:00'::time), ('23:59:59.999999'::time), ('12:30:45.123456'::time), ('06:15:30'::time), (null);`,
			querySQL:     "SELECT * FROM test_time ORDER BY val NULLS LAST",
			expectedRows: 5,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				timeCol, ok := record.Column(0).(*array.Time64)
				require.True(t, ok)

				values := make([]int64, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !timeCol.IsNull(i) {
						values = append(values, int64(timeCol.Value(i)))
					}
				}

				// Check for expected values
				foundMidnight := false
				foundEndOfDay := false
				for _, val := range values {
					switch {
					case val == 0:
						foundMidnight = true // 00:00:00
					case val >= 86399999999: // Close to 23:59:59.999999
						foundEndOfDay = true
					}
				}

				assert.True(t, foundMidnight, "Should find midnight time (00:00:00)")
				assert.True(t, foundEndOfDay, "Should find end-of-day time")
				assert.Len(t, values, 4, "Should have 4 non-NULL time values")
				assert.True(t, timeCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name: "mixed_temporal_types",
			setupSQL: `CREATE TABLE test_mixed_temporal (
						id int4, 
						event_date date, 
						event_time time,
						description text
					   ); 
					   INSERT INTO test_mixed_temporal VALUES 
					   (1, '2000-01-01'::date, '00:00:00'::time, 'millennium'),
					   (2, '1970-01-01'::date, '12:30:45'::time, 'unix epoch'),
					   (3, '2024-12-31'::date, '23:59:59.999999'::time, 'year end'),
					   (4, null, null, 'nulls');`,
			querySQL:     "SELECT * FROM test_mixed_temporal ORDER BY id",
			expectedRows: 4,
			expectedCols: 4,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "event_date", schema.Field(1).Name)
				assert.Equal(t, "event_time", schema.Field(2).Name)
				assert.Equal(t, "description", schema.Field(3).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				dateCol, ok := record.Column(1).(*array.Date32)
				require.True(t, ok)
				timeCol, ok := record.Column(2).(*array.Time64)
				require.True(t, ok)
				descCol, ok := record.Column(3).(*array.String)
				require.True(t, ok)

				// Verify data for each row
				for i := range int(record.NumRows()) {
					switch idCol.Value(i) {
					case 1: // Millennium row
						assert.Equal(t, "millennium", descCol.Value(i))
						assert.Equal(t, int32(10957), int32(dateCol.Value(i))) // 2000-01-01
						assert.Equal(t, int64(0), int64(timeCol.Value(i)))     // 00:00:00
					case 2: // Unix epoch row
						assert.Equal(t, "unix epoch", descCol.Value(i))
						assert.Equal(t, int32(0), int32(dateCol.Value(i))) // 1970-01-01
						// 12:30:45 = 12*3600 + 30*60 + 45 = 45045 seconds = 45045000000 microseconds
						assert.Equal(t, int64(45045000000), int64(timeCol.Value(i)))
					case 3: // Year end row
						assert.Equal(t, "year end", descCol.Value(i))
						// 2024-12-31 is 20088 days from 1970-01-01
						assert.Greater(t, int32(dateCol.Value(i)), int32(19000))       // Should be around 20088
						assert.Greater(t, int64(timeCol.Value(i)), int64(86399000000)) // Near end of day
					case 4: // NULL row
						assert.Equal(t, "nulls", descCol.Value(i))
						assert.True(t, dateCol.IsNull(i))
						assert.True(t, timeCol.IsNull(i))
					}
				}
			},
		},
		{
			name: "mixed_types_with_bytea",
			setupSQL: `CREATE TABLE test_mixed_bytea (
						id int4, 
						name text, 
						data bytea, 
						active bool
					   ); 
					   INSERT INTO test_mixed_bytea VALUES 
					   (1, 'first', E'\\x48656c6c6f', true),
					   (2, 'second', E'\\x', false),
					   (3, 'third', null, true);`,
			querySQL:     "SELECT * FROM test_mixed_bytea ORDER BY id",
			expectedRows: 3,
			expectedCols: 4,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "name", schema.Field(1).Name)
				assert.Equal(t, "data", schema.Field(2).Name)
				assert.Equal(t, "active", schema.Field(3).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				nameCol, ok := record.Column(1).(*array.String)
				require.True(t, ok)
				dataCol, ok := record.Column(2).(*array.Binary)
				require.True(t, ok)
				activeCol, ok := record.Column(3).(*array.Boolean)
				require.True(t, ok)

				// Verify data for each row
				for i := range int(record.NumRows()) {
					switch idCol.Value(i) {
					case 1: // First row
						assert.Equal(t, "first", nameCol.Value(i))
						assert.Equal(t, "Hello", string(dataCol.Value(i)))
						assert.True(t, activeCol.Value(i))
					case 2: // Second row
						assert.Equal(t, "second", nameCol.Value(i))
						assert.Empty(t, dataCol.Value(i)) // Empty bytea
						assert.False(t, activeCol.Value(i))
					case 3: // Third row
						assert.Equal(t, "third", nameCol.Value(i))
						assert.True(t, dataCol.IsNull(i)) // NULL bytea
						assert.True(t, activeCol.Value(i))
					}
				}
			},
		},
		{
			name: "mixed_string_types",
			setupSQL: `CREATE TABLE test_mixed_strings (
						id int4, 
						text_col text, 
						varchar_col varchar(50), 
						bpchar_col char(10), 
						name_col name, 
						char_col "char"
					   ); 
					   INSERT INTO test_mixed_strings VALUES 
					   (1, 'text_value', 'varchar_value', 'bpchar', 'name_val', 'A'),
					   (2, '', '', '', '', '0'),
					   (3, null, null, null, null, null);`,
			querySQL:     "SELECT * FROM test_mixed_strings ORDER BY id",
			expectedRows: 3,
			expectedCols: 6,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "text_col", schema.Field(1).Name)
				assert.Equal(t, "varchar_col", schema.Field(2).Name)
				assert.Equal(t, "bpchar_col", schema.Field(3).Name)
				assert.Equal(t, "name_col", schema.Field(4).Name)
				assert.Equal(t, "char_col", schema.Field(5).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				textCol, ok := record.Column(1).(*array.String)
				require.True(t, ok)
				varcharCol, ok := record.Column(2).(*array.String)
				require.True(t, ok)
				bpcharCol, ok := record.Column(3).(*array.String)
				require.True(t, ok)
				nameCol, ok := record.Column(4).(*array.String)
				require.True(t, ok)
				charCol, ok := record.Column(5).(*array.String)
				require.True(t, ok)

				// Verify data for each row
				for i := range int(record.NumRows()) {
					switch idCol.Value(i) {
					case 1: // First row - all have values
						assert.Equal(t, "text_value", textCol.Value(i))
						assert.Equal(t, "varchar_value", varcharCol.Value(i))
						// bpchar may be padded, so check it starts with expected value
						bpcharVal := bpcharCol.Value(i)
						assert.True(t, len(bpcharVal) >= 6 && bpcharVal[:6] == "bpchar", "bpchar should start with 'bpchar'")
						assert.Equal(t, "name_val", nameCol.Value(i))
						// char_col should contain 'A'
						assert.Equal(t, "A", charCol.Value(i))
					case 2: // Second row - all empty strings
						assert.Empty(t, textCol.Value(i))
						assert.Empty(t, varcharCol.Value(i))
						// bpchar empty might be spaces, just check it's not null
						assert.False(t, bpcharCol.IsNull(i))
						assert.Empty(t, nameCol.Value(i))
						// char_col with '0' - just check it's not null
						assert.False(t, charCol.IsNull(i))
						assert.Equal(t, "0", charCol.Value(i))
					case 3: // Third row - all NULLs
						assert.True(t, textCol.IsNull(i))
						assert.True(t, varcharCol.IsNull(i))
						assert.True(t, bpcharCol.IsNull(i))
						assert.True(t, nameCol.IsNull(i))
						assert.True(t, charCol.IsNull(i))
					}
				}
			},
		},
		{
			name: "date_signed_integer_regression",
			setupSQL: `CREATE TABLE test_date_signed (val date); 
					   INSERT INTO test_date_signed VALUES 
					   ('1950-01-01'::date), ('1960-01-01'::date), ('1970-01-01'::date), 
					   ('1980-01-01'::date), ('1990-01-01'::date), ('1999-12-31'::date);`,
			querySQL:     "SELECT * FROM test_date_signed ORDER BY val",
			expectedRows: 6,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				dateCol, ok := record.Column(0).(*array.Date32)
				require.True(t, ok)

				// This test specifically verifies that dates before 2000-01-01
				// (PostgreSQL epoch) are handled correctly as signed integers.
				// The old uint32() cast would have corrupted negative values.

				expectedDates := []struct {
					date              string
					expectedArrowDays int32
				}{
					{"1950-01-01", -7305}, // Very negative
					{"1960-01-01", -3653}, // Negative
					{"1970-01-01", 0},     // Arrow epoch (critical boundary)
					{"1980-01-01", 3652},  // Positive
					{"1990-01-01", 7305},  // Positive
					{"1999-12-31", 10956}, // Just before PG epoch
				}

				require.Equal(t, len(expectedDates), int(record.NumRows()))

				for i, expected := range expectedDates {
					actualDays := int32(dateCol.Value(i))
					assert.Equal(t, expected.expectedArrowDays, actualDays,
						"Date %s should convert to %d Arrow days, got %d",
						expected.date, expected.expectedArrowDays, actualDays)
				}
			},
		},
		{
			name:         "interval_basic_values",
			setupSQL:     `CREATE TABLE test_interval (val interval); INSERT INTO test_interval VALUES ('1 year 2 months 3 days 4 hours 5 minutes 6.789 seconds'::interval), ('0'::interval), ('-1 year -2 months -3 days -4 hours -5 minutes -6.789 seconds'::interval), (null);`,
			querySQL:     "SELECT * FROM test_interval ORDER BY val NULLS LAST",
			expectedRows: 4,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				intervalCol, ok := record.Column(0).(*array.MonthDayNanoInterval)
				require.True(t, ok)

				values := make([]arrow.MonthDayNanoInterval, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !intervalCol.IsNull(i) {
						values = append(values, intervalCol.Value(i))
					}
				}

				// Check for expected patterns
				foundZero := false
				foundPositive := false
				foundNegative := false
				for _, val := range values {
					if val.Months == 0 && val.Days == 0 && val.Nanoseconds == 0 {
						foundZero = true
					} else if val.Months > 0 && val.Days > 0 && val.Nanoseconds > 0 {
						foundPositive = true
						// 1 year 2 months = 14 months, 3 days, 4:05:06.789 = 14706789000000 nanoseconds
						assert.Equal(t, int32(14), val.Months) // 1 year + 2 months
						assert.Equal(t, int32(3), val.Days)
						assert.Equal(t, int64(14706789000000), val.Nanoseconds) // 4*3600 + 5*60 + 6.789 seconds in nanoseconds
					} else if val.Months < 0 && val.Days < 0 && val.Nanoseconds < 0 {
						foundNegative = true
						assert.Equal(t, int32(-14), val.Months) // -1 year -2 months
						assert.Equal(t, int32(-3), val.Days)
						assert.Equal(t, int64(-14706789000000), val.Nanoseconds)
					}
				}

				assert.True(t, foundZero, "Should find zero interval")
				assert.True(t, foundPositive, "Should find positive interval")
				assert.True(t, foundNegative, "Should find negative interval")
				assert.True(t, intervalCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name:         "interval_edge_cases",
			setupSQL:     `CREATE TABLE test_interval_edge (val interval); INSERT INTO test_interval_edge VALUES ('999 years'::interval), ('999999 days'::interval), ('999999999 microseconds'::interval), ('0 seconds'::interval), (null);`,
			querySQL:     "SELECT * FROM test_interval_edge ORDER BY val NULLS LAST",
			expectedRows: 5,
			expectedCols: 1,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				intervalCol, ok := record.Column(0).(*array.MonthDayNanoInterval)
				require.True(t, ok)

				values := make([]arrow.MonthDayNanoInterval, 0)
				for i := 0; i < int(record.NumRows()); i++ {
					if !intervalCol.IsNull(i) {
						values = append(values, intervalCol.Value(i))
					}
				}

				// Check for expected patterns
				foundLargeMonths := false
				foundLargeDays := false
				foundLargeNanos := false
				foundZero := false
				for _, val := range values {
					if val.Months >= 11988 { // 999 years ≈ 11988 months
						foundLargeMonths = true
					} else if val.Days >= 999999 {
						foundLargeDays = true
					} else if val.Nanoseconds >= 999999000 { // 999999 microseconds = 999999000 nanoseconds
						foundLargeNanos = true
					} else if val.Months == 0 && val.Days == 0 && val.Nanoseconds == 0 {
						foundZero = true
					}
				}

				assert.True(t, foundLargeMonths, "Should find large months interval")
				assert.True(t, foundLargeDays, "Should find large days interval")
				assert.True(t, foundLargeNanos, "Should find large nanoseconds interval")
				assert.True(t, foundZero, "Should find zero interval")
				assert.True(t, intervalCol.IsNull(int(record.NumRows())-1)) // Last should be NULL
			},
		},
		{
			name: "mixed_types_with_interval",
			setupSQL: `CREATE TABLE test_mixed_interval (
						id int4, 
						name text, 
						duration interval, 
						active bool
					   ); 
					   INSERT INTO test_mixed_interval VALUES 
					   (1, 'short', '5 minutes'::interval, true),
					   (2, 'medium', '2 hours 30 minutes'::interval, false),
					   (3, 'long', '1 day 12 hours'::interval, true),
					   (4, 'null_duration', null, false);`,
			querySQL:     "SELECT * FROM test_mixed_interval ORDER BY id",
			expectedRows: 4,
			expectedCols: 4,
			validateFunc: func(t *testing.T, record arrow.Record) {
				t.Helper()
				// Verify schema
				schema := record.Schema()
				assert.Equal(t, "id", schema.Field(0).Name)
				assert.Equal(t, "name", schema.Field(1).Name)
				assert.Equal(t, "duration", schema.Field(2).Name)
				assert.Equal(t, "active", schema.Field(3).Name)

				// Extract columns
				idCol, ok := record.Column(0).(*array.Int32)
				require.True(t, ok)
				nameCol, ok := record.Column(1).(*array.String)
				require.True(t, ok)
				durationCol, ok := record.Column(2).(*array.MonthDayNanoInterval)
				require.True(t, ok)
				activeCol, ok := record.Column(3).(*array.Boolean)
				require.True(t, ok)

				// Verify data for each row
				for i := range int(record.NumRows()) {
					switch idCol.Value(i) {
					case 1: // Short interval
						assert.Equal(t, "short", nameCol.Value(i))
						duration := durationCol.Value(i)
						assert.Equal(t, int32(0), duration.Months)
						assert.Equal(t, int32(0), duration.Days)
						assert.Equal(t, int64(300000000000), duration.Nanoseconds) // 5 minutes = 300 seconds = 300000000000 nanoseconds
						assert.True(t, activeCol.Value(i))
					case 2: // Medium interval
						assert.Equal(t, "medium", nameCol.Value(i))
						duration := durationCol.Value(i)
						assert.Equal(t, int32(0), duration.Months)
						assert.Equal(t, int32(0), duration.Days)
						assert.Equal(t, int64(9000000000000), duration.Nanoseconds) // 2.5 hours = 9000 seconds = 9000000000000 nanoseconds
						assert.False(t, activeCol.Value(i))
					case 3: // Long interval
						assert.Equal(t, "long", nameCol.Value(i))
						duration := durationCol.Value(i)
						assert.Equal(t, int32(0), duration.Months)
						assert.Equal(t, int32(1), duration.Days)                     // 1 day
						assert.Equal(t, int64(43200000000000), duration.Nanoseconds) // 12 hours = 43200 seconds = 43200000000000 nanoseconds
						assert.True(t, activeCol.Value(i))
					case 4: // NULL interval
						assert.Equal(t, "null_duration", nameCol.Value(i))
						assert.True(t, durationCol.IsNull(i))
						assert.False(t, activeCol.Value(i))
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create isolated test environment with custom tables
			testPool, testCleanup := createIsolatedTestEnv(t, tt.setupSQL)
			defer testCleanup()

			// Execute query
			var reader array.RecordReader
			reader, err := testPool.QueryArrow(ctx, tt.querySQL)

			require.NoError(t, err, "Query failed for test %s", tt.name)
			require.NotNil(t, reader, "Reader should not be nil for test %s", tt.name)
			defer reader.Release()

			totalRows := int64(0)
			for reader.Next() {
				record := reader.Record()
				totalRows += record.NumRows()

				// Validate basic expectations for each batch
				assert.Equal(t, tt.expectedCols, record.NumCols(), "Column count mismatch for test %s", tt.name)

				// Run custom validation on each record batch
				if tt.validateFunc != nil {
					tt.validateFunc(t, record)
				}
			}

			// Check for reader errors
			require.NoError(t, reader.Err())

			// Validate total row count
			assert.Equal(t, tt.expectedRows, totalRows, "Row count mismatch for test %s", tt.name)
		})
	}
}
