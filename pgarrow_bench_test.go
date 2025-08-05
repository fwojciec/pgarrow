package pgarrow_test

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// getBenchDatabaseURL returns the benchmark database URL, skipping if not available
func getBenchDatabaseURL(b *testing.B) string {
	b.Helper()

	// Reuse same logic as test helpers
	if dbURL := os.Getenv("TEST_DATABASE_URL"); dbURL != "" {
		return dbURL
	}
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" {
		return dbURL
	}
	b.Skip("No database URL provided. Set TEST_DATABASE_URL or DATABASE_URL to run benchmarks.")
	return ""
}

// consumeArrowReader iterates through all records and returns total rows processed
func consumeArrowReader(b *testing.B, reader array.RecordReader) int64 {
	b.Helper()
	var totalRows int64
	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()
		record.Release()
	}
	require.NoError(b, reader.Err())
	return totalRows
}

// consumePgxRows iterates through pgx rows and returns count
func consumePgxRows(b *testing.B, rows pgx.Rows) int64 {
	b.Helper()
	var count int64
	for rows.Next() {
		count++
		_ = rows.RawValues() // Access data to ensure fair comparison
	}
	require.NoError(b, rows.Err())
	rows.Close()
	return count
}

// BenchmarkThroughput measures rows/second processing capability
func BenchmarkThroughput(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	// Check if we have performance_test table first
	conn, err := pgx.Connect(ctx, databaseURL)
	require.NoError(b, err)

	var hasPerformanceTable bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_name = 'performance_test'
		)
	`).Scan(&hasPerformanceTable)
	conn.Close(ctx)
	require.NoError(b, err)

	pool, err := pgarrow.NewPool(ctx, databaseURL,
		pgarrow.WithAllocator(memory.NewGoAllocator()))
	require.NoError(b, err)
	defer pool.Close()

	// Test cases
	testCases := []struct {
		name string
		rows int
		sql  string
	}{
		{
			name: "100K_rows_generate_series",
			rows: 100000,
			sql: `SELECT 
				i::int8 as id,
				(i * 3.14)::float8 as score,
				(i % 2 = 0)::bool as active,
				'user_' || i::text as name,
				'2023-01-01'::date + (i % 365) as created_date
			FROM generate_series(1, 100000) i`,
		},
		{
			name: "1M_rows_generate_series",
			rows: 1000000,
			sql: `SELECT 
				i::int8 as id,
				(i * 3.14)::float8 as score,
				(i % 2 = 0)::bool as active,
				'user_' || i::text as name,
				'2023-01-01'::date + (i % 365) as created_date
			FROM generate_series(1, 1000000) i`,
		},
	}

	// Add performance_test table benchmarks if available
	if hasPerformanceTable {
		testCases = append(testCases,
			struct {
				name string
				rows int
				sql  string
			}{
				name: "1M_rows_with_order",
				rows: 1000000,
				sql: `SELECT id, score, active, name, created_date 
				      FROM performance_test 
				      ORDER BY id 
				      LIMIT 1000000`,
			},
			struct {
				name string
				rows int
				sql  string
			}{
				name: "1M_rows_no_order",
				rows: 1000000,
				sql: `SELECT id, score, active, name, created_date 
				      FROM performance_test 
				      LIMIT 1000000`,
			},
		)
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Warm up
			reader, err := pool.QueryArrow(ctx, tc.sql)
			require.NoError(b, err)
			_ = consumeArrowReader(b, reader)
			reader.Release()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				start := time.Now()
				reader, err := pool.QueryArrow(ctx, tc.sql)
				require.NoError(b, err)

				totalRows := int64(0)
				batchCount := 0
				for reader.Next() {
					record := reader.Record()
					totalRows += record.NumRows()
					batchCount++
					record.Release()
				}
				require.NoError(b, reader.Err())
				reader.Release()

				elapsed := time.Since(start)

				// Report metrics
				rowsPerSec := float64(totalRows) / elapsed.Seconds()
				b.ReportMetric(rowsPerSec/1000000, "Mrows/sec")
				b.ReportMetric(float64(batchCount), "batches")
				b.ReportMetric(float64(totalRows)/float64(batchCount), "rows/batch")
			}
		})
	}
}

// BenchmarkQueryComparison compares PGArrow vs pgx for various scenarios
func BenchmarkQueryComparison(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	scenarios := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "simple_types",
			sql:  "SELECT $1::int4 as id, $2::text as name, $3::bool as active",
			args: []any{1, "test", true},
		},
		{
			name: "all_types",
			sql: `SELECT 
				$1::bool, $2::int2, $3::int4, $4::int8,
				$5::float4, $6::float8, $7::text`,
			args: []any{true, int16(123), int32(456789), int64(123456789012),
				float32(3.14), 2.718281828, "Hello World"},
		},
		{
			name: "small_result",
			sql:  "SELECT i, 'row_' || i::text FROM generate_series($1, $2) i",
			args: []any{1, 100},
		},
		{
			name: "medium_result",
			sql:  "SELECT i, 'row_' || i::text FROM generate_series($1, $2) i",
			args: []any{1, 10000},
		},
		{
			name: "with_nulls",
			sql: `SELECT * FROM (VALUES 
				(1, 'Alice', 25.5::float8, true),
				(2, NULL, 30.0::float8, false),
				(NULL, 'Charlie', NULL, true),
				(4, 'Diana', 28.7::float8, NULL)
			) AS t(id, name, score, active)`,
		},
	}

	// Setup pools once
	pgArrowPool, err := pgarrow.NewPool(ctx, databaseURL)
	require.NoError(b, err)
	defer pgArrowPool.Close()

	pgxConfig, err := pgxpool.ParseConfig(databaseURL)
	require.NoError(b, err)
	pgxPool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	require.NoError(b, err)
	defer pgxPool.Close()

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			b.Run("pgarrow", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					reader, err := pgArrowPool.QueryArrow(ctx, sc.sql, sc.args...)
					require.NoError(b, err)
					rows := consumeArrowReader(b, reader)
					reader.Release()
					b.ReportMetric(float64(rows), "rows")
				}
			})

			b.Run("pgx", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					rows, err := pgxPool.Query(ctx, sc.sql, sc.args...)
					require.NoError(b, err)
					count := consumePgxRows(b, rows)
					b.ReportMetric(float64(count), "rows")
				}
			})
		})
	}
}

// BenchmarkTypeConversion measures conversion performance for each type
func BenchmarkTypeConversion(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	pool, err := pgarrow.NewPool(ctx, databaseURL)
	require.NoError(b, err)
	defer pool.Close()

	types := []struct {
		name  string
		sql   string
		check func(arrow.Array) // Optional validation
	}{
		{"bool", "SELECT true::bool FROM generate_series(1, 1000)", nil},
		{"int2", "SELECT 123::int2 FROM generate_series(1, 1000)", nil},
		{"int4", "SELECT 456789::int4 FROM generate_series(1, 1000)", nil},
		{"int8", "SELECT 123456789012::int8 FROM generate_series(1, 1000)", nil},
		{"float4", "SELECT 3.14::float4 FROM generate_series(1, 1000)", nil},
		{"float8", "SELECT 2.718281828::float8 FROM generate_series(1, 1000)", nil},
		{"text", "SELECT 'Hello World'::text FROM generate_series(1, 1000)", nil},
		{"varchar", "SELECT 'varchar test'::varchar(50) FROM generate_series(1, 1000)", nil},
		{"date", "SELECT '2023-01-01'::date FROM generate_series(1, 1000)", nil},
		{"timestamp", "SELECT '2023-01-01 12:00:00'::timestamp FROM generate_series(1, 1000)", nil},
		{"timestamptz", "SELECT '2023-01-01 12:00:00+00'::timestamptz FROM generate_series(1, 1000)", nil},
	}

	for _, tt := range types {
		b.Run(tt.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				reader, err := pool.QueryArrow(ctx, tt.sql)
				require.NoError(b, err)

				// Process data to ensure conversion happens
				for reader.Next() {
					record := reader.Record()
					if tt.check != nil && i == 0 { // Validate once
						tt.check(record.Column(0))
					}
					record.Release()
				}
				reader.Release()
			}
		})
	}
}

// BenchmarkMemoryAllocation compares allocation patterns
func BenchmarkMemoryAllocation(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	queries := []struct {
		name string
		sql  string
		rows int
	}{
		{"100_rows", "SELECT i, 'data_' || i::text FROM generate_series(1, 100) i", 100},
		{"1K_rows", "SELECT i, 'data_' || i::text FROM generate_series(1, 1000) i", 1000},
		{"10K_rows", "SELECT i, 'data_' || i::text FROM generate_series(1, 10000) i", 10000},
	}

	for _, q := range queries {
		b.Run(q.name, func(b *testing.B) {
			b.Run("pgarrow", func(b *testing.B) {
				pool, err := pgarrow.NewPool(ctx, databaseURL,
					pgarrow.WithAllocator(memory.NewGoAllocator()))
				require.NoError(b, err)
				defer pool.Close()

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					reader, err := pool.QueryArrow(ctx, q.sql)
					require.NoError(b, err)
					rows := consumeArrowReader(b, reader)
					require.Equal(b, int64(q.rows), rows)
					reader.Release()
				}
			})

			b.Run("pgx", func(b *testing.B) {
				conn, err := pgx.Connect(ctx, databaseURL)
				require.NoError(b, err)
				defer conn.Close(ctx)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					rows, err := conn.Query(ctx, q.sql)
					require.NoError(b, err)

					count := 0
					for rows.Next() {
						var id int32
						var text string
						err := rows.Scan(&id, &text)
						require.NoError(b, err)
						count++
					}
					rows.Close()
					require.Equal(b, q.rows, count)
				}
			})
		})
	}
}

// BenchmarkConnectionSetup measures initialization overhead
func BenchmarkConnectionSetup(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	b.Run("pgarrow_pool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool, err := pgarrow.NewPool(ctx, databaseURL)
			require.NoError(b, err)
			pool.Close()
		}
	})

	b.Run("pgx_connect", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn, err := pgx.Connect(ctx, databaseURL)
			require.NoError(b, err)
			conn.Close(ctx)
		}
	})

	b.Run("pgx_pool", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool, err := pgxpool.New(ctx, databaseURL)
			require.NoError(b, err)
			pool.Close()
		}
	})
}

// BenchmarkPostgresBinaryParsing tests our approach to parsing PostgreSQL binary format
func BenchmarkPostgresBinaryParsing(b *testing.B) {
	// Simulate PostgreSQL binary format data
	// These would come from pgx RawValues() in real usage

	// int8 (8 bytes, big-endian)
	int8Data := make([]byte, 8)
	binary.BigEndian.PutUint64(int8Data, uint64(12345678))

	// float8 (8 bytes, IEEE 754 double)
	float8Data := make([]byte, 8)
	binary.BigEndian.PutUint64(float8Data, math.Float64bits(3.14159))

	// bool (1 byte, 0 or 1)
	boolTrue := []byte{1}
	boolFalse := []byte{0}

	// text (UTF-8 bytes)
	textData := []byte("Hello, PostgreSQL!")

	// date (4 bytes, days since 2000-01-01)
	dateData := make([]byte, 4)
	binary.BigEndian.PutUint32(dateData, 7305) // ~20 years

	// timestamp (8 bytes, microseconds since 2000-01-01)
	timestampData := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampData, 631152000000000) // ~20 years in microseconds

	b.Run("int8_binary_parse", func(b *testing.B) {
		b.ReportAllocs()
		var sum int64
		for i := 0; i < b.N; i++ {
			// Our actual parsing code from select_parser.go
			val := int64(binary.BigEndian.Uint64(int8Data))
			sum += val
		}
		_ = sum
	})

	b.Run("float8_binary_parse", func(b *testing.B) {
		b.ReportAllocs()
		var sum float64
		for i := 0; i < b.N; i++ {
			// Our actual parsing code
			bits := binary.BigEndian.Uint64(float8Data)
			val := math.Float64frombits(bits)
			sum += val
		}
		_ = sum
	})

	b.Run("bool_binary_parse", func(b *testing.B) {
		b.ReportAllocs()
		var trueCount, falseCount int
		for i := 0; i < b.N; i++ {
			// Our actual parsing code
			if boolTrue[0] != 0 {
				trueCount++
			}
			if boolFalse[0] == 0 {
				falseCount++
			}
		}
		_ = trueCount
		_ = falseCount
	})

	b.Run("text_binary_parse", func(b *testing.B) {
		b.ReportAllocs()
		var s string
		for i := 0; i < b.N; i++ {
			// Simplified parsing for benchmark - actual production parsing of PostgreSQL wire format text is more complex
			s = string(textData)
		}
		_ = s
	})

	b.Run("date_binary_parse", func(b *testing.B) {
		b.ReportAllocs()
		var sum int32
		for i := 0; i < b.N; i++ {
			// Our actual parsing code
			pgDays := int32(binary.BigEndian.Uint32(dateData))
			arrowDays := pgDays + 10957 // PostgreSQL epoch adjustment
			sum += arrowDays
		}
		_ = sum
	})

	b.Run("timestamp_binary_parse", func(b *testing.B) {
		b.ReportAllocs()
		var sum int64
		for i := 0; i < b.N; i++ {
			// Our actual parsing code
			pgMicros := int64(binary.BigEndian.Uint64(timestampData))
			// Convert to nanoseconds and adjust epoch
			const microToNano = 1000
			const epochOffsetMicros = 946684800000000
			arrowNanos := (pgMicros + epochOffsetMicros) * microToNano
			sum += arrowNanos
		}
		_ = sum
	})

}
