package pgarrow_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
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

// BenchmarkMetadataDiscovery measures the overhead of metadata discovery per query
func BenchmarkMetadataDiscovery(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	pool, err := pgarrow.NewPool(ctx, databaseURL,
		pgarrow.WithAllocator(memory.NewGoAllocator()))
	require.NoError(b, err)
	defer pool.Close()

	// Also create a pgx pool for acquiring connections
	pgxPool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(b, err)
	defer pgxPool.Close()

	// Test queries with different column counts
	queries := []struct {
		name string
		sql  string
	}{
		{
			name: "simple_1_column",
			sql:  "SELECT 1::int8",
		},
		{
			name: "simple_5_columns",
			sql:  "SELECT 1::int8, 2::int4, 3.14::float8, true::bool, 'text'::text",
		},
		{
			name: "table_5_columns",
			sql: `SELECT 
				i::int8 as id,
				(i * 3.14)::float8 as score,
				(i % 2 = 0)::bool as active,
				'user_' || i::text as name,
				'2023-01-01'::date + i as created_date
			FROM generate_series(1, 10) i`,
		},
	}

	for _, q := range queries {
		b.Run(q.name, func(b *testing.B) {
			// Acquire connection once for all iterations
			conn, err := pgxPool.Acquire(ctx)
			require.NoError(b, err)
			defer conn.Release()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				schema, _, err := pool.GetQueryMetadata(ctx, conn, q.sql)
				if err != nil {
					b.Fatal(err)
				}
				_ = schema // Prevent compiler optimization
			}
		})
	}
}

// BenchmarkMetadataByColumnCount measures how metadata discovery scales with column count
func BenchmarkMetadataByColumnCount(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	pool, err := pgarrow.NewPool(ctx, databaseURL,
		pgarrow.WithAllocator(memory.NewGoAllocator()))
	require.NoError(b, err)
	defer pool.Close()

	// Also create a pgx pool for acquiring connections
	pgxPool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(b, err)
	defer pgxPool.Close()

	// Test cases with increasing column counts
	columnCounts := []int{1, 5, 10, 25, 50}

	for _, colCount := range columnCounts {
		b.Run(fmt.Sprintf("%d_columns", colCount), func(b *testing.B) {
			// Build query with specified number of columns
			var sql string
			if colCount == 1 {
				sql = "SELECT 1::int8"
			} else {
				cols := make([]string, colCount)
				for i := 0; i < colCount; i++ {
					// Mix of data types
					switch i % 5 {
					case 0:
						cols[i] = fmt.Sprintf("%d::int8 as col_%d", i, i)
					case 1:
						cols[i] = fmt.Sprintf("%d::int4 as col_%d", i, i)
					case 2:
						cols[i] = fmt.Sprintf("%f::float8 as col_%d", float64(i)*3.14, i)
					case 3:
						cols[i] = fmt.Sprintf("true::bool as col_%d", i)
					case 4:
						cols[i] = fmt.Sprintf("'text_%d'::text as col_%d", i, i)
					}
				}
				sql = "SELECT " + strings.Join(cols, ", ")
			}

			conn, err := pgxPool.Acquire(ctx)
			require.NoError(b, err)
			defer conn.Release()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				schema, _, err := pool.GetQueryMetadata(ctx, conn, sql)
				if err != nil {
					b.Fatal(err)
				}
				if schema.NumFields() != colCount {
					b.Fatalf("expected %d columns, got %d", colCount, schema.NumFields())
				}
			}
		})
	}
}

// BenchmarkRepeatedSameQuery measures overhead when running the same query multiple times
func BenchmarkRepeatedSameQuery(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	pool, err := pgarrow.NewPool(ctx, databaseURL,
		pgarrow.WithAllocator(memory.NewGoAllocator()))
	require.NoError(b, err)
	defer pool.Close()

	// Use a typical query
	sql := `SELECT 
		i::int8 as id,
		(i * 3.14)::float8 as score,
		(i % 2 = 0)::bool as active,
		'user_' || i::text as name,
		'2023-01-01'::date + i as created_date
	FROM generate_series(1, 1000) i`

	b.Run("metadata_discovery_per_query", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Each iteration includes full metadata discovery
			reader, err := pool.QueryArrow(ctx, sql)
			if err != nil {
				b.Fatal(err)
			}
			// Consume results to make it realistic
			_ = consumeArrowReader(b, reader)
			reader.Release()
		}
	})

	// For comparison, also benchmark just the query execution without measuring metadata separately
	b.Run("full_query_execution", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader, err := pool.QueryArrow(ctx, sql)
			if err != nil {
				b.Fatal(err)
			}
			_ = consumeArrowReader(b, reader)
			reader.Release()
		}
	})
}

// BenchmarkQueryWithMetadataComparison attempts to show the difference between
// queries with and without metadata discovery overhead
func BenchmarkQueryWithMetadataComparison(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)
	ctx := context.Background()

	pool, err := pgarrow.NewPool(ctx, databaseURL,
		pgarrow.WithAllocator(memory.NewGoAllocator()))
	require.NoError(b, err)
	defer pool.Close()

	// Also create a pgx pool for acquiring connections
	pgxPool, err := pgxpool.New(ctx, databaseURL)
	require.NoError(b, err)
	defer pgxPool.Close()

	// Simple query for minimal execution overhead
	sql := "SELECT i::int8 FROM generate_series(1, 100) i"

	b.Run("with_metadata_discovery", func(b *testing.B) {
		conn, err := pgxPool.Acquire(ctx)
		require.NoError(b, err)
		defer conn.Release()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Measure just metadata discovery
			start := time.Now()
			schema, _, err := pool.GetQueryMetadata(ctx, conn, sql)
			metadataTime := time.Since(start)
			if err != nil {
				b.Fatal(err)
			}
			_ = schema
			b.ReportMetric(float64(metadataTime.Nanoseconds()), "metadata_ns/op")
		}
	})

	b.Run("query_execution_only", func(b *testing.B) {
		// Pre-discover metadata once
		conn, err := pgxPool.Acquire(ctx)
		require.NoError(b, err)
		defer conn.Release()

		schema, _, err := pool.GetQueryMetadata(ctx, conn, sql)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// This simulates what execution would be like if metadata was cached
			// Note: We can't actually bypass metadata discovery in the current implementation
			// but we can measure the full query and subtract metadata time
			reader, err := pool.QueryArrow(ctx, sql)
			if err != nil {
				b.Fatal(err)
			}
			_ = consumeArrowReader(b, reader)
			reader.Release()
		}
		_ = schema // Use schema to prevent optimization
	})
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
