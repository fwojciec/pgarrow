package pgarrow_test

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// BenchmarkQueryArrowVsPgxText compares PGArrow binary format parsing
// against pgx text format parsing for various query types
func BenchmarkQueryArrowVsPgxText(b *testing.B) {
	// Skip if no database available
	databaseURL := getBenchDatabaseURL(b)

	// Create PGArrow pool
	pool, err := pgarrow.NewPool(context.Background(), databaseURL)
	require.NoError(b, err)
	defer pool.Close()

	// Create regular pgx connection for text comparison
	conn, err := pgx.Connect(context.Background(), databaseURL)
	require.NoError(b, err)
	defer conn.Close(context.Background())

	ctx := context.Background()

	benchmarks := []struct {
		name string
		sql  string
	}{
		{
			name: "SimpleTypes",
			sql:  "SELECT 1::int4 as id, 'test'::text as name, true::bool as active",
		},
		{
			name: "AllSupportedTypes",
			sql: `SELECT 
				true::bool as col_bool,
				123::int2 as col_int2,
				456789::int4 as col_int4,
				123456789012::int8 as col_int8,
				3.14::float4 as col_float4,
				2.718281828::float8 as col_float8,
				'Hello World'::text as col_text`,
		},
		{
			name: "SmallResultSet",
			sql:  "SELECT i as id, 'user_' || i::text as name FROM generate_series(1, 10) i",
		},
		{
			name: "MediumResultSet",
			sql:  "SELECT i as id, 'user_' || i::text as name FROM generate_series(1, 100) i",
		},
		{
			name: "LargeResultSet",
			sql:  "SELECT i as id, 'user_' || i::text as name FROM generate_series(1, 1000) i",
		},
		{
			name: "MixedTypesWithNulls",
			sql: `SELECT * FROM (VALUES 
				(1, 'Alice', 25.5::float8, true),
				(2, NULL, 30.0::float8, false),
				(NULL, 'Charlie', NULL, true),
				(4, 'Diana', 28.7::float8, NULL)
			) AS data(id, name, score, active)`,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.Run("PGArrow", func(b *testing.B) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer func() {
					if alloc.CurrentAlloc() != 0 {
						b.Errorf("Memory leak detected: %d bytes", alloc.CurrentAlloc())
					}
				}()

				b.ResetTimer()
				for b.Loop() {
					reader, err := pool.QueryArrow(ctx, bm.sql)
					if err != nil {
						b.Fatalf("PGArrow query failed: %v", err)
					}

					// Simulate accessing data to ensure fair comparison
					totalRows := int64(0)
					totalCols := int64(0)
					for reader.Next() {
						record := reader.Record()
						totalRows += record.NumRows()
						totalCols = record.NumCols()
					}
					_ = totalRows
					_ = totalCols

					reader.Release()
				}
			})

			b.Run("PgxText", func(b *testing.B) {
				b.ResetTimer()
				for b.Loop() {
					rows, err := conn.Query(ctx, bm.sql)
					if err != nil {
						b.Fatalf("pgx query failed: %v", err)
					}

					rowCount := 0
					for rows.Next() {
						rowCount++
						// Simulate accessing data
						values := rows.RawValues()
						_ = len(values)
					}
					rows.Close()
				}
			})
		})
	}
}

// BenchmarkMemoryUsage compares memory allocation patterns
func BenchmarkMemoryUsage(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)

	pool, err := pgarrow.NewPool(context.Background(), databaseURL)
	require.NoError(b, err)
	defer pool.Close()

	conn, err := pgx.Connect(context.Background(), databaseURL)
	require.NoError(b, err)
	defer conn.Close(context.Background())

	ctx := context.Background()
	sql := "SELECT i as id, 'data_' || i::text as text_data FROM generate_series(1, 100) i"

	b.Run("PGArrow", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			reader, err := pool.QueryArrow(ctx, sql)
			if err != nil {
				b.Fatalf("PGArrow query failed: %v", err)
			}
			for reader.Next() {
				// Process the record
				_ = reader.Record()
			}
			reader.Release()
		}
	})

	b.Run("PgxText", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			rows, err := conn.Query(ctx, sql)
			if err != nil {
				b.Fatalf("pgx query failed: %v", err)
			}

			for rows.Next() {
				var id int32
				var text string
				err := rows.Scan(&id, &text)
				if err != nil {
					b.Fatalf("scan failed: %v", err)
				}
				// Use the values to ensure they're processed
				_ = id
				_ = text
			}
			rows.Close()
		}
	})
}

// BenchmarkConnectionInitialization tests the key performance benefit:
// instant connections vs metadata preloading
func BenchmarkConnectionInitialization(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)

	b.Run("PGArrow", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			pool, err := pgarrow.NewPool(context.Background(), databaseURL)
			if err != nil {
				b.Fatalf("Failed to create pool: %v", err)
			}
			pool.Close()
		}
	})

	// Note: We can't benchmark ADBC initialization here since it's not implemented,
	// but this benchmark shows PGArrow's instant connection capability
	b.Run("PgxPool", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			conn, err := pgx.Connect(context.Background(), databaseURL)
			if err != nil {
				b.Fatalf("Failed to connect: %v", err)
			}
			conn.Close(context.Background())
		}
	})
}

// getBenchDatabaseURL returns the test database URL, skipping the benchmark if not available
func getBenchDatabaseURL(b *testing.B) string {
	b.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		b.Skip("No database URL provided. Set TEST_DATABASE_URL or DATABASE_URL environment variable to run benchmarks.")
	}
	return databaseURL
}

// BenchmarkDataTypeConversion focuses on type conversion performance
func BenchmarkDataTypeConversion(b *testing.B) {
	databaseURL := getBenchDatabaseURL(b)

	pool, err := pgarrow.NewPool(context.Background(), databaseURL)
	require.NoError(b, err)
	defer pool.Close()

	conn, err := pgx.Connect(context.Background(), databaseURL)
	require.NoError(b, err)
	defer conn.Close(context.Background())

	ctx := context.Background()

	// Test each data type individually for fine-grained performance analysis
	typeTests := []struct {
		name string
		sql  string
	}{
		{"Bool", "SELECT true::bool FROM generate_series(1, 100)"},
		{"Int2", "SELECT 123::int2 FROM generate_series(1, 100)"},
		{"Int4", "SELECT 456789::int4 FROM generate_series(1, 100)"},
		{"Int8", "SELECT 123456789012::int8 FROM generate_series(1, 100)"},
		{"Float4", "SELECT 3.14::float4 FROM generate_series(1, 100)"},
		{"Float8", "SELECT 2.718281828::float8 FROM generate_series(1, 100)"},
		{"Text", "SELECT 'Hello World'::text FROM generate_series(1, 100)"},
	}

	for _, tt := range typeTests {
		b.Run(tt.name, func(b *testing.B) {
			b.Run("PGArrow", func(b *testing.B) {
				b.ResetTimer()
				for b.Loop() {
					reader, err := pool.QueryArrow(ctx, tt.sql)
					if err != nil {
						b.Fatalf("Query failed: %v", err)
					}
					// Access the data to ensure conversion happens
					for reader.Next() {
						record := reader.Record()
						col := record.Column(0)
						_ = col.Len()
					}
					reader.Release()
				}
			})

			b.Run("PgxText", func(b *testing.B) {
				b.ResetTimer()
				for b.Loop() {
					rows, err := conn.Query(ctx, tt.sql)
					if err != nil {
						b.Fatalf("Query failed: %v", err)
					}

					for rows.Next() {
						var val any
						err := rows.Scan(&val)
						if err != nil {
							b.Fatalf("Scan failed: %v", err)
						}
					}
					rows.Close()
				}
			})
		})
	}
}
