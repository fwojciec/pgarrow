package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
)

func main() {
	pool := setupPool()
	defer pool.Close()

	fmt.Println("=== PGArrow Supported Data Types Demo ===")

	// Demonstrate all 7 supported types
	demonstrateAllTypes(pool)

	// Demonstrate NULL value handling
	demonstrateNullHandling(pool)

	// Demonstrate mixed data with some NULLs
	demonstrateMixedData(pool)

	fmt.Println("\nPGArrow types example completed successfully!")
}

func setupPool() *pgarrow.Pool {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	// Create PGArrow pool with memory tracking
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer func() {
		if alloc.CurrentAlloc() != 0 {
			fmt.Printf("Warning: %d bytes still allocated\n", alloc.CurrentAlloc())
		}
	}()

	pool, err := pgarrow.NewPool(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}
	return pool
}

func demonstrateAllTypes(pool *pgarrow.Pool) {
	query := `
		SELECT 
			true as bool_col,                    -- bool (OID 16)
			123::int2 as int2_col,              -- int2/smallint (OID 21)  
			456789::int4 as int4_col,           -- int4/integer (OID 23)
			123456789012::int8 as int8_col,     -- int8/bigint (OID 20)
			3.14::float4 as float4_col,         -- float4/real (OID 700)
			2.718281828::float8 as float8_col,  -- float8/double precision (OID 701)
			'Hello PGArrow!'::text as text_col  -- text (OID 25)
	`

	reader, err := pool.QueryArrow(context.Background(), query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	fmt.Printf("Schema: %s\n\n", reader.Schema())

	for reader.Next() {
		record := reader.Record()
		fmt.Printf("Batch returned %d rows, %d columns\n", record.NumRows(), record.NumCols())
		displayTypedResults(record)
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Reader error: %v", err)
	}
}

func displayTypedResults(record arrow.Record) {
	// Extract typed columns with error checking
	cols := extractTypedColumns(record)

	// Display results
	for i := 0; i < int(record.NumRows()); i++ {
		fmt.Printf("Row %d:\n", i)
		fmt.Printf("  bool:   %t\n", cols.boolCol.Value(i))
		fmt.Printf("  int2:   %d\n", cols.int2Col.Value(i))
		fmt.Printf("  int4:   %d\n", cols.int4Col.Value(i))
		fmt.Printf("  int8:   %d\n", cols.int8Col.Value(i))
		fmt.Printf("  float4: %.6f\n", cols.float4Col.Value(i))
		fmt.Printf("  float8: %.9f\n", cols.float8Col.Value(i))
		fmt.Printf("  text:   %s\n", cols.textCol.Value(i))
	}
}

type typedColumns struct {
	boolCol   *array.Boolean
	int2Col   *array.Int16
	int4Col   *array.Int32
	int8Col   *array.Int64
	float4Col *array.Float32
	float8Col *array.Float64
	textCol   *array.String
}

func extractTypedColumns(record arrow.Record) typedColumns {
	boolCol, ok := record.Column(0).(*array.Boolean)
	if !ok {
		log.Fatal("Failed to cast column 0 to Boolean")
	}
	int2Col, ok := record.Column(1).(*array.Int16)
	if !ok {
		log.Fatal("Failed to cast column 1 to Int16")
	}
	int4Col, ok := record.Column(2).(*array.Int32)
	if !ok {
		log.Fatal("Failed to cast column 2 to Int32")
	}
	int8Col, ok := record.Column(3).(*array.Int64)
	if !ok {
		log.Fatal("Failed to cast column 3 to Int64")
	}
	float4Col, ok := record.Column(4).(*array.Float32)
	if !ok {
		log.Fatal("Failed to cast column 4 to Float32")
	}
	float8Col, ok := record.Column(5).(*array.Float64)
	if !ok {
		log.Fatal("Failed to cast column 5 to Float64")
	}
	textCol, ok := record.Column(6).(*array.String)
	if !ok {
		log.Fatal("Failed to cast column 6 to String")
	}

	return typedColumns{boolCol, int2Col, int4Col, int8Col, float4Col, float8Col, textCol}
}

func demonstrateNullHandling(pool *pgarrow.Pool) {
	fmt.Println("\n=== NULL Value Handling ===")
	nullQuery := `
		SELECT 
			NULL::bool as null_bool,
			NULL::int2 as null_int2,
			NULL::int4 as null_int4,
			NULL::int8 as null_int8,
			NULL::float4 as null_float4,
			NULL::float8 as null_float8,
			NULL::text as null_text
	`

	reader, err := pool.QueryArrow(context.Background(), nullQuery)
	if err != nil {
		log.Fatalf("NULL query failed: %v", err)
	}
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		record := reader.Record()
		totalRows += int(record.NumRows())

		// Check nulls in each column
		for i := 0; i < int(record.NumCols()); i++ {
			col := record.Column(i)
			colName := record.Schema().Field(i).Name
			nullCount := col.NullN()
			fmt.Printf("Column '%s': %d null values out of %d total\n",
				colName, nullCount, col.Len())
		}
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Reader error: %v", err)
	}

	fmt.Printf("NULL query returned %d rows\n", totalRows)
}

func demonstrateMixedData(pool *pgarrow.Pool) {
	fmt.Println("\n=== Mixed Data with NULLs ===")
	mixedQuery := `
		SELECT * FROM (VALUES 
			(1, 'Alice', 25.5::float8, true),
			(2, NULL, 30.0::float8, false),
			(NULL, 'Charlie', NULL::float8, true),
			(4, 'Diana', 28.7::float8, NULL)
		) AS mixed_data(id, name, score, active)
	`

	reader, err := pool.QueryArrow(context.Background(), mixedQuery)
	if err != nil {
		log.Fatalf("Mixed query failed: %v", err)
	}
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		record := reader.Record()
		totalRows += int(record.NumRows())
		printMixedResults(record)
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Reader error: %v", err)
	}

	fmt.Printf("Mixed data query returned %d rows\n", totalRows)
}

func printMixedResults(record arrow.Record) {
	// Access mixed data columns (we know these types from the query)
	mixedIDCol, ok := record.Column(0).(*array.Int32)
	if !ok {
		log.Fatal("Failed to cast mixed column 0 to Int32")
	}
	mixedNameCol, ok := record.Column(1).(*array.String)
	if !ok {
		log.Fatal("Failed to cast mixed column 1 to String")
	}
	mixedScoreCol, ok := record.Column(2).(*array.Float64)
	if !ok {
		log.Fatal("Failed to cast mixed column 2 to Float64")
	}
	mixedActiveCol, ok := record.Column(3).(*array.Boolean)
	if !ok {
		log.Fatal("Failed to cast mixed column 3 to Boolean")
	}

	for i := 0; i < int(record.NumRows()); i++ {
		fmt.Printf("Row %d: ", i)

		if mixedIDCol.IsNull(i) {
			fmt.Print("id=NULL, ")
		} else {
			fmt.Printf("id=%d, ", mixedIDCol.Value(i))
		}

		if mixedNameCol.IsNull(i) {
			fmt.Print("name=NULL, ")
		} else {
			fmt.Printf("name=%s, ", mixedNameCol.Value(i))
		}

		if mixedScoreCol.IsNull(i) {
			fmt.Print("score=NULL, ")
		} else {
			fmt.Printf("score=%.1f, ", mixedScoreCol.Value(i))
		}

		if mixedActiveCol.IsNull(i) {
			fmt.Print("active=NULL")
		} else {
			fmt.Printf("active=%t", mixedActiveCol.Value(i))
		}

		fmt.Println()
	}
}
