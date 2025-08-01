package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
)

func main() {
	pool := setupSimplePool()
	defer pool.Close()

	// Simple query demonstrating basic usage
	runBasicQuery(pool)

	// Example with table data
	runTableQuery(pool)

	// Memory tracking demonstration
	demonstrateMemoryTracking()

	fmt.Println("\nPGArrow simple example completed successfully!")
}

func setupSimplePool() *pgarrow.Pool {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	pool, err := pgarrow.NewPool(context.Background(), databaseURL)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}
	return pool
}

func runBasicQuery(pool *pgarrow.Pool) {
	reader, err := pool.QueryArrow(context.Background(), "SELECT 1 as id, 'Hello World' as message, true as active")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer reader.Release()

	fmt.Printf("Schema: %s\n", reader.Schema())

	// Iterate through record batches
	for reader.Next() {
		record := reader.Record()
		fmt.Printf("Batch returned %d rows, %d columns\n", record.NumRows(), record.NumCols())

		// Access data by column
		idCol, ok := record.Column(0).(*array.Int32)
		if !ok {
			log.Fatal("Failed to cast column 0 to Int32")
		}
		messageCol, ok := record.Column(1).(*array.String)
		if !ok {
			log.Fatal("Failed to cast column 1 to String")
		}
		activeCol, ok := record.Column(2).(*array.Boolean)
		if !ok {
			log.Fatal("Failed to cast column 2 to Boolean")
		}

		// Print results
		for i := 0; i < int(record.NumRows()); i++ {
			fmt.Printf("Row %d: id=%d, message=%s, active=%t\n",
				i, idCol.Value(i), messageCol.Value(i), activeCol.Value(i))
		}
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Reader error: %v", err)
	}
}

func runTableQuery(pool *pgarrow.Pool) {
	fmt.Println("\n--- Table Query Example ---")
	reader, err := pool.QueryArrow(context.Background(),
		"SELECT id, name FROM (VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')) AS users(id, name)")
	if err != nil {
		log.Fatalf("Table query failed: %v", err)
	}
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		record := reader.Record()
		totalRows += int(record.NumRows())
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Reader error: %v", err)
	}

	fmt.Printf("Table query returned %d rows\n", totalRows)
}

func demonstrateMemoryTracking() {
	// Use memory.CheckedAllocator for production code to track memory usage
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer func() {
		if alloc.CurrentAlloc() != 0 {
			fmt.Printf("Warning: %d bytes still allocated\n", alloc.CurrentAlloc())
		}
	}()
}
