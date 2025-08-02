package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
)

func main() {
	// Create PGArrow pool with memory tracking
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer func() {
		if alloc.CurrentAlloc() != 0 {
			fmt.Printf("Warning: %d bytes still allocated\n", alloc.CurrentAlloc())
		}
	}()

	pool, err := setupPool(alloc)
	if err != nil {
		fmt.Printf("Failed to setup pool: %v\n", err)
		return
	}
	defer pool.Close()

	// Create context with timeout for all operations
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fmt.Println("=== PGArrow Supported Data Types Demo ===")

	// Demonstrate all 17 supported types
	if err := demonstrateAllTypes(ctx, pool); err != nil {
		fmt.Printf("All types demo failed: %v\n", err)
		return
	}

	// Demonstrate NULL value handling
	if err := demonstrateNullHandling(ctx, pool); err != nil {
		fmt.Printf("NULL handling demo failed: %v\n", err)
		return
	}

	// Demonstrate mixed data with some NULLs
	if err := demonstrateMixedData(ctx, pool); err != nil {
		fmt.Printf("Mixed data demo failed: %v\n", err)
		return
	}

	fmt.Println("\nPGArrow types example completed successfully!")
}

func setupPool(alloc memory.Allocator) (*pgarrow.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	pool, err := pgarrow.NewPool(context.Background(), databaseURL, pgarrow.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}
	return pool, nil
}

// mustCastColumn provides safe type casting with clear error messages
func mustCastColumn[T any](record arrow.Record, index int) (T, error) {
	col, ok := record.Column(index).(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("failed to cast column %d to expected type", index)
	}
	return col, nil
}

func demonstrateAllTypes(ctx context.Context, pool *pgarrow.Pool) error {
	fmt.Println("\n=== All 17 Supported Data Types ===")

	query := `
		SELECT 
			true as bool_col,                                   -- bool (OID 16)
			E'\\x48656c6c6f'::bytea as bytea_col,              -- bytea (OID 17)
			123::int2 as int2_col,                             -- int2/smallint (OID 21)  
			456789::int4 as int4_col,                          -- int4/integer (OID 23)
			123456789012::int8 as int8_col,                    -- int8/bigint (OID 20)
			3.14::float4 as float4_col,                        -- float4/real (OID 700)
			2.718281828::float8 as float8_col,                 -- float8/double precision (OID 701)
			'Hello PGArrow!'::text as text_col,                -- text (OID 25)
			'Variable length'::varchar(50) as varchar_col,     -- varchar (OID 1043)
			'Fixed    '::char(10) as bpchar_col,               -- bpchar/char(n) (OID 1042)
			'table_name'::name as name_col,                    -- name (OID 19)
			'A'::"char" as char_col,                           -- "char" (OID 18)
			'2024-01-15'::date as date_col,                    -- date (OID 1082)
			'12:30:45.123456'::time as time_col,               -- time (OID 1083)
			'2024-01-15 12:30:45.123456'::timestamp as timestamp_col,      -- timestamp (OID 1114)
			'2024-01-15 12:30:45.123456+00'::timestamptz as timestamptz_col, -- timestamptz (OID 1184)
			'1 year 2 months 3 days 4 hours 5 minutes 6.789 seconds'::interval as interval_col -- interval (OID 1186)
	`

	reader, err := pool.QueryArrow(ctx, query)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer reader.Release()

	fmt.Printf("Schema: %s\n\n", reader.Schema())

	for reader.Next() {
		record := reader.Record()
		fmt.Printf("Batch returned %d rows, %d columns\n", record.NumRows(), record.NumCols())

		if err := displayAllTypesResults(record); err != nil {
			return err
		}
	}

	if err := reader.Err(); err != nil {
		return fmt.Errorf("reader error: %w", err)
	}

	return nil
}

func displayAllTypesResults(record arrow.Record) error {
	allTypesColumns, err := extractAllTypesColumns(record)
	if err != nil {
		return err
	}

	return printAllTypesRows(record, allTypesColumns)
}

// allTypesColumns holds all 17 column types for the comprehensive data types demo
type allTypesColumns struct {
	boolCol        *array.Boolean
	byteaCol       *array.Binary
	int2Col        *array.Int16
	int4Col        *array.Int32
	int8Col        *array.Int64
	float4Col      *array.Float32
	float8Col      *array.Float64
	textCol        *array.String
	varcharCol     *array.String
	bpcharCol      *array.String
	nameCol        *array.String
	charCol        *array.String
	dateCol        *array.Date32
	timeCol        *array.Time64
	timestampCol   *array.Timestamp
	timestamptzCol *array.Timestamp
	intervalCol    *array.MonthDayNanoInterval
}

func extractAllTypesColumns(record arrow.Record) (*allTypesColumns, error) {
	cols := &allTypesColumns{}

	if err := extractBasicTypeColumns(record, cols); err != nil {
		return nil, err
	}
	if err := extractStringTypeColumns(record, cols); err != nil {
		return nil, err
	}
	if err := extractTemporalTypeColumns(record, cols); err != nil {
		return nil, err
	}

	return cols, nil
}

func extractBasicTypeColumns(record arrow.Record, cols *allTypesColumns) error {
	var err error
	if cols.boolCol, err = mustCastColumn[*array.Boolean](record, 0); err != nil {
		return err
	}
	if cols.byteaCol, err = mustCastColumn[*array.Binary](record, 1); err != nil {
		return err
	}
	if cols.int2Col, err = mustCastColumn[*array.Int16](record, 2); err != nil {
		return err
	}
	if cols.int4Col, err = mustCastColumn[*array.Int32](record, 3); err != nil {
		return err
	}
	if cols.int8Col, err = mustCastColumn[*array.Int64](record, 4); err != nil {
		return err
	}
	if cols.float4Col, err = mustCastColumn[*array.Float32](record, 5); err != nil {
		return err
	}
	if cols.float8Col, err = mustCastColumn[*array.Float64](record, 6); err != nil {
		return err
	}
	return nil
}

func extractStringTypeColumns(record arrow.Record, cols *allTypesColumns) error {
	var err error
	if cols.textCol, err = mustCastColumn[*array.String](record, 7); err != nil {
		return err
	}
	if cols.varcharCol, err = mustCastColumn[*array.String](record, 8); err != nil {
		return err
	}
	if cols.bpcharCol, err = mustCastColumn[*array.String](record, 9); err != nil {
		return err
	}
	if cols.nameCol, err = mustCastColumn[*array.String](record, 10); err != nil {
		return err
	}
	if cols.charCol, err = mustCastColumn[*array.String](record, 11); err != nil {
		return err
	}
	return nil
}

func extractTemporalTypeColumns(record arrow.Record, cols *allTypesColumns) error {
	var err error
	if cols.dateCol, err = mustCastColumn[*array.Date32](record, 12); err != nil {
		return err
	}
	if cols.timeCol, err = mustCastColumn[*array.Time64](record, 13); err != nil {
		return err
	}
	if cols.timestampCol, err = mustCastColumn[*array.Timestamp](record, 14); err != nil {
		return err
	}
	if cols.timestamptzCol, err = mustCastColumn[*array.Timestamp](record, 15); err != nil {
		return err
	}
	if cols.intervalCol, err = mustCastColumn[*array.MonthDayNanoInterval](record, 16); err != nil {
		return err
	}
	return nil
}

func printAllTypesRows(record arrow.Record, cols *allTypesColumns) error {
	for i := 0; i < int(record.NumRows()); i++ {
		fmt.Printf("Row %d:\n", i)
		printBasicTypes(i, cols)
		printStringTypes(i, cols)
		printTemporalTypes(i, cols)
	}
	return nil
}

func printBasicTypes(i int, cols *allTypesColumns) {
	fmt.Printf("  bool:       %t\n", cols.boolCol.Value(i))
	fmt.Printf("  bytea:      %s (hex: %x)\n", string(cols.byteaCol.Value(i)), cols.byteaCol.Value(i))
	fmt.Printf("  int2:       %d\n", cols.int2Col.Value(i))
	fmt.Printf("  int4:       %d\n", cols.int4Col.Value(i))
	fmt.Printf("  int8:       %d\n", cols.int8Col.Value(i))
	fmt.Printf("  float4:     %.6f\n", cols.float4Col.Value(i))
	fmt.Printf("  float8:     %.9f\n", cols.float8Col.Value(i))
}

func printStringTypes(i int, cols *allTypesColumns) {
	fmt.Printf("  text:       %s\n", cols.textCol.Value(i))
	fmt.Printf("  varchar:    %s\n", cols.varcharCol.Value(i))
	fmt.Printf("  bpchar:     %s\n", cols.bpcharCol.Value(i))
	fmt.Printf("  name:       %s\n", cols.nameCol.Value(i))
	fmt.Printf("  char:       %s\n", cols.charCol.Value(i))
}

func printTemporalTypes(i int, cols *allTypesColumns) {
	fmt.Printf("  date:       %d (days since 1970-01-01)\n", cols.dateCol.Value(i))
	fmt.Printf("  time:       %d (microseconds since midnight)\n", cols.timeCol.Value(i))
	fmt.Printf("  timestamp:  %d (microseconds since 1970-01-01)\n", cols.timestampCol.Value(i))
	fmt.Printf("  timestamptz: %d (microseconds since 1970-01-01 UTC)\n", cols.timestamptzCol.Value(i))
	interval := cols.intervalCol.Value(i)
	fmt.Printf("  interval:   %d months, %d days, %d nanoseconds\n", interval.Months, interval.Days, interval.Nanoseconds)
}

func demonstrateNullHandling(ctx context.Context, pool *pgarrow.Pool) error {
	fmt.Println("\n=== NULL Value Handling ===")

	nullQuery := `
		SELECT 
			NULL::bool as null_bool,
			NULL::bytea as null_bytea,
			NULL::int2 as null_int2,
			NULL::int4 as null_int4,
			NULL::int8 as null_int8,
			NULL::float4 as null_float4,
			NULL::float8 as null_float8,
			NULL::text as null_text,
			NULL::varchar as null_varchar,
			NULL::char(10) as null_bpchar,
			NULL::name as null_name,
			NULL::"char" as null_char,
			NULL::date as null_date,
			NULL::time as null_time,
			NULL::timestamp as null_timestamp,
			NULL::timestamptz as null_timestamptz,
			NULL::interval as null_interval
	`

	reader, err := pool.QueryArrow(ctx, nullQuery)
	if err != nil {
		return fmt.Errorf("NULL query failed: %w", err)
	}
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		record := reader.Record()
		totalRows += int(record.NumRows())

		// Check nulls in each column
		fmt.Printf("NULL statistics for all 17 data types:\n")
		for i := 0; i < int(record.NumCols()); i++ {
			col := record.Column(i)
			colName := record.Schema().Field(i).Name
			nullCount := col.NullN()
			fmt.Printf("  %-16s: %d/%d NULLs\n", colName, nullCount, col.Len())
		}
	}

	if err := reader.Err(); err != nil {
		return fmt.Errorf("reader error: %w", err)
	}

	fmt.Printf("NULL query returned %d rows\n", totalRows)
	return nil
}

func demonstrateMixedData(ctx context.Context, pool *pgarrow.Pool) error {
	fmt.Println("\n=== Mixed Data with NULLs ===")

	mixedQuery := `
		SELECT * FROM (VALUES 
			(1, 'Alice', 25.5::float8, true),
			(2, NULL, 30.0::float8, false),
			(NULL, 'Charlie', NULL::float8, true),
			(4, 'Diana', 28.7::float8, NULL)
		) AS mixed_data(id, name, score, active)
	`

	reader, err := pool.QueryArrow(ctx, mixedQuery)
	if err != nil {
		return fmt.Errorf("mixed query failed: %w", err)
	}
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		record := reader.Record()
		totalRows += int(record.NumRows())

		if err := displayMixedResults(record); err != nil {
			return err
		}
	}

	if err := reader.Err(); err != nil {
		return fmt.Errorf("reader error: %w", err)
	}

	fmt.Printf("Mixed data query returned %d rows\n", totalRows)
	return nil
}

func displayMixedResults(record arrow.Record) error {
	mixedCols, err := extractMixedDataColumns(record)
	if err != nil {
		return err
	}

	printMixedDataRows(record, mixedCols)
	printMixedDataNullStats(mixedCols)

	return nil
}

type mixedDataColumns struct {
	idCol     *array.Int32
	nameCol   *array.String
	scoreCol  *array.Float64
	activeCol *array.Boolean
}

func extractMixedDataColumns(record arrow.Record) (*mixedDataColumns, error) {
	cols := &mixedDataColumns{}
	var err error

	if cols.idCol, err = mustCastColumn[*array.Int32](record, 0); err != nil {
		return nil, err
	}
	if cols.nameCol, err = mustCastColumn[*array.String](record, 1); err != nil {
		return nil, err
	}
	if cols.scoreCol, err = mustCastColumn[*array.Float64](record, 2); err != nil {
		return nil, err
	}
	if cols.activeCol, err = mustCastColumn[*array.Boolean](record, 3); err != nil {
		return nil, err
	}

	return cols, nil
}

func printMixedDataRows(record arrow.Record, cols *mixedDataColumns) {
	for i := 0; i < int(record.NumRows()); i++ {
		fmt.Printf("Row %d: ", i)

		if cols.idCol.IsNull(i) {
			fmt.Print("id=NULL, ")
		} else {
			fmt.Printf("id=%d, ", cols.idCol.Value(i))
		}

		if cols.nameCol.IsNull(i) {
			fmt.Print("name=NULL, ")
		} else {
			fmt.Printf("name=%s, ", cols.nameCol.Value(i))
		}

		if cols.scoreCol.IsNull(i) {
			fmt.Print("score=NULL, ")
		} else {
			fmt.Printf("score=%.1f, ", cols.scoreCol.Value(i))
		}

		if cols.activeCol.IsNull(i) {
			fmt.Print("active=NULL")
		} else {
			fmt.Printf("active=%t", cols.activeCol.Value(i))
		}

		fmt.Println()
	}
}

func printMixedDataNullStats(cols *mixedDataColumns) {
	fmt.Printf("\nNULL Statistics:\n")
	fmt.Printf("  id column:     %d/%d NULLs\n", cols.idCol.NullN(), cols.idCol.Len())
	fmt.Printf("  name column:   %d/%d NULLs\n", cols.nameCol.NullN(), cols.nameCol.Len())
	fmt.Printf("  score column:  %d/%d NULLs\n", cols.scoreCol.NullN(), cols.scoreCol.Len())
	fmt.Printf("  active column: %d/%d NULLs\n", cols.activeCol.NullN(), cols.activeCol.Len())
}
