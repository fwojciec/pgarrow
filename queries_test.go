package pgarrow_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

func TestQueries(t *testing.T) {
	t.Parallel()
	runTests(t, []testCase{
		// Basic queries
		{
			name:  "literal_values",
			query: "SELECT 1 as num, 'hello' as txt, true as flag",
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 1)
				validateSchema(t, records, []string{"num", "txt", "flag"})
			},
		},
		{
			name:  "parameterized_query",
			query: "SELECT $1::int4, $2::text, $3::bool",
			args:  []any{42, "world", false},
			validate: func(t *testing.T, records []arrow.Record) {
				require.Len(t, records, 1)
				rec := records[0]
				require.Equal(t, int32(42), rec.Column(0).(*array.Int32).Value(0))
				require.Equal(t, "world", rec.Column(1).(*array.String).Value(0))
				require.False(t, rec.Column(2).(*array.Boolean).Value(0))
			},
		},

		// Empty results
		{
			name:  "empty_result_set",
			query: "SELECT 1 WHERE false",
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 0)
				// Empty result sets may not return any records
				if len(records) > 0 {
					validateSchema(t, records, []string{"?column?"})
				}
			},
		},
		{
			name:  "empty_table_query",
			setup: "CREATE TABLE empty_table (id int4, name text)",
			query: "SELECT * FROM empty_table",
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 0)
				// Empty result sets may not return any records
				if len(records) > 0 {
					validateSchema(t, records, []string{"id", "name"})
				}
			},
		},

		// Large result sets (test batching)
		{
			name: "large_result_set",
			setup: `
				CREATE TABLE numbers (n int4);
				INSERT INTO numbers SELECT generate_series(1, 1000);
			`,
			query: "SELECT n FROM numbers ORDER BY n",
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 1000)

				// Verify values are correct and in order
				expectedValue := int32(1)
				for _, rec := range records {
					col := rec.Column(0).(*array.Int32)
					for i := 0; i < int(rec.NumRows()); i++ {
						require.Equal(t, expectedValue, col.Value(i))
						expectedValue++
					}
				}
			},
		},

		// NULL handling
		{
			name: "null_patterns",
			setup: `
				CREATE TABLE null_test (
					id int4,
					name text,
					active bool
				);
				INSERT INTO null_test VALUES
					(1, 'Alice', true),
					(2, NULL, false),
					(NULL, 'Bob', NULL),
					(NULL, NULL, NULL);
			`,
			query: "SELECT * FROM null_test ORDER BY id NULLS FIRST",
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 4)

				// Collect all rows for easier validation
				var rows []struct {
					idNull     bool
					id         int32
					nameNull   bool
					name       string
					activeNull bool
					active     bool
				}

				for _, rec := range records {
					idCol := rec.Column(0).(*array.Int32)
					nameCol := rec.Column(1).(*array.String)
					activeCol := rec.Column(2).(*array.Boolean)

					for i := 0; i < int(rec.NumRows()); i++ {
						row := struct {
							idNull     bool
							id         int32
							nameNull   bool
							name       string
							activeNull bool
							active     bool
						}{
							idNull:     idCol.IsNull(i),
							nameNull:   nameCol.IsNull(i),
							activeNull: activeCol.IsNull(i),
						}
						if !row.idNull {
							row.id = idCol.Value(i)
						}
						if !row.nameNull {
							row.name = nameCol.Value(i)
						}
						if !row.activeNull {
							row.active = activeCol.Value(i)
						}
						rows = append(rows, row)
					}
				}

				// Verify NULL patterns
				require.Len(t, rows, 4)

				// Row 0: (NULL, 'Bob', NULL)
				require.True(t, rows[0].idNull)
				require.False(t, rows[0].nameNull)
				require.Equal(t, "Bob", rows[0].name)
				require.True(t, rows[0].activeNull)

				// Row 1: (NULL, NULL, NULL)
				require.True(t, rows[1].idNull)
				require.True(t, rows[1].nameNull)
				require.True(t, rows[1].activeNull)

				// Row 2: (1, 'Alice', true)
				require.False(t, rows[2].idNull)
				require.Equal(t, int32(1), rows[2].id)
				require.Equal(t, "Alice", rows[2].name)
				require.True(t, rows[2].active)

				// Row 3: (2, NULL, false)
				require.Equal(t, int32(2), rows[3].id)
				require.True(t, rows[3].nameNull)
				require.False(t, rows[3].active)
			},
		},

		// WHERE clauses and filtering
		{
			name: "where_clause_filtering",
			setup: `
				CREATE TABLE products (
					id int4,
					name text,
					price float8,
					in_stock bool
				);
				INSERT INTO products VALUES
					(1, 'Widget', 19.99, true),
					(2, 'Gadget', 29.99, false),
					(3, 'Doohickey', 39.99, true),
					(4, 'Thingamajig', 49.99, true);
			`,
			query: "SELECT name, price FROM products WHERE in_stock = true AND price < $1 ORDER BY price",
			args:  []any{40.0},
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 2)

				// Should get Widget and Doohickey
				rec := records[0]
				require.Equal(t, "Widget", rec.Column(0).(*array.String).Value(0))
				require.InDelta(t, 19.99, rec.Column(1).(*array.Float64).Value(0), 0.001)
				require.Equal(t, "Doohickey", rec.Column(0).(*array.String).Value(1))
				require.InDelta(t, 39.99, rec.Column(1).(*array.Float64).Value(1), 0.001)
			},
		},

		// JOIN queries
		{
			name: "join_query",
			setup: `
				CREATE TABLE users (id int4 PRIMARY KEY, name text);
				CREATE TABLE orders (id int4, user_id int4, amount float8);
				INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
				INSERT INTO orders VALUES (1, 1, 100.00), (2, 1, 200.00), (3, 2, 150.00);
			`,
			query: `
				SELECT u.name, SUM(o.amount) as total
				FROM users u
				JOIN orders o ON u.id = o.user_id
				GROUP BY u.name
				ORDER BY u.name
			`,
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 2)
				rec := records[0]

				require.Equal(t, "Alice", rec.Column(0).(*array.String).Value(0))
				require.InDelta(t, 300.0, rec.Column(1).(*array.Float64).Value(0), 0.001)

				require.Equal(t, "Bob", rec.Column(0).(*array.String).Value(1))
				require.InDelta(t, 150.0, rec.Column(1).(*array.Float64).Value(1), 0.001)
			},
		},

		// Aggregations
		{
			name: "aggregation_functions",
			setup: `
				CREATE TABLE sales (amount float8, category text);
				INSERT INTO sales VALUES 
					(100, 'electronics'), (200, 'electronics'), 
					(150, 'clothing'), (75, 'clothing'), (125, 'clothing');
			`,
			query: `
				SELECT 
					category,
					COUNT(*) as count,
					SUM(amount) as total,
					AVG(amount) as average,
					MIN(amount) as minimum,
					MAX(amount) as maximum
				FROM sales
				GROUP BY category
				ORDER BY category
			`,
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 2)
				rec := records[0]

				// Clothing row
				require.Equal(t, "clothing", rec.Column(0).(*array.String).Value(0))
				require.Equal(t, int64(3), rec.Column(1).(*array.Int64).Value(0))
				require.InDelta(t, 350.0, rec.Column(2).(*array.Float64).Value(0), 0.001)
				require.InDelta(t, 116.67, rec.Column(3).(*array.Float64).Value(0), 0.01)
				require.InDelta(t, 75.0, rec.Column(4).(*array.Float64).Value(0), 0.001)
				require.InDelta(t, 150.0, rec.Column(5).(*array.Float64).Value(0), 0.001)

				// Electronics row
				require.Equal(t, "electronics", rec.Column(0).(*array.String).Value(1))
				require.Equal(t, int64(2), rec.Column(1).(*array.Int64).Value(1))
				require.InDelta(t, 300.0, rec.Column(2).(*array.Float64).Value(1), 0.001)
				require.InDelta(t, 150.0, rec.Column(3).(*array.Float64).Value(1), 0.001)
				require.InDelta(t, 100.0, rec.Column(4).(*array.Float64).Value(1), 0.001)
				require.InDelta(t, 200.0, rec.Column(5).(*array.Float64).Value(1), 0.001)
			},
		},

		// CTEs
		{
			name: "common_table_expression",
			setup: `
				CREATE TABLE employees (id int4, name text, department text, salary float8);
				INSERT INTO employees VALUES 
					(1, 'Alice', 'Engineering', 100000),
					(2, 'Bob', 'Engineering', 90000),
					(3, 'Charlie', 'Sales', 80000),
					(4, 'David', 'Sales', 85000);
			`,
			query: `
				WITH dept_avg AS (
					SELECT department, AVG(salary) as avg_salary
					FROM employees
					GROUP BY department
				)
				SELECT e.name, e.salary, da.avg_salary
				FROM employees e
				JOIN dept_avg da ON e.department = da.department
				WHERE e.salary > da.avg_salary
				ORDER BY e.name
			`,
			validate: func(t *testing.T, records []arrow.Record) {
				validateRowCount(t, records, 2)
				rec := records[0]

				// Alice and David earn more than their department average
				require.Equal(t, "Alice", rec.Column(0).(*array.String).Value(0))
				require.InDelta(t, 100000.0, rec.Column(1).(*array.Float64).Value(0), 0.001)
				require.InDelta(t, 95000.0, rec.Column(2).(*array.Float64).Value(0), 0.001)

				require.Equal(t, "David", rec.Column(0).(*array.String).Value(1))
				require.InDelta(t, 85000.0, rec.Column(1).(*array.Float64).Value(1), 0.001)
				require.InDelta(t, 82500.0, rec.Column(2).(*array.Float64).Value(1), 0.001)
			},
		},

		// Context cancellation
		{
			name: "context_cancellation",
			setup: `
				CREATE TABLE large_table AS 
				SELECT generate_series(1, 1000000) as id;
			`,
			query: "SELECT COUNT(*) FROM large_table",
			validate: func(t *testing.T, records []arrow.Record) {
				// Cancel context immediately
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				// Try to query with cancelled context
				pool, cleanup := isolatedTest(t, "")
				defer cleanup()

				_, err := pool.QueryArrow(ctx, "SELECT 1")
				require.Error(t, err)
				require.Contains(t, err.Error(), "context")
			},
		},

		// Schema inference
		{
			name:  "schema_inference",
			query: "SELECT 1::int2 as a, 2::int4 as b, 3::int8 as c, 4.5::float4 as d, 6.7::float8 as e",
			validate: func(t *testing.T, records []arrow.Record) {
				require.NotEmpty(t, records)
				schema := records[0].Schema()

				require.Equal(t, "a", schema.Field(0).Name)
				require.Equal(t, arrow.INT16, schema.Field(0).Type.ID())

				require.Equal(t, "b", schema.Field(1).Name)
				require.Equal(t, arrow.INT32, schema.Field(1).Type.ID())

				require.Equal(t, "c", schema.Field(2).Name)
				require.Equal(t, arrow.INT64, schema.Field(2).Type.ID())

				require.Equal(t, "d", schema.Field(3).Name)
				require.Equal(t, arrow.FLOAT32, schema.Field(3).Type.ID())

				require.Equal(t, "e", schema.Field(4).Name)
				require.Equal(t, arrow.FLOAT64, schema.Field(4).Type.ID())
			},
		},
	})
}
