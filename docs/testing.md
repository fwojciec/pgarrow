# Testing Guide

## Table-Based Integration Testing Strategy

PGArrow uses a comprehensive table-based integration testing approach inspired by SQLite and DuckDB methodologies. This strategy focuses on testing the complete data pipeline with real PostgreSQL data rather than individual components in isolation.

### Core Philosophy

- **Real Data Testing**: Uses actual PostgreSQL COPY TO BINARY format instead of mock data
- **Comprehensive Coverage**: Single table-based test covers all data types and edge cases  
- **Schema Isolation**: Each test creates its own isolated schema for safe parallel execution
- **Future-Ready**: Adding new PostgreSQL types requires only adding test cases to the table

### Primary Test: TestQueryArrowDataTypes

The main comprehensive test (`TestQueryArrowDataTypes` in `integration_test.go`) covers:

- **All supported data types**: `bool`, `int2`, `int4`, `int8`, `float4`, `float8`, `text`
- **Edge cases**: Min/max values, precision limits, special float values (NaN, Infinity)
- **NULL handling**: Comprehensive NULL testing across all data types
- **Mixed type queries**: Complex queries with multiple data types
- **Text encoding**: Unicode, special characters, empty strings
- **Custom table creation**: Each test case creates its own isolated tables

### Test Structure

```go
tests := []struct {
    name         string
    setupSQL     string           // CREATE TABLE and INSERT statements
    querySQL     string           // SELECT query to test
    expectedRows int64           
    expectedCols int64           
    validateFunc func(t *testing.T, record arrow.Record) // Custom validation
}{
    {
        name: "bool_all_values",
        setupSQL: `CREATE TABLE test_bool (val bool); 
                   INSERT INTO test_bool VALUES (true), (false), (null);`,
        querySQL: "SELECT * FROM test_bool ORDER BY val NULLS LAST",
        expectedRows: 3,
        expectedCols: 1,
        validateFunc: func(t *testing.T, record arrow.Record) {
            // Detailed validation logic
        },
    },
    // ... more comprehensive test cases
}
```

### Setup

1. **Create test database and user:**
   ```sql
   -- Connect to PostgreSQL as superuser
   CREATE DATABASE pgarrow_test;
   CREATE USER pgarrow_test WITH PASSWORD 'pgarrow_test_password';
   GRANT ALL PRIVILEGES ON DATABASE pgarrow_test TO pgarrow_test;
   
   -- Connect to pgarrow_test database
   \c pgarrow_test
   GRANT CREATE ON SCHEMA public TO pgarrow_test;
   ```

2. **Run tests:**
   ```bash
   # Run all tests with race detection
   TEST_DATABASE_URL=postgres://user:pass@host:port/db?sslmode=disable go test -race ./...
   
   # Run comprehensive data type tests
   TEST_DATABASE_URL=postgres://user:pass@host:port/db?sslmode=disable go test -v -run TestQueryArrowDataTypes
   
   # Run specific test case
   TEST_DATABASE_URL=postgres://user:pass@host:port/db?sslmode=disable go test -v -run TestQueryArrowDataTypes/bool_all_values
   ```

### Test Isolation & Schema Management

Each test case:
- Creates a unique isolated schema: `test_{randomID}_{timestamp}`
- Sets up custom tables within that schema using `setupSQL`
- Creates a dedicated connection pool with `search_path` pointing to the test schema
- Automatically cleans up the schema after test completion
- Runs in parallel with other tests safely

**Schema Isolation Implementation:**
```go
// Each test gets its own schema and connection pool
schemaName := fmt.Sprintf("test_%s_%d", randomID(), time.Now().UnixNano())
connStrWithSchema := fmt.Sprintf("%s&search_path=%s,public", baseConnStr, schemaName)
pool, err := pgarrow.NewPool(ctx, connStrWithSchema)
```

### Environment Variables

- `TEST_DATABASE_URL`: PostgreSQL connection string for tests
  - **Required** for integration tests
  - Tests are **skipped** if not set
  - Format: `postgres://user:pass@host:port/db?sslmode=disable`

### Benefits of Table-Based Testing

1. **Simplified Maintenance**: Adding new PostgreSQL types = adding rows to test table
2. **Higher Fidelity**: Tests real PostgreSQL data conversion pipeline
3. **Reduced Code**: ~60% reduction in test code while improving coverage  
4. **Better Debugging**: Test failures show exactly which data type/scenario failed
5. **Extensible**: Easy to add comprehensive test coverage for new types
6. **No Mock Data**: Eliminates complex mock data generators

### File Structure

```
integration_test.go             # Main table-based integration tests
pgarrow_bench_test.go           # Performance benchmarks  
types_bench_test.go             # Type-specific benchmarks
```

### Adding New Test Cases

To test a new PostgreSQL data type:

1. Add a new test case to the `tests` slice in `TestQueryArrowDataTypes`
2. Provide `setupSQL` to create table and insert test data
3. Provide `querySQL` to test the data type conversion
4. Implement `validateFunc` to verify Arrow record contents
5. Include edge cases, NULL handling, and boundary conditions

Example:
```go
{
    name: "new_type_comprehensive",
    setupSQL: `CREATE TABLE test_new_type (val new_type); 
               INSERT INTO test_new_type VALUES (normal_val), (edge_case), (null);`,
    querySQL: "SELECT * FROM test_new_type ORDER BY val NULLS LAST",
    expectedRows: 3,
    expectedCols: 1,
    validateFunc: func(t *testing.T, record arrow.Record) {
        col, ok := record.Column(0).(*array.NewTypeArray)
        require.True(t, ok)
        // Validate converted values...
    },
}
```

This approach ensures comprehensive, maintainable, and reliable testing as PGArrow expands its PostgreSQL type support.