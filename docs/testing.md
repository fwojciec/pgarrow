# Testing Guide for LLMs

This guide provides clear instructions for LLMs on how to work with the PGArrow test suite.

## Core Testing Principle

**Test the interface, not the implementation**: Our interface is PostgreSQL-compatible SQL → Arrow data. Always prefer testing through SQL queries that validate real user scenarios.

## Test Organization

### Integration Tests (Default Approach)

Integration tests validate the complete PostgreSQL → Arrow pipeline and should be your default choice.

**Pattern**: Declarative test cases using the `testCase` struct

```go
testCase{
    name:     "descriptive_test_name",
    setup:    "CREATE TABLE ...; INSERT ...",  // Optional setup SQL
    query:    "SELECT ...",                    // The query to test
    args:     []any{...},                      // Optional query arguments
    wantErr:  "expected error substring",      // For error testing
    validate: func(t *testing.T, records []arrow.Record) {
        // Validate Arrow data
    },
}
```

**Where to add integration tests**:
- `datatypes_test.go` - Type conversion tests
- `queries_test.go` - SQL semantics (JOINs, CTEs, aggregations)
- `errors_test.go` - Error conditions and edge cases

**How to add a quality integration test**:

1. **Choose the right file** based on what you're testing
2. **Add to existing test function** (e.g., `TestDataTypes`, `TestQueries`)
3. **Write minimal SQL** that demonstrates the feature
4. **Use clear test names** that describe what's being tested
5. **Validate comprehensively** in the validate function

Example of adding a new data type test:

```go
// In datatypes_test.go, add to TestDataTypes:
{
    name:  "json_values",
    query: `SELECT '{"key": "value"}'::json, NULL::json`,
    validate: func(t *testing.T, records []arrow.Record) {
        require.Len(t, records, 1)
        rec := records[0]
        require.Equal(t, `{"key": "value"}`, rec.Column(0).(*array.String).Value(0))
        require.True(t, rec.Column(1).IsNull(0))
    },
},
```

### Unit Tests (Exception Cases)

Unit tests are for edge cases that are important but difficult to reproduce through SQL.

**Pattern**: Standard Go test functions testing specific implementations

**Naming convention**: Test files must match the file they test
- `select_parser.go` → `select_parser_test.go`
- `pgarrow.go` → `pgarrow_test.go`

**When to write unit tests**:
- Testing error paths that can't be triggered through SQL
- Testing internal state management
- Testing performance-critical code paths

Example of adding a unit test:

```go
// In select_parser_test.go
func TestSelectParser_EdgeCase(t *testing.T) {
    t.Parallel()
    // Direct testing of SelectParser implementation
}
```

## Best Practices

### 1. Always Use Test Isolation

Every integration test runs in an isolated schema:

```go
// This is handled automatically by runTests()
pool, cleanup := isolatedTest(t, tc.setup)
defer cleanup()
```

### 2. Test Comprehensively

Include in every test case:
- Normal values
- Edge cases (min/max values)
- NULL handling
- Empty results where applicable

### 3. Use Parallel Testing

Always mark tests as parallel:

```go
t.Parallel()
```

### 4. Memory Safety

For unit tests that create Arrow resources:

```go
alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
t.Cleanup(func() { alloc.AssertSize(t, 0) })
```

### 5. Clear Test Names

Use descriptive names that indicate what's being tested:
- ✅ `timestamp_microsecond_precision`
- ❌ `test1`

## Adding Tests for New Functionality

### Step 1: Determine Test Type

Ask yourself: "Can I test this through SQL?"
- **Yes** → Write integration test (preferred)
- **No** → Write unit test

### Step 2: Find the Right Location

Integration tests:
- Type conversion → `datatypes_test.go`
- SQL features → `queries_test.go`
- Errors → `errors_test.go`

Unit tests:
- Create/use `<filename>_test.go` matching the implementation file

### Step 3: Write the Test

Integration test template:
```go
{
    name:  "feature_being_tested",
    query: "SELECT ... that exercises the feature",
    validate: func(t *testing.T, records []arrow.Record) {
        // Comprehensive validation
        require.Len(t, records, 1)
        // Check data correctness
        // Check NULL handling
        // Check edge cases
    },
},
```

### Step 4: Verify Coverage

Run tests with coverage:
```bash
TEST_DATABASE_URL="..." go test -coverprofile=coverage.out
go tool cover -html=coverage.out -o coverage.html
```

## Common Patterns

### Testing Type Conversions

```go
{
    name:  "type_edge_cases",
    query: "SELECT min_val::type, max_val::type, normal::type, NULL::type",
    validate: func(t *testing.T, records []arrow.Record) {
        // Validate each case
    },
},
```

### Testing SQL Features

```go
{
    name:  "complex_join",
    setup: "CREATE TABLE t1 ...; CREATE TABLE t2 ...; INSERT ...",
    query: "SELECT ... FROM t1 JOIN t2 ON ...",
    validate: func(t *testing.T, records []arrow.Record) {
        // Validate join results
    },
},
```

### Testing Error Conditions

```go
{
    name:    "invalid_operation",
    query:   "SELECT 1/0",
    wantErr: "division by zero",
},
```

## Environment Setup

Tests require PostgreSQL access via `TEST_DATABASE_URL`:

```bash
export TEST_DATABASE_URL="postgres://user:pass@localhost:5432/testdb?sslmode=disable"
```

## Summary

1. **Default to integration tests** - they test what users actually do
2. **Add tests to existing files** - avoid creating new test files
3. **Test through SQL** - this validates the real interface
4. **Be comprehensive** - include edge cases and NULL handling
5. **Keep tests focused** - one concept per test case

Remember: If you can test it through SQL, that's the preferred approach. Unit tests are the exception, not the rule.