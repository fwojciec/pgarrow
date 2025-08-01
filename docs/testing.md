# Testing Guide

## Integration Tests

Integration tests require a PostgreSQL instance to be running. They use isolated schemas for each test to ensure no interference between tests.

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

2. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env to match your PostgreSQL setup
   ```

3. **Run tests:**
   ```bash
   # Run all tests (unit + integration)
   go test ./...
   
   # Verbose output
   go test -v ./...
   
   # Run specific test
   go test -run TestPoolQueryArrow ./...
   ```

### Test Isolation

Each integration test:
- Creates a unique schema with a random name
- Runs all operations within that schema
- Cleans up the schema automatically after the test
- Uses parallel execution for faster test runs

This approach ensures:
- No test interference
- Safe concurrent test execution  
- Clean test environment for each run
- No manual cleanup required

### Environment Variables

- `TEST_DATABASE_URL`: PostgreSQL connection string for tests
  - **Required** for integration tests
  - Tests are **skipped** if not set
  - Format: `postgres://user:pass@host:port/db?sslmode=disable`

### Test Categories

- **Unit tests**: Fast, no database required, run by default  
- **Integration tests**: Require PostgreSQL, automatically skipped if `TEST_DATABASE_URL` not set