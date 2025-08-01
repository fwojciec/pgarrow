# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PGArrow is a pure Go library that provides ADBC-like functionality for converting PostgreSQL query results directly to Apache Arrow format, without CGO dependencies. The project addresses scalability issues in ADBC where connection initialization takes 6-8 seconds for databases with 20k+ tables due to upfront metadata loading.

**Key Benefits:**
- Instant connections (no metadata preloading)
- Zero CGO dependencies (pure Go)
- High performance (direct binary format conversion)
- pgx compatibility (uses pgxpool for connection management)

## Commands

### Testing
```bash
go test ./...                    # Run all tests
go test -v ./...                 # Run tests with verbose output
go test -race ./...              # Run tests with race detector
go test -cover ./...             # Run tests with coverage
```

### Linting
```bash
golangci-lint run               # Run linter (installed via tools.go)
golangci-lint run --fix         # Run linter and auto-fix issues
```

### Build
```bash
go build ./...                  # Build all packages
go mod tidy                     # Clean up dependencies
go mod download                 # Download dependencies
```

## Architecture

### Core Components

1. **Connection Management** (`pgarrow.go`)
   - `Pool` struct wraps `pgxpool.Pool`
   - Provides `QueryArrow()` method for executing queries

2. **Binary Format Parser** (planned in `binary.go`)
   - Parses PostgreSQL COPY TO BINARY format
   - Handles network byte order conversion
   - Supports NULL value detection

3. **Type System** (planned in `types.go`)
   - Maps PostgreSQL OIDs to Arrow types
   - Extensible type handler interface
   - Built-in support for common types (int2, int4, int8, float4, float8, bool, text)

4. **Arrow Builder** (planned in `arrow.go`)
   - Converts parsed data to Arrow records
   - Memory-efficient record building
   - Schema generation from query metadata

### Data Flow
```
PostgreSQL → COPY TO BINARY → Binary Parser → Type Handlers → Arrow Record
```

### File Structure
- All implementation files in root directory
- All test files in root directory with `_test.go` suffix
- Documentation in `docs/` directory
- Binary test samples in `testdata/` (when created)

## Testing Strategy

The project uses Test-Driven Development (TDD) with:

- **Unit Tests**: Binary parser, type handlers, Arrow builders
- **Integration Tests**: End-to-end queries using `pgxmock`
- **Performance Tests**: Benchmarking against ADBC
- **Test Framework**: `github.com/stretchr/testify` for assertions
- **Mocking**: `github.com/pashagolub/pgxmock/v4` for database interactions

### Test Data Generation
Helper functions generate valid PostgreSQL binary COPY format for testing without requiring a live database connection.

## Key Dependencies

- `github.com/apache/arrow-go/v18` - Apache Arrow Go implementation
- `github.com/jackc/pgx/v5` - PostgreSQL driver and toolkit
- `github.com/golangci/golangci-lint` - Comprehensive Go linter
- `github.com/stretchr/testify` - Testing framework (for tests)

## Development Notes

- Go version: 1.24.5 (as specified in go.mod)
- All code should be in root package `pgarrow`
- Follow PostgreSQL binary format specification in docs/
- Target Phase 1 data types: bool, int2, int4, int8, float4, float8, text
- Connection speed target: <100ms (vs 6-8s for ADBC)
- Current status: Early development (basic Pool structure implemented)