# PGArrow Examples

Example applications demonstrating PGArrow usage.

## Prerequisites

- Go 1.23 or later
- PostgreSQL database
- `DATABASE_URL` environment variable

## Running Examples

### Simple Example
Basic usage with connection pooling and queries:
```bash
cd simple
export DATABASE_URL="postgres://user:password@localhost/dbname"
go run main.go
```

### Types Example
Demonstrates all supported PostgreSQL data types:
```bash
cd types
export DATABASE_URL="postgres://user:password@localhost/dbname"
go run main.go
```

## Environment Setup

```bash
# Local PostgreSQL
export DATABASE_URL="postgres://postgres:password@localhost:5432/postgres"

# With SSL
export DATABASE_URL="postgres://user:pass@host:5432/db?sslmode=require"

# With connection pooling
export DATABASE_URL="postgres://user:pass@host:5432/db?pool_max_conns=10"
```

## Notes

- Examples use inline data - no tables required
- Supports parameterized queries for SQL injection prevention
- See individual example directories for specific functionality