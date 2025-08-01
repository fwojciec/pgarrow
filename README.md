# PGArrow

PGArrow is a pure Go library that provides ADBC-like functionality for converting PostgreSQL query results directly to Apache Arrow format, without CGO dependencies.

**Key Benefits:**
- ✅ **Instant connections** (no metadata preloading)  
- ✅ **Zero CGO dependencies** (pure Go)
- ✅ **High performance** (direct binary format conversion)
- ✅ **pgx compatibility** (uses pgxpool for connection management)

## Status

This project is currently in active development. See [docs/implementation-plan.md](docs/implementation-plan.md) for detailed implementation roadmap and technical specifications.