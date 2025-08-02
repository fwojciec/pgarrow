# CLAUDE.md

Strategic guidance for LLMs working with this codebase.

## Why This Codebase Exists

**Core Problem**: PostgreSQL → Arrow conversion typically requires either CGO dependencies, expensive metadata preloading that doesn't scale with large schemas, or ad-hoc per-query connections that can't support high-performance query systems.

**Design Philosophy**: 
- **Separation of concerns** - metadata discovery, streaming, and conversion are distinct phases
- **Lazy evaluation** - compute only what's needed, when it's needed
- **Zero-copy where possible** - minimize allocations and data movement
- **Connection pooling** - designed for high-performance systems with many queries
- **Fail-fast validation** - comprehensive testing catches errors early in development cycle and serves as robust regression suite

**Quality Philosophy**: **Process over polish** - systematic validation results in quality rather than fixing issues post-hoc.

## Quality-First Development

**Feedback Loops**: TDD → Systematic Validation → Continuous Integration → Performance Baselines

## Architecture Patterns

**Data Flow**: `PostgreSQL → COPY BINARY → Stream Parser → Arrow Batches`

**Essential Patterns**:
- Consistent signed/unsigned conversion: `int64(binary.BigEndian.Uint64(data))`
- Parallel-safe testing: `t.Parallel()` + race detection
- Public API testing: `*_test` packages ensure tests use public interface only
- Memory safety: checked allocators + proper resource cleanup
- PostgreSQL schema isolation: unique schemas per test enable safe concurrent execution

## Essential Commands

```bash
make validate  # Complete quality gate - run before completing any task
```

## Reference Documentation

- `docs/testing.md` - Testing strategy and patterns
- `docs/benchmarks.md` - Performance analysis  
- `docs/postgresql-binary-format-reference.md` - Binary format spec
- `.claude/commands/` - Specialized workflows

## File Structure

Root package `pgarrow` with `*_test.go` tests, `examples/`, and `docs/`.