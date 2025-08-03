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

**Safety Philosophy**: **Memory safety over micro-optimization** - use safe standard library approaches unless unsafe provides critical performance benefits that justify the risks. When unsafe is considered, document the trade-offs explicitly and evaluate safe alternatives first.

**Documentation Philosophy**: **Objectivity over marketing** - provide measurable facts that enable informed decisions rather than promotional language.

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

## Documentation Standards

**Objective Tone**: Present measurable facts, not marketing claims
- ✅ "89% reduction in allocations vs previous implementation"
- ✅ "2-36 ns/op measured depending on data type complexity" 
- ✅ "Uses pgx internally with Arrow format optimization"
- ❌ "delivers exceptional performance", "ultra-fast", "revolutionary"

**Honest Comparisons**: Compare comparable things with clear context
- ✅ PGArrow vs previous PGArrow implementation (fair baseline)
- ✅ PGArrow vs pgx with clear notes about different use cases
- ❌ Pool creation vs actual connection establishment (apples-to-oranges)

**Verifiable Claims**: All performance statements must be backed by benchmarks
- Link to specific benchmark functions where possible
- Include measurement conditions and context
- Acknowledge limitations and trade-offs

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