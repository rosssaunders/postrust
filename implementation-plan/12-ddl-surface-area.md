# Step 12: Full DDL Surface

Goal:
- Expand DDL to broad PostgreSQL-compatible coverage.

Scope:
- `CREATE/ALTER/DROP` for schemas, indexes, views, materialized views, sequences.
- Partitioning DDL.
- Trigger and rule object DDL.

Implementation files:
- Extend `src/ddl/` modules by object type.
- Add `src/ddl/index.rs`
- Add `src/ddl/view.rs`
- Add `src/ddl/sequence.rs`
- Add `src/ddl/partition.rs`
- Add `src/ddl/trigger.rs`

Deliverables:
- DDL execution for major PostgreSQL object classes.
- Dependency-integrated lifecycle rules.

Tests:
- Statement-level DDL coverage tests.
- Cross-object dependency and cascade tests.
- Catalog introspection validation tests.

Done criteria:
- Most PostgreSQL DDL statements parse/analyze/execute in-memory.

Dependencies:
- `09-system-catalog-and-dependencies.md`
- `07-parser-coverage.md`

