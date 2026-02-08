# Step 09: System Catalog Behavior and Dependencies

Goal:
- Model PostgreSQL catalog semantics beyond user-table metadata.

Scope:
- Internal catalog tables/views abstraction.
- Object dependency graph.
- Rename/drop cascade/restrict semantics.

Implementation files:
- Add `src/catalog/system.rs`
- Add `src/catalog/dependency.rs`
- Extend DDL pipeline in `src/ddl/`

Deliverables:
- Dependency tracking and validation.
- Correct cascading behavior for dependent objects.
- Catalog introspection primitives for planner/analyzer.

Tests:
- Dependency graph correctness tests.
- Cascade/restrict DDL tests.
- Catalog consistency invariant checks.

Done criteria:
- Object lifecycle rules reflect PostgreSQL-style dependencies.

Dependencies:
- `01-catalog-and-names.md`
- `02-ddl-core-create-drop-alter.md`

