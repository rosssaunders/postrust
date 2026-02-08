# Step 03: In-Memory Row Store and Table Scans

Goal:
- Back table references with real in-memory tuple storage.

Scope:
- Heap-like row container per relation.
- Tuple insert/read primitives.
- Sequential scan execution node.

Implementation files:
- Add `src/storage/mod.rs`
- Add `src/storage/heap.rs`
- Add `src/storage/tuple.rs`
- Add `src/executor/scan.rs`
- Integrate in `src/tcop/engine.rs`

Deliverables:
- Table rows exist independently from query text.
- `SELECT ... FROM table` scans persisted in-memory tuples.

Tests:
- Insert fixtures through API and scan results.
- Null handling and column-order correctness.
- Multi-table scan correctness with joins.

Done criteria:
- Table scans use storage layer, not subquery-only row sources.

Dependencies:
- `01-catalog-and-names.md`
- `02-ddl-core-create-drop-alter.md`

