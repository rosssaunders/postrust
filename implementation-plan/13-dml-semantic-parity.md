# Step 13: DML Semantic Parity

Goal:
- Reach PostgreSQL-level DML behavior for edge semantics.

Scope:
- `INSERT ... ON CONFLICT`
- `MERGE`
- Writable CTE semantics
- Trigger interaction with row changes

Implementation files:
- Extend `src/executor/modify.rs`
- Add `src/executor/on_conflict.rs`
- Add `src/executor/merge.rs`
- Integrate with trigger subsystem (`src/ddl/trigger.rs` + runtime hooks)

Deliverables:
- Upsert/merge correctness and determinism.
- Trigger-visible mutation paths.

Tests:
- Conflict target behavior and exclusion semantics.
- Merge branch behavior tests.
- Trigger-before/after mutation tests.

Done criteria:
- Advanced DML statements match expected PostgreSQL behavior.

Dependencies:
- `11-executor-node-parity.md`
- `12-ddl-surface-area.md`

