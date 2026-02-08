# Step 06: Transactions and MVCC-Lite Visibility

Goal:
- Provide transactional semantics suitable for in-memory PostgreSQL behavior.

Scope:
- `BEGIN`/`COMMIT`/`ROLLBACK`
- Snapshot boundaries
- Statement visibility rules (MVCC-lite model)
- Error-state transaction behavior parity

Implementation files:
- Add `src/txn/mod.rs`
- Add `src/txn/snapshot.rs`
- Add `src/txn/visibility.rs`
- Integrate with `src/tcop/postgres.rs` and `src/tcop/engine.rs`

Deliverables:
- Transaction contexts and snapshot IDs.
- Visibility filtering in scans and writes.
- Rollback of uncommitted mutations.

Tests:
- Multi-statement transaction tests.
- Rollback/commit correctness tests.
- Visibility tests across simulated sessions.

Done criteria:
- DML/DDL obey transaction boundaries and abort semantics.

Dependencies:
- `03-row-store-and-scans.md`
- `04-dml-insert-update-delete-returning.md`

