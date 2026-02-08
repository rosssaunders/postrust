# Step 04: DML (`INSERT`/`UPDATE`/`DELETE`/`RETURNING`)

Goal:
- Implement core row mutation operations.

Scope:
- `INSERT INTO ... VALUES/SELECT`
- `UPDATE ... SET ... WHERE ...`
- `DELETE FROM ... WHERE ...`
- `RETURNING` projection for all three

Implementation files:
- Add `src/executor/modify.rs`
- Add `src/executor/returning.rs`
- Extend parser support in `src/parser/sql_parser.rs` and `src/parser/ast.rs`
- Route command execution in `src/tcop/postgres.rs`

Deliverables:
- DML modifies in-memory table storage.
- Command tags and affected row counts are accurate.
- `RETURNING` emits row descriptions and data rows.

Tests:
- Statement-level DML tests and data verification.
- Multi-row updates/deletes with predicates.
- `RETURNING` with expressions and aliases.

Done criteria:
- Full basic CRUD on in-memory tables with query-visible effects.

Dependencies:
- `03-row-store-and-scans.md`

