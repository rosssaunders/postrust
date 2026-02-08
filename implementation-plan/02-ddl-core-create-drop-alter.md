# Step 02: Core DDL (`CREATE`/`DROP`/Basic `ALTER`)

Goal:
- Support foundational table lifecycle DDL against the in-memory catalog.

Scope:
- `CREATE TABLE`
- `DROP TABLE`
- Basic `ALTER TABLE` (add/drop/rename column)

Implementation files:
- Add `src/ddl/mod.rs`
- Add `src/ddl/create_table.rs`
- Add `src/ddl/drop_table.rs`
- Add `src/ddl/alter_table.rs`
- Hook command dispatch in `src/tcop/postgres.rs` and `src/tcop/engine.rs`

Deliverables:
- DDL statements mutate `Catalog`.
- Result command tags align with PostgreSQL patterns.

Tests:
- End-to-end CLI tests: create, alter, drop.
- Error tests for duplicates/missing objects/invalid alter operations.

Done criteria:
- User can create table metadata and alter/drop it in session memory.
- Catalog state reflects DDL immediately for subsequent commands.

Dependencies:
- `01-catalog-and-names.md`

