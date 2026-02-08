# In-Memory PostgreSQL Engine Plan

Objective:
- Build an in-memory-only PostgreSQL-compatible engine with full DML/DDL behavior parity over time.

Operating constraints:
- No durability/WAL/replication requirements in initial target.
- Priority is SQL behavior compatibility and protocol/client compatibility.

Execution model:
- Implement in ordered phases.
- Each step file defines scope, deliverables, tests, and done criteria.
- Do not start a later step until done criteria from dependency steps are met.

Milestone map:
1. `01-catalog-and-names.md`
2. `02-ddl-core-create-drop-alter.md`
3. `03-row-store-and-scans.md`
4. `04-dml-insert-update-delete-returning.md`
5. `05-constraints-and-defaults.md`
6. `06-transactions-and-mvcc-lite.md`
7. `07-parser-coverage.md`
8. `08-parse-analysis-and-type-system.md`
9. `09-system-catalog-and-dependencies.md`
10. `10-planner-optimizer.md`
11. `11-executor-node-parity.md`
12. `12-ddl-surface-area.md`
13. `13-dml-semantic-parity.md`
14. `14-type-function-operator-parity.md`
15. `15-security-roles-rls.md`
16. `16-protocol-and-client-compat.md`
17. `17-regression-compat-and-hardening.md`

Current code anchors:
- Parser/AST: `src/parser/`
- Query loop/protocol skeleton: `src/tcop/postgres.rs`
- Execution engine: `src/tcop/engine.rs`

