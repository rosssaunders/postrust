# Step 05: Constraints, Defaults, and Integrity Checks

Goal:
- Enforce core table integrity semantics at write time.

Scope:
- Column defaults
- `NOT NULL`
- `CHECK`
- Basic `PRIMARY KEY`/`UNIQUE`
- Basic `FOREIGN KEY` validation

Implementation files:
- Add `src/catalog/constraints.rs`
- Add `src/executor/constraints.rs`
- Add `src/executor/defaults.rs`
- Extend DDL handlers in `src/ddl/`

Deliverables:
- Constraint metadata stored in catalog.
- DML path validates constraints before commit.
- Deterministic error codes/messages for violations.

Tests:
- Positive and negative cases for each constraint type.
- Multi-row insert/update constraint behavior.
- FK parent/child mutation checks.

Done criteria:
- Constraint correctness across inserts/updates/deletes.

Dependencies:
- `02-ddl-core-create-drop-alter.md`
- `04-dml-insert-update-delete-returning.md`

