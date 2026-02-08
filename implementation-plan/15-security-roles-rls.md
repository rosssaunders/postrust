# Step 15: Security Model (Roles/Grants/RLS)

Goal:
- Implement PostgreSQL-like security enforcement semantics.

Scope:
- Roles and membership.
- GRANT/REVOKE privilege checks.
- Row-level security policies.

Implementation files:
- Add `src/security/mod.rs`
- Add `src/security/roles.rs`
- Add `src/security/acl.rs`
- Add `src/security/rls.rs`
- Hook checks into analyzer/executor.

Deliverables:
- Authorization checks for DDL/DML/query execution.
- Policy-based row filtering/enforcement.

Tests:
- Privilege grant/revoke test matrix.
- RLS behavior tests for read/write operations.
- Ownership and default privilege tests.

Done criteria:
- Engine enforces permission model and row policies consistently.

Dependencies:
- `09-system-catalog-and-dependencies.md`
- `11-executor-node-parity.md`

Current progress:
- Added `src/security/mod.rs`, `src/security/roles.rs`, `src/security/acl.rs`, and `src/security/rls.rs`.
- Added role + membership model with superuser/login/password flags and `SET ROLE` capability checks.
- Added table ACL grant/revoke enforcement for `SELECT`/`INSERT`/`UPDATE`/`DELETE`/`TRUNCATE`.
- Added ownership tracking for relations and owner-gated DDL paths (`ALTER`, `DROP`, `CREATE INDEX`, materialized view refresh).
- Added row-level security state and policy model with:
  - `ALTER TABLE ... ENABLE|DISABLE ROW LEVEL SECURITY`
  - `CREATE POLICY ... FOR ... TO ... USING (...) WITH CHECK (...)`
  - `DROP POLICY ...`
- Wired privilege checks into engine relation scans and DML execution (including `MERGE`) and wired RLS visibility/check enforcement.
- Extended transactional snapshots to include security state so role/grant/policy changes rollback correctly.
- Added session-level tests covering grants/revokes, RLS filtering/check enforcement, and transactional rollback of security commands.
