# Step 17: Regression Compatibility and Hardening

Goal:
- Establish compatibility confidence and operational robustness.

Scope:
- PostgreSQL regression-style test harness integration.
- Differential testing against PostgreSQL outputs.
- Fuzzing, property tests, and performance guardrails.

Implementation files:
- Add `tests/regression/`
- Add `tests/differential/`
- Add `scripts/compat/`
- Add CI jobs in repository pipeline config.

Deliverables:
- Compatibility scorecard by feature area.
- Reproducible benchmark and correctness gates in CI.

Tests:
- Standardized SQL regression corpus.
- Differential assertion tests (engine vs PostgreSQL).
- Stress tests for memory behavior and edge syntax.

Done criteria:
- Release-quality confidence for targeted compatibility surface.
- Known-gap list is explicit and continuously tracked.

Dependencies:
- All prior steps.

Current progress:
- Added regression integration corpus under `tests/regression/`:
  - `tests/regression/basic_sql.rs`
  - `tests/regression/corpus/basic.sql`
- Added differential-style expected-output fixtures under `tests/differential/`:
  - `tests/differential/sql_fixtures.rs`
- Added compatibility runner scripts under `scripts/compat/`:
  - `scripts/compat/run_regression.sh`
  - `scripts/compat/psql_smoke.sh`
  - `scripts/compat/README.md`
- Added CI workflow gate:
  - `.github/workflows/compat.yml` runs the compatibility script on push/PR.
