# Step 14: Type/Function/Operator Parity

Goal:
- Expand built-in type system and function/operator behavior toward PostgreSQL parity.

Scope:
- Numeric/text/date/time/json/array/range type families.
- Function library coverage.
- Operator semantics and precedence behavior.

Implementation files:
- Add `src/types/mod.rs`
- Add `src/types/builtin/`
- Add `src/functions/mod.rs`
- Add `src/operators/mod.rs`
- Integrate with analyzer and executor.

Deliverables:
- Broad built-in type registry.
- Coercion and function/operator evaluation parity improvements.

Tests:
- Type behavior matrix tests.
- Function/operator compatibility tests.
- Expression result parity tests against PostgreSQL baselines.

Done criteria:
- Common PostgreSQL workloads run without major type/function gaps.

Dependencies:
- `08-parse-analysis-and-type-system.md`
- `11-executor-node-parity.md`

Current progress:
- Implemented SQL three-valued logic for boolean operators (`AND`/`OR`) and comparison null semantics in expression evaluation.
- Implemented null propagation and safe errors for arithmetic operators (`/` and `%` now return `division by zero` errors instead of panicking).
- Expanded scalar function surface in executor:
  - `nullif`, `greatest`, `least`
  - `concat`, `concat_ws`
  - `substring`/`substr`, `left`, `right`
  - `btrim`, `ltrim`, `rtrim`, `replace`
  - `char_length` alias and null-strict behavior for `lower`/`upper`/`length`/`abs`
- Added parser/executor support for `IS NULL` and `IS NOT NULL`.
- Added parser/executor support for:
  - `IS DISTINCT FROM` / `IS NOT DISTINCT FROM`
  - `BETWEEN` / `NOT BETWEEN`
  - `LIKE` / `ILIKE` and negated forms
- Added explicit cast expression support:
  - `CAST(value AS type)`
  - `value::type`
- Added temporal operator arithmetic support:
  - `date/timestamp + int_days`
  - `date/timestamp - int_days`
  - `date - date` (days)
  - `timestamp - timestamp` (seconds)
- Added mixed-type coercion upgrades for expression evaluation:
  - numeric operators now coerce numeric text values
  - predicate comparison/in-membership now compare mixed numeric/bool/text values more consistently
- Added date/time builtin tranche:
  - `date`, `timestamp`
  - `current_date`, `current_timestamp`, `now`
  - `extract`, `date_part`, `date_trunc`
  - `date_add`, `date_sub`
- Added regression tests covering the above semantics and parser cases.
