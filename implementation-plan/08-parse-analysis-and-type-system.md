# Step 08: Parse Analysis, Type Coercion, and Semantics

Goal:
- Add PostgreSQL-like semantic analysis between parsing and planning.

Scope:
- Name binding and scope resolution.
- Type inference/coercion and cast rules.
- Function/operator resolution.
- Collation and expression semantic checks.

Implementation files:
- Add `src/analyzer/mod.rs`
- Add `src/analyzer/binding.rs`
- Add `src/analyzer/types.rs`
- Add `src/analyzer/functions.rs`
- Integrate in planning path before execution.

Deliverables:
- Distinct analyzed query representation (post-AST).
- Stable coercion rules and semantic errors.

Tests:
- Type coercion matrix tests.
- Function/operator overload resolution tests.
- Scope and alias binding tests.

Done criteria:
- Query execution depends on analyzed forms, not raw AST shortcuts.

Dependencies:
- `01-catalog-and-names.md`
- `07-parser-coverage.md`

