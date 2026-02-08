# Step 10: Planner and Optimizer Parity Road

Goal:
- Replace direct execution shortcuts with a plan-node optimizer pipeline.

Scope:
- Logical plan generation from analyzed queries.
- Physical path selection for scans/joins/aggregates.
- Cost model and basic statistics integration.

Implementation files:
- Add `src/planner/mod.rs`
- Add `src/planner/logical.rs`
- Add `src/planner/physical.rs`
- Add `src/planner/cost.rs`
- Add `src/planner/stats.rs`

Deliverables:
- Stable plan node tree consumed by executor.
- Costed alternatives with deterministic selection.

Tests:
- Plan-shape tests for representative query classes.
- Cost-model sanity regression tests.
- Performance smoke tests on in-memory datasets.

Done criteria:
- Executor runs plans, not AST/analyzed trees directly.

Dependencies:
- `08-parse-analysis-and-type-system.md`

