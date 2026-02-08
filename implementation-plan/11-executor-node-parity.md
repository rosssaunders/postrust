# Step 11: Executor Node Parity

Goal:
- Implement executor support for full plan-node families.

Scope:
- Scan nodes, join nodes, aggregate/window nodes.
- Sort/materialize/hash nodes.
- CTE, subplan, and function scan behavior.

Implementation files:
- Add `src/executor/mod.rs`
- Add `src/executor/nodes/`
- Refactor `src/tcop/engine.rs` into node dispatch and runtime state.

Deliverables:
- Executor runtime with per-node state transitions.
- Correct row production semantics across node combinations.

Tests:
- Node-level unit tests.
- End-to-end query tests covering composed node plans.
- Memory behavior tests for materialization nodes.

Done criteria:
- Plan tree produced by planner can be fully executed.

Dependencies:
- `10-planner-optimizer.md`

