// node_cte.rs — CTE / WITH execution support
//
// Corresponds to PostgreSQL's:
//   src/backend/executor/nodeRecursiveunion.c  — recursive CTE evaluation
//   src/backend/executor/nodeCtescan.c         — CTE scan (reading from a
//                                                materialised CTE result)
//
// In PostgRust the recursive CTE evaluation lives in `exec_main.rs`
// (`evaluate_recursive_cte_binding`) because the interpreter-style executor
// evaluates CTEs eagerly before executing the outer query.  This differs from
// PostgreSQL's Volcano-style pull model where `RecursiveUnion` and `CteScan`
// are separate plan nodes, but the *algorithm* is identical:
//
//   1.  Run the non-recursive (seed) term → store in working table.
//   2.  Loop:
//       a. Run the recursive term reading ONLY from the working table.
//       b. No new rows?  → terminate.
//       c. For UNION (not UNION ALL): de-duplicate against accumulated rows.
//       d. New rows become the next working table AND are appended to the
//          accumulated result.
//       e. Goto 2.
//
// Key correctness invariant: the recursive term sees **only** the working
// table (rows from the *previous* iteration), never the full accumulated
// result.  Violating this invariant causes infinite loops because the
// termination condition (WHERE n < limit) re-evaluates against old rows.
//
// See `exec_main.rs::evaluate_recursive_cte_binding` for the implementation.
