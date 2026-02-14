# Step 00: Refactor to Match PostgreSQL Source Layout

## Goal

Restructure the Rust codebase to mirror the PostgreSQL C backend directory layout.
The 21k-line `src/tcop/engine.rs` monolith must be decomposed into modules that
map 1:1 to PostgreSQL's `src/backend/` directories.

## Reference

PostgreSQL source is available at `postgres/src/backend/` (submodule).

## Current State

```
src/
├── tcop/engine.rs       # 21k lines — execution, DDL, DML, storage, functions, everything
├── tcop/postgres.rs     # Wire protocol session (≈ PG's tcop/postgres.c)
├── parser/              # Lexer, AST, parser (≈ PG's parser/)
├── catalog/             # OID, schemas, tables, search_path (≈ PG's catalog/)
├── txn/                 # Transactions, snapshots, visibility (≈ PG's access/transam/)
├── protocol/            # Wire protocol messages (≈ PG's libpq/)
├── security/            # Roles, ACL, RLS (≈ PG's commands/ + catalog/)
├── browser.rs           # WASM bindings (no PG equivalent)
├── bin/pg_server.rs     # TCP server (≈ PG's postmaster/)
├── bin/web_server.rs    # HTTP server for WASM harness
```

## Target Layout

Map Rust modules to PostgreSQL directories. Each Rust file should correspond to a
C file or logical grouping in the PG source.

```
src/
├── tcop/                          # ← PG: src/backend/tcop/
│   ├── mod.rs
│   ├── postgres.rs                # Session/wire protocol dispatch (tcop/postgres.c)
│   ├── utility.rs                 # DDL/utility command dispatch (tcop/utility.c)
│   └── pquery.rs                  # Portal/query execution dispatch (tcop/pquery.c)
│
├── executor/                      # ← PG: src/backend/executor/
│   ├── mod.rs
│   ├── exec_main.rs               # Top-level executor entry (executor/execMain.c)
│   ├── exec_scan.rs               # Sequential scan node (executor/execScan.c)
│   ├── exec_expr.rs               # Expression evaluation (executor/execExpr.c)
│   ├── exec_grouping.rs           # GROUP BY / GROUPING SETS (executor/execGrouping.c)
│   ├── exec_srf.rs                # Set-returning functions (executor/execSRF.c)
│   ├── node_agg.rs                # Aggregate execution (executor/nodeAgg.c)
│   ├── node_hash_join.rs          # Hash join (executor/nodeHashjoin.c)
│   ├── node_merge_join.rs         # Merge join (executor/nodeMergejoin.c)
│   ├── node_nested_loop.rs        # Nested loop join (executor/nodeNestloop.c)
│   ├── node_sort.rs               # Sort (executor/nodeSort.c)
│   ├── node_limit.rs              # LIMIT/OFFSET (executor/nodeLimit.c)
│   ├── node_subquery.rs           # Subquery scan (executor/nodeSubqueryscan.c)
│   ├── node_window_agg.rs         # Window functions (executor/nodeWindowAgg.c)
│   ├── node_modify_table.rs       # INSERT/UPDATE/DELETE (executor/nodeModifyTable.c)
│   ├── node_result.rs             # Result node / VALUES (executor/nodeResult.c)
│   ├── node_append.rs             # UNION (executor/nodeAppend.c)
│   ├── node_set_op.rs             # INTERSECT/EXCEPT (executor/nodeSetOp.c)
│   └── node_cte.rs                # CTE scan (executor/nodeCtescan.c)
│
├── commands/                      # ← PG: src/backend/commands/
│   ├── mod.rs
│   ├── create_table.rs            # CREATE TABLE (commands/tablecmds.c)
│   ├── drop.rs                    # DROP (commands/dropcmds.c)
│   ├── alter.rs                   # ALTER TABLE (commands/tablecmds.c)
│   ├── view.rs                    # CREATE/DROP VIEW (commands/view.c)
│   ├── index.rs                   # CREATE/DROP INDEX (commands/indexcmds.c)
│   ├── sequence.rs                # CREATE/DROP SEQUENCE (commands/sequence.c)
│   ├── schema.rs                  # CREATE/DROP SCHEMA (commands/schemacmds.c)
│   ├── extension.rs               # CREATE/DROP EXTENSION (commands/extension.c)
│   ├── function.rs                # CREATE FUNCTION (commands/functioncmds.c)
│   ├── copy.rs                    # COPY (commands/copy.c)
│   ├── explain.rs                 # EXPLAIN (commands/explain.c)
│   ├── matview.rs                 # Materialized views (commands/matview.c)
│   ├── variable.rs                # SET/SHOW/RESET (commands/variable.c)
│   └── do_block.rs                # DO blocks (commands/functioncmds.c)
│
├── parser/                        # ← PG: src/backend/parser/ (already exists)
│   ├── mod.rs
│   ├── ast.rs                     # AST node definitions (nodes/)
│   ├── lexer.rs                   # Lexical scanner (parser/scan.l)
│   ├── sql_parser.rs              # Grammar/parser (parser/gram.y)
│   └── scansup.rs                 # Scanner support (parser/scansup.c)
│
├── catalog/                       # ← PG: src/backend/catalog/ (already exists)
│   ├── mod.rs
│   ├── oid.rs                     # OID allocation
│   ├── schema.rs                  # Schema catalog
│   ├── table.rs                   # Table/relation catalog
│   ├── search_path.rs             # search_path resolution
│   ├── dependency.rs              # Object dependencies (catalog/pg_depend.c)
│   └── system_catalogs.rs         # pg_class, pg_type, info_schema (catalog/namespace.c)
│
├── storage/                       # ← PG: src/backend/storage/
│   ├── mod.rs
│   ├── heap.rs                    # In-memory heap storage (storage/buffer/ + access/heap/)
│   └── tuple.rs                   # Tuple representation (access/common/heaptuple.c)
│
├── access/                        # ← PG: src/backend/access/
│   ├── mod.rs
│   └── transam/                   # ← PG: src/backend/access/transam/
│       ├── mod.rs
│       ├── xact.rs                # Transaction control (access/transam/xact.c)
│       ├── snapshot.rs            # Snapshot management
│       └── visibility.rs          # Tuple visibility (access/heap/heapam_visibility.c)
│
├── utils/                         # ← PG: src/backend/utils/
│   ├── mod.rs
│   ├── adt/                       # Abstract data types (utils/adt/)
│   │   ├── mod.rs
│   │   ├── string_functions.rs    # String functions (utils/adt/varlena.c)
│   │   ├── math_functions.rs      # Math functions (utils/adt/float.c, numeric.c)
│   │   ├── datetime.rs            # Date/time functions (utils/adt/datetime.c)
│   │   ├── json.rs                # JSON/JSONB functions (utils/adt/jsonb.c, jsonbsubs.c)
│   │   ├── uuid.rs                # UUID functions (utils/adt/uuid.c)
│   │   └── misc.rs                # Other functions
│   └── fmgr.rs                    # Function manager/registry (utils/fmgr/)
│
├── optimizer/                     # ← PG: src/backend/optimizer/ (future)
│   └── mod.rs                     # Placeholder for query planner
│
├── protocol/                      # ← PG: src/backend/libpq/ (already exists)
│   ├── mod.rs
│   ├── messages.rs                # Wire protocol encoding
│   ├── startup.rs                 # Connection handshake
│   └── copy.rs                    # COPY protocol
│
├── security/                      # ← PG: commands/ + catalog/ (already exists)
│   ├── mod.rs
│   ├── roles.rs
│   ├── acl.rs
│   └── rls.rs
│
├── browser.rs                     # WASM bindings (no PG equivalent)
├── lib.rs                         # Library root
├── main.rs                        # CLI entry
└── bin/
    ├── pg_server.rs               # ≈ PG's postmaster/
    └── web_server.rs              # WASM HTTP server
```

## Refactoring Steps

Execute in this order. Each step must pass `cargo test` and `cargo clippy -- -D warnings`.

### Phase 1: Extract commands/ (DDL + utility statements)

1. Create `src/commands/mod.rs`
2. Extract all CREATE TABLE logic from engine.rs → `src/commands/create_table.rs`
3. Extract DROP logic → `src/commands/drop.rs`
4. Extract ALTER TABLE logic → `src/commands/alter.rs`
5. Extract CREATE/DROP VIEW → `src/commands/view.rs`
6. Extract CREATE/DROP INDEX → `src/commands/index.rs`
7. Extract sequence handling → `src/commands/sequence.rs`
8. Extract schema DDL → `src/commands/schema.rs`
9. Extract extension handling → `src/commands/extension.rs`
10. Extract CREATE FUNCTION → `src/commands/function.rs`
11. Extract EXPLAIN → `src/commands/explain.rs`
12. Extract materialized view → `src/commands/matview.rs`
13. Extract SET/SHOW/RESET → `src/commands/variable.rs`
14. Extract DO blocks → `src/commands/do_block.rs`
15. Create `src/tcop/utility.rs` to dispatch DDL/utility commands (like PG's ProcessUtility)

### Phase 2: Extract storage/

1. Create `src/storage/mod.rs`
2. Extract in-memory heap/table storage from engine.rs → `src/storage/heap.rs`
3. Extract tuple representation → `src/storage/tuple.rs`

### Phase 3: Move txn/ → access/transam/

1. Create `src/access/transam/` directory
2. Move `src/txn/mod.rs` → `src/access/transam/xact.rs`
3. Move `src/txn/snapshot.rs` → `src/access/transam/snapshot.rs`
4. Move `src/txn/visibility.rs` → `src/access/transam/visibility.rs`
5. Update all imports

### Phase 4: Extract executor/

This is the largest extraction — the query execution engine.

1. Create `src/executor/mod.rs`
2. Extract top-level execution entry → `src/executor/exec_main.rs`
3. Extract expression evaluation → `src/executor/exec_expr.rs`
4. Extract sequential scan → `src/executor/exec_scan.rs`
5. Extract aggregate execution → `src/executor/node_agg.rs`
6. Extract join nodes → `src/executor/node_hash_join.rs`, `node_nested_loop.rs`, `node_merge_join.rs`
7. Extract sort → `src/executor/node_sort.rs`
8. Extract LIMIT/OFFSET → `src/executor/node_limit.rs`
9. Extract subquery scan → `src/executor/node_subquery.rs`
10. Extract window functions → `src/executor/node_window_agg.rs`
11. Extract INSERT/UPDATE/DELETE/MERGE → `src/executor/node_modify_table.rs`
12. Extract UNION/INTERSECT/EXCEPT → `src/executor/node_append.rs`, `node_set_op.rs`
13. Extract CTE scan → `src/executor/node_cte.rs`
14. Create `src/tcop/pquery.rs` for portal/query dispatch

### Phase 5: Extract utils/ (functions)

1. Create `src/utils/adt/` directory
2. Extract string functions → `src/utils/adt/string_functions.rs`
3. Extract math functions → `src/utils/adt/math_functions.rs`
4. Extract datetime functions → `src/utils/adt/datetime.rs`
5. Extract JSON/JSONB functions → `src/utils/adt/json.rs`
6. Extract misc functions → `src/utils/adt/misc.rs`
7. Create function registry → `src/utils/fmgr.rs`

### Phase 6: Extract system catalogs

1. Move system catalog query logic → `src/catalog/system_catalogs.rs`
   (pg_class, pg_type, pg_attribute, information_schema queries)

### Phase 7: Cleanup

1. `src/tcop/engine.rs` should now be minimal — just coordinating between
   executor, commands, and catalog. Rename or redistribute any remaining logic.
2. Verify the full PostgreSQL directory mapping is correct.
3. Update `implementation-plan/README.md` with new file layout.
4. Update project README.

## Mapping Reference

| PostgreSQL C directory        | OpenAssay Rust module         |
|-------------------------------|------------------------------|
| `src/backend/tcop/`           | `src/tcop/`                  |
| `src/backend/executor/`       | `src/executor/`              |
| `src/backend/commands/`       | `src/commands/`              |
| `src/backend/parser/`         | `src/parser/`                |
| `src/backend/catalog/`        | `src/catalog/`               |
| `src/backend/storage/`        | `src/storage/`               |
| `src/backend/access/transam/` | `src/access/transam/`        |
| `src/backend/utils/adt/`      | `src/utils/adt/`             |
| `src/backend/utils/fmgr/`     | `src/utils/fmgr.rs`          |
| `src/backend/optimizer/`      | `src/optimizer/`             |
| `src/backend/libpq/`          | `src/protocol/`              |
| `src/backend/postmaster/`     | `src/bin/pg_server.rs`       |

## Constraints

- Every step must pass `cargo test` and `cargo clippy -- -D warnings`
- No functionality changes — pure refactor
- Public API surface stays the same (WASM exports, binary entry points)
- Each phase should be a separate commit (or series of commits)
- engine.rs should shrink monotonically — never add code to it

## Done Criteria

- `src/tcop/engine.rs` is either removed or < 500 lines (coordination only)
- All 350+ tests pass
- Directory structure matches the mapping table above
- `cargo clippy -- -D warnings` clean
