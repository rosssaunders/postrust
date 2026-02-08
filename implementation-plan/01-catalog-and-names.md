# Step 01: In-Memory Catalog and Name Resolution

Goal:
- Introduce persistent in-memory metadata for schemas, tables, columns, and type signatures.

Scope:
- Session/global in-memory catalog structs.
- OID allocator and stable object identity.
- Name resolution rules for `schema.table`, unqualified lookup, and `search_path`.

Implementation files:
- Add `src/catalog/mod.rs`
- Add `src/catalog/oid.rs`
- Add `src/catalog/schema.rs`
- Add `src/catalog/table.rs`
- Add `src/catalog/search_path.rs`
- Integrate read access into `src/tcop/engine.rs`

Deliverables:
- `Catalog` API with create/read/drop primitives.
- Lookup functions used by executor/planner.
- Basic built-in schemas: `pg_catalog`, `public`.

Tests:
- Unit tests for create/lookup/drop.
- Name resolution tests with schema-qualified and unqualified identifiers.
- Conflict behavior tests (duplicate objects, missing schemas).

Done criteria:
- Engine can resolve logical table metadata through catalog APIs.
- No table execution path depends on hardcoded relation names.

Dependencies:
- None.

