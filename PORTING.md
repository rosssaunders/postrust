# PostgreSQL -> Rust Porting Notes

Source snapshot:
- Repository: `postgres-src/`
- Commit: `7467041`

Current converted module:
- PostgreSQL C file: `postgres-src/src/backend/parser/scansup.c`
- Rust module: `src/parser/scansup.rs`

Lexer/parser phase:
- PostgreSQL source files:
  - `postgres-src/src/backend/parser/scan.l`
  - `postgres-src/src/backend/parser/gram.y`
- Rust modules:
  - `src/parser/lexer.rs`
  - `src/parser/sql_parser.rs`
  - `src/parser/ast.rs`

TCOP query-loop phase:
- PostgreSQL source file:
  - `postgres-src/src/backend/tcop/postgres.c`
- Rust modules:
  - `src/tcop/mod.rs`
  - `src/tcop/postgres.rs`
  - `src/tcop/engine.rs`

Converted functions:
- `downcase_truncate_identifier`
- `downcase_identifier`
- `truncate_identifier`
- `scanner_isspace`

Converted grammar scope:
- `SELECT [ALL|DISTINCT] target_list`
- `FROM`:
  - comma-separated table expressions
  - qualified relation names + alias
  - `JOIN` variants: `INNER`, `LEFT [OUTER]`, `RIGHT [OUTER]`, `FULL [OUTER]`, `CROSS`, `NATURAL ... JOIN`
  - join conditions: `ON` and `USING (...)`
  - subqueries in `FROM` with alias
- `WHERE`
- `GROUP BY`
- `HAVING`
- `ORDER BY` (`ASC`/`DESC`)
- `LIMIT` / `OFFSET`
- set operations with precedence:
  - `UNION [ALL|DISTINCT]`
  - `INTERSECT [ALL|DISTINCT]`
  - `EXCEPT [ALL|DISTINCT]`
- Expression parsing with precedence for:
  - `OR`, `AND`, `NOT`
  - `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`
  - `+`, `-`, `*`, `/`, `%`
- Function calls and wildcard (`*`)

Behavior notes:
- Identifier length limit follows PostgreSQL default `NAMEDATALEN = 64`.
- Truncation respects UTF-8 character boundaries.
- Lowercasing is ASCII-only for exact byte-length stability.
- `tcop::postgres` implements a `PostgresMain`-style loop for:
  - frontend message dispatch (`Query`, `Parse`, `Bind`, `Execute`, `Describe`, `Close`, `Flush`, `Sync`, `Terminate`)
  - prepared statement and portal lifecycle
  - transaction state and "ignore till Sync" extended-protocol error behavior
  - `ReadyForQuery` state transitions (`Idle`, `InTransaction`, `FailedTransaction`)
- `tcop::engine` now provides real planning/execution for parsed query plans:
  - query output metadata (`RowDescription`) and row streaming (`DataRow`)
  - simple scalar `SELECT` execution with expression evaluation and parameters
  - set operation execution (`UNION`/`INTERSECT`/`EXCEPT`) with `ALL`/`DISTINCT`
  - query modifiers execution (`ORDER BY`, `LIMIT`, `OFFSET`)
  - `FROM` execution for:
    - subquery row sources
    - join execution (`INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, `NATURAL`, `USING`, `ON`)
    - minimal bootstrap relation support for `dual`
  - grouped query execution:
    - `GROUP BY`
    - `HAVING`
    - aggregate functions: `count`, `sum`, `avg`, `min`, `max`
- Current execution limitations:
  - general persisted relations are parsed but not yet backed by storage/catalog execution

Next high-value targets for conversion:
1. Port `postgres-src/src/backend/storage/buffer/` memory/buffer manager core
2. Port parse-analysis layer (`parse_expr.c`, `parse_target.c`) on top of Rust AST
3. Add parser coverage for CTEs (`WITH`), window clauses, and JSON table/expr grammar
4. Add execution support for general relations/catalog-backed scans
