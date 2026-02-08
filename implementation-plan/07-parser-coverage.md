# Step 07: Parser Coverage Expansion

Goal:
- Reach near-complete PostgreSQL grammar coverage required for DDL/DML surface.

Scope:
- CTEs (`WITH`/recursive)
- Window functions
- Full `INSERT`/`UPDATE`/`DELETE`/`MERGE` grammar
- DDL grammar breadth (`CREATE`/`ALTER` families)

Implementation files:
- Extend `src/parser/lexer.rs`
- Extend `src/parser/sql_parser.rs`
- Extend `src/parser/ast.rs`
- Add parser-focused fixtures under `tests/parser/`

Deliverables:
- AST coverage for target SQL statements.
- Parser errors aligned to PostgreSQL behavior where feasible.

Tests:
- Grammar acceptance tests for major statement families.
- Ambiguity/precedence regression tests.
- Invalid syntax cases with expected error spans.

Done criteria:
- Parser can ingest planned DDL/DML roadmapped surface.

Dependencies:
- Current parser foundation in `src/parser/`.

