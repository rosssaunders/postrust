# PostgreSQL 18 Compatibility Assessment

**Target:** PostgreSQL 18 (master branch)  
**Method:** 39 regression test files run via wire protocol (psql → pg_server)  
**Date:** 2026-02-11

## Overall Score

**23% statement-level pass rate** (2,995 / 12,480 statements)

This is a raw score — many failures cascade (one unsupported `CREATE` causes all subsequent statements referencing that table to fail). The effective compatibility for supported features is higher, but this is the honest number against the real PostgreSQL regression suite.

## Per-File Results

| File | Passed | Total | Rate |
|------|--------|-------|------|
| select_having.sql | 21 | 23 | **91%** |
| numeric.sql | 790 | 1,059 | **74%** |
| case.sql | 49 | 67 | **73%** |
| select_distinct.sql | 66 | 113 | **58%** |
| test_setup.sql | 36 | 70 | **51%** |
| delete.sql | 5 | 10 | **50%** |
| union.sql | 91 | 206 | **44%** |
| explain.sql | 32 | 76 | **42%** |
| select_implicit.sql | 18 | 44 | **40%** |
| merge.sql | 230 | 642 | **35%** |
| aggregates.sql | 215 | 619 | **34%** |
| matview.sql | 64 | 187 | **34%** |
| strings.sql | 185 | 551 | **33%** |
| sequence.sql | 84 | 261 | **32%** |
| select.sql | 32 | 106 | **30%** |
| boolean.sql | 28 | 98 | **28%** |
| create_view.sql | 90 | 311 | **28%** |
| float4.sql | 27 | 101 | **26%** |
| subselect.sql | 87 | 366 | **23%** |
| create_index.sql | 155 | 684 | **22%** |
| text.sql | 14 | 73 | **19%** |
| insert_conflict.sql | 48 | 269 | **17%** |
| int8.sql | 29 | 174 | **16%** |
| arrays.sql | 84 | 529 | **15%** |
| int4.sql | 14 | 94 | **14%** |
| float8.sql | 24 | 186 | **12%** |
| update.sql | 38 | 302 | **12%** |
| date.sql | 32 | 272 | **11%** |
| interval.sql | 48 | 450 | **10%** |
| groupingsets.sql | 16 | 219 | **7%** |
| insert.sql | 22 | 392 | **5%** |
| window.sql | 10 | 429 | **2%** |
| with.sql | 8 | 312 | **2%** |
| json.sql | 6 | 469 | **1%** |
| jsonb.sql | 5 | 1,100 | **0%** |
| timestamp.sql | 2 | 178 | **1%** |
| create_table.sql | 0 | 329 | **0%** |
| int2.sql | 0 | 76 | **0%** |
| time.sql | 0 | 44 | **0%** |

## Error Analysis (Top 15 by frequency)

| Count | Error Category | Impact |
|-------|---------------|--------|
| 1,779 | `current transaction is aborted` | **Cascade** — caused by earlier errors in same transaction |
| 1,403 | `unexpected token after end of statement` | **Parser** — syntax not recognised |
| 1,353 | `relation "X" does not exist` | **Cascade** — table wasn't created due to earlier error |
| 1,274 | `unsupported cast type name` | **Parser** — `::type` cast for custom/domain types |
| 846 | `expected query term (SELECT, VALUES...)` | **Parser** — can't parse certain statement structures |
| 477 | `expected identifier` | **Parser** — `table.*`, qualified names, etc. |
| 432 | `expected ')' after function arguments` | **Parser** — complex function call syntax |
| 369 | `expected TABLE, SCHEMA... after CREATE` | **Parser** — CREATE TYPE, CREATE DOMAIN, CREATE TEMP TABLE |
| 271 | `expected '(' after CREATE TABLE name` | **Parser** — CREATE TABLE variants |
| 144 | `relation already exists` | **Engine** — IF NOT EXISTS handling |
| 123 | `expected AS in CTE` | **Parser** — CTE syntax variants |
| 89 | `expected expression` | **Parser** — expression parsing gaps |
| 78 | `unknown column` | **Engine** — column resolution |
| 77 | `expected '(' after OVER` | **Parser** — window function syntax |
| 74 | `unsupported function: pg_get_viewdef` | **Missing function** |

## Prioritised Fix Plan

### Phase 1: Cascade Breakers (highest ROI)

These fixes would unblock hundreds of cascading failures:

1. **`CREATE TEMP TABLE` / `CREATE TEMPORARY TABLE`** — 369+ errors. Many test files create temp tables for setup. Without this, everything downstream fails.

2. **`CREATE TYPE` / `CREATE DOMAIN`** — Needed by case.sql, json.sql, many others. Blocks custom type tests.

3. **Transaction error recovery** — 1,779 "transaction aborted" errors are cascades from earlier failures. Better error handling (auto-rollback or implicit transactions per statement) would let tests continue past failures.

### Phase 2: Parser Gaps (medium ROI)

4. **`table.*` column expansion** — `SELECT t.*` syntax. 477 "expected identifier" errors, many from this.

5. **`ORDER BY ... USING >` operator syntax** — PG-specific but appears in 13+ test files.

6. **Complex `::type` casts** — Custom types, domains, arrays with casts. 1,274 errors.

7. **Window function `OVER` clause variants** — Named windows, complex frame specs. 77+ errors blocking window.sql (2% pass rate).

8. **CTE syntax variants** — Materialized/not materialized CTEs, recursive variants. 123 errors blocking with.sql (2% pass rate).

### Phase 3: Missing Functions & Features

9. **`format()` function** — 40 errors
10. **`pg_get_viewdef()`** — 74 errors (needed for view tests)
11. **`pg_input_is_valid()`** — 35 errors
12. **`generate_series` via wire protocol** — Works in WASM, broken over wire

### Phase 4: Type System

13. **Time/timestamp types via wire protocol** — time.sql: 0%, timestamp.sql: 1%
14. **Integer overflow handling** — int2.sql: 0%
15. **Array indexing and operations** — arrays.sql: 15%

## Estimated Impact

| Phase | Effort | Errors Fixed | New Score (est.) |
|-------|--------|-------------|-----------------|
| Phase 1 | 1-2 weeks | ~3,000 cascade | ~45-50% |
| Phase 2 | 2-3 weeks | ~2,500 parser | ~65-70% |
| Phase 3 | 1 week | ~200 functions | ~72% |
| Phase 4 | 2 weeks | ~500 types | ~75-80% |

## Notes

- The 23% raw score understates actual capability because of cascading failures — one missing `CREATE TEMP TABLE` can cause 50+ subsequent errors in a single file
- The WASM engine likely scores higher because the internal API bypasses wire protocol parsing
- Many features work individually but fail in the regression tests due to missing prerequisite objects
- The test fixture tables (onek, tenk1) don't load because `COPY FROM` file paths don't resolve — fixing this alone would unlock many select/join/aggregate tests
