# Async Engine Refactor

## Goal
Make the postrust SQL engine fully async, so that built-in functions like `http_get()` and WebSocket operations can use native async I/O instead of blocking calls.

## Why
- Synchronous XHR (used by `http_get` in WASM) is deprecated and will be removed from browsers
- WebSocket message callbacks need async to work properly
- The vision: an async-native SQL engine where network data sources (HTTP APIs, WebSockets, CSV/JSON URLs) are first-class — no ETL, no Python glue

## Architecture

### Current (sync)
```
PostgresSession::exec_simple_query()
  → execute_operation()
    → execute_planned_query()
      → evaluate_expression()
        → eval_function_call()
          → eval_http_get_builtin()  ← blocks here (sync XHR / ureq)
```

### Target (async)
```
PostgresSession::exec_simple_query()  ← async
  → execute_operation()              ← async  
    → execute_planned_query()        ← async
      → evaluate_expression()        ← async
        → eval_function_call()       ← async
          → eval_http_get_builtin()  ← .await (fetch API / reqwest)
```

## Rules

1. **Every function in the execution chain** from `exec_simple_query` down to leaf expression evaluation must become `async fn` and use `.await` at call sites
2. **The parser and lexer stay synchronous** — they don't do I/O
3. **Use `async fn` and `.await`** — NOT `block_on()` or manual Future polling
4. **Platform-specific async HTTP:**
   - WASM: use `web_sys::window().fetch_with_str()` + `JsFuture` (re-add `wasm-bindgen-futures`)
   - Native: use `reqwest` (async HTTP client, replace `ureq`)
5. **Runtime:**
   - WASM: no runtime needed (JS event loop is the runtime)
   - Native: use `tokio` with `#[tokio::main]` in main.rs and `#[tokio::test]` for tests
6. **browser.rs WASM exports:** The `execute_sql_http_json` etc functions should become `async` again, using `wasm_bindgen_futures`
7. **browser.rs non-WASM functions** (used by tests): should use `tokio::runtime::Runtime::block_on()` or just be `async` with `#[tokio::test]`
8. **WebSocket functions** (`ws.connect`, `ws.send`, etc.) should also become properly async
9. **All 348 existing tests must pass** after the refactor. Convert `#[test]` to `#[tokio::test]` where needed.
10. **Commit incrementally** — don't try to do it all in one commit. Suggested order:
    a. Add tokio + reqwest + wasm-bindgen-futures dependencies
    b. Make engine.rs core execution path async (execute_planned_query, evaluate_expression, etc.)
    c. Make postgres.rs session execution async
    d. Update browser.rs WASM exports
    e. Convert tests to #[tokio::test]
    f. Replace ureq with reqwest for native http_get
    g. Replace sync XHR with fetch API for WASM http_get
    h. Run all tests, fix any remaining issues

## Key Files
- `src/tcop/engine.rs` (18k lines) — the big one. Query execution, expression evaluation, all built-in functions
- `src/tcop/postgres.rs` (5k lines) — PostgreSQL wire protocol session, SimpleQuery dispatch
- `src/browser.rs` (440 lines) — WASM browser bindings
- `src/main.rs` — CLI entry point (needs tokio runtime)
- `src/lib.rs` — library root
- `Cargo.toml` — dependencies

## Dependencies to Add
```toml
[dependencies]
tokio = { version = "1", features = ["rt", "macros"] }

[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
reqwest = { version = "0.12", features = ["rustls-tls"], default-features = false }

[target.'cfg(target_arch = "wasm32")'.dependencies]  
wasm-bindgen-futures = "0.4"
```

## Testing
Run `cargo test` after each commit. All 348 tests must pass.
Run `cargo check --target wasm32-unknown-unknown --lib` to verify WASM compilation.

## Do NOT
- Change the parser or lexer (they stay sync)
- Change the SQL syntax or semantics
- Remove any existing functionality
- Use `block_on` inside async functions (that causes panics in tokio)
