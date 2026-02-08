# Postrust

In-memory PostgreSQL-inspired engine in Rust, with:

- SQL parser + execution engine
- PostgreSQL wire protocol server mode
- Browser/WASM SQL harness
- HTTP + JSON/JSONB function support in the harness path

## Screenshot

![Postrust Browser SQL Harness](assets/postrust-browser-harness.png)

## Quick Start

Build and test:

```bash
cargo test
```

Run the PostgreSQL-compatible server:

```bash
cargo run --bin pg_server -- 55432
```

Run the browser/WASM harness:

```bash
scripts/build_wasm.sh
cargo run --bin web_server -- 8080
```

Open:

- [http://127.0.0.1:8080](http://127.0.0.1:8080)

## Project Layout

- `src/` core engine, parser, protocol, security, txn
- `web/` browser harness UI
- `tests/` regression + differential tests
- `implementation-plan/` staged parity roadmap

