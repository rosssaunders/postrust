# Browser / WASM

Build the WASM package:

```bash
scripts/build_wasm.sh
```

This writes `web/pkg/postgrust.js` + `web/pkg/postgrust_bg.wasm`.

Run the local static server:

```bash
cargo run --bin web_server -- 8080
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080).

The browser harness calls one of these exports from the WASM module:

- `execute_sql(sql)`
- `execute_sql_json(sql)` (structured JSON payload with `results[{columns,rows,...}]`)
- `exec_sql(sql)`
- `run_sql(sql)`

It also uses snapshot helpers for browser persistence:

- `export_state_snapshot()`
- `import_state_snapshot(snapshot)`
- `reset_state_snapshot()`

HTTP helpers:

- `http_get(url)` (async, browser fetch)
- `execute_sql_http(sql)` / `run_sql_http(sql)` (async SQL runner that resolves `http_get('...')` calls before execution)
- `execute_sql_http_json(sql)` / `run_sql_http_json(sql)` (same as above, with structured JSON result payload)

Behavior in `web/index.html`:

- successful SQL runs auto-save a snapshot into `localStorage`
- load-time auto-restore replays the saved snapshot
- toolbar buttons support manual export/import/reset
- SQL execution prefers `execute_sql_http` when available
- `web/index.html` also renders tabular result sets into AG Grid when a structured JSON runner is available
