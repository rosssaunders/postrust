# openassay SQL Notebook

SQL notebooks that run entirely in your browser. No server, no Python, no kernel — just SQL.

## Why?

Jupyter notebooks are great, but:
- They need Python, a kernel, package installs, virtual environments
- `import pandas as pd` for every single thing
- Sharing means "install these 47 packages first"
- The notebook server is a security liability

**SQL Notebooks** instead:
- 🐘 Full PostgreSQL SQL (CTEs, window functions, JOINs, JSON, 165+ functions)
- ⚡ Runs in-browser via WebAssembly (zero install)
- 📁 Drag & drop CSV/JSON files to import as tables
- 📊 Auto-charts when results have 2 columns (label + number)
- 💾 Save/load notebooks as `.sqlnb` files
- 🔗 Share via URL (notebook encoded in the link)
- 🔒 Your data never leaves your browser

## Quick Start

```bash
# From repo root
wasm-pack build --target web --out-dir notebook/pkg --release

# Serve (any static server works)
cd notebook
python3 -m http.server 8080
# or: npx serve .
```

Open http://localhost:8080

## Features

### Import Data
- **Drag & drop** CSV, TSV, or JSON files onto the notebook
- Files become SQL tables automatically
- Type inference for columns

### SQL Cells
- **Ctrl+Enter** — Run cell
- **Ctrl+Shift+Enter** — Run cell & add new cell below
- **Tab** — Indent
- Full PostgreSQL: JOINs, CTEs, window functions, aggregates, GROUPING SETS, JSON operators

### Auto-Visualization
When a query returns 2 columns where the second is numeric, a bar chart renders automatically.

```sql
-- This auto-charts!
SELECT department, SUM(salary) AS total
FROM employees
GROUP BY department
ORDER BY total DESC;
```

### Save & Share
- **Save** — Download as `.sqlnb` (JSON format)
- **Open** — Load `.sqlnb` or `.sql` files
- **Share** — Generates a URL with the notebook encoded in the hash

## Use Cases

- **Data exploration** — Import a CSV, explore with SQL
- **Reporting** — Build reusable query notebooks for weekly metrics
- **Teaching** — SQL tutorials that run in-browser
- **Interviews** — SQL coding exercises with zero setup
- **Quick analysis** — Faster than spinning up Jupyter + pandas

## Make Data Analysis Great Again 🫡
