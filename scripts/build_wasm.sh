#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "wasm-pack is required. Install with: cargo install wasm-pack"
  exit 1
fi

wasm-pack build \
  --target web \
  --out-dir web/pkg \
  --release

echo "WASM bundle generated at web/pkg"
