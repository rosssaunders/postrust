#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo "[compat] running unit + integration regression suite"
cargo test -q

echo "[compat] running focused regression corpus"
cargo test -q --test regression

echo "[compat] running focused differential fixture suite"
cargo test -q --test differential

echo "[compat] running optional psql smoke suite"
scripts/compat/psql_smoke.sh

echo "[compat] done"
