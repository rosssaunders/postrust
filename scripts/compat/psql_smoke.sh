#!/usr/bin/env bash
set -euo pipefail

if ! command -v psql >/dev/null 2>&1; then
  echo "[compat] psql not found; skipping pgwire smoke test"
  exit 0
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PORT="${1:-55433}"
LOG="/tmp/postgrust-pgwire-smoke.log"

cargo run --quiet --bin pg_server -- "127.0.0.1:${PORT}" >"${LOG}" 2>&1 &
PID=$!
cleanup() {
  kill "${PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 1

echo "[compat] running psql smoke against 127.0.0.1:${PORT}"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "SELECT 1 AS one" \
  -c "CREATE TABLE wire_smoke_t (id int8)" \
  -c "INSERT INTO wire_smoke_t VALUES (42)" \
  -c "SELECT id FROM wire_smoke_t"

echo "[compat] running SCRAM auth smoke"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "CREATE ROLE wire_scram LOGIN PASSWORD 'wire_pw'"
PGPASSWORD='wire_pw' psql "host=127.0.0.1 port=${PORT} user=wire_scram dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "SELECT 1 AS scram_ok"

echo "[compat] running COPY BINARY smoke"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "CREATE TABLE wire_copy_t (id int8, note text, ok boolean, score float8)" \
  -c "INSERT INTO wire_copy_t VALUES (1,'a',true,1.5),(2,'b',false,2.5)"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -q \
  -c "COPY wire_copy_t TO STDOUT BINARY" > /tmp/postgrust-wire-copy.bin
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "TRUNCATE wire_copy_t"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -q \
  -c "COPY wire_copy_t FROM STDIN BINARY" < /tmp/postgrust-wire-copy.bin
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "SELECT * FROM wire_copy_t ORDER BY 1"

echo "[compat] running COPY TEXT smoke"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -q \
  -c "COPY wire_copy_t TO STDOUT" > /tmp/postgrust-wire-copy.txt
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "TRUNCATE wire_copy_t"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -q \
  -c "COPY wire_copy_t FROM STDIN" < /tmp/postgrust-wire-copy.txt
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "SELECT * FROM wire_copy_t ORDER BY 1"

echo "[compat] running COPY CSV smoke"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "CREATE TABLE wire_copy_csv_t (id int8, note text, ok boolean)" \
  -c "INSERT INTO wire_copy_csv_t VALUES (1,'hello,world',true),(2,'quote\"inside',false)"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -q \
  -c "COPY wire_copy_csv_t TO STDOUT CSV" > /tmp/postgrust-wire-copy.csv
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "TRUNCATE wire_copy_csv_t"
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -q \
  -c "COPY wire_copy_csv_t FROM STDIN CSV" < /tmp/postgrust-wire-copy.csv
PGPASSWORD='' psql "host=127.0.0.1 port=${PORT} user=postgres dbname=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "SELECT * FROM wire_copy_csv_t ORDER BY 1"

echo "[compat] psql smoke passed"
