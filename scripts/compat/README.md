# Compatibility Scripts

- `run_regression.sh`: runs the core compatibility checks:
  - full test suite
  - regression corpus integration test
  - differential fixture integration test
- `psql_smoke.sh`: launches `pg_server` locally and validates:
  - TLS (`sslmode=require`) connectivity
  - SCRAM authentication flow
  - `COPY ... TO/FROM STDOUT/STDIN BINARY` round-trip
