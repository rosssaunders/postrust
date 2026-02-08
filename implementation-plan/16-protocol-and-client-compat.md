# Step 16: Protocol and Client Compatibility

Goal:
- Move from internal loop simulation to broad client interoperability.

Scope:
- Startup/auth handshake compatibility.
- Simple and extended query protocol completeness.
- Copy protocol/binary formats/notices.

Implementation files:
- Add `src/protocol/mod.rs`
- Add `src/protocol/startup.rs`
- Add `src/protocol/messages.rs`
- Add `src/protocol/copy.rs`
- Integrate with `src/tcop/postgres.rs`

Deliverables:
- Clients can connect and run workloads through standard PostgreSQL protocol.
- Correct response ordering and message framing.

Tests:
- Wire protocol integration tests with client libraries.
- Extended protocol behavior tests (`Parse/Bind/Execute/Sync`).
- Error and sync recovery tests.

Done criteria:
- `psql` and common drivers can run non-trivial sessions successfully.

Dependencies:
- `11-executor-node-parity.md`
- `15-security-roles-rls.md`

Current progress:
- Added `src/protocol/mod.rs`, `src/protocol/startup.rs`, `src/protocol/messages.rs`, and `src/protocol/copy.rs`.
- Added startup/query message encode/decode helpers and COPY text row encode/decode helpers with unit tests.
- Extended `FrontendMessage`/`BackendMessage` in `src/tcop/postgres.rs` with startup/auth/status capabilities:
  - `Startup`, `Password`, `SslRequest`, `CancelRequest`
  - `AuthenticationOk`, `AuthenticationCleartextPassword`, `ParameterStatus`, `BackendKeyData`, `NoticeResponse`
- Added startup-gated session mode (`PostgresSession::new_startup_required`) with:
  - startup packet validation
  - cleartext password flow when role password is configured
  - server parameter status + backend key data + ready response sequencing
- Added session tests for startup handshake + password-authenticated query flow.
- Added real pgwire server binary:
  - `src/bin/pg_server.rs`
  - handles startup packets, SSL negotiation response (`N`), simple and extended query frames, and backend framing.
- Verified real `psql` connectivity and query execution over localhost against the wire server.
