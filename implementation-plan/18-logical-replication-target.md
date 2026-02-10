# Step 18: Logical Replication Target

Goal:
- Make postrust a PostgreSQL logical replication subscriber so it can replicate
  data from an upstream PostgreSQL server and serve local read queries over an
  in-memory copy.

## Architecture

```
┌──────────────┐  logical replication   ┌──────────────┐
│   Upstream   │  (pgoutput WAL stream) │   postrust   │
│  PostgreSQL  │ ─────────────────────▶ │  (in-memory) │
│   server     │  standby status msgs   │              │
│              │ ◀───────────────────── │              │
└──────────────┘                        └──────────────┘
                                              │
                                         local queries
                                         (standard PG wire protocol)
                                              │
                                        ┌──────────┐
                                        │  Client  │
                                        │ psql/app │
                                        └──────────┘
```

## Scope

### Phase A: Replication Protocol Client

Connect to upstream PostgreSQL as a replication client.

1. **Replication startup** — establish a walsender connection using
   `replication=database` in the startup parameters.
2. **IDENTIFY_SYSTEM** — query upstream for system ID, timeline, xlogpos.
3. **CREATE_REPLICATION_SLOT** — create a logical slot using `pgoutput` plugin
   with `proto_version '1', publication_names 'pub_name'`.
4. **Standby status updates** — periodically send `StandbyStatusUpdate` messages
   reporting the last applied/flushed/written LSN to prevent WAL buildup.

Implementation files:
- `src/replication/mod.rs` — module root
- `src/replication/client.rs` — replication connection and protocol client
- `src/replication/slot.rs` — replication slot management

### Phase B: pgoutput Message Decoder

Decode the binary pgoutput logical decoding messages.

Message types to handle:
- **Relation (R)** — table OID, namespace, name, column definitions, replica identity
- **Begin (B)** — transaction begin, final LSN, commit timestamp, xid
- **Commit (C)** — flags, commit LSN, end LSN, commit timestamp
- **Insert (I)** — relation OID, new tuple data
- **Update (U)** — relation OID, optional old key/tuple, new tuple
- **Delete (D)** — relation OID, old key/tuple
- **Truncate (T)** — list of relation OIDs, cascade/restart options
- **Origin (O)** — origin name and LSN (for cascading replication)
- **Type (Y)** — custom type definitions
- **Message (M)** — logical decoding messages

Tuple data format:
- Column count (Int16)
- Per column: kind (n=null, u=unchanged, t=text, b=binary), length, data

Implementation files:
- `src/replication/pgoutput.rs` — message parser/decoder
- `src/replication/tuple_decoder.rs` — tuple data decoder with type conversion

### Phase C: Schema Synchronization

Mirror upstream table definitions into postrust's catalog.

1. **Initial schema discovery** — query `pg_catalog.pg_class`,
   `pg_catalog.pg_attribute`, `pg_catalog.pg_namespace`, `pg_catalog.pg_type`
   on the upstream server to get table/column definitions.
2. **Relation message handling** — when pgoutput sends Relation messages,
   verify/create/update the local table definition in the catalog.
3. **Type mapping** — map upstream PostgreSQL type OIDs to postrust's supported
   types. Unsupported types fall back to text representation.
4. **Publication filtering** — only replicate tables in the specified publication.

Implementation files:
- `src/replication/schema_sync.rs` — schema discovery and catalog sync

### Phase D: Initial Table Sync (Copy)

Before streaming starts, copy existing data from upstream.

1. **COPY export** — for each table in the publication, run
   `COPY table_name TO STDOUT` over the replication connection.
2. **Row parsing** — parse COPY text/binary format rows.
3. **Bulk insert** — insert rows into postrust's in-memory storage.
4. **Snapshot consistency** — use the replication slot's consistent point to
   ensure initial copy and subsequent streaming are seamless.

Implementation files:
- `src/replication/initial_sync.rs` — COPY-based initial data load

### Phase E: Change Application (Apply Worker)

Apply streamed changes to in-memory storage.

1. **Transaction batching** — collect changes between Begin/Commit messages and
   apply them atomically using postrust's transaction system.
2. **INSERT** — insert new tuple into the target table's in-memory storage.
3. **UPDATE** — find matching row (by primary key or replica identity) and
   replace with new tuple.
4. **DELETE** — find matching row and remove it.
5. **TRUNCATE** — clear all rows for the specified tables.
6. **Conflict handling** — log and skip conflicts (this is a read replica, so
   conflicts indicate bugs, not user error).

Implementation files:
- `src/replication/apply.rs` — change application logic

### Phase F: Subscription Management

User-facing commands to configure replication.

1. **CREATE SUBSCRIPTION** — parse and execute:
   ```sql
   CREATE SUBSCRIPTION sub_name
     CONNECTION 'host=upstream dbname=mydb'
     PUBLICATION pub_name
     WITH (copy_data = true, slot_name = 'postrust_sub');
   ```
2. **ALTER SUBSCRIPTION** — enable/disable, refresh publication.
3. **DROP SUBSCRIPTION** — disconnect and drop the replication slot on upstream.
4. **pg_stat_subscription** — virtual table showing subscription status, last
   received LSN, apply lag.

Implementation files:
- `src/replication/subscription.rs` — subscription lifecycle management
- `src/commands/subscription.rs` — DDL command handlers
- Parser additions to `src/parser/ast.rs` and `src/parser/sql_parser.rs`

### Phase G: Monitoring and Resilience

1. **Reconnection** — auto-reconnect on connection loss with exponential backoff.
2. **LSN persistence** — write last applied LSN to a file so postrust can resume
   after restart (optional, since it's in-memory anyway).
3. **Lag reporting** — expose replication lag in pg_stat_subscription.
4. **Health checks** — periodic keepalive/status updates to upstream.

Implementation files:
- `src/replication/monitor.rs` — health monitoring and reconnection

## Dependencies

- Working wire protocol client (can reuse parts of `src/protocol/`)
- In-memory storage with DML operations (done)
- Catalog with schema management (done)
- Transaction support (done)
- Tokio or async runtime for persistent connection (already used)

## External Dependencies (new crates)

- `tokio` — async runtime (likely already present or needed)
- `tokio-postgres` or raw TCP — for the replication connection
- `byteorder` — for binary protocol parsing
- `chrono` — for timestamp handling (likely already present)

## Tests

- Unit tests for pgoutput message decoder (binary fixtures)
- Unit tests for tuple decoder with various PG types
- Integration test: mock upstream sending Relation + Insert messages
- Integration test: initial sync via COPY
- Integration test: UPDATE/DELETE application with PK lookup
- End-to-end test (requires a real PostgreSQL instance):
  - Create publication on upstream
  - CREATE SUBSCRIPTION on postrust
  - Insert rows upstream, verify they appear in postrust
  - Update/delete upstream, verify changes replicate

## Done Criteria

- postrust can subscribe to a PostgreSQL publication
- Initial data copy works
- Streaming INSERT/UPDATE/DELETE changes are applied in real-time
- Clients can connect to postrust and query replicated data
- Replication lag is visible via pg_stat_subscription
- Reconnection works after upstream restart

## Constraints

- Read-only replica — writes to replicated tables should be rejected
- Single upstream for now (no multi-source replication)
- Supported types: all types postrust currently handles; others stored as text
- No support for DDL replication (schema changes require manual refresh)
