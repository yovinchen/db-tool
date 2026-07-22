# General Mutation Input Budget Evidence

Task: IF-T78

Run date: 2026-07-16

## Contract

Every newly negotiated mutation uses `InputBudget(max_items,
max_item_bytes,max_batch_bytes)` and `InputLimiter` before the first backend
write. The process hard ceilings are 100,000 logical items and 16 MiB for each
byte dimension; the finite default is 1,000 items, 8 MiB per item, and 8 MiB
for the complete request. Compact JSON accounting includes every
caller-controlled target such as table, key, collection, index, document ID,
column, filter, patch, point, and raw command argument.

An exact request at N bytes succeeds; N-1 returns the stable
`INPUT_BUDGET_EXCEEDED` error before a remote mutation. Adapter-native limits
remain independent and are also checked before dispatch. Once any mutation may
have reached a backend, transport, acknowledgement, response-budget, response
shape, commit, or rollback ambiguity returns non-retryable
`OUTCOME_INDETERMINATE`.

## Exact Operations

| Family | Negotiated operations |
| --- | --- |
| SQL | `sql.execute_budgeted`, `sql.insert_rows_atomic_budgeted` |
| CQL | `cql.execute_budgeted` |
| KV | `kv.set_budgeted`, `kv.restore_with_expiry_budgeted`, `kv.delete_budgeted`, `kv.raw_command_io_budgeted` |
| Document | `document.insert_budgeted`, `document.update_one_budgeted`, `document.update_many_budgeted`, `document.delete_one_budgeted`, `document.delete_many_budgeted`, `document.aggregate_write_budgeted`, `document.drop_collection_budgeted` |
| Search | `search.index_doc_budgeted`, `search.put_doc_budgeted`, `search.update_doc_budgeted`, `search.delete_doc_budgeted`, `search.delete_index_budgeted` |
| Time series | `time_series.write_points_budgeted` |

Exact methods default to `UNSUPPORTED_CAPABILITY`; a coarse family bit never
authorizes them. The core test suite verifies all exact defaults fail closed
without invoking a legacy method.

## Backend Verification Matrix

| Backend | Test and result | Complete mutation/readback assertions | Cleanup evidence |
| --- | --- | --- | --- |
| SQLite | service-free adapter lifecycle; PASS | N-1 CREATE left `sqlite_master` unchanged; exact CREATE, bound INSERT, UPDATE, two-row atomic insert, SELECT of all three rows, targeted DELETE; duplicate execute reported outcome-indeterminate | `exact_crud` absent after exact DROP |
| PostgreSQL 16.14 | Docker exact lifecycle 1/1 PASS | N-1 CREATE produced zero catalog rows; exact CREATE/INSERT/UPDATE, late-row budget rejection left count 1, two-row atomic insert returned 2, SELECT returned all rows, targeted DELETE | unique `dbtool_exact_*` table absent after DROP |
| MySQL 8.4.9 | Docker exact lifecycle 1/1 PASS | same SQL checklist using native placeholders and InnoDB; late-row rejection produced no partial batch | unique `dbtool_exact_*` table absent after DROP |
| SQL Server | service-free 9/9 PASS | exact statement/parameter counting, N/N-1, NUL and 2,100-parameter ceiling, unsupported dynamic params rejected before client lock | exact input boundary remains service-free; later x86_64 product CRUD/types/catalog/cleanup LIVE_PASS is recorded in `sqlserver.md` |
| IBM Db2 | service-free 23/23 PASS | exact statement/parameter counting, N/N-1, NUL and fixed Db2 ceilings, unsupported params rejected before ODBC connection | live run remains IF-T52 BLOCKED until host IBM ODBC exists |
| Cassandra 5.0 | Docker exact lifecycle 1/1 PASS | N-1 CREATE did not create rejected keyspace; both native `execute_cql_budgeted` and SQL-compatible `execute_budgeted` use the canonical finite contract; exact keyspace/table CREATE, INSERT, UPDATE, SELECT of `(1,updated)`, targeted DELETE and empty readback, table DROP | unique `dbtool_it_input_*` keyspace absent after DROP KEYSPACE |
| Redis 7.4.9 | Docker exact lifecycle 1/1 PASS | exact binary SET/readback, lifetime restore and NX condition, two-key DEL, allowlisted raw SET/readback; every N-1 request left target keys unchanged; one-byte response budget after raw SET returned outcome-indeterminate while the written value was observable | exact delete removed all test keys; prefix scan empty |
| MongoDB 7.0.37 | Docker exact lifecycle 1/1 PASS | N-1 insert did not create collection; exact three-document insert; update-one 1/1, update-many 3/3, delete-one 1, delete-many 2; N-1 mutating aggregate created no target, read-only aggregate rejected `$out/$merge`, exact `$out` and `$merge` each produced three verified target documents; N-1 update/delete/drop preserved state | source and aggregate target collections absent after exact drops; final `dbtool_it_input_*` catalog scan empty |
| OpenSearch 2.17.1 | Docker shared exact lifecycle 1/1 PASS | N-1 put did not create index; exact auto-ID index, stable-ID puts, patch update/readback, N-1 document delete preserved document, exact delete removed it, N-1 index delete preserved index | main and peer indices absent from final catalog |
| Elasticsearch 8.15.5 | Docker shared exact lifecycle 1/1 PASS | same five-operation contract and readback as OpenSearch | final `_cat/indices` excluded both unique indices |
| Prometheus 2.55.1 | Docker exact lifecycle 1/1 PASS | N-1 request and N+1 item request exposed zero series; exact two-point remote write produced two queryable series | integration-only admin delete-series plus tombstone cleanup; final range query empty |

## Native Preflight Boundaries

- SQL callers and all four first-party SQL adapter families serialize the same
  public `SqlExecuteInput { sql, params }` envelope; SQL adapters additionally validate statements, identifiers, parameter counts, value
  conversion, generated insert statements, and every late row before opening a
  transaction. Atomic insert returns success only after commit; a confirmed
  rollback remains a known no-write failure, while failed rollback or uncertain
  commit is outcome-indeterminate.
- Cassandra validates complete CQL text and its fixed 16 MiB process ceiling
  before session dispatch. Portable affected-row counts remain unavailable.
- Redis additionally checks bulk-string limits and a fail-closed raw mutation
  allowlist. Raw mutation responses have an independent `ReadBudget`.
- MongoDB validates namespace syntax/length, every BSON document and write
  operation, the 100,000-document batch ceiling, and a conservative complete
  OP_MSG estimate before calling the driver. Inserts are ordered.
- Search validates lowercase index syntax, 255-byte index names, 512-byte IDs,
  object bodies, 16 MiB HTTP request bodies, and percent-encoded path segments
  before TCP/TLS request bytes are written.
- Prometheus validates metric/label syntax, non-empty fields, the reserved
  `__name__` label, the complete point batch, encoded protobuf, and Snappy body
  before connecting.

## Verification Commands

```text
cargo test -p dbtool-core                         # 151 passed
cargo test -p adapter-sql --lib                   # 49 passed
cargo test -p adapter-sqlserver --lib             # 9 passed
cargo test -p adapter-db2 --lib                   # 23 passed
cargo test -p adapter-cassandra --lib             # 14 passed
cargo test -p adapter-redis --lib                 # 53 passed
cargo test -p adapter-mongo --lib                 # 19 passed
cargo test -p adapter-search --lib                # 40 passed
cargo test -p adapter-timeseries --lib            # 30 passed
```

All listed packages also passed all-target strict Clippy (`-D warnings`),
rustfmt, and diff checks. Docker commands use the disposable ports and
credentials from `scripts/integration-env.sh`; committed evidence redacts raw
credentials.

## Boundaries

- SQL Server is implemented and service-free tested, but the current arm64
  host cannot run the x86_64-only local image gate.
- Db2 is implemented and service-free tested, but the host has no registered
  IBM Data Server ODBC driver.
- Redshift, AutoMQ, WarpStream, and Confluent endpoints were not supplied;
  compatibility aliases are not promoted to product-native passes. ScyllaDB was
  subsequently verified as a named product in `scylladb.md`.
- Elasticsearch product-native HTTPS was not rerun in this slice; its existing
  plain HTTP product test and shared TLS transport tests remain separate.

## Implementation Commits

- `3641ed2` core input budget, limiter, exact operations, and fail-closed defaults
- `cfbb998` Redis KV mutations
- `3c9c2d4` Prometheus remote write
- `a6e60c5` SQLite/PostgreSQL/MySQL exact SQL mutations
- `325aa3c` SQL Server exact execute
- `b89f222` Db2 exact execute
- `6fcd23c` Cassandra exact CQL plus reproducible Docker lifecycle
- `83db841` MongoDB document mutations
- `3822948` OpenSearch/Elasticsearch mutations and shared live lifecycle
- `4b6b6e2` OpenSearch/Elasticsearch peer-index content readback and zero-residual rerun
- `94f3ffb` Cassandra SQL-compatible exact execute negotiation
- `ab06b88` MongoDB `$out/$merge` exact aggregate-write contract and Docker lifecycle
- `096d7cc` TUI exact mutation dispatch and pre-connection budgets
- `d6801a5` canonical shared SQL execute envelope plus embedded exact example
- `c24c7b5` CLI/transfer exact mutation dispatch, preflight, and timeout semantics
