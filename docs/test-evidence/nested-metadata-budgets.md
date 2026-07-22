# Nested Metadata Budget Evidence

Task ID: IF-T67

Result: LIVE_PASS_WITH_EXTERNAL_PRODUCT_BOUNDARIES

Run at (UTC): 2026-07-16

Environment: macOS arm64; disposable Docker PostgreSQL/MySQL/Cassandra/Redis/Redpanda/RabbitMQ/NATS; SQLite in-process

## Contract

`MetadataBudget(max_items,max_bytes)` is a complete-object contract. Nested columns, indexes and
index members, topic config entries, partition watermarks, queue counts, consumer lag candidates,
and generated DDL are charged before the adapter may return. Exceeding either dimension returns
`METADATA_BUDGET_EXCEEDED`; no partial schema, DDL, detail, or lag response is successful.

First-party callers require the exact operations:

- `sql.describe_table_bounded`, `cql.describe_table_bounded`, `db2.generate_ddl_bounded`;
- `message.admin.topic_detail_bounded`, `message.admin.consumer_lag_bounded`.

The CLI derives the budget from `--limit` and `--max-bytes` before DSN resolution. For the SQL
schema surface it supports, the TUI uses the same exact operation with the default 8 MiB byte
envelope; the TUI does not expose CQL, Db2, or messaging detail/lag commands.

## Verification

| Family | Evidence |
| --- | --- |
| Core limiter/default trait | complete object and nested item/byte N/N-1; legacy methods never invoked |
| SQLite/PostgreSQL/MySQL | columns, index identity/membership and complete TableSchema accounting |
| Cassandra | paged schema rows and generated CQL inside one budget |
| SQL Server | disposable TDS metadata stream, exact N+1 and recursive bytes |
| Db2 | bounded catalog reads, fail-closed unsupported DDL semantics and ODBC truncation checks |
| AMQP/RabbitMQ | direct and management detail bounded by protocol body and complete object |
| Kafka pure/native | config/watermark/lag work accounting and 16 MiB metadata receive ceiling |
| Redis Streams | fixed Lua detail/lag response and exact remaining+1 work |
| NATS JetStream | server `INFO.max_payload` gate, bounded detail and lag API |

SQL Server and Db2 were verified by compile/unit paths in this original slice.
SQL Server later completed an x86_64 product CRUD/types/catalog/cleanup run in
`sqlserver.md`; the exact nested N/N+1/byte edges here remain adapter-level.
Db2 product live remains an IF-T52 prerequisite until its separate gate passes.

Representative verification commands:

```bash
cargo test -p dbtool-core
cargo test -p adapter-sql -p adapter-cassandra -p adapter-sqlserver -p adapter-db2
cargo test -p adapter-amqp -p adapter-redis -p adapter-kafka -p adapter-nats
cargo test -p dbtool-cli -p dbtool-tui
cargo clippy -p dbtool-cli -p dbtool-tui --all-targets -- -D warnings
```

Docker-backed adapter runs used `source scripts/integration-env.sh` and their explicit
`DBTOOL_RUN_*_INTEGRATION=1` gates. A plain ungated Cargo PASS is not counted as live evidence.

Cleanup: PASS for all disposable Docker resources used by messaging and SQL/CQL live checks.
