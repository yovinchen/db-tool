# Recursive Read Budget Evidence

Task ID: IF-T69, IF-T71

Result: LIVE_PASS_WITH_EXTERNAL_PRODUCT_BOUNDARIES

Run at (UTC): 2026-07-16

Environment: macOS arm64; SQLite in-process; Docker PostgreSQL 16, MySQL 8.4, Cassandra 5.0, MongoDB 7

## Shared contract

`ReadBudget(max_items,max_bytes)` validates `max_items + 1`, rejects zero, and caps bytes at 16 MiB
(8 MiB default). `ReadLimiter` charges complete headers/items before retention, retains at most N,
observes at most one N+1 probe, and finally measures the complete caller-visible response plus the
probe. Byte failure has stable code `READ_BUDGET_EXCEEDED` and never returns partial success.

Exact operations are `sql.query_budgeted`, `cql.query_budgeted`,
`document.find_budgeted`, and `document.aggregate_budgeted`. Legacy family booleans and old
row/item-only methods neither advertise nor implement these contracts.

## Verification matrix

| Backend/caller | Result |
| --- | --- |
| Core | 111/111 plus doc test; recursive Array/Map/JSON/Bytes, complete envelope and N/N-1 |
| SQLite | 10,000-row N+1, oversized first row, exact bytes and recursive values |
| PostgreSQL 16 Docker | adapter and CLI N/N+1 plus `--max-bytes 1` failure |
| MySQL 8.4 Docker | adapter and CLI N/N+1 plus `--max-bytes 1` failure |
| Cassandra 5.0 Docker | CLI paged N/N+1 plus `--max-bytes 1` failure |
| SQL Server | unit/compile disposable TDS stream and recursive envelope; external live pending IF-T52 |
| Db2 | unit/compile single-row ODBC rowset and truncation fail-closed; external live pending IF-T52 |
| MongoDB 7 Docker | find and aggregate N/N+1, `--max-bytes 1`, exact operation report, drop cleanup |
| CLI/TUI/export | default-feature regression; exact operation negotiation only; invalid budgets before DSN; SQL/Document artifact bounded by caller bytes |

Driver residual boundaries: SQLx must decode one streamed row before portable accounting; Cassandra
may prefetch one `page_size=1` page; Mongo must materialize one legal server response before raw BSON
can be charged. These are one protocol unit, not an unbounded collection. Mongo `batch_size=1`
confines its residual to one Mongo-legal document/wire message.

Representative live invocations (the environment gates are required; an ungated skipped test is
not a live PASS):

```bash
source scripts/integration-env.sh
DBTOOL_RUN_INTEGRATION=1 cargo test -p adapter-sql live_budgeted_query
DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test bounded_sql
DBTOOL_RUN_CASSANDRA_INTEGRATION=1 cargo test -p dbtool-cli --features cassandra --test bounded_sql cassandra_live_streams_one_probe_row_for_paged_results -- --exact
DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test bounded_document_search_ts mongo_live_find_and_aggregate_use_cumulative_document_budgets -- --exact
```

Cleanup: PASS. The Mongo test drops its isolated collection/database resources; SQL/CQL queries are read-only.
