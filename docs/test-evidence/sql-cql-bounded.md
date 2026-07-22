# SQL/CQL Adapter-Bounded Read Evidence

Task ID: IF-T44

Result: LIVE_PASS

Run at (UTC): 2026-07-15T17:42:13Z

Environment: Docker on macOS arm64; Rust 1.96.0

Products: PostgreSQL 16.14; MySQL 8.4.9; Cassandra 5.0.8; SQLite in-memory

## Contract verified

| Path | Large result | Exact limit | Invalid limit | Result |
| --- | --- | --- | --- | --- |
| SQLite / SQLx | recursive 10,000 rows, limit 3 | 3 rows with limit 3 | zero/`usize::MAX` rejected | PASS |
| PostgreSQL / SQLx | `generate_series(1,10000)`, limit 3 | 3 rows with limit 3 | CLI overflow rejected before connection | LIVE_PASS |
| MySQL / SQLx | recursive 1,000 rows, limit 3 | 3 rows with limit 3 | shared CLI validation | LIVE_PASS |
| Cassandra / Scylla driver | multi-page `system_schema.columns`, limit 3 | server `LIMIT 3` with client limit 3 | adapter unit validation | LIVE_PASS |
| SQL Server / Tiberius | first-result-set stream stops after probe | exact probe boundary covered by implementation/unit checks; later product lifecycle ran on x86_64 | shared core validation | COMPILE_PASS for exact probe boundary; product lifecycle LIVE_PASS in `sqlserver.md` |
| Db2 / ODBC | one-row rowset stops after probe | covered by implementation and compile checks | shared core validation | COMPILE_PASS; Db2 ODBC runtime required |

For every live path, a result larger than 3 returned exactly 3 rows with both
`data.truncated=true` and `meta.truncated=true`. A backend query returning exactly
3 rows returned both flags as false.

PostgreSQL and MySQL bounded paths acquire a pool connection and retire that
socket after observing the probe row, preventing a later checkout from draining
the discarded protocol tail. Tiberius bounded queries use and close a dedicated
connection for the same reason. SQLite finalizes its statement stream,
Cassandra limits the driver page size, and each Db2 bounded call already owns a
short-lived connection with a one-row ODBC rowset.

## Safety and artifact checks

- `sql query` rejects ordinary writes and DDL even when `--allow-write` is present;
  writes must use `sql exec`.
- `export sql` rejects ordinary writes and DDL before DSN resolution or connection.
- SQL export v2 persists required `truncated`; import rejects `truncated=true`
  and malformed v2 artifacts.
- Legacy v1 artifacts without the field are completeness-unknown and rejected
  by default; restoration requires an explicit `--accept-legacy-unmarked` risk override.
- CLI, TUI, export, and embedded examples all call the required bounded trait.

## Commands

```text
cargo test -p dbtool-core -p adapter-sql -p adapter-cassandra -p adapter-sqlserver -p adapter-db2
cargo test -p dbtool-cli --test bounded_sql
DBTOOL_RUN_INTEGRATION=1 ... cargo test -p dbtool-cli --test bounded_sql postgres_live_streams_one_probe_row_for_large_results -- --exact --nocapture
DBTOOL_RUN_INTEGRATION=1 ... cargo test -p dbtool-cli --test bounded_sql mysql_live_streams_one_probe_row_for_large_results -- --exact --nocapture
DBTOOL_RUN_CASSANDRA_INTEGRATION=1 ... cargo test -p dbtool-cli --features cassandra --test bounded_sql cassandra_live_streams_one_probe_row_for_paged_results -- --exact --nocapture
```

No tables or rows were created by these live bound checks; PostgreSQL/MySQL use
generated result sets and Cassandra reads only `system_schema`.
