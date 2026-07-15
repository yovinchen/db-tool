# SQL Bounded Catalog Evidence

Task ID: IF-T66-SQL

Result: LIVE_PASS

Run at (UTC): 2026-07-15T21:41:17Z

Environment: Docker on macOS arm64; Rust 1.96.0

Products: SQLite in-memory; PostgreSQL 16.14; MySQL 8.4.9; MariaDB 11.4

## Frozen contract

- `sql.list_schemas_bounded` and `sql.list_tables_bounded` are optional,
  method-level capabilities. The coarse `sql=true` flag and the legacy
  unbounded list methods never imply them.
- Every implementation constructs `ListLimiter`, validates the caller budget,
  and calculates the N+1 probe before backend access. Zero, `usize::MAX`, and a
  probe that cannot fit the backend SQL `LIMIT` parameter fail closed.
- SQLite, PostgreSQL-family, Redshift, and MySQL-family queries order the
  catalog deterministically and send `LIMIT N+1` to the backend. The adapter
  retains at most N items and sets `truncated=true` only after observing the
  probe item.
- Tables retain their effective schema/database in `TableInfo.schema`, so a
  returned identity remains reusable and unambiguous.
- CLI `sql schemas` and `sql tables` negotiate the bounded operation before
  dispatch, call only the bounded trait method, and copy adapter completeness
  exactly to `meta.truncated`. TUI `schemas` and `tables [schema]` apply the same
  negotiation and expose `{data, meta.truncated}`; neither surface falls back
  to `list_schemas` or `list_tables`.

## Backend and alias matrix

| Adapter | Schemes covered | Server-side bound | Deterministic identity | Result |
| --- | --- | --- | --- | --- |
| SQLite | `sqlite:` | table-valued pragma / `sqlite_schema`, `LIMIT ?` | attachment order for schemas; schema + relation name for tables | PASS |
| PostgreSQL | `postgres://`, `postgresql://`, `cockroach://`, `timescale://` | `LIMIT $n` | schema + relation name; table/view/materialized-view kind retained | LIVE_PASS on PostgreSQL |
| Redshift compatibility | `redshift://` | `information_schema`, `LIMIT $n` | schema + table name | COMPILE_PASS; external endpoint not supplied |
| MySQL | `mysql://`, `mariadb://`, `tidb://` | `information_schema`, `LIMIT ?` | database + table name | LIVE_PASS on MySQL and MariaDB alias |

SQL Server is implemented by the separate `adapter-sqlserver` crate, not
`adapter-sql`; it does not advertise these new operations in this task and is
therefore rejected instead of receiving an unbounded compatibility fallback.

## Evidence

| Check | Evidence | Result |
| --- | --- | --- |
| N versus N+1 | SQLite schemas/tables; isolated PostgreSQL schema; MySQL live catalog cardinality | PASS |
| Exact result | N items returns N and `truncated=false` | PASS |
| Probe result | N+1 items returns N and `truncated=true` | PASS |
| Invalid budgets | zero and `usize::MAX` rejected before invalid schema/SQL or unreachable DSN access | PASS |
| Capability negotiation | CLI and TUI tests reject legacy-only SQL operation sets | PASS |
| CLI JSON | SQLite list tests assert exact `meta.truncated`; PostgreSQL/MySQL lifecycle commands require and invoke bounded lists | LIVE_PASS |
| Cleanup | PostgreSQL isolated schema, MySQL three-table fixture, and CLI lifecycle tables removed | PASS |

Commands:

```text
cargo test -p adapter-sql --lib
cargo test -p dbtool-cli --test caps_operations --test cli_json
cargo test -p dbtool-tui --bin dbtool-tui sql_catalog_commands_
DBTOOL_IT_POSTGRES_DSN=... DBTOOL_IT_MYSQL_DSN=... \
  cargo test -p adapter-sql --lib bounded_catalog -- --nocapture
DBTOOL_RUN_INTEGRATION=1 DBTOOL_IT_POSTGRES_DSN=... \
  cargo test -p dbtool-cli --test live_services postgres_live_sql_lifecycle -- --exact --nocapture
DBTOOL_RUN_INTEGRATION=1 DBTOOL_IT_MYSQL_DSN=... \
  cargo test -p dbtool-cli --test live_services mysql_live_sql_lifecycle -- --exact --nocapture
DBTOOL_RUN_COMPAT_INTEGRATION=1 DBTOOL_RUN_MARIADB_COMPAT=1 DBTOOL_IT_MARIADB_DSN=... \
  cargo test -p dbtool-cli --test live_services mariadb_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture
```

No credentials are stored in this evidence file. Live tests used local Docker
DSNs supplied through environment variables and cleaned every created catalog
resource.
