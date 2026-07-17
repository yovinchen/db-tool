# TimescaleDB Completeness Evidence

Task ID: DB-TIMESCALE-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:15:20Z

Environment: Docker on macOS arm64; Rust 1.96.0; TimescaleDB 2.17.2 on PostgreSQL 16.6

Product version: TimescaleDB 2.17.2 on PostgreSQL 16.6

Command: `DBTOOL_RUN_PG_COMPAT_INTEGRATION=1 DBTOOL_RUN_TIMESCALE_COMPAT=1 cargo test -p dbtool-cli --test live_services timescale_pg_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_timescale_users_27514_1784056484633` | table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/bool/text/null and limit 2/truncated PASS | table absent after DROP; volume removed PASS |
| `dbtool_it_timescale_atomic_*` | table + PK PASS | public bound-parameter import inserted 2/2 with `atomic=true` PASS | SQL-like text and both rows exact PASS | late duplicate key rejected and entire attempted batch rolled back PASS | N/A | atomic import exact operation advertised PASS | import required write permission PASS | row set stayed exactly 2 after failure PASS | table and artifacts removed PASS |

Assertions: the real TimescaleDB image accepted the `timescale://` scheme via
the PostgreSQL protocol adapter and passed every advertised SQL checklist item.

Product boundary: dbtool currently exposes generic SQL, not a TimescaleDB
hypertable or continuous-aggregate capability. Extension-specific DDL, time
bucket queries, retention policies, and TLS are therefore not claimed. Generic
SQL atomic import and late-failure rollback are now product-tested.

Cleanup: PASS

Commits: `642bfd9`, `974886f`, `c2a77fe`, `152dc18`
