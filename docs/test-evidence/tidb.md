# TiDB Validation Evidence

Task ID: DB-TIDB-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:20:36Z

Environment: Docker on macOS arm64; Rust 1.96.0; TiDB/PD/TiKV Community 8.5.6

Command: `DBTOOL_RUN_TIDB_INTEGRATION=1 cargo test -p dbtool-cli --test live_services tidb_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_tidb.dbtool_it_tidb_users_37569_1784056703875` | database-qualified table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/binary/null and limit 2/truncated PASS | table absent after DROP; database and volume removed PASS |

Assertions: the real three-component TiDB stack accepted `tidb://`, exposed the
SQL capability, and passed the complete basic MySQL-protocol SQL checklist.

Defect found and fixed: TiDB exposes `STATISTICS.NON_UNIQUE` as text. The
adapter previously decoded it as `i64` and silently omitted the primary index.
Commit `c2fc4ec` normalizes and validates this catalog value; TiDB, MySQL, and
MariaDB live lifecycles passed after the fix. The failed diagnostic table was
removed with the disposable volume.

Pending before campaign COMPLETE: secure TLS/auth lifecycle, dual SQL-node
roundtrip, TiProxy, and configured resilience drills. Basic insecure SQL is
LIVE_PASS and is not presented as those higher-level guarantees.

Cleanup: PASS

Commits: `974886f`, `c2fc4ec`, this commit
