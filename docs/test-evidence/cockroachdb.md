# CockroachDB Completeness Evidence

Task ID: DB-COCKROACH-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:15:20Z

Environment: Docker on macOS arm64; Rust 1.96.0; CockroachDB CCL v24.3.8 linux arm64

Command: `DBTOOL_RUN_PG_COMPAT_INTEGRATION=1 DBTOOL_RUN_COCKROACH_COMPAT=1 cargo test -p dbtool-cli --test live_services cockroach_pg_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_cockroach_users_27042_1784056474028` | table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/bool/text/null and limit 2/truncated PASS | table absent after DROP; volume removed PASS |

Assertions: the real CockroachDB product accepted the `cockroach://` scheme via
the PostgreSQL protocol adapter and passed every advertised SQL checklist item.

Product boundary: this is a single-node insecure profile. Distributed
transactions, multi-node failover, CockroachDB administration, TLS, and a
product-specific export/import rerun are outside the advertised dbtool SQL
capability and were not claimed.

Cleanup: PASS

Commits: `642bfd9`, `974886f`, this commit
