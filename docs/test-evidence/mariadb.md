# MariaDB Completeness Evidence

Task ID: DB-MARIADB-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:13:16Z

Environment: Docker on macOS arm64; Rust 1.96.0; MariaDB 11.4.12 aarch64

Command: `DBTOOL_RUN_COMPAT_INTEGRATION=1 DBTOOL_RUN_MARIADB_COMPAT=1 cargo test -p dbtool-cli --test live_services mariadb_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_mariadb_users_17757_1784056308789` | table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/binary/null and limit 2/truncated PASS | table absent after DROP; volume removed PASS |

Assertions: real MariaDB protocol connection, advertised SQL capability, typed
decoding, bounded results, full two-row CRUD, schema/index metadata, guards,
and post-drop cleanup all passed.

Shared capability boundary: export/import is implemented above the MySQL-family
adapter and its complete content roundtrip is recorded in `mysql.md`; this run
proves the named MariaDB product's database-facing capability checklist.

Cleanup: PASS

Commits: `974886f`, this commit
