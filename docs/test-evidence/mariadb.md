# MariaDB Completeness Evidence

Task ID: DB-MARIADB-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:13:16Z

Environment: Docker on macOS arm64; Rust 1.96.0; MariaDB 11.4.12 aarch64

Product version: MariaDB 11.4.12

Command: `DBTOOL_RUN_COMPAT_INTEGRATION=1 DBTOOL_RUN_MARIADB_COMPAT=1 cargo test -p dbtool-cli --test live_services mariadb_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_mariadb_users_17757_1784056308789` | table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/binary/null and limit 2/truncated PASS | table absent after DROP; volume removed PASS |
| `dbtool_it_mariadb_atomic_*` | InnoDB table + PK PASS | public bound-parameter import inserted 2/2 with `atomic=true` PASS | SQL-like text preserved exactly; both rows exact PASS | late second-row duplicate key rejected and first attempted row rolled back PASS | N/A | `sql.insert_rows_atomic_budgeted` advertised PASS | import required `--allow-write` PASS | full batch remained exactly 2 rows after failure PASS | table and temporary artifacts removed PASS |

Assertions: real MariaDB protocol connection, advertised SQL capability, typed
decoding, bounded results, full two-row CRUD, schema/index metadata, guards,
and post-drop cleanup all passed.

Atomic import refresh: `named_sql_atomic::mariadb_named_product_*` ran through
the public CLI against MariaDB and proved both successful bound insertion and
whole-batch rollback on a late constraint failure.

Cleanup: PASS

Commits: `974886f`, `6f423fb`, `152dc18`
