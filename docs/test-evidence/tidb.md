# TiDB Validation Evidence

Task ID: DB-TIDB-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:28:01Z

Environment: Docker on macOS arm64; Rust 1.96.0; TiDB/PD/TiKV Community 8.5.6

Command: `DBTOOL_RUN_TIDB_INTEGRATION=1 cargo test -p dbtool-cli --test live_services tidb_compat_live_sql_lifecycle_and_typed_values -- --exact --nocapture`; `./scripts/integration-tidb-secure-test.sh`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_tidb.dbtool_it_tidb_users_37569_1784056703875` | database-qualified table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/binary/null and limit 2/truncated PASS | table absent after DROP; database and volume removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_secure_node1_51191_1784057225492` | TLS node-1 table + PK PASS | 2/2 rows PASS | all rows exact PASS | ID 1 updated PASS | ID 2 removed PASS | schema/PK/index PASS | wrong/no TLS rejected; write and confirmation guards PASS | typed values and truncation PASS | table absent; secure volume removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_secure_node2_51191_1784057226283` | TLS node-2 table + PK PASS | 2/2 rows PASS | all rows exact PASS | ID 1 updated PASS | ID 2 removed PASS | schema/PK/index PASS | password-authenticated TLS; write/confirmation guards PASS | typed values and truncation PASS | table absent; secure volume removed PASS |
| `dbtool_it_tidb_secure.dbtool_it_tidb_x509_51191_1784057227836` | X.509 user table + PK PASS | 2/2 rows PASS | all rows exact PASS | ID 1 updated PASS | ID 2 removed PASS | schema/PK/index PASS | client certificate required; write/confirmation guards PASS | typed values and truncation PASS | table absent; secure volume removed PASS |

Assertions: the real three-component TiDB stack accepted `tidb://`, exposed the
SQL capability, and passed the complete basic MySQL-protocol SQL checklist.

Defect found and fixed: TiDB exposes `STATISTICS.NON_UNIQUE` as text. The
adapter previously decoded it as `i64` and silently omitted the primary index.
Commit `c2fc4ec` normalizes and validates this catalog value; TiDB, MySQL, and
MariaDB live lifecycles passed after the fix. The failed diagnostic table was
removed with the disposable volume.

Secure phase: the regenerated CA/server/client chain was validated, both TLS
SQL nodes passed full lifecycles, password and X.509 users were accepted, and
missing/incorrect TLS identity paths were rejected.

Pending before campaign COMPLETE: cross-node logical roundtrip, TiProxy, and
configured resilience drills. Basic and secure SQL are LIVE_PASS and are not
presented as those remaining higher-level guarantees.

Cleanup: PASS

Commits: `974886f`, `c2fc4ec`, `5edf95a`, this commit
