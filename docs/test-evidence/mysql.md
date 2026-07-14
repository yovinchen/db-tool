# MySQL Completeness Evidence

Task ID: DB-MYSQL-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T19:08:56Z

Environment: Docker on macOS arm64; Rust 1.96.0; MySQL Community Server 8.4.9 aarch64

Product version: MySQL Community Server 8.4.9

Command: `DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_services mysql_live_sql_lifecycle -- --exact --nocapture`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_mysql_users_96210_1784055766440` | table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/binary/null and limit 2/truncated PASS | table absent after DROP PASS |
| `dbtool_it_fixture_people` | deterministic source table PASS | Alice/Bob/Carol 3/3 PASS | all IDs, names, roles and active flags exact PASS | N/A | N/A | PK present through source DDL | bounded synthetic fixture only | exported all 3 rows PASS | source table dropped PASS |
| `dbtool_it_fixture_people_restore_1784056024_6897` | restore table PASS | public import inserted 3/3 PASS | all 3 rows and every exported field equal to source PASS | N/A | N/A | destination schema accepted import PASS | import required `--allow-write` | JSON export/import roundtrip PASS | restore table dropped PASS |

Assertions: connection and SQL capability, typed decoding including binary,
limit/truncation, schema/table/index inspection, complete two-row CRUD
lifecycle, write and confirmation guards, complete three-row fixture readback,
and public export/import all passed.

Cleanup: PASS

Commits: `974886f`, `561ea93`, `bea6bed`, `fe7cfb9`
