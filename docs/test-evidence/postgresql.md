# PostgreSQL Completeness Evidence

Task ID: DB-POSTGRES-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T19:55:00Z (focused atomic-import refresh; original full-family run 2026-07-14T19:08:56Z)

Environment: Docker on macOS arm64; Rust 1.96.0; PostgreSQL 16.14

Product version: PostgreSQL 16.14

Command: `DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_services postgres_live_sql_lifecycle -- --exact --nocapture`; `./scripts/integration-fixture-data-test.sh`; `./scripts/integration-data-roundtrip-test.sh`; focused adapter PostgreSQL atomic insert/rollback test with `DBTOOL_IT_POSTGRES_DSN`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_postgres_users_94919_1784055737357` | table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | IDs `1,2`, names exact 2/2 PASS | ID 1 became `alice-updated` PASS | ID 2 removed; ID 1 remained PASS | schemas, table, columns, non-null PK and primary index PASS | INSERT without write permission blocked; CREATE/DROP and unbounded DELETE required confirmation PASS | typed int/float/bool/text/null and limit 2/truncated PASS | table absent after DROP PASS |
| `dbtool_it_fixture_people` | deterministic source table PASS | Alice/Bob/Carol 3/3 PASS | all IDs, names, roles and active flags exact PASS | N/A | N/A | PK present through source DDL | bounded synthetic fixture only | exported all 3 rows PASS | source table dropped PASS |
| `dbtool_it_fixture_people_restore_1784056024_6897` | restore table PASS | public import inserted 3/3 PASS | all 3 rows and every exported field equal to source PASS | N/A | N/A | destination schema accepted import PASS | import required `--allow-write` | JSON export/import roundtrip PASS | restore table dropped PASS |
| `dbtool_atomic_*` focused fixture | table PASS | text resembling SQL injection, bytea, timestamptz and jsonb sent as bound values PASS | duplicate-key second row caused complete rollback to count 0; valid batch read back exact typed values PASS | N/A | N/A | adapter advertised `sql.insert_rows_atomic` PASS | identifiers and row shapes revalidated in adapter PASS | one transaction; any constraint error rolled back all rows; success count 1 PASS | residual `dbtool_atomic_*` tables 0 PASS |

Assertions: connection and SQL capability, typed decoding, limit/truncation,
schema/table/index inspection, complete two-row CRUD lifecycle, write and
confirmation guards, complete three-row fixture readback, public export/import,
typed parameterized atomic insert and late-error whole-batch rollback all passed.

## IF-T78 exact mutation refresh

Run at (UTC): 2026-07-16T12:13:31Z

Focused Docker test
`postgres_live_budgeted_mutation_crud_rejects_before_write_and_cleans_up`
passed 1/1. N-1 CREATE produced zero matching `information_schema.tables`
rows. The unique `dbtool_exact_*` table then completed exact CREATE, bound
INSERT, UPDATE, late-row budget rejection with count still 1, two-row atomic
INSERT with returned count 2, complete ordered readback, targeted DELETE, and
DROP. The final catalog query proved both accepted and rejected test tables
absent. Statement, identifier, parameter, value-conversion, generated SQL, and
every late row are checked before transaction access; ambiguous commit or
rollback failures return `OUTCOME_INDETERMINATE`.

IF-T78 fixture resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| accepted `dbtool_exact_<suffix>` | exact CREATE + PK PASS | bound INSERT 1/1 plus atomic INSERT 2/2 PASS | after targeted delete 2/2 rows; stable IDs `1,2`; ID 1 state `updated` PASS | ID 1 `created` to `updated` PASS | ID 3 removed; IDs `1,2` remained PASS | final `information_schema.tables` count 0 PASS | late oversized second row rejected before transaction; existing count stayed 1 PASS | exact statement/params/batch accepted; N-1 late-row envelope rejected PASS | DROP; accepted and rejected tables absent PASS |
| rejected `dbtool_rejected_<suffix>` | N-1 CREATE rejected before pool/dispatch PASS | N/A | catalog count 0/0 PASS | N/A | N/A | `information_schema.tables` never contained table PASS | `INPUT_BUDGET_EXCEEDED` PASS | one-byte request envelope rejected PASS | N/A; resource was never created |

Verification: shared adapter-sql 49/49 PASS; PostgreSQL Docker exact mutation
1/1 PASS; strict Clippy, rustfmt, and diff check PASS. Implementation commit:
`a6e60c5`.

Cleanup: PASS

Commits: `642bfd9`, `974886f`, `561ea93`, `bea6bed`, `fe7cfb9`, `a6e60c5`,
IF-T58/IF-T78
