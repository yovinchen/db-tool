# SQLite Completeness Evidence

Task ID: DB-SQLITE-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T19:55:00Z (focused atomic-import refresh; original full-family run 2026-07-14T18:52:17Z)

Environment: macOS arm64; Rust 1.96.0; service-free file-backed SQLite

Command: `cargo test -p dbtool-cli --test db_completeness sqlite_full_crud -- --nocapture`; focused CLI SQL roundtrip and late-constraint rollback tests; adapter SQLite atomic insert test

Product version: SQLite 3.46.0

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_sqlite_records` | table + unique index PASS | 3/3 typed rows PASS | 3/3 rows; IDs `1,2,3`; text/bool/real/blob/null values PASS | ID 2 became `bob-updated`, score `4.25` PASS | ID 3 removed; IDs `1,2` remained PASS | 5 columns, PK and unique index PASS | CREATE and unbounded DELETE required confirmation PASS | limit 2 truncated; 2-row export/import readback PASS | table absent after DROP PASS |
| `dbtool_it_sqlite_records_restored` | table PASS | 2/2 exported rows imported PASS | IDs `1,2` and names match source PASS | N/A | N/A | table visible before cleanup PASS | import required `--allow-write` through shared command contract | artifact roundtrip PASS | table absent after DROP PASS |
| `atomic_rows` focused fixture | table PASS | quoted/injection-shaped text inserted only as bound data; duplicate second ID rejected PASS | failed two-row batch left count 0; later valid row preserved exact text PASS | N/A | N/A | `sql.insert_rows_atomic` advertised only by SQLite adapter PASS | invalid table/column/duplicate column/row width rejected PASS | complete artifact committed in one transaction and returned `atomic=true`; late constraint rolled back every row PASS | temporary file database removed PASS |

Assertions: ping/caps, DDL confirmation, full fixture contents, typed values,
schema/index metadata, update readback, targeted-delete readback, unbounded-delete
confirmation, result truncation, parameterized atomic export/import, late-error
whole-batch rollback, and post-drop table listing all passed.

## IF-T78 exact mutation refresh

Run at (UTC): 2026-07-16T12:13:31Z

`sql.execute_budgeted` and `sql.insert_rows_atomic_budgeted` now validate the
complete statement/parameter or table/column/row request before pool or
transaction access. The service-free `exact_crud` lifecycle proved N-1 CREATE
left `sqlite_master` unchanged; exact CREATE, bound INSERT, UPDATE, two-row
atomic INSERT, full three-row readback, targeted DELETE, and DROP all passed.
A late oversized row produced `INPUT_BUDGET_EXCEEDED` before any row was
inserted. A duplicate execute is conservatively `OUTCOME_INDETERMINATE` because
it followed dispatch. Final catalog count for both test tables was zero.

IF-T78 fixture resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| accepted `exact_crud` | exact CREATE PASS | bound INSERT 1/1 plus atomic INSERT 2/2 PASS | 3/3 rows; stable IDs `1,2,3`; ID 1 state `updated` PASS | ID 1 `created` to `updated` PASS | ID 1 removed; 2/2 rows remained PASS | `sqlite_master` final count 0 PASS | duplicate PK after dispatch returned `OUTCOME_INDETERMINATE`; late oversized second row wrote 0/2 PASS | exact statement/params and atomic batch accepted; N-1 late-row envelope rejected PASS | exact DROP; catalog absence PASS |
| rejected `rejected_budget` | one-byte budget CREATE rejected before dispatch PASS | N/A | catalog count 0/0 PASS | N/A | N/A | `sqlite_master` never contained table PASS | `INPUT_BUDGET_EXCEEDED` PASS | N-1 request rejected PASS | N/A; resource was never created |

Verification: adapter-sql 49/49 PASS; strict Clippy, rustfmt, and diff check
PASS. Implementation commit: `a6e60c5`.

Cleanup: PASS

Commits: `d6bd18b`, `a6e60c5`, IF-T58/IF-T78
