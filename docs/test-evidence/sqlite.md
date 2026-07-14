# SQLite Completeness Evidence

Task ID: DB-SQLITE-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T18:52:17Z

Environment: macOS arm64; Rust 1.96.0; service-free file-backed SQLite

Command: `cargo test -p dbtool-cli --test db_completeness sqlite_full_crud -- --nocapture`

Product version: SQLite 3.46.0

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata | Guard | Limit/transfer | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_sqlite_records` | table + unique index PASS | 3/3 typed rows PASS | 3/3 rows; IDs `1,2,3`; text/bool/real/blob/null values PASS | ID 2 became `bob-updated`, score `4.25` PASS | ID 3 removed; IDs `1,2` remained PASS | 5 columns, PK and unique index PASS | CREATE and unbounded DELETE required confirmation PASS | limit 2 truncated; 2-row export/import readback PASS | table absent after DROP PASS |
| `dbtool_it_sqlite_records_restored` | table PASS | 2/2 exported rows imported PASS | IDs `1,2` and names match source PASS | N/A | N/A | table visible before cleanup PASS | import required `--allow-write` through shared command contract | artifact roundtrip PASS | table absent after DROP PASS |

Assertions: ping/caps, DDL confirmation, full fixture contents, typed values,
schema/index metadata, update readback, targeted-delete readback, unbounded-delete
confirmation, result truncation, public export/import, and post-drop table listing
all passed.

Cleanup: PASS

Commit: `d6bd18b`
