# SQL Parameter Binding Evidence

Task: IF-T43

Result: LIVE_PASS

Run at (UTC): 2026-07-15T17:02:40Z

Environment: Docker on macOS arm64; PostgreSQL 16; MySQL 8.4; SQLite in-process

Commands:

- `cargo test -p adapter-sql`
- `cargo test -p dbtool-cli --test cli_json`
- `DBTOOL_RUN_SQL_PARAM_INTEGRATION=1 cargo test -p dbtool-cli --test live_sql_params -- --nocapture`

## Per-table operations

Every lifecycle used the same logical columns and values:

| Column | Stored value | Parameter type |
| --- | --- | --- |
| `id` | `7` | Int |
| `note` | `O'Reilly'); drop table protected_data; --` | Text/injection-shaped literal |
| `score` | `12.75` | Float |
| `enabled` | `true` | Bool |
| `payload` | bytes `00 7f ff` | Bytes |
| `optional` | SQL NULL | Null |
| `occurred_at` | `1700000000123` epoch ms | Timestamp |
| `metadata` | `{"source":"<backend>","tags":["bound","safe"]}` | JSON |

| Backend/table | Create | Parameterized insert | Parameterized read | Injection check | Metadata/count | Cleanup |
| --- | --- | --- | --- | --- | --- | --- |
| SQLite `bound_values` | six-column service-free table PASS | Null/Bool/Int/Float/Text/Bytes PASS; tagged timestamp/JSON query PASS | every scalar/blob/null value exact PASS | injection-shaped note returned verbatim; table remained PASS | count=1 PASS | temporary DB directory removed PASS |
| PostgreSQL `dbtool_it_pg_params_<run>` | bigint/text/double/bool/bytea/text/timestamptz/jsonb PASS | all eight parameter types through `$1..$8` PASS | all eight values, including exact timestamp and parsed JSON, PASS | note matched through a second bound parameter; count remained 1 PASS | count=1 PASS | public confirmed DROP PASS; prefix remaining=0 |
| MySQL `dbtool_it_mysql_params_<run>` | bigint/text/double/bool/blob/text/datetime(3)/json PASS | all eight parameter types through `?` PASS | all eight values, including bool, exact timestamp and parsed JSON, PASS | note matched through a second bound parameter; count remained 1 PASS | count=1 PASS | public confirmed DROP PASS; prefix remaining=0 |

The first MySQL run used an overly narrow test expectation (`1` instead of the
adapter's more precise `true`) after the database operations had succeeded. Its
single table `dbtool_it_mysql_params_39980_1784134881120817000` was separately
dropped through dbtool's target-bound confirmation flow. A final prefix scan
reported zero remaining PostgreSQL and MySQL test tables.

Unsupported boundary: SQL Server, Db2, and Cassandra still reject non-empty
dynamic parameter arrays explicitly. No adapter silently discards parameters.
