# Cassandra Validation Evidence

Task ID: DB-CASSANDRA-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:08:29Z

Environment: Docker on macOS arm64; Rust 1.96.0; `cassandra:5.0`; single node; `SimpleStrategy` replication factor 1

Product version: Apache Cassandra 5.0

Command: `./scripts/integration-cassandra-test.sh`; `./scripts/integration-cassandra-fixture-data-test.sh`

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata/types | Guard/limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_cassandra.dbtool_it_cassandra_users_26823_1784059527908` | SQL-compatible table + integer PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | both rows exact after order normalization PASS | ID 1 became `alice-updated` PASS | ID 2 removed and ID 1 remained PASS | keyspaces, tables, two-column schema, synthesized primary index PASS | write without permission blocked; unbounded delete and DDL confirmation required PASS | table absent after DROP; Docker volume removed PASS |
| `dbtool_it_cassandra.dbtool_it_cassandra_cql_users_26823_1784059532619` | dedicated `cql exec` table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | both rows exact; `--limit 1` returned one row with `truncated=true` PASS | ID 1 became `alice-updated` PASS | ID 2 absent after targeted delete PASS | CQL keyspaces/tables/schema; ID primary column and primary index PASS | `cql query` write and unguarded `cql exec` blocked; unbounded delete required confirmation PASS | table absent from CQL listing after DROP PASS |
| `dbtool_it_cassandra.dbtool_it_cassandra_typed_26823_1784059533869` | 12-column CQL table + PK PASS | one complete typed row PASS | int, text, double, boolean, list, set, map, tuple, blob, UUID, timestamp, and null all exact PASS | N/A | N/A | all 12 columns present through dedicated CQL schema path PASS | destructive create/drop used target-bound confirmation PASS | table absent after DROP PASS |
| `dbtool_it_cassandra.dbtool_it_cassandra_fixture_people` | keyspace + five-column fixture table PASS | Alice, Bob, Carol 3/3 PASS | all 15 stored field values exact, sorted by ID only for comparison PASS | N/A | N/A | exact column-name set, five types, non-null ID, primary/unique index PASS | seed DDL used public CQL confirmation path PASS | explicit DROP verified by table listing; trap cleans partial failures; no container remained PASS |

Assertions: the real Cassandra 5.0 container accepted `cassandra://`, exposed
both SQL-compatible and dedicated CQL capabilities, and passed the complete
single-node CRUD, schema, representative-type, safety, truncation, fixture, and
cleanup checklist.

Defects found and fixed:

- Cassandra primary keys are described in `system_schema.columns`, not in the
  secondary-index catalog. Commit `cd259b2` synthesizes the ordered common
  primary index from partition and clustering columns.
- Cassandra 5.0 stores secondary-index targets in
  `system_schema.indexes.options['target']`; it has no `column_name` field.
  Commit `e6a1f9a` reads the current catalog and normalizes collection targets.
- CQL destructive operations previously needed only `--allow-write`. Commit
  `497b704` adds target-bound confirmation and named-readonly enforcement.
- Commit `8ca3908` makes the live and fixture suites validate every resource and
  every expected stored value, then prove explicit removal.

Scylla boundary for this historical Cassandra run: the `scylla://` scheme was
routed against the same Cassandra server as an alias compatibility probe. No
ScyllaDB image was used in this slice, so it is not included in the Cassandra
claim. Product-native ScyllaDB was subsequently completed separately in
[`scylladb.md`](scylladb.md).

Other boundaries: the adapter currently uses an unpaged driver query before
the legacy unlimited result path, while exact `query_cql_budgeted` uses a
one-row page and caller row/byte envelope. Dynamic parameters remain
unsupported, execution reports no backend row count, and this single-node RF1
run does not prove multi-node consistency, repair, replication, or failover.

## IF-T78 exact mutation refresh

Run at (UTC): 2026-07-16T12:13:31Z

The repository-owned
`cassandra_live_budgeted_cql_rejects_before_write_and_cleans_keyspace` test
passed 1/1 against the running Cassandra 5.0 container. A one-byte budget
rejected CREATE and the rejected keyspace was absent. The unique
`dbtool_it_input_*` keyspace then completed exact keyspace/table CREATE,
INSERT, UPDATE, `(1, updated)` SELECT, targeted DELETE with empty readback,
table DROP, and keyspace DROP. The final keyspace catalog contained no test
keyspace. CQL text/NUL/fixed-byte checks run before session dispatch; every
post-dispatch error is `OUTCOME_INDETERMINATE`.

Verification: adapter-cassandra 14/14 PASS; Docker exact CQL lifecycle 1/1
PASS; strict Clippy, rustfmt, and diff check PASS. Implementation commit:
`6fcd23c`. Real ScyllaDB remains a separate product claim; its later LIVE_PASS
is recorded in [`scylladb.md`](scylladb.md).

SQL-compatible exact follow-up run (UTC): 2026-07-16T12:22:19Z. Cassandra now
also advertises `sql.execute_budgeted`; both public SQL compatibility and native
CQL mutation entry points use finite preflight and the same post-dispatch
outcome-indeterminate rule. The Docker lifecycle executes one exact
SQL-compatible UPDATE in addition to native CQL CREATE/INSERT/UPDATE/DELETE,
reads the updated row back, and still removes the table and keyspace completely.
All SQL adapters and callers subsequently converged on the shared
`SqlExecuteInput { sql, params }` wire envelope in `d6801a5`.

IF-T78 fixture resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| accepted keyspace `dbtool_it_input_<suffix>` | exact native CQL CREATE PASS | N/A | keyspace usable by the complete table fixture PASS | N/A | N/A | keyspace catalog present during lifecycle and absent at end PASS | exact mutation operation advertised PASS | finite CQL request envelope PASS | exact DROP KEYSPACE; final catalog absence PASS |
| accepted table `dbtool_it_input_<suffix>.items` | exact native CQL CREATE PASS | native row ID 1 plus SQL-compatible row ID 2, 2/2 PASS | 2/2 rows; stable IDs `1,2`; values `updated`,`sql-compatible` PASS | native ID 1 `created` to `updated` PASS | native ID 1 and SQL-compatible ID 2 removed; both reads empty PASS | table lifecycle inside isolated keyspace PASS | native and SQL-compatible writes share finite preflight; post-dispatch errors indeterminate PASS | caller row/byte read envelope and finite mutation envelope PASS | exact DROP TABLE then DROP KEYSPACE PASS |
| rejected keyspace `dbtool_it_input_<suffix>_rejected` | one-byte CREATE rejected before session dispatch PASS | N/A | keyspace catalog count 0/0 PASS | N/A | N/A | keyspace list never contained resource PASS | `INPUT_BUDGET_EXCEEDED` PASS | N-1 request rejected PASS | N/A; resource was never created |

Implementation commits: `94f3ffb`, `d6801a5`.

Cleanup: PASS

Commits: `cd259b2`, `497b704`, `e6a1f9a`, `8ca3908`, `f3712b3`, `6fcd23c`,
`94f3ffb`, `d6801a5`
