# Cassandra Validation Evidence

Task ID: DB-CASSANDRA-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:08:29Z

Environment: Docker on macOS arm64; Rust 1.96.0; `cassandra:5.0`; single node; `SimpleStrategy` replication factor 1

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

Scylla boundary: the `scylla://` scheme was routed against the same Cassandra
server as an alias compatibility probe. No ScyllaDB image or real ScyllaDB node
was used, so task `DB-SCYLLA-001` remains PARTIAL and is not included in this
Cassandra completion claim.

Other boundaries: the adapter currently uses an unpaged driver query before
the CLI limiter, dynamic parameters are unsupported, execution reports no
backend row count, and this single-node RF1 run does not prove paging,
multi-node consistency, repair, replication, or failover.

Cleanup: PASS

Commits: `cd259b2`, `497b704`, `e6a1f9a`, `8ca3908`, this commit
