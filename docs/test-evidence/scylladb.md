# ScyllaDB Validation Evidence

Task ID: DB-SCYLLA-001

Result: LIVE_PASS

Run at (UTC): 2026-07-17T16:31:05Z

Environment: Docker 29.4.0 on macOS arm64; Rust 1.96.0; one shard; developer
mode; `SimpleStrategy` replication factor 1

Product version: ScyllaDB 2026.1.8-0.20260705.be3e0d31004f

Image: `scylladb/scylla:2026.1.8`; multi-architecture manifest digest
`sha256:7f536795dde84c82fb625f496a9d043eed7674fbb2eb08c88998b51c414cd276`
with native `linux/arm64/v8`

Command: `./scripts/integration-scylla-test.sh`

Implementation commit: `336f4bd`

## Executed gates

| Gate | Result |
| --- | --- |
| `live_services::cql_live_cql_lifecycle_and_typed_values` | 1/1 PASS |
| `bounded_sql::cql_live_streams_one_probe_row_for_paged_results` | 1/1 PASS |
| `live_bounded_cql::cql_catalogs_are_bounded_before_cli_rendering` | 1/1 PASS |
| `adapter-cassandra::cassandra_live_budgeted_cql_rejects_before_write_and_cleans_keyspace` | 1/1 PASS |
| Container/network cleanup | PASS; no `dbtool-it-scylla` container remained |

Resource operations:

| Resource | Create | Insert/write | Read all fixture data | Update | Targeted delete | Metadata/types | Guard/limit | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_scylla.dbtool_it_scylla_users_80434_1784305766706` | SQL-compatible table + integer PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | both rows exact after stable ID sort PASS | ID 1 became `alice-updated` PASS | ID 2 removed while ID 1 remained PASS | schemas, tables, columns and primary index PASS | unguarded write blocked; destructive operations confirmed; N/N+1 and byte read budget PASS | table absent after DROP PASS |
| `dbtool_it_scylla.dbtool_it_scylla_cql_users_80434_1784305770836` | native CQL table + PK PASS | rows `(1,alice)`, `(2,bob)` 2/2 PASS | 2/2 exact; limit 1 returned one row with `truncated=true` PASS | ID 1 became `alice-updated` PASS | ID 2 absent after targeted delete PASS | keyspaces, tables, schema and primary index PASS | `cql query` mutation and unguarded exec blocked; destructive confirmation PASS | table absent after DROP PASS |
| `dbtool_it_scylla.dbtool_it_scylla_typed_80434_1784305771877` | 12-column native CQL table PASS | one complete row PASS | int, text, double, boolean, list, set, map, tuple, blob, UUID, timestamp and null exact PASS | N/A | N/A | all 12 columns returned through public schema path PASS | target-bound create/drop confirmation PASS | table absent after DROP PASS |
| `dbtool_it_bnd_*` keyspace with three empty tables | keyspace + 3/3 tables PASS | N/A | no fixture rows by design | N/A | N/A | exact table limit 3; probe limit 2; keyspace exact N/N-1 PASS | zero/overflow limits rejected before connection; public capability operations present PASS | keyspace dropped and absent from final catalog PASS |
| `dbtool_it_input_*` keyspace and `items` table | exact keyspace/table CREATE PASS | native row ID 1 plus SQL-compatible row ID 2, 2/2 PASS | both rows exact PASS | ID 1 `created` to `updated` PASS | both stable IDs deleted and empty reads verified PASS | exact SQL-compatible and CQL mutation operations advertised PASS | one-byte rejected CREATE produced no keyspace; finite input/read envelopes PASS | table and keyspace dropped; final catalog absence PASS |

## Product and alias assertions

- The service was the named ScyllaDB product, not Cassandra standing in for it.
- The primary connection used `scylla://`; the same real node also accepted the
  shared `cassandra://` protocol alias probe.
- Host-port discovery used `address-translator=contact-point`, so driver
  topology addresses remained bounded to the published test endpoint.
- The low-memory one-shard profile first failed safely when 1 GiB exceeded the
  constrained VM allowance; the committed default is a configurable 384 MiB
  database memory budget, and the final clean run passed.

## Boundaries

This proves the current dbtool core surface against a real single-node
ScyllaDB product. It does not claim multi-node consistency, repair, replication
under failure, rolling upgrade, tablet migration, or production tuning.
Dynamic CQL parameters remain explicitly unsupported rather than silently
ignored. Those boundaries do not weaken the complete single-node CQL family
checklist recorded here.

Cleanup: PASS
