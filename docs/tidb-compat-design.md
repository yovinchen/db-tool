# TiDB Compatibility Design

## Goal

Prove that the `tidb://` alias works against a real TiDB server, not only against the MySQL adapter with a MySQL container. The test must stay opt-in because TiDB needs multiple processes and more memory than the lightweight compatibility services.

## Runtime Topology

The `tidb` Docker Compose profile starts three services:

- `tidb-pd`: single PD node for metadata, placement, timestamps, and cluster bootstrap.
- `tidb-tikv`: single TiKV node for storage.
- `tidb`: SQL server exposing the MySQL-compatible protocol on `${DBTOOL_IT_TIDB_PORT:-14000}` and status HTTP on `${DBTOOL_IT_TIDB_STATUS_PORT:-11080}`.

This is intentionally separate from the `compat` profile. MariaDB, Valkey, KeyDB, and Dragonfly are single-process compatibility checks; TiDB is a small distributed topology and should not slow down the default compatibility loop.

## Resource Defaults

The profile applies bounded defaults that can be overridden per run:

- PD: `${DBTOOL_IT_TIDB_PD_CPUS:-0.25}`, `${DBTOOL_IT_TIDB_PD_MEMORY:-256m}`
- TiKV: `${DBTOOL_IT_TIDB_TIKV_CPUS:-0.75}`, `${DBTOOL_IT_TIDB_TIKV_MEMORY:-1g}`
- TiDB SQL: `${DBTOOL_IT_TIDB_CPUS:-0.50}`, `${DBTOOL_IT_TIDB_MEMORY:-512m}`

The default TiDB version is `${DBTOOL_IT_TIDB_VERSION:-v8.5.6}`. Keep the three TiDB images on the same version unless testing a deliberate upgrade path.

## DSN Strategy

The default DSN is:

```text
tidb://root@127.0.0.1:${DBTOOL_IT_TIDB_PORT:-14000}
```

It omits the database path on purpose. TiDB's local bootstrap accepts the `root` user with an empty password, then the test creates `${DBTOOL_IT_TIDB_DB:-dbtool_it_tidb}` explicitly and uses schema-qualified table names. That proves both no-database bootstrap connections and `database.table` SQL paths through the CLI.

## Verification Flow

`./scripts/integration-tidb-test.sh` performs the full local loop:

1. Source `scripts/integration-env.sh`.
2. Install the cleanup trap unless `DBTOOL_IT_KEEP_SERVICES=1`.
3. Start `tidb-pd`, `tidb-tikv`, and `tidb` through `scripts/integration-tidb-up.sh`.
4. Wait for Docker Compose health checks.
5. Set `DBTOOL_RUN_TIDB_INTEGRATION=1`.
6. Run `cargo test -p dbtool-cli --test live_services tidb_compat -- --nocapture`.
7. Remove the TiDB containers and network on exit unless `DBTOOL_IT_KEEP_SERVICES=1`.

The live test verifies:

- `ping` and `caps` preserve the caller-facing kind as `tidb`.
- MySQL-family typed values decode correctly for integer, float, binary bytes, and null.
- Result limiting marks truncated output.
- Destructive SQL still requires confirmation.
- Database creation works through a root no-database DSN.
- Create, insert, query, schema, and drop work against a schema-qualified TiDB table.

## CI Policy

Normal CI validates the TiDB Compose profile with:

```bash
docker compose -f docker-compose.integration.yml --profile tidb config
```

The live TiDB run is opt-in through the `run_live_tidb` workflow_dispatch input. This keeps PR feedback fast while preserving an automated path for compatibility verification.

## Failure Recovery

Use the shared cleanup script if a run is interrupted:

```bash
./scripts/integration-down.sh
```

For custom project names, pass the same `DBTOOL_IT_PROJECT` value used to start the services.

If the SQL server exits immediately, inspect:

```bash
docker logs dbtool-it-tidb
docker logs dbtool-it-tidb-pd
docker logs dbtool-it-tidb-tikv
```

The most likely local causes are a TiDB flag/version mismatch, not enough Docker memory for TiKV, or a stale container from a previous failed run.

## Known Boundaries

- The topology is a single-node compatibility harness, not an HA TiDB cluster.
- TLS, password rotation, user management, placement rules, and upgrade scenarios are outside this test.
- The profile uses the local insecure bootstrap user only for disposable integration tests.
- TiDB remains implemented through the MySQL-family adapter; the alias kind is preserved for user-facing metadata and test assertions.
