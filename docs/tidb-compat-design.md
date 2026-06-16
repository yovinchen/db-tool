# TiDB Compatibility And Secure HA Design

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
- Table listing works with an explicit TiDB database schema.
- Create, insert, query, schema, and drop work against a schema-qualified TiDB table.

## CI Policy

Normal CI validates the TiDB Compose profile with:

```bash
docker compose -f docker-compose.integration.yml --profile tidb config
```

Normal CI also validates the secure HA profile without starting containers:

```bash
docker compose -f docker-compose.integration.yml --profile tidb-secure config
```

The live TiDB runs are opt-in through the `run_live_tidb` and `run_live_tidb_secure` workflow_dispatch inputs. This keeps PR feedback fast while preserving an automated path for compatibility and security verification.

## Secure HA Profile

`./scripts/integration-tidb-secure-test.sh` adds a heavier local topology:

- 3 PD nodes: `tidb-secure-pd-1`, `tidb-secure-pd-2`, and `tidb-secure-pd-3`.
- 2 TiKV nodes: `tidb-secure-tikv-1` and `tidb-secure-tikv-2`.
- 2 TiDB SQL nodes exposed on `${DBTOOL_IT_TIDB_SECURE_PORT_1:-14100}` and `${DBTOOL_IT_TIDB_SECURE_PORT_2:-14101}`.

The script sources `scripts/integration-tidb-secure-prepare.sh`, which generates a short-lived CA, a local server certificate with SANs for all TiDB component DNS names plus `127.0.0.1`, a client certificate, and the TiDB/TiKV config files under `${DBTOOL_IT_TIDB_SECURE_DIR:-.tmp/dbtool-it-tidb-secure}`.

The secure profile uses TLS in two layers:

- Component mTLS between TiDB, PD, and TiKV through the TiDB `cluster-ssl-*`, PD `--cacert/--cert/--key`, and TiKV `[security]` settings.
- SQL client TLS through TiDB `ssl-ca`, `ssl-cert`, and `ssl-key`, with host-side DSNs using `ssl-mode=VERIFY_CA`.

The secure live test verifies:

- Both TiDB SQL nodes accept TLS root connections.
- A `REQUIRE SSL` user can connect over TLS and is rejected when `ssl-mode=DISABLED`.
- A `REQUIRE X509` user is rejected without a client certificate and succeeds with `ssl-cert` plus `ssl-key`.
- SQL lifecycle coverage runs through both SQL nodes against the same secure database.
- The X509 user can run the same SQL lifecycle with a client certificate.

## TiProxy Profile

`./scripts/integration-tidb-tiproxy-test.sh` adds TiProxy to the secure HA
topology through a separate `tidb-tiproxy` Compose profile. The profile uses
PingCAP's `pingcap/tiproxy:${DBTOOL_IT_TIDB_TIPROXY_VERSION:-v1.3.2}` image and
a generated `tiproxy.toml` under the same secure working directory.

The TiProxy config follows PingCAP's documented model:

- `[proxy]` listens on `0.0.0.0:6000`, uses the three TLS-enabled PD addresses
  for service discovery, and exposes the host port
  `${DBTOOL_IT_TIDB_TIPROXY_PORT:-14200}`.
- `[api]` listens on `0.0.0.0:3080` and exposes the host status port
  `${DBTOOL_IT_TIDB_TIPROXY_STATUS_PORT:-11200}`.
- `[security.cluster-tls]` lets TiProxy talk to PD over component TLS.
- `[security.sql-tls]` plus `require-backend-tls = true` require TLS between
  TiProxy and the TiDB SQL nodes.
- `[security.server-tls]` enables TLS on the client-facing TiProxy SQL port.

The TiProxy drill performs this flow:

1. Start the secure HA topology through `integration-tidb-secure-up.sh`.
2. Start `tidb-secure-tiproxy`.
3. Create a fixture database, table, and `REQUIRE SSL` proxy user through a
   direct secure SQL node.
4. Wait for dbtool to `ping` the TLS TiProxy DSN as that proxy user.
5. Prove the `REQUIRE SSL` user can read through TiProxy.
6. Stop `tidb-secure-1`, require the direct node DSN to fail, then write/read
   through TiProxy.
7. Restart `tidb-secure-1`, then stop `tidb-secure-2` and repeat the TiProxy
   write/read check.

Pass/fail criteria:

- TiProxy must accept TLS SQL connections from dbtool.
- TiProxy must route new dbtool SQL connections while either SQL node is
  stopped.
- Direct DSNs for stopped SQL nodes must fail `ping`, proving the check is not
  accidentally using the stopped endpoint.
- A `REQUIRE SSL` SQL user must be usable through the proxy TLS DSN.

This drill proves dbtool can use a local TiProxy entrypoint for new SQL
connections. It does not claim existing-session migration or virtual-IP HA:
PingCAP documents TiProxy's unexpected TiDB downtime and certificate-based
authentication limitations, so those remain explicit production-readiness
boundaries.

## Secure HA Failover Drill

`./scripts/integration-tidb-ha-drill.sh` reuses the secure HA topology for an
explicit SQL-node failover exercise. It is separate from
`integration-tidb-secure-test.sh` so auth/TLS checks and disruptive container
operations can be run independently.

The drill performs this flow:

1. Start the secure HA topology through `integration-tidb-secure-up.sh`.
2. Create a fixture table through `tidb-secure-1`.
3. Read that row through `tidb-secure-2` to prove both SQL nodes see the same
   TiKV-backed state.
4. Stop `tidb-secure-1`, require its DSN to reject `ping`, then write and read
   through `tidb-secure-2`.
5. Restart `tidb-secure-1` and require it to read the row written while it was
   stopped.
6. Stop `tidb-secure-2`, repeat the surviving-node write/read check through
   `tidb-secure-1`, then restart `tidb-secure-2`.
7. Require `tidb-secure-2` to read the row written while it was stopped.

Pass/fail criteria:

- Both secure SQL DSNs must become reachable before the drill starts.
- A stopped SQL node must become unreachable from dbtool's `ping` command.
- The surviving SQL node must continue accepting TLS SQL writes and reads.
- A restarted SQL node must read rows written during its outage.
- The script removes the topology on exit unless `DBTOOL_IT_KEEP_SERVICES=1`;
  if services are kept, it attempts to restart any SQL node it stopped.

This drill proves local TiDB SQL frontend failover through dbtool's MySQL-family
adapter. Together with the TiProxy drill, it still does not replace placement,
PD leadership, TiKV failure, backup/restore, upgrade, certificate-rotation, or
existing-session migration drills.

## PD Quorum Drill

`./scripts/integration-tidb-pd-drill.sh` reuses the secure HA topology for a
single-PD outage exercise. It is local-only while the CI budget freeze is in
effect.

The drill performs this flow:

1. Start the secure HA topology through `integration-tidb-secure-up.sh`.
2. Create a fixture table through `tidb-secure-1`.
3. Read that row through `tidb-secure-2` to prove both SQL nodes share the same
   TiKV-backed state.
4. Stop `tidb-secure-pd-1`, require both SQL nodes to accept TLS `ping`, write
   one row through each SQL node, and read each row from the opposite SQL node.
5. Restart `tidb-secure-pd-1` and require both SQL nodes to become reachable.
6. Repeat the same stop/write/read/restart flow for `tidb-secure-pd-2` and
   `tidb-secure-pd-3`.

Pass/fail criteria:

- The 3-PD secure topology must start before the drill begins.
- With any one PD container stopped, both TiDB SQL nodes must keep accepting
  TLS SQL writes and reads through dbtool.
- Rows written while a PD node is down must be readable through the other SQL
  node, proving the check is exercising shared TiKV state.
- The stopped PD node must restart cleanly before the drill proceeds to the
  next PD node.

This drill proves dbtool remains usable through the local TiDB SQL endpoints
while the PD quorum loses one member at a time. It does not identify or require
stopping the current PD leader specifically, and it does not cover a TiKV store
failure, placement-rule behavior, backup/restore, upgrade, certificate
rotation, or existing-session migration. The script applies
`DBTOOL_IT_TIDB_PD_DRILL_REQUEST_TIMEOUT` and
`DBTOOL_IT_TIDB_PD_DRILL_DEADLINE` to each dbtool call so failed quorum behavior
terminates as a bounded test failure.

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

For secure HA runs, inspect the secure service names:

```bash
docker logs dbtool-it-tidb-secure-1
docker logs dbtool-it-tidb-secure-pd-1
docker logs dbtool-it-tidb-secure-tikv-1
```

The most likely local causes are a TiDB flag/version mismatch, not enough Docker memory for TiKV, or a stale container from a previous failed run.

## Known Boundaries

- The default `tidb` profile is a single-node compatibility harness; use `tidb-secure` for local HA/security coverage.
- The secure HA topology is still a local integration harness, not a production deployment model.
- Password rotation, placement rules, TiKV failover, certificate rotation, and upgrade scenarios are outside these tests.
- The profile uses the local bootstrap root user only for disposable integration tests.
- TiDB remains implemented through the MySQL-family adapter; the alias kind is preserved for user-facing metadata and test assertions.
