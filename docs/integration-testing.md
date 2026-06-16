# Live Integration Testing

The default verification path is service-free:

```bash
./scripts/verify.sh
```

Live integration tests can start local databases with Docker Compose:

```bash
./scripts/integration-test.sh
```

The script starts Postgres, MySQL, Redis, and MongoDB, waits for health checks, runs the live CLI tests, and then removes the containers and volumes.

Run a narrower flow-control smoke when you need to validate throttling-facing
CLI flags against real services without running the whole live test binary:

```bash
./scripts/integration-flow-control-test.sh
```

The flow-control smoke starts the same Postgres, MySQL, Redis, and MongoDB
containers, creates disposable fixture data, and verifies:

- PostgreSQL `--request-timeout` and `--deadline` abort a slow `pg_sleep`
  query with `TIMEOUT`.
- PostgreSQL and MySQL SQL result limits mark output as truncated.
- Redis `kv scan --limit` truncates fixture keys.
- MongoDB `doc find --limit` truncates fixture documents.
- SQL rate/admission flags such as `--rate` and `--acquire-timeout` are
  accepted on a real MySQL query.

Tune the smoke with `DBTOOL_IT_FLOW_CONTROL_TIMEOUT_REQUEST`,
`DBTOOL_IT_FLOW_CONTROL_TIMEOUT_DEADLINE`, `DBTOOL_IT_FLOW_CONTROL_RATE`,
`DBTOOL_IT_FLOW_CONTROL_ACQUIRE_TIMEOUT`,
`DBTOOL_IT_FLOW_CONTROL_REQUEST_TIMEOUT`, and
`DBTOOL_IT_FLOW_CONTROL_DEADLINE`.

Run the reusable fixture-data smoke when you need file-backed test data for the
base database suite:

```bash
./scripts/integration-fixture-data-test.sh
```

The fixture smoke starts the same Postgres, MySQL, Redis, and MongoDB
containers, loads backend-specific seed files from `testdata/`, then verifies
that dbtool can query the seeded rows, keys, and documents back through the
normal CLI surfaces:

- [base-postgres-seed.sql](../testdata/base-postgres-seed.sql) for PostgreSQL.
- [base-mysql-seed.sql](../testdata/base-mysql-seed.sql) for MySQL.
- [base-redis-seed.commands](../testdata/base-redis-seed.commands) for Redis.
- [base-mongo-seed.ndjson](../testdata/base-mongo-seed.ndjson) for MongoDB.

Run the fixture-image smoke when you need to validate database Dockerfiles that
carry the same fixture data inside service startup:

```bash
./scripts/integration-fixture-images-test.sh
```

The fixture-image smoke builds the Dockerfiles under
[docker/fixtures](../docker/fixtures), starts the `fixture-images` Compose
profile, then verifies dbtool can read the preloaded PostgreSQL rows, MySQL
rows, Redis keys, and MongoDB documents without first injecting fixture data
from the host.

Compatible database integration tests use a separate profile:

```bash
./scripts/integration-compat-test.sh
```

The compatibility script starts MariaDB and Valkey by default, waits for health checks, runs the compatibility live tests, and removes the containers and volumes. Run the optional Redis-compatible matrix as well:

```bash
DBTOOL_IT_COMPAT_EXTRA=1 ./scripts/integration-compat-test.sh
```

That extra mode adds KeyDB and Dragonfly. KeyDB is pinned to `linux/amd64` by default because its published Alpine image is amd64-only on Apple Silicon.

PostgreSQL-family compatibility tests use their own profile:

```bash
./scripts/integration-pg-compat-test.sh
```

The PostgreSQL-family compatibility script starts CockroachDB and TimescaleDB,
waits for health checks, runs `cockroach://` and `timescale://` live SQL tests,
and removes the containers and volumes.

SQL Server uses a separate opt-in profile because the image is larger and the
Linux container is intended for x86-64 Docker environments:

```bash
./scripts/integration-sqlserver-test.sh
```

The SQL Server script starts `mcr.microsoft.com/mssql/server`, waits for the
server-ready log health check, runs `sqlserver://` live SQL lifecycle tests, and
removes the container by default. The default DSN uses
`trust-server-certificate=true` for the local developer certificate. On non
`x86_64` hosts the up script exits before pulling the image unless
`DBTOOL_IT_SQLSERVER_ALLOW_UNSUPPORTED_ARCH=1` is set.

Cassandra/CQL uses a separate opt-in profile because the JVM image needs more
memory and can take longer to become healthy:

```bash
./scripts/integration-cassandra-test.sh
```

The Cassandra script starts the official `cassandra` image, waits for `cqlsh`
health checks, creates the test keyspace from the live test, runs
`cassandra://` and `scylla://` CQL lifecycle coverage through the existing
`sql` command family, and removes the container by default. The default DSN does
not include a keyspace so the first connection can create it safely. It also
uses `address-translator=contact-point` so the Rust CQL driver translates
Docker-internal broadcast addresses back to the published host port.

TiDB compatibility uses its own profile because it starts a small PD/TiKV/TiDB topology:

```bash
./scripts/integration-tidb-test.sh
```

The TiDB script waits for the three containers, runs the `tidb://` live SQL compatibility test, and removes the containers and network by default.

See [TiDB compatibility design](tidb-compat-design.md) for the topology, DSN strategy, validation flow, and known boundaries.

TiDB secure HA integration starts a larger local topology with component TLS and SQL TLS:

```bash
./scripts/integration-tidb-secure-test.sh
```

The secure script generates a short-lived local CA plus server/client certificates under `.tmp/`, starts 3 PD nodes, 2 TiKV nodes, and 2 TiDB SQL nodes, verifies `REQUIRE SSL` and `REQUIRE X509` users, and removes the containers and network by default.

The secure HA launcher starts PD, TiKV, and TiDB SQL in phases so slower Docker
runners do not start SQL nodes before TiKV has bootstrapped the cluster. Tune
`DBTOOL_IT_TIDB_SECURE_PD_BOOTSTRAP_DELAY` and
`DBTOOL_IT_TIDB_SECURE_TIKV_BOOTSTRAP_DELAY` if a constrained runner needs a
longer or shorter warm-up window.

Run the secure HA failover drill when you need to prove the two exposed TiDB SQL nodes can be stopped one at a time while the remaining node keeps accepting TLS SQL traffic:

```bash
./scripts/integration-tidb-ha-drill.sh
```

The drill reuses the secure HA profile, writes shared fixture rows through both SQL nodes, stops `tidb-secure-1`, verifies `tidb-secure-2`, restarts node 1, then repeats the same check with `tidb-secure-2` stopped. Passing means each surviving SQL node can still write and read the shared TiKV-backed table, and each restarted SQL node can read rows written during its outage.

Run the secure HA PD drill when you need to prove the local 3-PD quorum can lose
one PD container at a time while both SQL nodes continue serving TLS SQL traffic:

```bash
./scripts/integration-tidb-pd-drill.sh
```

The drill reuses the secure HA profile, creates shared fixture rows, stops
`tidb-secure-pd-1`, `tidb-secure-pd-2`, and `tidb-secure-pd-3` one at a time,
and requires both SQL nodes to keep writing and reading during each outage.
Its dbtool calls use bounded defaults through
`DBTOOL_IT_TIDB_PD_DRILL_REQUEST_TIMEOUT` and
`DBTOOL_IT_TIDB_PD_DRILL_DEADLINE` so a broken quorum fails fast instead of
hanging indefinitely.

Run the secure HA PD leader drill when you need to prove dbtool remains usable
after the current PD leader, not just an arbitrary PD member, is stopped:

```bash
./scripts/integration-tidb-pd-leader-drill.sh
```

The leader drill queries the TLS PD API from a running PD container, maps the
reported `pd-1`/`pd-2`/`pd-3` leader to the matching Compose service, stops that
service, waits for a replacement leader, and then requires both TiDB SQL nodes
to keep accepting TLS writes and reads.

Run the secure HA certificate regeneration drill when you need to prove the
local TLS material can be regenerated and the topology can cold-start again
with the new CA/server/client certificates:

```bash
./scripts/integration-tidb-cert-regeneration-test.sh
```

The drill uses its own default project name and working directory, starts the
secure HA topology with one generated certificate set, verifies TLS SQL writes
and reads, stops the topology, regenerates the certificate set, verifies the
certificate fingerprints changed, then starts the topology again and repeats
the TLS SQL check. It is a cold-restart certificate lifecycle test, not a claim
of online production certificate rotation.

Messaging integration tests use a separate profile so day-to-day database checks stay lighter:

```bash
./scripts/integration-mq-test.sh
```

The messaging script starts Redis, Redpanda (Kafka API), RabbitMQ, and NATS, waits for health checks, runs the live CLI tests with `--features full`, and removes the containers and volumes.

Messaging TLS integration starts RabbitMQ TLS and NATS TLS with a short-lived local CA:

```bash
./scripts/integration-mq-tls-test.sh
```

The TLS script generates certificates under `.tmp/`, validates `amqps://` queue
produce/detail/consume, validates `nats+tls://` publish/subscribe plus
JetStream topics/detail/lag, and passes the local CA with the `tls-ca` DSN
parameter.

Run the same suite with the optional native Kafka backend:

```bash
./scripts/integration-mq-native-test.sh
```

That script uses `--no-default-features --features full-native`, so Kafka commands go through librdkafka while the other message backends remain unchanged.

Search and time-series integration tests use the observability profile:

```bash
./scripts/integration-observability-test.sh
```

The observability script starts OpenSearch, an OpenSearch-compatible HTTPS
harness, and Prometheus, waits for health checks, runs live CLI tests for
`search` and `ts`, and removes the containers and volumes. The HTTPS harness
is built from [docker/search-tls/Dockerfile](../docker/search-tls/Dockerfile),
loads seed documents from
[testdata/search-tls-seed.ndjson](../testdata/search-tls-seed.ndjson), uses a
short-lived local CA under `.tmp/`, and validates the `opensearch+https://` path
with the `tls-ca` DSN parameter.

## Custom Names And Ports

Every service name, database name, and host port can be overridden with environment variables:

```bash
DBTOOL_IT_PROJECT=my-dbtool-run \
DBTOOL_IT_POSTGRES_DB=my_pg \
DBTOOL_IT_MYSQL_DB=my_mysql \
DBTOOL_IT_MONGO_DB=my_mongo \
DBTOOL_IT_POSTGRES_PORT=25432 \
DBTOOL_IT_MYSQL_PORT=23306 \
DBTOOL_IT_REDIS_PORT=26379 \
DBTOOL_IT_MONGO_PORT=27018 \
./scripts/integration-test.sh
```

Fixture-image service settings use separate defaults so they can run without
colliding with the base service suite:

```bash
DBTOOL_IT_PROJECT=my-dbtool-fixture-images \
DBTOOL_IT_POSTGRES_FIXTURE_PORT=26432 \
DBTOOL_IT_MYSQL_FIXTURE_PORT=24306 \
DBTOOL_IT_REDIS_FIXTURE_PORT=27379 \
DBTOOL_IT_MONGO_FIXTURE_PORT=28017 \
./scripts/integration-fixture-images-test.sh
```

Compatible service settings follow the same pattern:

```bash
DBTOOL_IT_PROJECT=my-dbtool-compat-run \
DBTOOL_IT_MARIADB_DB=my_mariadb \
DBTOOL_IT_MARIADB_PORT=33306 \
DBTOOL_IT_VALKEY_PORT=36379 \
DBTOOL_IT_KEYDB_PORT=36380 \
DBTOOL_IT_DRAGONFLY_PORT=36381 \
DBTOOL_IT_COMPAT_EXTRA=1 \
./scripts/integration-compat-test.sh
```

PostgreSQL-family compatible service settings can be overridden separately:

```bash
DBTOOL_IT_PROJECT=my-dbtool-pg-compat-run \
DBTOOL_IT_COCKROACH_PORT=36257 \
DBTOOL_IT_COCKROACH_HTTP_PORT=38080 \
DBTOOL_IT_TIMESCALE_DB=my_timescale \
DBTOOL_IT_TIMESCALE_PORT=35432 \
./scripts/integration-pg-compat-test.sh
```

SQL Server settings can be overridden independently:

```bash
DBTOOL_IT_PROJECT=my-dbtool-sqlserver-run \
DBTOOL_IT_SQLSERVER_PORT=31433 \
DBTOOL_IT_SQLSERVER_PASSWORD='My_Strong_SQLServer_123!' \
./scripts/integration-sqlserver-test.sh
```

Cassandra settings can be overridden independently:

```bash
DBTOOL_IT_PROJECT=my-dbtool-cassandra-run \
DBTOOL_IT_CASSANDRA_KEYSPACE=my_cassandra_ks \
DBTOOL_IT_CASSANDRA_PORT=39042 \
./scripts/integration-cassandra-test.sh
```

TiDB service settings follow the same pattern:

```bash
DBTOOL_IT_PROJECT=my-dbtool-tidb-run \
DBTOOL_IT_TIDB_DB=my_tidb \
DBTOOL_IT_TIDB_PORT=34000 \
DBTOOL_IT_TIDB_STATUS_PORT=31080 \
./scripts/integration-tidb-test.sh
```

TiDB secure HA service settings can be overridden independently:

```bash
DBTOOL_IT_PROJECT=my-dbtool-tidb-secure-run \
DBTOOL_IT_TIDB_SECURE_DB=my_tidb_secure \
DBTOOL_IT_TIDB_SECURE_PORT_1=34100 \
DBTOOL_IT_TIDB_SECURE_PORT_2=34101 \
DBTOOL_IT_TIDB_SECURE_STATUS_PORT_1=31100 \
DBTOOL_IT_TIDB_SECURE_STATUS_PORT_2=31101 \
./scripts/integration-tidb-secure-test.sh
```

Use the same variables for the secure HA drill:

```bash
DBTOOL_IT_PROJECT=my-dbtool-tidb-ha-drill \
DBTOOL_IT_TIDB_SECURE_DB=my_tidb_secure \
DBTOOL_IT_TIDB_SECURE_PORT_1=44100 \
DBTOOL_IT_TIDB_SECURE_PORT_2=44101 \
./scripts/integration-tidb-ha-drill.sh
```

Use the same secure HA variables for the PD drill:

```bash
DBTOOL_IT_PROJECT=my-dbtool-tidb-pd-drill \
DBTOOL_IT_TIDB_SECURE_DB=my_tidb_secure \
DBTOOL_IT_TIDB_SECURE_PORT_1=45100 \
DBTOOL_IT_TIDB_SECURE_PORT_2=45101 \
./scripts/integration-tidb-pd-drill.sh
```

Use the same secure HA variables for the PD leader drill:

```bash
DBTOOL_IT_PROJECT=my-dbtool-tidb-pd-leader-drill \
DBTOOL_IT_TIDB_SECURE_DB=my_tidb_secure \
DBTOOL_IT_TIDB_SECURE_PORT_1=45200 \
DBTOOL_IT_TIDB_SECURE_PORT_2=45201 \
./scripts/integration-tidb-pd-leader-drill.sh
```

Use isolated secure HA variables for the certificate regeneration drill:

```bash
DBTOOL_IT_PROJECT=my-dbtool-tidb-cert-drill \
DBTOOL_IT_TIDB_SECURE_DIR=.tmp/my-dbtool-tidb-cert-drill \
DBTOOL_IT_TIDB_SECURE_DB=my_tidb_secure \
DBTOOL_IT_TIDB_SECURE_PORT_1=46100 \
DBTOOL_IT_TIDB_SECURE_PORT_2=46101 \
./scripts/integration-tidb-cert-regeneration-test.sh
```

Add TiProxy-specific host ports when running the secure HA topology through the
local TiProxy profile:

```bash
DBTOOL_IT_PROJECT=my-dbtool-tidb-tiproxy \
DBTOOL_IT_TIDB_SECURE_DB=my_tidb_secure \
DBTOOL_IT_TIDB_SECURE_PORT_1=54100 \
DBTOOL_IT_TIDB_SECURE_PORT_2=54101 \
DBTOOL_IT_TIDB_TIPROXY_PORT=54200 \
DBTOOL_IT_TIDB_TIPROXY_STATUS_PORT=51200 \
./scripts/integration-tidb-tiproxy-test.sh
```

Messaging service settings follow the same pattern:

```bash
DBTOOL_IT_PROJECT=my-dbtool-mq-run \
DBTOOL_IT_KAFKA_PORT=29092 \
DBTOOL_IT_AMQP_USER=my_user \
DBTOOL_IT_AMQP_PASSWORD=my_pass \
DBTOOL_IT_AMQP_VHOST=my_vhost \
DBTOOL_IT_AMQP_PORT=25672 \
DBTOOL_IT_AMQP_MANAGEMENT_PORT=25673 \
DBTOOL_IT_REDIS_PORT=26379 \
DBTOOL_IT_NATS_PORT=24222 \
./scripts/integration-mq-test.sh
```

Messaging TLS settings can be overridden independently:

```bash
DBTOOL_IT_PROJECT=my-dbtool-mq-tls-run \
DBTOOL_IT_AMQP_USER=my_user \
DBTOOL_IT_AMQP_PASSWORD=my_pass \
DBTOOL_IT_AMQP_VHOST=my_vhost \
DBTOOL_IT_AMQPS_PORT=45671 \
DBTOOL_IT_NATS_TLS_PORT=44222 \
DBTOOL_IT_NATS_TLS_MONITOR_PORT=48222 \
DBTOOL_IT_MQ_TLS_DIR=.tmp/my-dbtool-mq-tls \
./scripts/integration-mq-tls-test.sh
```

Observability service settings follow the same pattern:

```bash
DBTOOL_IT_PROJECT=my-dbtool-observability-run \
DBTOOL_IT_OPENSEARCH_PORT=29200 \
DBTOOL_IT_OPENSEARCH_TLS_PORT=29201 \
DBTOOL_IT_PROMETHEUS_PORT=29090 \
./scripts/integration-observability-test.sh
```

Set `DBTOOL_IT_KEEP_SERVICES=1` to leave containers running for manual inspection, then clean up with:

```bash
./scripts/integration-down.sh
```

For a custom project name, pass the same value used to start the services:

```bash
DBTOOL_IT_PROJECT=my-dbtool-run ./scripts/integration-down.sh
```

## CI Profiles

Daily push and pull request CI run service-free verification through
`./scripts/verify.sh` and validate base, compatibility, PostgreSQL-family
compatibility, SQL Server, Cassandra, TiDB, fixture-image, messaging,
messaging TLS, and observability Docker Compose configs without starting
containers through:

```bash
./scripts/validate-compose-configs.sh
```

Live integration jobs are opt-in from the GitHub Actions **Run workflow** button:

- `run_live_services` runs `./scripts/integration-test.sh` for Postgres, MySQL, Redis, and MongoDB.
- `run_live_compat` can run `./scripts/integration-compat-test.sh` for MariaDB and Valkey, with `DBTOOL_IT_COMPAT_EXTRA=1` for KeyDB and Dragonfly.
- `run_live_pg_compat` runs `./scripts/integration-pg-compat-test.sh` for CockroachDB and TimescaleDB.
- `run_live_sqlserver` runs `./scripts/integration-sqlserver-test.sh` for SQL Server/TDS coverage.
- `run_live_cassandra` runs `./scripts/integration-cassandra-test.sh` for Cassandra/CQL coverage.
- `run_live_tidb` runs `./scripts/integration-tidb-test.sh` for TiDB through a local PD/TiKV/TiDB topology.
- `run_live_tidb_secure` runs `./scripts/integration-tidb-secure-test.sh` for TiDB auth/TLS/HA coverage.
- `run_live_tidb_ha_drill` runs `./scripts/integration-tidb-ha-drill.sh` for SQL-node failover on the secure HA topology.
- `run_live_tidb_tiproxy` runs `./scripts/integration-tidb-tiproxy-test.sh` for TiProxy TLS entrypoint and new-connection routing coverage on the secure HA topology.
- `run_live_messaging` runs `./scripts/integration-mq-test.sh` for Redis Streams/Pub/Sub, Redpanda, RabbitMQ, and NATS.
- `run_live_messaging_tls` runs `./scripts/integration-mq-tls-test.sh` for RabbitMQ TLS and NATS TLS.
- `run_live_messaging_native` can run `./scripts/integration-mq-native-test.sh` when native Kafka coverage is desired.
- `run_live_observability` runs `./scripts/integration-observability-test.sh` for OpenSearch and Prometheus.

The CI jobs use separate Compose project names and host ports so the database,
fixture-image, compatibility, TiDB, messaging, and observability suites can run
in parallel.

## Resource Limits

The compose file applies conservative defaults:

- Postgres: `0.50` CPU, `512m` memory
- MySQL: `0.75` CPU, `768m` memory
- Redis: `0.25` CPU, `256m` memory plus `128mb` Redis maxmemory
- MongoDB: `0.50` CPU, `512m` memory
- PostgreSQL fixture image: `0.50` CPU, `512m` memory
- MySQL fixture image: `0.75` CPU, `768m` memory
- Redis fixture image: `0.25` CPU, `256m` memory plus `128mb` Redis maxmemory
- MongoDB fixture image: `0.50` CPU, `512m` memory
- MariaDB compat: `0.50` CPU, `512m` memory
- CockroachDB pg-compat: `0.50` CPU, `512m` memory
- TimescaleDB pg-compat: `0.50` CPU, `512m` memory
- SQL Server: `1.00` CPU, `2g` memory
- Cassandra: `1.00` CPU, `2g` memory, JVM heap `512M`
- Valkey compat: `0.25` CPU, `256m` memory plus `128mb` maxmemory
- KeyDB compat-extra: `0.25` CPU, `256m` memory plus `128mb` maxmemory
- Dragonfly compat-extra: `0.25` CPU, `384m` memory plus `256mb` maxmemory
- TiDB PD: `0.25` CPU, `256m` memory
- TiDB TiKV: `0.75` CPU, `1g` memory
- TiDB SQL server: `0.50` CPU, `512m` memory
- TiDB secure HA PD nodes: `0.25` CPU, `256m` memory each
- TiDB secure HA TiKV nodes: `0.50` CPU, `1g` memory each
- TiDB secure HA SQL nodes: `0.50` CPU, `512m` memory each
- TiDB TiProxy: `0.25` CPU, `256m` memory
- Redpanda/Kafka API: `0.75` CPU, `1g` memory, broker memory `512M`
- RabbitMQ/AMQP: `0.50` CPU, `512m` memory
- NATS: `0.25` CPU, `256m` memory
- RabbitMQ TLS/AMQPS: `0.50` CPU, `512m` memory
- NATS TLS: `0.25` CPU, `256m` memory
- OpenSearch: `1.00` CPU, `1g` memory, JVM heap `256m`
- OpenSearch-compatible HTTPS harness: `0.25` CPU, `128m` memory
- Prometheus: `0.25` CPU, `256m` memory

Override with variables such as `DBTOOL_IT_MYSQL_MEMORY=1g` or `DBTOOL_IT_REDIS_MAXMEMORY=64mb`.

The base service suite and fixture-image suite are each capped at roughly 2 GiB
of container memory, the PostgreSQL-family compatibility suite is capped at
roughly 1 GiB, the SQL Server suite is capped at roughly 2 GiB, the messaging
suite is capped at roughly 2 GiB, the messaging TLS suite is capped at roughly
768 MiB, the observability suite is capped at roughly 1.4 GiB, the TiDB suite
is capped at roughly 1.75 GiB, the TiDB secure HA suite, SQL-node failover
drill, PD drill, or certificate regeneration drill is capped at roughly 3.75
GiB, and the TiProxy drill is capped at roughly 4 GiB. If several suites are
kept running at the same time, reserve Docker memory for their combined limits
plus headroom. Redpanda, OpenSearch, SQL Server, CockroachDB, RabbitMQ, and
TiKV are the largest single services; increase `DBTOOL_IT_KAFKA_MEMORY` with
`DBTOOL_IT_KAFKA_BROKER_MEMORY`, `DBTOOL_IT_OPENSEARCH_MEMORY`,
`DBTOOL_IT_SQLSERVER_MEMORY`, `DBTOOL_IT_COCKROACH_MEMORY`,
`DBTOOL_IT_AMQP_MEMORY`, `DBTOOL_IT_TIDB_TIKV_MEMORY`,
`DBTOOL_IT_TIDB_SECURE_TIKV_MEMORY`, or `DBTOOL_IT_TIDB_TIPROXY_MEMORY` if one
fails to become healthy under local load.

## Live Test Scope

The live tests cover:

- Postgres and MySQL ping, destructive SQL confirmation, insert/query/schema/drop.
- Docker-backed flow-control smoke for Postgres slow-query timeout, MySQL
  rate/admission flags, SQL result limits, Redis scan limits, MongoDB find
  limits, and disposable fixture cleanup.
- Reusable base fixture loading for PostgreSQL, MySQL, Redis, and MongoDB from
  SQL, command, and NDJSON files under `testdata/`.
- Dockerfile-backed fixture images for PostgreSQL, MySQL, Redis, and MongoDB,
  with dbtool readback of rows, keys, and documents preloaded during service
  initialization.
- MariaDB/TiDB alias DSNs against the MySQL protocol adapter, typed MySQL values, and result limiting.
- CockroachDB/TimescaleDB alias DSNs against the PostgreSQL protocol adapter, typed Postgres-family values, result limiting, table listing, schema inspection, and SQL lifecycle.
- Redis ping, set/get/scan/raw typed output, TTL, scan truncation, multi-key delete, blocked destructive raw command, and blocked mutating raw command without `--allow-write`.
- Valkey/KeyDB/Dragonfly alias DSNs against the Redis protocol adapter.
- Real MariaDB compatibility through `mariadb://` against a MariaDB container.
- Real TiDB compatibility through `tidb://` against a PD/TiKV/TiDB topology, including database creation, typed values, result limiting, destructive confirmation, table listing, insert/query/schema/drop, and schema-qualified table names.
- TiDB secure HA through `tidb://` with 3 PD nodes, 2 TiKV nodes, 2 TiDB SQL nodes, component TLS, SQL TLS, TLS-required users, client-certificate-required users, insecure-login rejection, and SQL lifecycle coverage through both SQL nodes.
- TiDB secure HA failover drill with one SQL node stopped at a time, writes through the surviving SQL node, and post-restart reads of rows created during each outage.
- TiDB secure HA PD drill with one PD node stopped at a time, writes through both SQL nodes during each PD outage, and post-restart SQL reachability checks.
- TiDB secure HA PD leader drill with TLS PD API leader discovery, targeted
  leader stop, replacement-leader detection, and SQL continuity checks through
  both SQL nodes.
- TiDB secure HA certificate regeneration drill with changed CA/server/client
  certificate fingerprints across a cold restart and TLS SQL verification
  through both generations.
- TiDB TiProxy drill with a TLS proxy entrypoint, a `REQUIRE SSL` user, and new proxy connections that keep writing/reading while each TiDB SQL node is stopped in turn.
- Real Valkey compatibility through `valkey://`; optional KeyDB and Dragonfly compatibility through `DBTOOL_IT_COMPAT_EXTRA=1`.
- MongoDB ping, insert/find/update/aggregate/delete.
- Redis Streams produce, topics, detail, consume; Redis Pub/Sub subscribe/publish round trip.
- Kafka ping through metadata, produce, topics, detail/watermarks, and consume.
- Optional native Kafka/librdkafka coverage through the same Redpanda test data.
- RabbitMQ queue publish, passive detail/message count, acked consume, write guard, and HTTP management queue listing/detail/lag.
- NATS live subscribe/publish round trip, JetStream topics/detail/lag, and write guard.
- AMQPS queue publish/detail/consume and NATS TLS publish/subscribe plus JetStream topics/detail/lag through local CA-backed TLS services.
- OpenSearch ping, write guard, single-document indexing, search, and index listing over plain HTTP plus TLS transport through `opensearch+https://`.
- OpenSearch TLS harness fixture loading by searching the seeded `dbtool_seed` index from the Dockerfile-built image.
- Prometheus ping, metric listing, and range query through `ts`.

Core NATS and Redis Pub/Sub do not expose durable subject/channel listing, and AMQP 0.9.1 does not expose queue listing without RabbitMQ management APIs; use an explicit `rabbitmq+http://` management DSN for RabbitMQ queue discovery.
