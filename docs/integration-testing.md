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

Compatible database integration tests use a separate profile:

```bash
./scripts/integration-compat-test.sh
```

The compatibility script starts MariaDB and Valkey by default, waits for health checks, runs the compatibility live tests, and removes the containers and volumes. Run the optional Redis-compatible matrix as well:

```bash
DBTOOL_IT_COMPAT_EXTRA=1 ./scripts/integration-compat-test.sh
```

That extra mode adds KeyDB and Dragonfly. KeyDB is pinned to `linux/amd64` by default because its published Alpine image is amd64-only on Apple Silicon.

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

Messaging integration tests use a separate profile so day-to-day database checks stay lighter:

```bash
./scripts/integration-mq-test.sh
```

The messaging script starts Redis, Redpanda (Kafka API), RabbitMQ, and NATS, waits for health checks, runs the live CLI tests with `--features full`, and removes the containers and volumes.

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
uses a short-lived local CA under `.tmp/` and validates the `opensearch+https://`
path with the `tls-ca` DSN parameter.

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

Daily push and pull request CI run service-free verification through `./scripts/verify.sh` and validate base, compatibility, TiDB, messaging, and observability Docker Compose configs without starting containers.

Live integration jobs are opt-in from the GitHub Actions **Run workflow** button:

- `run_live_services` runs `./scripts/integration-test.sh` for Postgres, MySQL, Redis, and MongoDB.
- `run_live_compat` can run `./scripts/integration-compat-test.sh` for MariaDB and Valkey, with `DBTOOL_IT_COMPAT_EXTRA=1` for KeyDB and Dragonfly.
- `run_live_tidb` runs `./scripts/integration-tidb-test.sh` for TiDB through a local PD/TiKV/TiDB topology.
- `run_live_tidb_secure` runs `./scripts/integration-tidb-secure-test.sh` for TiDB auth/TLS/HA coverage.
- `run_live_messaging` runs `./scripts/integration-mq-test.sh` for Redis Streams/Pub/Sub, Redpanda, RabbitMQ, and NATS.
- `run_live_messaging_native` can run `./scripts/integration-mq-native-test.sh` when native Kafka coverage is desired.
- `run_live_observability` runs `./scripts/integration-observability-test.sh` for OpenSearch and Prometheus.

The CI jobs use separate Compose project names and host ports so the database, compatibility, TiDB, messaging, and observability suites can run in parallel.

## Resource Limits

The compose file applies conservative defaults:

- Postgres: `0.50` CPU, `512m` memory
- MySQL: `0.75` CPU, `768m` memory
- Redis: `0.25` CPU, `256m` memory plus `128mb` Redis maxmemory
- MongoDB: `0.50` CPU, `512m` memory
- MariaDB compat: `0.50` CPU, `512m` memory
- Valkey compat: `0.25` CPU, `256m` memory plus `128mb` maxmemory
- KeyDB compat-extra: `0.25` CPU, `256m` memory plus `128mb` maxmemory
- Dragonfly compat-extra: `0.25` CPU, `384m` memory plus `256mb` maxmemory
- TiDB PD: `0.25` CPU, `256m` memory
- TiDB TiKV: `0.75` CPU, `1g` memory
- TiDB SQL server: `0.50` CPU, `512m` memory
- TiDB secure HA PD nodes: `0.25` CPU, `256m` memory each
- TiDB secure HA TiKV nodes: `0.50` CPU, `1g` memory each
- TiDB secure HA SQL nodes: `0.50` CPU, `512m` memory each
- Redpanda/Kafka API: `0.75` CPU, `1g` memory, broker memory `512M`
- RabbitMQ/AMQP: `0.50` CPU, `512m` memory
- NATS: `0.25` CPU, `256m` memory
- OpenSearch: `1.00` CPU, `1g` memory, JVM heap `256m`
- OpenSearch-compatible HTTPS harness: `0.25` CPU, `128m` memory
- Prometheus: `0.25` CPU, `256m` memory

Override with variables such as `DBTOOL_IT_MYSQL_MEMORY=1g` or `DBTOOL_IT_REDIS_MAXMEMORY=64mb`.

The base service suite is capped at roughly 2 GiB of container memory, the messaging suite is capped at roughly 2 GiB, the observability suite is capped at roughly 1.4 GiB, the TiDB suite is capped at roughly 1.75 GiB, and the TiDB secure HA suite is capped at roughly 3.75 GiB. If several suites are kept running at the same time, reserve Docker memory for their combined limits plus headroom. Redpanda, OpenSearch, and TiKV are the largest single services; increase `DBTOOL_IT_KAFKA_MEMORY` with `DBTOOL_IT_KAFKA_BROKER_MEMORY`, `DBTOOL_IT_OPENSEARCH_MEMORY`, `DBTOOL_IT_TIDB_TIKV_MEMORY`, or `DBTOOL_IT_TIDB_SECURE_TIKV_MEMORY` if one fails to become healthy under local load.

## Live Test Scope

The live tests cover:

- Postgres and MySQL ping, destructive SQL confirmation, insert/query/schema/drop.
- MariaDB/TiDB alias DSNs against the MySQL protocol adapter, typed MySQL values, and result limiting.
- Redis ping, set/get/scan/raw typed output, TTL, scan truncation, multi-key delete, blocked destructive raw command, and blocked mutating raw command without `--allow-write`.
- Valkey/KeyDB/Dragonfly alias DSNs against the Redis protocol adapter.
- Real MariaDB compatibility through `mariadb://` against a MariaDB container.
- Real TiDB compatibility through `tidb://` against a PD/TiKV/TiDB topology, including database creation, typed values, result limiting, destructive confirmation, table listing, insert/query/schema/drop, and schema-qualified table names.
- TiDB secure HA through `tidb://` with 3 PD nodes, 2 TiKV nodes, 2 TiDB SQL nodes, component TLS, SQL TLS, TLS-required users, client-certificate-required users, insecure-login rejection, and SQL lifecycle coverage through both SQL nodes.
- Real Valkey compatibility through `valkey://`; optional KeyDB and Dragonfly compatibility through `DBTOOL_IT_COMPAT_EXTRA=1`.
- MongoDB ping, insert/find/update/aggregate/delete.
- Redis Streams produce, topics, detail, consume; Redis Pub/Sub subscribe/publish round trip.
- Kafka ping through metadata, produce, topics, detail/watermarks, and consume.
- Optional native Kafka/librdkafka coverage through the same Redpanda test data.
- RabbitMQ queue publish, passive detail/message count, acked consume, write guard, and HTTP management queue listing/detail/lag.
- NATS live subscribe/publish round trip, JetStream topics/detail/lag, and write guard.
- OpenSearch ping, write guard, single-document indexing, search, and index listing over plain HTTP plus TLS transport through `opensearch+https://`.
- Prometheus ping, metric listing, and range query through `ts`.

Core NATS and Redis Pub/Sub do not expose durable subject/channel listing, and AMQP 0.9.1 does not expose queue listing without RabbitMQ management APIs; use an explicit `rabbitmq+http://` management DSN for RabbitMQ queue discovery.
