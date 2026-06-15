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

Messaging integration tests use a separate profile so day-to-day database checks stay lighter:

```bash
./scripts/integration-mq-test.sh
```

The messaging script starts Redis, Redpanda (Kafka API), RabbitMQ, and NATS, waits for health checks, runs the live CLI tests with `--features full`, and removes the containers and volumes.

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

Set `DBTOOL_IT_KEEP_SERVICES=1` to leave containers running for manual inspection, then clean up with:

```bash
./scripts/integration-down.sh
```

For a custom project name, pass the same value used to start the services:

```bash
DBTOOL_IT_PROJECT=my-dbtool-run ./scripts/integration-down.sh
```

## CI Profiles

Daily push and pull request CI run service-free verification through `./scripts/verify.sh` and validate both base and messaging Docker Compose configs without starting containers.

Live integration jobs are opt-in from the GitHub Actions **Run workflow** button:

- `run_live_services` runs `./scripts/integration-test.sh` for Postgres, MySQL, Redis, and MongoDB.
- `run_live_messaging` runs `./scripts/integration-mq-test.sh` for Redis Streams/Pub/Sub, Redpanda, RabbitMQ, and NATS.

The CI jobs use separate Compose project names and host ports so the database and messaging suites can run in parallel.

## Resource Limits

The compose file applies conservative defaults:

- Postgres: `0.50` CPU, `512m` memory
- MySQL: `0.75` CPU, `768m` memory
- Redis: `0.25` CPU, `256m` memory plus `128mb` Redis maxmemory
- MongoDB: `0.50` CPU, `512m` memory
- Redpanda/Kafka API: `0.75` CPU, `1g` memory, broker memory `512M`
- RabbitMQ/AMQP: `0.50` CPU, `512m` memory
- NATS: `0.25` CPU, `256m` memory

Override with variables such as `DBTOOL_IT_MYSQL_MEMORY=1g` or `DBTOOL_IT_REDIS_MAXMEMORY=64mb`.

The base service suite is capped at roughly 2 GiB of container memory, and the messaging suite is capped at roughly 2 GiB. If both suites are kept running at the same time, reserve at least 4 GiB of Docker memory and 2 CPUs. Redpanda is the largest single service; increase `DBTOOL_IT_KAFKA_MEMORY` and `DBTOOL_IT_KAFKA_BROKER_MEMORY` together if it fails to become healthy under local load.

## Live Test Scope

The live tests cover:

- Postgres and MySQL ping, destructive SQL confirmation, insert/query/schema/drop.
- MariaDB/TiDB alias DSNs against the MySQL protocol adapter, typed MySQL values, and result limiting.
- Redis ping, set/get/scan/raw typed output, TTL, scan truncation, multi-key delete, blocked destructive raw command, and blocked mutating raw command without `--allow-write`.
- Valkey/KeyDB/Dragonfly alias DSNs against the Redis protocol adapter.
- MongoDB ping, insert/find/update/aggregate/delete.
- Redis Streams produce, topics, detail, consume; Redis Pub/Sub subscribe/publish round trip.
- Kafka ping through metadata, produce, topics, detail/watermarks, and consume.
- RabbitMQ queue publish, passive detail/message count, acked consume, write guard, and HTTP management queue listing/detail/lag.
- NATS live subscribe/publish round trip, JetStream topics/detail/lag, and write guard.

Core NATS and Redis Pub/Sub do not expose durable subject/channel listing, and AMQP 0.9.1 does not expose queue listing without RabbitMQ management APIs; use an explicit `rabbitmq+http://` management DSN for RabbitMQ queue discovery.
