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
DBTOOL_IT_REDIS_PORT=26379 \
DBTOOL_IT_NATS_PORT=24222 \
./scripts/integration-mq-test.sh
```

Set `DBTOOL_IT_KEEP_SERVICES=1` to leave containers running for manual inspection, then clean up with:

```bash
./scripts/integration-down.sh
```

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

## Live Test Scope

The live tests cover:

- Postgres and MySQL ping, destructive SQL confirmation, insert/query/schema/drop.
- Redis ping, set/get/scan/raw typed output, and blocked destructive raw command.
- MongoDB ping, insert/find/update/aggregate/delete.
- Redis Streams produce, topics, detail, consume; Redis Pub/Sub subscribe/publish round trip.
- Kafka ping through metadata, produce, topics, detail/watermarks, and consume.
- RabbitMQ queue publish, passive detail/message count, acked consume, and write guard.
- NATS live subscribe/publish round trip and write guard.

Core NATS and Redis Pub/Sub do not expose durable subject/channel listing, and AMQP 0.9.1 does not expose queue listing without RabbitMQ management APIs; those broader admin paths remain future work.
