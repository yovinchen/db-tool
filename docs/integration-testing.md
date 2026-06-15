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

Override with variables such as `DBTOOL_IT_MYSQL_MEMORY=1g` or `DBTOOL_IT_REDIS_MAXMEMORY=64mb`.

## Live Test Scope

The live tests cover:

- Postgres and MySQL ping, destructive SQL confirmation, insert/query/schema/drop.
- Redis ping, set/get/scan/raw typed output, and blocked destructive raw command.
- MongoDB ping, insert/find/update/aggregate/delete.

Messaging backends are not live-tested yet because Kafka/AMQP/NATS are explicitly staged as P5 work.
