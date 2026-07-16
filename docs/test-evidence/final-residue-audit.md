# Final Docker Residue Audit

Run at (UTC): 2026-07-16T13:51:26Z

Result: PASS_WITH_DECLARED_HARNESS_NAMESPACE

This read-only audit ran after the exact CRUD/mutation campaigns and the final
workspace/package gates. It inspected only the disposable `dbtool_it_*` test
namespace. Credentials were read inside their containers and were not printed.

| Product | Audited resource | Final result |
| --- | --- | --- |
| PostgreSQL 16 | public tables matching `^dbtool_it_` | 0 |
| MySQL 8 | current-database tables matching `^dbtool_it_` | 0 |
| Cassandra 5 | `dbtool_it_cassandra` harness keyspace | keyspace retained intentionally; 0 tables |
| Redis 7 | keys matching `dbtool_it_*` | 0 |
| MongoDB 7 | collections matching `dbtool_it_*` | 0 |
| OpenSearch 2.17.1 | indices matching `dbtool-it-*` | 0 |
| Elasticsearch 8.15.5 | indices matching `dbtool-it-*` | 0 |
| Prometheus 2.55.1 | series whose metric matches `dbtool_it_.*` | 0 |
| Kafka API / Redpanda | topics matching `dbtool_it_*` | 0 after public confirmed cleanup |
| RabbitMQ 3.13 | queues in test vhost | 0 |
| NATS 2.10 JetStream | streams / consumers / messages | 0 / 0 / 0 |

The first Kafka inventory found one historical test topic,
`dbtool_it_kafka_topic_16721_1784144037118`, which earlier scoped lifecycle
tests had correctly left untouched because they did not create it. The final
campaign cleanup used `dbtool mq delete --kind kafka-topic` with the normal
target-bound confirmation token, then `rpk topic list` returned no test topic.

The retained Cassandra keyspace is the configured integration harness target,
not a CRUD residue. Its table catalog was empty; per-run IF-T78 keyspaces and
all tables were already absent. SQLite used temporary database files and its
tests verified table absence before removing those files.

Valkey, KeyDB, Dragonfly, MariaDB, TiDB, CockroachDB, TimescaleDB, TLS profiles,
and fixture-image profiles were not restarted solely for this final inventory;
their product-specific cleanup evidence remains in their existing evidence
files. SQL Server, Db2, Redshift, real ScyllaDB, and vendor Kafka endpoints keep
their declared external/runtime boundaries.
