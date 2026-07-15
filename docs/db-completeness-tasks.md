# Database Capability Completeness Tasks

This is the execution ledger for real dbtool backend verification. It is
separate from `docs/tasks.md`: implementation, a runnable harness, and a real
successful service run are intentionally different states.

## Scope And Safety

- Only disposable resources prefixed with `dbtool_it_` are created or mutated.
- "All contents" means every row/document/key/message created by the fixture
  named in an evidence file. The suite never performs an unbounded dump of an
  arbitrary user or production database.
- Credentials, raw DSNs, certificates, and full container logs are not committed.
- A skipped external endpoint or a missing runtime is not a pass.
- Heavy profiles run serially on the current 2 CPU / 8 GiB Docker allocation.

## Status Model

`NOT_RUN -> HARNESS_READY -> LIVE_PASS -> COMPLETE`

- `HARNESS_READY`: code and an invocation path exist, but the real product has
  not passed in the current validation campaign.
- `LIVE_PASS`: the real product completed its capability-family checklist and
  has a committed evidence file.
- `COMPLETE`: `LIVE_PASS` plus documentation and the manifest are synchronized.
- `BLOCKED`: a concrete architecture/runtime prerequisite is unavailable.
- `EXTERNAL`: a real endpoint and credentials must be supplied.
- `PARTIAL`: alias routing or a compatible harness passed, not the named product.

## Capability-Family Checklists

| Family | Required operations for a complete result |
| --- | --- |
| SQL | ping/caps, schemas/tables, CREATE, INSERT, SELECT, UPDATE, targeted DELETE, schema/index metadata, typed values, limit/truncation, bound-parameter import with whole-batch rollback where advertised, write guard/confirm, cleanup |
| CQL | ping/caps, keyspaces/tables, CREATE, INSERT, SELECT, UPDATE, targeted DELETE, schema/primary-key metadata, typed values, limit, write guard, cleanup |
| KV/cache | ping/caps, SET, GET, overwrite, TTL, exact N/N+1 SCAN, multi-page/error propagation, raw read, raw write guard, DELETE, post-delete read, cleanup |
| Document | ping/caps with explicit one/many operations, collections, INSERT, FIND, single UPDATE/DELETE, confirmed multi UPDATE/DELETE with exact counts, aggregate, empty result verification, cleanup |
| Search | ping/caps, write guard, index document, list indices, search/readback; update/delete are recorded `UNSUPPORTED` until the public capability exposes them |
| Time series | ping/caps, write guard, remote write, measurement list, range readback; update/delete are not applicable to the Prometheus model |
| Messaging | ping/caps, write guard, produce/publish, bounded consume, list/detail/lag where the protocol supports them, timeout/ack behavior, cleanup where the public API supports it |

## Execution Task Table

The machine-readable source for this table is
`testdata/db-completeness.manifest`. `Commit` identifies the campaign commit
that synchronized the completed evidence for that task; earlier implementation
and test-hardening commits remain listed inside each evidence file.

| Task | Family | Product / scheme | Environment | Harness | Live result | Evidence | Commit / boundary |
| --- | --- | --- | --- | --- | --- | --- | --- |
| DB-SQLITE-001 | SQL | SQLite `sqlite:` | service-free | Ready | COMPLETE | `docs/test-evidence/sqlite.md` | `d6bd18b`, IF-T58 atomic import refresh |
| DB-POSTGRES-001 | SQL | PostgreSQL `postgres://` | Docker base | Ready | COMPLETE | `docs/test-evidence/postgresql.md` | `fe7cfb9`, IF-T58 atomic import refresh |
| DB-MYSQL-001 | SQL | MySQL `mysql://` | Docker base | Ready | COMPLETE | `docs/test-evidence/mysql.md` | `fe7cfb9`, IF-T58 atomic/MyISAM refresh |
| DB-MARIADB-001 | SQL | MariaDB `mariadb://` | Docker compat | Ready | COMPLETE | `docs/test-evidence/mariadb.md` | `6f423fb` |
| DB-TIDB-001 | SQL | TiDB `tidb://` | Docker tidb | Ready | COMPLETE | `docs/test-evidence/tidb.md` | `4c2faa8`; basic, secure, transfer, TiProxy, SQL/PD resilience, TiKV boundary, and cold cert regeneration passed |
| DB-COCKROACH-001 | SQL | CockroachDB `cockroach://` | Docker pg-compat | Ready | COMPLETE | `docs/test-evidence/cockroachdb.md` | `a776d20`; single-node insecure SQL surface |
| DB-TIMESCALE-001 | SQL | TimescaleDB `timescale://` | Docker pg-compat | Ready | COMPLETE | `docs/test-evidence/timescaledb.md` | `c2a77fe`; generic SQL surface only |
| DB-SQLSERVER-001 | SQL | SQL Server `sqlserver://` | Docker sqlserver | Ready | BLOCKED | - | local host is arm64; image gate requires x86_64 |
| DB-REDSHIFT-001 | SQL | Redshift `redshift://` | external | Ready | EXTERNAL | - | `DBTOOL_IT_REDSHIFT_DSN` is not supplied |
| DB-CASSANDRA-001 | CQL | Cassandra `cassandra://` | Docker cassandra | Ready | COMPLETE | `docs/test-evidence/cassandra.md` | `f3712b3`; SQL/CQL CRUD, types, full fixture, safety, limit, metadata, and cleanup passed |
| DB-SCYLLA-001 | CQL | ScyllaDB `scylla://` | compatible alias only | Ready | PARTIAL | - | no real ScyllaDB product profile |
| DB-DB2-001 | SQL/Db2 | IBM Db2 `db2://` | Docker db2 + host ODBC | Ready | BLOCKED | - | IBM Db2 ODBC driver is not registered on the host |
| DB-REDIS-001 | KV/cache | Redis `redis://` | Docker base | Ready | COMPLETE | `docs/test-evidence/redis.md` | `1ceffc8`, IF-T57; atomic NX+TTL and exact multi-page SCAN refreshed |
| DB-VALKEY-001 | KV/cache | Valkey `valkey://` | Docker compat | Ready | COMPLETE | `docs/test-evidence/valkey.md` | `1ceffc8`; atomic NX+TTL refreshed |
| DB-KEYDB-001 | KV/cache | KeyDB `keydb://` | Docker compat-extra | Ready | COMPLETE | `docs/test-evidence/keydb.md` | `1ceffc8`; atomic NX+TTL refreshed |
| DB-DRAGONFLY-001 | KV/cache | Dragonfly `dragonfly://` | Docker compat-extra | Ready | COMPLETE | `docs/test-evidence/dragonfly.md` | `1ceffc8`; atomic NX+TTL refreshed |
| DB-MONGO-001 | Document | MongoDB `mongodb://` | Docker base | Ready | COMPLETE | `docs/test-evidence/mongodb.md` | `fe7cfb9`, IF-T61; explicit one/many counts, confirmation binding and zero collection residual |
| DB-OPENSEARCH-001 | Search | OpenSearch `opensearch://` | Docker observability | Ready | COMPLETE | `docs/test-evidence/opensearch.md` | IF-T45; auto/stable ID writes, get/update/delete, aggregation, confirmed delete-index, zero residual test indices |
| DB-OPENSEARCH-TLS-001 | Search | OpenSearch security HTTPS | Docker opensearch-security | Ready | COMPLETE | `docs/test-evidence/opensearch-security.md` | `b9dd9fd`; real plugin, CA/auth positive and negative checks |
| DB-ELASTICSEARCH-001 | Search | Elasticsearch `elasticsearch://` | Docker elasticsearch | Ready | COMPLETE | `docs/test-evidence/elasticsearch.md` | IF-T45; full document/index CRUD and aggregation on 8.15.5; product-native HTTPS not covered |
| DB-PROMETHEUS-001 | Time series | Prometheus `prometheus://` | Docker observability | Ready | COMPLETE | `docs/test-evidence/prometheus.md` | `b9dd9fd`; exact remote-write/readback/global-limit run |
| DB-REDIS-MQ-001 | Messaging | Redis Streams/PubSub | Docker messaging | Ready | COMPLETE | `docs/test-evidence/redis-messaging.md` | `d2c88a2`; Streams deleted, Pub/Sub ephemeral |
| DB-KAFKA-001 | Messaging | Kafka API on Redpanda | Docker messaging | Ready | COMPLETE | `docs/test-evidence/kafka-redpanda.md` | `d2c88a2`; pure/native and `kafka://`/`redpanda://`; lag/delete unsupported |
| DB-RABBITMQ-001 | Messaging | AMQP + RabbitMQ management | Docker messaging | Ready | COMPLETE | `docs/test-evidence/rabbitmq.md` | `d2c88a2`, IF-T47/IF-T59; confirms, ACKs, exact detail, conditional delete and zero residual queues passed |
| DB-NATS-001 | Messaging | NATS Core + JetStream | Docker messaging | Ready | COMPLETE | `docs/test-evidence/nats.md` | `d2c88a2`; Core ephemeral and JetStream delete verified |
| DB-MQ-TLS-001 | Messaging | AMQPS + NATS TLS | Docker messaging-tls | Ready | COMPLETE | `docs/test-evidence/messaging-tls.md` | `d2c88a2`; regenerated CA-backed TLS passed |
| DB-KAFKA-VENDORS-001 | Messaging | AutoMQ/WarpStream/Confluent | external | Ready | EXTERNAL | - | no vendor DSNs are supplied |

## Per-Resource Evidence Contract

Every `LIVE_PASS` evidence file under `docs/test-evidence/` contains one row per
fixture resource:

| Resource | Create | Insert/write | Read all fixture data | Update/overwrite | Targeted delete | Metadata/admin | Guard | Limit/timeout | Cleanup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dbtool_it_*` | PASS/N/A | PASS/N/A | expected and actual row/key/document/message counts plus stable IDs | PASS/N/A/UNSUPPORTED | PASS/N/A/UNSUPPORTED | PASS/N/A | PASS | PASS/N/A | PASS/UNSUPPORTED |

The evidence must also record the exact runner, UTC timestamp, image/product
version, architecture, result, and any unsupported capability boundary.

## Design-Only, Not Registered

Oracle, etcd, InfluxDB, VictoriaMetrics, Pulsar, MQTT, and RocketMQ still appear
as design candidates in `dbtool-design.md`, but no factory is registered for
them. They are not counted as current support and cannot be marked tested.
