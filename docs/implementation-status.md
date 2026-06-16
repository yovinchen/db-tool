# dbtool Implementation Status

Last updated: 2026-06-16

This document is the current implementation inventory for dbtool. It separates
implemented behavior, compatibility that has been live-tested, compatibility
that is only routed through an existing adapter, and features that are still not
usable.

## Overall Status

| Area | Status | Notes |
| --- | --- | --- |
| Core contracts | Implemented | `Connector`, capability traits, shared models, registry, DSN parsing, redaction, and protocol aliases are in place. |
| CLI | Implemented | `ping`, `caps`, `conn`, `sql`, `kv`, `doc`, `mq`, `search`, and `ts` command families exist with default backends for core read paths. |
| Output formats | Implemented | JSON is the default. `--format table` and `--format ndjson` are implemented for successful command output; errors always stay JSON so `error.code` and confirmation tokens remain machine-readable. |
| SQL safety | Implemented | Read statements are allowed, writes need `--allow-write`, destructive SQL needs a confirm token bound to the target. |
| Flow control | Implemented | Core `FlowControl` covers per-process concurrency, optional token-bucket rate limiting, acquire timeout, request timeout, shared overall deadline, and retry budget. CLI data commands load `[defaults.limits]` and named-connection overrides from `connections.toml`, then apply CLI overrides such as `--rate`, `--request-timeout`, and `--deadline`; CLI execution uses the one-shot path so writes are not replayed by retries. |
| Docker integration | Implemented | Base databases, fixture-image databases, compatibility databases, SQL Server, Cassandra, TiDB, TiDB secure HA, messaging, messaging TLS, and observability profiles are available. A Dockerfile-backed dbtool CLI runtime image can be smoke-tested with the same SQLite core flow. |
| CI | Implemented | Service-free verification runs by default; live Docker jobs are manual workflow inputs. |
| TUI | Partial | Connection picker, read command dispatch, read limits, write confirmation, and smoke tests exist; richer per-capability forms remain future work. |

## Usable Database And Protocol Matrix

| Backend | DSN schemes | Adapter | Usable operations | Verification |
| --- | --- | --- | --- | --- |
| SQLite | `sqlite:` | SQL | `ping`, `sql query`, `sql exec`, `sql tables`, `sql schema` | In-memory unit and CLI tests |
| PostgreSQL | `postgres://` | SQL | SQL query/exec/tables/schema with write safety | Base Docker live test |
| PostgreSQL alias | `postgresql://` | SQL | Same as Postgres adapter | Registry alias test only |
| CockroachDB | `cockroach://` | SQL | Postgres-family SQL lifecycle, typed values, result limiting, table listing, schema inspection | Real CockroachDB compatibility live test |
| TimescaleDB | `timescale://` | SQL | Postgres-family SQL lifecycle, typed values, result limiting, table listing, schema inspection | Real TimescaleDB compatibility live test |
| Redshift | `redshift://` | SQL | Routed to Postgres adapter | Not live-tested against Redshift |
| SQL Server | `sqlserver://`, `mssql://` | SQL Server/TDS | SQL query/exec/tables/schema, typed scalar values, result limiting | Service-free adapter tests plus real SQL Server Docker live test on GitHub Actions x86_64 runner |
| Cassandra/ScyllaDB | `cassandra://`, `scylla://` | CQL over constrained SQL surface | `ping`, CQL query/exec through `sql` commands, keyspace table listing, schema inspection, primitive/collection typed values | Adapter tests plus real Cassandra Docker live test |
| MySQL | `mysql://` | SQL | SQL query/exec/tables/schema, typed values, result limiting | Base Docker live test |
| MariaDB | `mariadb://` | SQL | MySQL-family SQL lifecycle, typed values, result limiting | Real MariaDB compatibility live test |
| TiDB | `tidb://` | SQL | MySQL-family SQL lifecycle, typed values, table listing, schema-qualified tables | Real PD/TiKV/TiDB live test |
| TiDB secure HA | `tidb://` with TLS params | SQL | SQL TLS, component TLS, `REQUIRE SSL`, `REQUIRE X509`, insecure-login rejection, two SQL-node lifecycle | Real 3 PD + 2 TiKV + 2 TiDB live test |
| TiDB PD quorum | `tidb://` with TLS params | SQL | Both SQL nodes continue TLS writes/reads while one PD node is stopped at a time | Local secure HA PD drill |
| TiDB PD leader | `tidb://` with TLS params | SQL | Current PD leader is discovered through the TLS PD API, stopped, replaced, and both SQL nodes continue TLS writes/reads | Local secure HA PD leader drill |
| TiDB TiKV outage boundary | `tidb://` with TLS params | SQL | One local TiKV service is stopped and dbtool SQL probes must either continue successfully or fail within bounded request/deadline time | Local secure HA TiKV outage boundary drill |
| TiDB certificate regeneration | `tidb://` with regenerated TLS params | SQL | Cold-restart secure HA after regenerated CA/server/client certificates, with TLS writes/reads through both generations | Local secure HA certificate regeneration drill |
| TiDB logical roundtrip | `tidb://` with TLS params | SQL | Rows are exported through one secure SQL node, restored through the other node, and read back from both nodes | Local secure HA logical roundtrip smoke |
| TiDB TiProxy | `tidb://` through TiProxy TLS port | SQL | TLS proxy entrypoint, `REQUIRE SSL` user, SQL lifecycle, new-connection routing while either TiDB SQL node is stopped | Opt-in TiProxy Docker drill |
| Redis | `redis://` | Redis | KV get/set/delete/scan/raw, TTL, Streams, Pub/Sub | Base and messaging Docker live tests |
| Valkey | `valkey://` | Redis | Redis-compatible KV lifecycle, TTL, raw write guard | Real Valkey compatibility live test |
| KeyDB | `keydb://` | Redis | Redis-compatible KV lifecycle, TTL, raw write guard | Optional real KeyDB live test with `DBTOOL_IT_COMPAT_EXTRA=1` |
| Dragonfly | `dragonfly://` | Redis | Redis-compatible KV lifecycle, TTL, raw write guard | Optional real Dragonfly live test with `DBTOOL_IT_COMPAT_EXTRA=1` |
| MongoDB | `mongodb://` | MongoDB | collections, find, insert, update, delete, aggregate | Base Docker live test |
| Kafka | `kafka://` | Kafka | ping, produce, consume, topics, detail/watermarks | Redpanda live test through pure Rust backend |
| Kafka native | `kafka://` with `full-native` | Kafka | librdkafka-backed ping, produce, consume, topics/detail | Optional native live test |
| Redpanda | `redpanda://` | Kafka | Routed to Kafka adapter | Redpanda service backs Kafka live tests |
| AutoMQ | `automq://` | Kafka | Routed to Kafka adapter | Not live-tested against AutoMQ |
| WarpStream | `warpstream://` | Kafka | Routed to Kafka adapter | Not live-tested against WarpStream |
| Confluent | `confluent://` | Kafka | Routed to Kafka adapter | Not live-tested against Confluent Cloud/Platform |
| AMQP/RabbitMQ | `amqp://`, `amqps://` | AMQP | produce, consume, queue detail | RabbitMQ plain and AMQPS TLS live tests |
| RabbitMQ management | `rabbitmq+http://` | RabbitMQ HTTP admin | queue list, detail, lag | RabbitMQ management live test |
| NATS | `nats://`, `nats+tls://` | NATS | publish, subscribe, JetStream topics/detail/lag | NATS plain and TLS live tests |
| OpenSearch | `opensearch://`, `opensearch+https://` | Search HTTP/HTTPS | index list, search, single-document index | Service-free HTTP/TLS mapping tests, real OpenSearch plain HTTP live profile, and HTTPS compatible harness |
| Elasticsearch | `elasticsearch://`, `elasticsearch+https://` | Search HTTP/HTTPS | Routed to OpenSearch-compatible HTTP adapter | Service-free HTTP/TLS mapping tests; OpenSearch-compatible live coverage covers the shared API surface |
| Prometheus | `prometheus://`, `prometheus+http://` | Time series HTTP | metric list and range query | Service-free adapter tests plus Prometheus live profile |

## Docker Service Profiles

| Script | Services | Main coverage | Resource note |
| --- | --- | --- | --- |
| `./scripts/integration-db-suite.sh` | Selectable local DB suite | Orchestrates Compose config validation, service-free checks, base DB workflows, flow-control, live connection config, custom environment smoke, fixture data/images, logical roundtrip, compatibility profiles, TiDB, and opt-in heavy DB phases | Default excludes heavy phases; `DBTOOL_IT_DB_SUITE_PHASES=all` includes the dbtool image smoke plus every DB/observability phase |
| `./scripts/integration-test.sh` | Postgres, MySQL, Redis, MongoDB | Canonical SQL, KV, and document workflows | Roughly 2 GiB container memory |
| `./scripts/integration-flow-control-test.sh` | Postgres, MySQL, Redis, MongoDB | Live request timeout, rate/admission flags, SQL/KV/document result limiting, and disposable fixture cleanup | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-connection-config-test.sh` | Postgres, MySQL, Redis, MongoDB | Temporary `connections.toml` named connections for SQL/KV/document workflows plus connection-level request timeout | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-custom-env-test.sh` | Postgres, MySQL, Redis, MongoDB | Custom project name, database names, credentials, host ports, generated DSNs, and read/write verification | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-fixture-data-test.sh` | Postgres, MySQL, Redis, MongoDB | File-backed reusable fixture loading for SQL rows, Redis keys, and MongoDB documents | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-fixture-images-test.sh` | Dockerfile-built Postgres, MySQL, Redis, MongoDB | Fixture data baked into database images and verified through dbtool readback | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-data-roundtrip-test.sh` | Postgres, MySQL, Redis, MongoDB | dbtool-mediated logical export and restore of fixture rows, keys, and documents into independent target resources | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-compat-test.sh` | MariaDB, Valkey | MySQL and Redis compatible databases | Extra KeyDB/Dragonfly via `DBTOOL_IT_COMPAT_EXTRA=1` |
| `./scripts/integration-pg-compat-test.sh` | CockroachDB, TimescaleDB | PostgreSQL-family compatible databases | Roughly 1 GiB container memory |
| `./scripts/integration-sqlserver-test.sh` | SQL Server | TDS SQL lifecycle, typed values, limiting, tables, and schema | Passed on GitHub Actions x86_64 runner; requires amd64-capable Docker locally; roughly 2 GiB container memory |
| `./scripts/integration-cassandra-test.sh` | Cassandra | CQL lifecycle, keyspace-qualified tables, schema inspection, typed scalar and collection values | Roughly 2 GiB container memory; startup can be slow |
| `./scripts/integration-tidb-test.sh` | PD, TiKV, TiDB | Real TiDB compatibility | Roughly 1.75 GiB container memory |
| `./scripts/integration-tidb-secure-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB auth/TLS/local HA | Roughly 3.75 GiB container memory |
| `./scripts/integration-tidb-ha-drill.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA SQL-node failover with one SQL node stopped at a time | Roughly 3.75 GiB container memory |
| `./scripts/integration-tidb-pd-drill.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA PD quorum continuity with one PD node stopped at a time | Roughly 3.75 GiB container memory |
| `./scripts/integration-tidb-pd-leader-drill.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA continuity after stopping the discovered current PD leader | Roughly 3.75 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-tidb-tikv-outage-boundary.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA behavior boundary after stopping one local TiKV service, with bounded dbtool probes | Roughly 3.75 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-tidb-cert-regeneration-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA cold restart after regenerated CA/server/client certificates | Roughly 3.75 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-tidb-logical-roundtrip-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB secure HA logical export/restore across the two TLS SQL nodes with cross-node readback | Roughly 3.75 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-tidb-tiproxy-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL, TiProxy | TiProxy TLS entrypoint and new-connection routing while one SQL node is stopped | Roughly 4 GiB container memory |
| `./scripts/integration-mq-test.sh` | Redis, Redpanda, RabbitMQ, NATS | Streams/PubSub, Kafka, AMQP, NATS | Roughly 2 GiB container memory |
| `./scripts/integration-mq-tls-test.sh` | RabbitMQ TLS, NATS TLS | AMQPS and NATS TLS aliases | Roughly 768 MiB container memory |
| `./scripts/integration-mq-native-test.sh` | Redis, Redpanda, RabbitMQ, NATS | Native Kafka backend plus messaging regression | Requires `full-native` build |
| `./scripts/integration-observability-test.sh` | OpenSearch, Dockerfile-built OpenSearch-compatible HTTPS harness, Prometheus | Search, seeded search TLS transport, and time-series workflows | Roughly 1.4 GiB container memory |

## Implemented CLI Operations

| Command family | Implemented commands | Write guard |
| --- | --- | --- |
| Connection | `conn list` | Read-only |
| General | `ping`, `caps` | Read-only |
| SQL | `sql query`, `sql exec`, `sql tables`, `sql schema` | `sql exec` and unsafe SQL require `--allow-write` and sometimes `--confirm` |
| KV | `kv get`, `kv set`, `kv scan`, `kv del`, `kv raw` | `set`, `del`, and mutating raw commands require `--allow-write` |
| Document | `doc collections`, `doc find`, `doc insert`, `doc update`, `doc delete`, `doc aggregate` | insert/update/delete require `--allow-write`; delete refuses empty filters adapter-side |
| Messaging | `mq produce`, `mq consume`, `mq topics`, `mq detail`, `mq lag` | produce requires `--allow-write` |
| Search | `search indices`, `search search`, `search index` | `index` requires `--allow-write` |
| Time series | `ts measurements`, `ts query` | Prometheus adapter is read-only; point writes are not exposed by CLI |

## Not Implemented Or Not Yet Fully Usable

| Item | Status | Why it is not fully usable | Recommended next task |
| --- | --- | --- | --- |
| Real OpenSearch security-plugin TLS profile | Partial | TLS transport is live-tested against a compatible HTTPS harness; the heavier OpenSearch security plugin TLS configuration is not part of the default profile. | Add only if security-plugin-specific OpenSearch behavior must be validated. |
| Prometheus remote write | Not supported | The implemented Prometheus adapter intentionally covers read APIs only; remote write is a separate protobuf/snappy protocol. | Add only if write-heavy time-series workflows become a requirement. |
| Cassandra trait split | Deferred | Cassandra is currently usable through a constrained CQL-over-`SqlEngine` surface so the existing CLI safety/limit/output paths work; a dedicated `CqlEngine` is not yet modeled. | Add `CqlEngine` only if CQL needs protocol-specific commands, prepared values, paging, or TUI forms. |
| TUI rich workflows | Partial | Basic command dispatch exists, but command history, form controls, and richer per-capability screens are not implemented. | Expand after core protocol coverage remains stable. |
| Production TiDB HA | Partial | Local secure HA topology, SQL-node failover drill, PD single-node outage drill, PD leader outage drill, TiKV outage boundary drill, certificate regeneration cold-restart drill, logical data roundtrip smoke, and TiProxy new-connection routing drill are available, but production TiKV failover, online certificate rotation, product-native backup/restore, and upgrade drills are not covered. | Add product-specific production drills only when production-readiness is in scope. |
| AMQP queue listing over pure AMQP | Not supported | AMQP 0.9.1 does not expose queue listing as a portable protocol operation. | Keep using `rabbitmq+http://` for RabbitMQ admin discovery. |
| Redis Pub/Sub durable listing | Not supported | Pub/Sub channels are live subscriptions, not durable topics. | Keep durable list/detail semantics on Redis Streams only. |
| NATS core subject listing | Not supported | Core NATS subjects are not durable catalog entries. | Keep list/detail/lag semantics on JetStream only. |

## Next Implementation Queue

1. Expand TUI command history and richer per-capability forms.
2. Run the TiDB secure HA, PD, logical roundtrip, and TiProxy drills in the target Docker environment when production-readiness evidence is needed.
3. Add real OpenSearch security-plugin TLS coverage only if that product-specific profile becomes necessary.
