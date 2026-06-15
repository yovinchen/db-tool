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
| CLI | Implemented | `ping`, `caps`, `conn`, `sql`, `kv`, `doc`, `mq`, `ts`, and `search` command shells exist. |
| Output formats | Implemented | JSON is the default. `--format table` and `--format ndjson` are implemented for successful command output; errors always stay JSON so `error.code` and confirmation tokens remain machine-readable. |
| SQL safety | Implemented | Read statements are allowed, writes need `--allow-write`, destructive SQL needs a confirm token bound to the target. |
| Docker integration | Implemented | Base databases, compatibility databases, TiDB, TiDB secure HA, and messaging profiles are available. |
| CI | Implemented | Service-free verification runs by default; live Docker jobs are manual workflow inputs. |
| TUI | Partial | A minimal ratatui shell exists, but it does not yet execute real capability-aware workflows. |

## Usable Database And Protocol Matrix

| Backend | DSN schemes | Adapter | Usable operations | Verification |
| --- | --- | --- | --- | --- |
| SQLite | `sqlite:` | SQL | `ping`, `sql query`, `sql exec`, `sql tables`, `sql schema` | In-memory unit and CLI tests |
| PostgreSQL | `postgres://` | SQL | SQL query/exec/tables/schema with write safety | Base Docker live test |
| PostgreSQL alias | `postgresql://` | SQL | Same as Postgres adapter | Registry alias test only |
| CockroachDB | `cockroach://` | SQL | Routed to Postgres adapter | Not live-tested against CockroachDB |
| TimescaleDB | `timescale://` | SQL | Routed to Postgres adapter | Not live-tested against TimescaleDB |
| Redshift | `redshift://` | SQL | Routed to Postgres adapter | Not live-tested against Redshift |
| MySQL | `mysql://` | SQL | SQL query/exec/tables/schema, typed values, result limiting | Base Docker live test |
| MariaDB | `mariadb://` | SQL | MySQL-family SQL lifecycle, typed values, result limiting | Real MariaDB compatibility live test |
| TiDB | `tidb://` | SQL | MySQL-family SQL lifecycle, typed values, table listing, schema-qualified tables | Real PD/TiKV/TiDB live test |
| TiDB secure HA | `tidb://` with TLS params | SQL | SQL TLS, component TLS, `REQUIRE SSL`, `REQUIRE X509`, insecure-login rejection, two SQL-node lifecycle | Real 3 PD + 2 TiKV + 2 TiDB live test |
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
| AMQP/RabbitMQ | `amqp://`, `amqps://` | AMQP | produce, consume, queue detail | RabbitMQ live test; `amqps://` routing is registered but TLS is not live-tested |
| RabbitMQ management | `rabbitmq+http://` | RabbitMQ HTTP admin | queue list, detail, lag | RabbitMQ management live test |
| NATS | `nats://`, `nats+tls://` | NATS | publish, subscribe, JetStream topics/detail/lag | NATS live test; `nats+tls://` routing is registered but TLS is not live-tested |

## Docker Service Profiles

| Script | Services | Main coverage | Resource note |
| --- | --- | --- | --- |
| `./scripts/integration-test.sh` | Postgres, MySQL, Redis, MongoDB | Canonical SQL, KV, and document workflows | Roughly 2 GiB container memory |
| `./scripts/integration-compat-test.sh` | MariaDB, Valkey | MySQL and Redis compatible databases | Extra KeyDB/Dragonfly via `DBTOOL_IT_COMPAT_EXTRA=1` |
| `./scripts/integration-tidb-test.sh` | PD, TiKV, TiDB | Real TiDB compatibility | Roughly 1.75 GiB container memory |
| `./scripts/integration-tidb-secure-test.sh` | 3 PD, 2 TiKV, 2 TiDB SQL | TiDB auth/TLS/local HA | Roughly 3.75 GiB container memory |
| `./scripts/integration-mq-test.sh` | Redis, Redpanda, RabbitMQ, NATS | Streams/PubSub, Kafka, AMQP, NATS | Roughly 2 GiB container memory |
| `./scripts/integration-mq-native-test.sh` | Redis, Redpanda, RabbitMQ, NATS | Native Kafka backend plus messaging regression | Requires `full-native` build |

## Implemented CLI Operations

| Command family | Implemented commands | Write guard |
| --- | --- | --- |
| Connection | `conn list` | Read-only |
| General | `ping`, `caps` | Read-only |
| SQL | `sql query`, `sql exec`, `sql tables`, `sql schema` | `sql exec` and unsafe SQL require `--allow-write` and sometimes `--confirm` |
| KV | `kv get`, `kv set`, `kv scan`, `kv del`, `kv raw` | `set`, `del`, and mutating raw commands require `--allow-write` |
| Document | `doc collections`, `doc find`, `doc insert`, `doc update`, `doc delete`, `doc aggregate` | insert/update/delete require `--allow-write`; delete refuses empty filters adapter-side |
| Messaging | `mq produce`, `mq consume`, `mq topics`, `mq detail`, `mq lag` | produce requires `--allow-write` |
| Search | `search indices`, `search search` | CLI shell exists, but no backend is registered |
| Time series | `ts measurements`, `ts query` | CLI shell exists, but no backend is registered |

## Not Implemented Or Not Yet Fully Usable

| Item | Status | Why it is not fully usable | Recommended next task |
| --- | --- | --- | --- |
| Search backend | Not implemented | The `SearchEngine` trait and CLI shell exist, but no Elasticsearch/OpenSearch adapter is registered. | Add an OpenSearch/Elasticsearch HTTP adapter with local Docker tests. |
| Time-series backend | Not implemented | The `TimeSeriesStore` trait and CLI shell exist, but no Prometheus/Influx adapter is registered. | Add a Prometheus HTTP read adapter first, then decide whether writes belong in scope. |
| SQL Server | Not implemented | No TDS adapter, DSN scheme, Docker profile, or tests exist. | Add `sqlserver://` adapter and a SQL Server container profile if image cost is acceptable. |
| Cassandra | Not implemented | No CQL adapter, DSN scheme, Docker profile, or tests exist. | Add only after SQL/KV/search/time-series scope stabilizes. |
| TUI workflows | Partial | Minimal shell renders panels but does not call core capabilities. | Add connection picker, capability-aware views, write confirmation, and smoke tests. |
| Production TiDB HA | Partial | Local secure HA topology is verified, but TiProxy/failover drills/cert rotation are not covered. | Add explicit failover tests or document that this is compatibility validation only. |
| AMQP queue listing over pure AMQP | Not supported | AMQP 0.9.1 does not expose queue listing as a portable protocol operation. | Keep using `rabbitmq+http://` for RabbitMQ admin discovery. |
| Redis Pub/Sub durable listing | Not supported | Pub/Sub channels are live subscriptions, not durable topics. | Keep durable list/detail semantics on Redis Streams only. |
| NATS core subject listing | Not supported | Core NATS subjects are not durable catalog entries. | Keep list/detail/lag semantics on JetStream only. |
| TLS live tests for aliases | Partial | `amqps://` and `nats+tls://` aliases are registered, but not live-tested. | Add TLS-enabled RabbitMQ/NATS compose services if needed. |

## Next Implementation Queue

1. Add Search backend, starting with OpenSearch/Elasticsearch HTTP because the CLI shell and trait already exist.
2. Add Time-series backend, starting with Prometheus HTTP read operations.
3. Harden PostgreSQL-family compatibility with live CockroachDB and TimescaleDB profiles.
4. Add TLS live coverage for `amqps://` and `nats+tls://` aliases if secure messaging compatibility matters.
5. Expand TUI only after the above core and CLI behavior remains stable.
