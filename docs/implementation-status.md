# dbtool Implementation Status

Last updated: 2026-06-18

This document is the current implementation inventory for dbtool. It separates
implemented behavior, compatibility that has been live-tested, compatibility
that is only routed through an existing adapter, and features that are still not
usable.

## Overall Status

| Area | Status | Notes |
| --- | --- | --- |
| Core contracts | Implemented | `Connector`, capability traits, shared models, registry, DSN parsing, redaction, and protocol aliases are in place. |
| Embedded library path | Implemented | `dbtool-registry` has a service-free embedded smoke that builds the registry directly, reuses a connection through `ConnectionManager`, applies `SafetyGuard`, and runs SQL under `FlowControl` without spawning the CLI. |
| CLI | Implemented | `ping`, `caps`, `conn`, `sql`, `cql`, `db2`, `kv`, `doc`, `mq`, `search`, and `ts` command families exist with default backends for core read paths. Root and command-family help describe safety boundaries, JSON inputs, bounded reads, and examples. |
| Output formats | Implemented | JSON is the default. `--format table` and `--format ndjson` are implemented for successful command output; errors always stay JSON so `error.code` and confirmation tokens remain machine-readable. |
| SQL safety | Implemented | Read statements are allowed, writes need `--allow-write`, destructive SQL needs a confirm token bound to the target. |
| Flow control | Implemented | Core `FlowControl` covers per-process concurrency, optional token-bucket rate limiting, acquire timeout, request timeout, shared overall deadline, and retry budget. CLI data commands load `[defaults.limits]` and named-connection overrides from `connections.toml`, then apply CLI overrides such as `--rate`, `--request-timeout`, and `--deadline`; CLI execution uses the one-shot path so writes are not replayed by retries. |
| Docker integration | Implemented | Base databases, fixture-image databases, compatibility databases, SQL Server, Cassandra, TiDB, TiDB secure HA, messaging, messaging TLS, observability, OpenSearch security-plugin TLS, and product-native Elasticsearch profiles are available. A Dockerfile-backed dbtool CLI runtime image can be smoke-tested with the same SQLite core flow. |
| CI | Implemented | Service-free verification runs by default; live Docker jobs are manual workflow inputs. |
| Release artifacts | Implemented | Release archives and npm/Python wrapper packages include generated bash, zsh, and fish completions plus `dbtool.1` manpage artifacts derived from the clap command metadata. |
| TUI | Implemented | Connection picker, capability-aware command dispatch, read limits, write confirmation, command history, full-screen status, per-capability forms, and smoke tests are in place. |

## Usable Database And Protocol Matrix

| Backend | DSN schemes | Adapter | Usable operations | Verification |
| --- | --- | --- | --- | --- |
| SQLite | `sqlite:` | SQL | `ping`, `sql query`, `sql exec`, `sql tables`, `sql schema` | In-memory unit and CLI tests |
| PostgreSQL | `postgres://` | SQL | SQL query/exec/tables/schema with write safety | Base Docker live test |
| PostgreSQL alias | `postgresql://` | SQL | Same as Postgres adapter | Registry alias test only |
| CockroachDB | `cockroach://` | SQL | Postgres-family SQL lifecycle, typed values, result limiting, table listing, schema inspection | Real CockroachDB compatibility live test |
| TimescaleDB | `timescale://` | SQL | Postgres-family SQL lifecycle, typed values, result limiting, table listing, schema inspection | Real TimescaleDB compatibility live test |
| Redshift | `redshift://` | SQL | Routed to Postgres adapter | Env-gated external Redshift smoke through `./scripts/integration-redshift-test.sh` when `DBTOOL_IT_REDSHIFT_DSN` is supplied |
| IBM Db2 | `db2://` | Db2 ODBC | `ping`, `sql query`, `sql exec`, `sql tables`, `sql schema`, `db2 schemas`, `db2 tables`, `db2 schema`, `db2 sequences`, `db2 routines`, `db2 tablespaces`, `db2 foreign-keys`, `db2 ddl` | Service-free adapter tests; live integration guarded by `DBTOOL_RUN_DB2_INTEGRATION=1` |
| IBM Db2 alias | `ibmdb2://`, `as400://` | Db2 ODBC | Same as Db2 adapter | Registry alias test only |
| SQL Server | `sqlserver://`, `mssql://` | SQL Server/TDS | SQL query/exec/tables/schema, typed scalar values, result limiting | Service-free adapter tests plus real SQL Server Docker live test on GitHub Actions x86_64 runner |
| Cassandra/ScyllaDB | `cassandra://`, `scylla://` | CQL | `ping`, `cql query`, `cql exec`, `cql keyspaces`, `cql tables`, `cql schema`, SQL-compatible CQL path, primitive/collection typed values | Adapter tests plus real Cassandra Docker live test |
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
| AutoMQ | `automq://` | Kafka | Routed to Kafka adapter; native backend accepts DSN-supplied SASL/TLS params | Env-gated external vendor smoke through `./scripts/integration-kafka-vendor-test.sh` when `DBTOOL_IT_AUTOMQ_DSN` is supplied |
| WarpStream | `warpstream://` | Kafka | Routed to Kafka adapter; native backend accepts DSN-supplied SASL/TLS params | Env-gated external vendor smoke through `./scripts/integration-kafka-vendor-test.sh` when `DBTOOL_IT_WARPSTREAM_DSN` is supplied |
| Confluent | `confluent://` | Kafka | Routed to Kafka adapter; native backend accepts DSN-supplied SASL/TLS params | Env-gated external vendor smoke through `./scripts/integration-kafka-vendor-test.sh` when `DBTOOL_IT_CONFLUENT_DSN` is supplied |
| AMQP/RabbitMQ | `amqp://`, `amqps://` | AMQP | produce, consume, queue detail | RabbitMQ plain and AMQPS TLS live tests |
| RabbitMQ management | `rabbitmq+http://` | RabbitMQ HTTP admin | queue list, detail, lag | RabbitMQ management live test |
| NATS | `nats://`, `nats+tls://` | NATS | publish, subscribe, JetStream topics/detail/lag | NATS plain and TLS live tests |
| OpenSearch | `opensearch://`, `opensearch+https://` | Search HTTP/HTTPS | index list, search, single-document index | Service-free HTTP/TLS mapping tests, real OpenSearch plain HTTP live profile, HTTPS compatible harness, and opt-in OpenSearch security-plugin HTTPS/basic-auth profile |
| Elasticsearch | `elasticsearch://`, `elasticsearch+https://` | Search HTTP/HTTPS | Routed to OpenSearch-compatible HTTP adapter | Service-free HTTP/TLS mapping tests plus product-native Elasticsearch Docker live profile for `elasticsearch://` |
| Prometheus | `prometheus://`, `prometheus+http://` | Time series HTTP | metric list, range query, and remote write | Service-free adapter tests plus Prometheus live profile |

## Docker Service Profiles

| Script | Services | Main coverage | Resource note |
| --- | --- | --- | --- |
| `./scripts/integration-db-suite.sh` | Selectable local DB suite | Orchestrates Compose config validation, service-free checks, base DB workflows, flow-control, database-side SQL timeout checks, live connection config, custom environment smoke, fixture data/images, logical roundtrip, compatibility profiles, TiDB, and opt-in heavy DB/protocol phases | Default excludes heavy phases; `DBTOOL_IT_DB_SUITE_PHASES=all` includes every declared DB, messaging, observability, search, and external-endpoint phase |
| `./scripts/integration-test.sh` | Postgres, MySQL, Redis, MongoDB | Canonical SQL, KV, and document workflows | Roughly 2 GiB container memory |
| `./scripts/integration-flow-control-test.sh` | Postgres, MySQL, Redis, MongoDB | Live request timeout, rate/admission flags, SQL/KV/document result limiting, and disposable fixture cleanup | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-server-timeout-test.sh` | Postgres, MySQL | Database-side SQL timeout checks for PostgreSQL `statement_timeout`, PostgreSQL `idle_in_transaction_session_timeout`, PostgreSQL `lock_timeout`, and MySQL `innodb_lock_wait_timeout` | Roughly 1.25 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-connection-config-test.sh` | Postgres, MySQL, Redis, MongoDB | Temporary `connections.toml` named connections for SQL/KV/document workflows plus connection-level request timeout | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-custom-env-test.sh` | Postgres, MySQL, Redis, MongoDB | Custom project name, database names, credentials, host ports, generated DSNs, and read/write verification | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-fixture-data-test.sh` | Postgres, MySQL, Redis, MongoDB | File-backed reusable fixture loading for SQL rows, Redis keys, and MongoDB documents | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-fixture-images-test.sh` | Dockerfile-built Postgres, MySQL, Redis, MongoDB | Fixture data baked into database images and verified through dbtool readback | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-data-roundtrip-test.sh` | Postgres, MySQL, Redis, MongoDB | Public `dbtool export` / `dbtool import` logical roundtrip of fixture rows, keys, and documents into independent target resources | Roughly 2 GiB container memory; local-only while CI budget is frozen |
| `./scripts/integration-compat-test.sh` | MariaDB, Valkey | MySQL and Redis compatible databases | Extra KeyDB/Dragonfly via `DBTOOL_IT_COMPAT_EXTRA=1` |
| `./scripts/integration-pg-compat-test.sh` | CockroachDB, TimescaleDB | PostgreSQL-family compatible databases | Roughly 1 GiB container memory |
| `./scripts/integration-redshift-test.sh` | Externally supplied Redshift endpoint | Env-gated SQL lifecycle, typed values, result limiting, table listing, and schema inspection; no secrets are committed | Skips when `DBTOOL_IT_REDSHIFT_DSN` is not supplied |
| `./scripts/integration-sqlserver-test.sh` | SQL Server | TDS SQL lifecycle, typed values, limiting, tables, and schema | Passed on GitHub Actions x86_64 runner; requires amd64-capable Docker locally; roughly 2 GiB container memory |
| `./scripts/integration-cassandra-test.sh` | Cassandra | CQL lifecycle, keyspace-qualified tables, schema inspection, typed scalar and collection values | Roughly 2 GiB container memory; startup can be slow |
| `./scripts/integration-cassandra-fixture-data-test.sh` | Cassandra | File-backed reusable CQL fixture loading, seeded row readback, table listing, and schema inspection | Roughly 2 GiB container memory; heavy/local-only while CI budget is frozen |
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
| `./scripts/integration-kafka-vendor-test.sh` | Externally supplied AutoMQ, WarpStream, Confluent endpoints | Env-gated native Kafka smoke for ping, topics, produce, detail, and consume; no secrets are committed | Skips when no vendor DSN env vars are supplied |
| `./scripts/integration-observability-test.sh` | OpenSearch, Dockerfile-built OpenSearch-compatible HTTPS harness, Prometheus | Search, seeded search TLS transport, and time-series workflows | Roughly 1.4 GiB container memory |
| `./scripts/integration-opensearch-security-test.sh` | OpenSearch security plugin | Real OpenSearch HTTPS/basic-auth with generated local CA/node certs and `tls-ca` validation | Roughly 1.5 GiB container memory; opt-in heavy/local-only while CI budget is frozen |
| `./scripts/integration-elasticsearch-test.sh` | Elasticsearch | Product-native `elasticsearch://` ping, write guard, single-document indexing, search, and index listing | Roughly 1.5 GiB container memory; opt-in heavy/local-only while CI budget is frozen |
| `./scripts/integration-db2-test.sh` | IBM Db2 Community Edition | SQL lifecycle, schema inspection, write guard, alias verification, `db2` subcommand (sequences, routines, tablespaces, foreign-keys, ddl) | Requires IBM Data Server Driver for ODBC at runtime; roughly 4 GiB container memory; startup up to 10 min; opt-in heavy/local-only |

## Implemented CLI Operations

| Command family | Implemented commands | Write guard |
| --- | --- | --- |
| Connection | `conn list` | Read-only |
| General | `ping`, `caps` | Read-only |
| SQL | `sql query`, `sql exec`, `sql tables`, `sql schema`, `sql schemas` | `sql exec` and unsafe SQL require `--allow-write` and sometimes `--confirm` |
| CQL | `cql query`, `cql exec`, `cql keyspaces`, `cql tables`, `cql schema` | `cql exec` requires `--allow-write` |
| Db2 | `db2 schemas`, `db2 tables`, `db2 schema`, `db2 sequences`, `db2 routines`, `db2 tablespaces`, `db2 foreign-keys`, `db2 ddl` | All read-only; no `--allow-write` required |
| KV | `kv get`, `kv set`, `kv scan`, `kv del`, `kv raw` | `set`, `del`, and mutating raw commands require `--allow-write` |
| Document | `doc collections`, `doc find`, `doc insert`, `doc update`, `doc delete`, `doc aggregate` | insert/update/delete require `--allow-write`; delete refuses empty filters adapter-side |
| Transfer | `export sql`, `export kv`, `export doc`, `import sql`, `import kv`, `import doc` | all import commands require `--allow-write` before DSN resolution, artifact reads, or connecting |
| Messaging | `mq produce`, `mq consume`, `mq topics`, `mq detail`, `mq lag` | produce requires `--allow-write` |
| Search | `search indices`, `search search`, `search index` | `index` requires `--allow-write` |
| Time series | `ts measurements`, `ts query`, `ts write` | Prometheus remote write is exposed through explicit `--allow-write`; remote write uses a minimal protobuf/snappy encoder with no new runtime dependency |

## Explicit Boundaries

These are protocol or product-specific boundaries rather than missing pieces of
the stated dbtool objective.

| Item | Boundary | Reason |
| --- | --- | --- |
| Production TiDB HA | Outside the local dbtool harness | Local secure HA topology, SQL-node failover, PD single-node outage, PD leader outage, TiKV outage boundary, certificate regeneration cold restart, logical roundtrip, and TiProxy new-connection routing are covered. Production TiKV failover, online certificate rotation, product-native backup/restore, and upgrade drills remain product-readiness exercises beyond dbtool's connector objective. |
| AMQP queue listing over pure AMQP | Not portable in AMQP 0.9.1 | RabbitMQ queue discovery is intentionally exposed through `rabbitmq+http://` management instead of pretending queue listing is portable AMQP behavior. |
| Redis Pub/Sub durable listing | Not a durable catalog | Pub/Sub channels are live subscriptions, not durable topics; durable list/detail semantics stay on Redis Streams. |
| NATS core subject listing | Not a durable catalog | Core NATS subjects are ephemeral routing names; durable list/detail/lag semantics stay on JetStream. |
| IBM Db2 end-to-end live Docker run | Requires IBM ODBC runtime at the OS level | The adapter, Docker Compose profile, and integration script exist. Running them requires IBM Data Server Driver for ODBC installed outside the container — this is an explicit runtime boundary analogous to Redshift needing a supplied external endpoint. Service-free adapter tests pass in all environments. |

## Active Verification Work

The implementation surface is broad, but real-product completeness is now
tracked separately in `docs/db-completeness-tasks.md`. A connector or test
script being present does not by itself mean that a product completed CRUD or
the equivalent family checklist. External DSN skips, compatible aliases, and
missing host runtimes remain explicit non-pass states in
`testdata/db-completeness.manifest`.

Design-only candidates such as Oracle, etcd, InfluxDB, VictoriaMetrics, Pulsar,
MQTT, and RocketMQ do not have registered factories and are not listed as
implemented backends.

## Completion Evidence

`docs/final-goal-audit.md` maps the final objective to concrete evidence, and
`./scripts/validate-final-goal.sh` verifies the repo-level completion evidence
without starting Docker.
