# dbtool Implementation Tasks

This task plan maps the design document milestones into commit-sized work.

## P0a: Compilable Core Skeleton

Goal: `dbtool-core` compiles and exposes stable contracts.

- [x] Workspace and crate layout.
- [x] L1 domain models: values, result sets, documents, messages, series, metadata.
- [x] L2 ports: `Connector` plus capability traits.
- [x] Registry and owned `Dsn` factory contract.
- [x] Protocol alias table and family registration.
- [x] Mock adapter and contract tests.
- [x] `cargo check -p dbtool-core` passes.
- [x] `cargo test -p dbtool-core` passes.

## P0b: Shared Core Services

Goal: frontends and adapters share one behavior layer.

- [x] Connection resolver for raw DSN, `DBTOOL_CONN_*`, and `connections.toml`.
- [x] JSON formatter envelope.
- [x] Result limiter.
- [x] SQL safety guard and two-step token confirmation.
- [x] Flow-control service with concurrency, rate limiting, timeout, and bounded retry budget.
- [x] `connections.toml` limits merged into CLI data-command flow control.
- [x] CLI flow-control flags override config limits for one-off data commands.
- [x] Replace keyword SQL classifier with `sqlparser`-backed classification.
- [x] Bind confirm token to target connection and impact summary.
- [x] Unit tests for resolver, formatter, flow control, and safety edge cases.

## P1: SQL Family

Goal: MySQL/Postgres/SQLite adapters and SQL CLI commands.

- [x] SQL adapter crate scaffold.
- [x] MySQL/Postgres/SQLite factories.
- [x] Protocol aliases for MariaDB/TiDB/Cockroach/Timescale/Redshift.
- [x] Live protocol alias verification for MariaDB/TiDB through the MySQL adapter.
- [x] Correct typed value extraction from SQL rows.
- [x] Safe identifier handling for schema/table commands.
- [x] SQLite smoke tests with in-memory database.
- [x] SQL CLI query/exec/tables/schema verified.

## P2: CLI And Claude Skill Surface

Goal: machine-readable CLI suitable for Claude Code skill calls.

- [x] `ping`, `caps`, `conn list`, `sql`, `kv`, `doc`, `mq`, `ts`, `search` command shell.
- [x] Named connection resolution via core.
- [x] Table/NDJSON format support.
- [x] `SKILL.md` with command examples and safety workflow.
- [x] CLI integration tests for JSON envelopes and error codes.
- [x] Service-free executable smoke script with SQLite fixture data, write confirmation, limits, and timeout coverage.

## P3: KV And Document Stores

Goal: Redis-compatible and MongoDB adapters.

- [x] Redis adapter scaffold and basic KV commands.
- [x] MongoDB adapter scaffold.
- [x] Redis raw command validation and typed result conversion.
- [x] Redis raw write commands require `--allow-write`.
- [x] Live protocol alias verification for Valkey/KeyDB/Dragonfly through the Redis adapter.
- [x] Redis Streams/PubSub capability split.
- [x] MongoDB filter/update/aggregate implementation.
- [x] Remove adapter-side `unwrap`.

## P4: TUI

Goal: ratatui frontend built on core `ConnectionManager`.

- [x] Minimal TUI shell.
- [x] Defer detailed TUI until core/CLI are stable.
- [x] Add basic connection picker, capability-aware command dispatch, and write confirmation.

## P5: Self-Contained Messaging

Goal: bounded message operations with no external runtime dependencies.

- [x] Kafka pure/native feature boundary scaffold.
- [x] AMQP/NATS adapter shells.
- [x] Messaging shells do not advertise unimplemented producer/consumer/admin capabilities.
- [x] Kafka pure backend real ping/list/detail/produce/consume.
- [x] AMQP real producer/consumer and queue detail.
- [x] NATS core real producer/consumer.
- [x] Redis Streams/PubSub support in Redis adapter.
- [x] RabbitMQ HTTP management queue listing/detail/lag through explicit `rabbitmq+http` boundary.
- [x] NATS JetStream admin/list/detail support.

## P6: Distribution And Extended Backends

Goal: release-quality packages and optional advanced backends.

- [x] CI/release workflow scaffold.
- [x] Make workflows reuse build artifacts and avoid duplicate builds.
- [x] npm/pip/uv/mise packaging.
- [x] Optional native Kafka implementation.
- [x] Dockerfile for containerized dbtool CLI runtime plus Docker image smoke script.
- [x] OpenSearch/Elasticsearch HTTP search adapter.
- [x] Prometheus HTTP time-series adapter.
- [x] SQL Server adapter behind an opt-in Docker profile with service-free coverage and documented amd64 live gate.
- [x] Cassandra/ScyllaDB CQL adapter through the constrained SQL command surface with real Cassandra live coverage.

## P7: Live Integration Automation

Goal: self-start local services with bounded resources and verify real CLI workflows.

- [x] Docker Compose integration environment for Postgres, MySQL, Redis, and MongoDB.
- [x] Docker Compose compatibility profile for MariaDB and Valkey.
- [x] Docker Compose compatibility-extra profile for KeyDB and Dragonfly.
- [x] Docker Compose TiDB profile for PD, TiKV, and TiDB SQL server.
- [x] Docker Compose TiDB secure HA profile for 3 PD, 2 TiKV, and 2 TiDB SQL nodes.
- [x] Docker Compose SQL Server profile for TDS/SQL Server coverage.
- [x] Docker Compose messaging profile for Redis, Redpanda, RabbitMQ, and NATS.
- [x] Docker Compose messaging TLS profile for RabbitMQ TLS and NATS TLS.
- [x] Custom project name, database names, credentials, and host ports through environment variables.
- [x] CPU/memory/resource limits for integration services.
- [x] Integration scripts for up/down/test lifecycle.
- [x] Service-free smoke script for core SQLite CRUD, safety, result limiting, and configured timeout behavior.
- [x] Docker image smoke script that runs the same core SQLite flow inside the containerized dbtool CLI.
- [x] Live CLI tests for SQL, KV, and document workflows.
- [x] Live CLI tests for MySQL protocol aliases and Redis-compatible protocol aliases.
- [x] Live CLI tests for real MariaDB, Valkey, KeyDB, and Dragonfly compatibility services.
- [x] Live CLI tests for real TiDB compatibility service.
- [x] Live CLI tests for TiDB auth, SQL TLS, component TLS, X509, and local HA topology.
- [x] TiDB secure HA SQL-node failover drill script with CI manual entry and documented pass/fail criteria.
- [x] Live CLI test entry for SQL Server SQL lifecycle, typed values, limiting, tables, and schema.
- [x] Live CLI tests for Redis Streams/PubSub, Kafka, AMQP, and NATS messaging workflows.
- [x] Live CLI tests for AMQPS and NATS TLS messaging workflows.
- [x] Live CLI tests for OpenSearch search and Prometheus time-series workflows.
- [x] Documented integration workflow and cleanup.

## Next Execution Queue

Use this as the next implementation order now that the core/CLI/live database loop is stable.

### T1: Messaging Adapters

Goal: replace safe shells with real bounded functionality.

- [x] Kafka pure backend ping/list/detail/produce/consume with bounded reads.
- [x] AMQP producer/consumer/detail with explicit ack and timeout behavior.
- [x] NATS core producer/consumer with subject validation.
- [x] NATS JetStream admin topics/detail/lag support.
- [x] Redis Streams/PubSub support behind explicit capabilities.
- [x] Live messaging tests with self-starting Redis/Redpanda/RabbitMQ/NATS and resource limits.
- [x] RabbitMQ HTTP management queue listing/detail/lag through explicit `rabbitmq+http` boundary.

### T2: CI And Integration Profiles

Goal: make verification repeatable locally and in CI without forcing Docker on every run.

- [x] CI profile for service-free `./scripts/verify.sh`.
- [x] Optional CI/manual profile for `./scripts/integration-test.sh`.
- [x] Compose config validation in CI.
- [x] Local `scripts/validate-compose-configs.sh` covers every integration profile without starting containers.
- [x] Optional CI/manual profiles for compatible database and TiDB integration scripts.
- [x] Document required Docker resources and failure recovery.
- [x] Optional native Kafka integration script using the messaging Docker profile.

### T3: Packaging

Goal: ship installable artifacts without duplicating build work.

- [x] Reuse release workflow build artifacts.
- [x] npm package wrapper.
- [x] pip/uv package wrapper.
- [x] mise install metadata.
- [x] Release smoke tests against packaged binaries.

### T6: Database Protocol Hardening

Goal: prove canonical and compatible database protocols against live services.

- [x] Normalize adapter driver URLs while preserving caller-facing alias kind.
- [x] MySQL protocol aliases `mariadb://` and `tidb://` verified against live MySQL.
- [x] MySQL live typed-value coverage for int, float, bytes, null, and result limiting.
- [x] Redis-compatible aliases `valkey://`, `keydb://`, and `dragonfly://` verified against live Redis.
- [x] Redis raw mutating commands blocked without `--allow-write`.
- [x] Redis live coverage for TTL, typed raw output, scan truncation, and multi-key delete.

### T7: RabbitMQ Management Admin

Goal: expose RabbitMQ queue discovery without blurring AMQP protocol boundaries.

- [x] Add an admin-only `rabbitmq+http` management connector.
- [x] Keep pure `amqp://` queue listing behavior protocol-native.
- [x] Map RabbitMQ management queues to `TopicInfo`, `TopicDetail`, and queue-level lag.
- [x] Build RabbitMQ management Docker service port into the messaging profile.
- [x] Live-test queue creation through AMQP and queue listing/detail/lag through HTTP management.

### T8: Compatible Database Matrix

Goal: prove compatible database protocols against real compatible services.

- [x] Add a lightweight `compat` Docker profile for MariaDB and Valkey.
- [x] Add a `compat-extra` Docker profile for KeyDB and Dragonfly.
- [x] Add `integration-compat-up.sh` and `integration-compat-test.sh`.
- [x] Add a resource-bounded TiDB profile with PD, TiKV, and TiDB SQL server.
- [x] Add `integration-tidb-up.sh` and `integration-tidb-test.sh`.
- [x] Add a secure HA TiDB profile with generated certificates, component TLS, SQL TLS, and two TiDB SQL nodes.
- [x] Add `integration-tidb-secure-prepare.sh`, `integration-tidb-secure-up.sh`, and `integration-tidb-secure-test.sh`.
- [x] Live-test `mariadb://` SQL lifecycle, typed values, and limiting against MariaDB.
- [x] Live-test `tidb://` SQL lifecycle, typed values, limiting, safety confirmation, table listing, and schema-qualified table names against TiDB.
- [x] Live-test TiDB `REQUIRE SSL`, `REQUIRE X509`, insecure-login rejection, client certificate login, and SQL lifecycle through both secure SQL nodes.
- [x] Live-test `valkey://`, `keydb://`, and `dragonfly://` KV lifecycle, raw write guard, and TTL against their real services.

### T4: TUI After Core Stability

Goal: build a TUI that consumes the same verified core/CLI behavior.

- [x] Connection picker backed by core config resolution.
- [x] Capability-aware SQL/KV/Document/Search/Time-series command dispatch.
- [x] Read limits and write-confirmation prompts.
- [x] TUI smoke tests for navigation and command dispatch.
- [ ] Full-screen polish, command history, and richer per-capability forms.

### T5: Extended Backends

Goal: add new families only after the core behavior remains stable under integration tests.

- [x] Search backend adapter with OpenSearch/Elasticsearch HTTP index list/search/index operations.
- [x] OpenSearch/Elasticsearch resource-bounded Docker profile and live CLI tests.
- [x] Search HTTPS/TLS transport support for `opensearch+https://` and `elasticsearch+https://`.
- [x] Search HTTPS/TLS live Docker profile and CLI test coverage through a local compatible harness.
- [x] Time-series HTTP adapter with Prometheus measurement list/query operations.
- [x] Prometheus resource-bounded Docker profile and live CLI tests.
- [x] SQL Server adapter.
- [x] Cassandra adapter.

## Remaining Task Board

This board tracks the remaining work after the core, CLI, Docker lifecycle, and
major database compatibility loop became stable.

| Task | Priority | Status | Scope | Verification gate |
| --- | --- | --- | --- | --- |
| T9 Search TLS live validation | P1 | Done | Added a Dockerfile-built TLS-enabled OpenSearch-compatible harness with generated local CA and seeded fixture data, then exercised `opensearch+https://` through `search indices`, `search search`, and `search index`. | `cargo test -p adapter-search`; `cargo test -p dbtool-cli --test live_observability`; observability compose config validation; optional `./scripts/integration-observability-test.sh`. |
| T10 PostgreSQL-family live compatibility | P1 | Done | Added resource-bounded CockroachDB and TimescaleDB profiles for the existing Postgres adapter aliases. | `./scripts/integration-pg-compat-test.sh` covers SQL lifecycle, typed values, limiting, tables, and schema tests for `cockroach://` and `timescale://`. |
| T11 Messaging TLS live validation | P2 | Done | Added TLS-enabled RabbitMQ and NATS profiles for already-registered `amqps://` and `nats+tls://` aliases, with generated local CA support through `tls-ca`/`ssl-ca` DSN params. | `./scripts/integration-mq-tls-test.sh` covers AMQPS produce/consume/detail and NATS TLS publish/subscribe plus JetStream topics/detail/lag. |
| T12 TUI workflow depth | P2 | Pending | Add command history, richer per-capability forms, and polished full-screen navigation while reusing core confirmation and limit behavior. | TUI smoke tests for history, forms, write confirmation, and error rendering. |
| T13 SQL Server adapter gate | P3 | In progress | Added `adapter-sqlserver`, `sqlserver://` and `mssql://` registration, Docker profile, integration scripts, and CLI live lifecycle coverage. Remaining gate is running the heavyweight Docker profile in an x86_64-capable environment. | `cargo test -p adapter-sqlserver`; `docker compose -f docker-compose.integration.yml --profile sqlserver config`; optional `./scripts/integration-sqlserver-test.sh`. |
| T14 Cassandra/CQL adapter gate | P3 | Done | Accepted a constrained CQL-over-`SqlEngine` surface, added `adapter-cassandra`, `cassandra://` and `scylla://` registration, Docker profile, integration scripts, and CLI live lifecycle coverage. The local Docker profile passed with address translation enabled for host-port mapping. | `cargo test -p adapter-cassandra`; `docker compose -f docker-compose.integration.yml --profile cassandra config`; `./scripts/integration-cassandra-test.sh`. |
| T15 Prometheus remote write | P3 | Deferred | Add protobuf/snappy remote-write support only if write-heavy time-series workflows become required. | Service-free encoding tests plus Prometheus-compatible remote-write receiver test. |
| T16 Production TiDB HA drills | P3 | In progress | Added a secure HA SQL-node failover drill that stops each TiDB SQL node in turn, proves the surviving TLS SQL node can keep writing/reading shared data, and confirms restarted nodes can read outage-time writes. TiProxy, PD/TiKV failover, certificate rotation, and production-readiness drills remain deferred. | `./scripts/integration-tidb-ha-drill.sh`; `run_live_tidb_ha_drill` workflow input; documented pass/fail criteria in `docs/tidb-compat-design.md`. |
