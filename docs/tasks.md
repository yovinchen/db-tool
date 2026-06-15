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
- [x] Flow-control service with bounded retry budget.
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
- [x] Table/NDJSON format support or documented JSON-only scope.
- [x] `SKILL.md` with command examples and safety workflow.
- [x] CLI integration tests for JSON envelopes and error codes.

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
- [ ] Defer detailed TUI until core/CLI are stable.

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
- [ ] Future adapters: search, time-series HTTP, SQL Server, Cassandra.

## P7: Live Integration Automation

Goal: self-start local services with bounded resources and verify real CLI workflows.

- [x] Docker Compose integration environment for Postgres, MySQL, Redis, and MongoDB.
- [x] Docker Compose compatibility profile for MariaDB and Valkey.
- [x] Docker Compose compatibility-extra profile for KeyDB and Dragonfly.
- [x] Docker Compose TiDB profile for PD, TiKV, and TiDB SQL server.
- [x] Docker Compose messaging profile for Redis, Redpanda, RabbitMQ, and NATS.
- [x] Custom project name, database names, credentials, and host ports through environment variables.
- [x] CPU/memory/resource limits for integration services.
- [x] Integration scripts for up/down/test lifecycle.
- [x] Live CLI tests for SQL, KV, and document workflows.
- [x] Live CLI tests for MySQL protocol aliases and Redis-compatible protocol aliases.
- [x] Live CLI tests for real MariaDB, Valkey, KeyDB, and Dragonfly compatibility services.
- [x] Live CLI tests for real TiDB compatibility service.
- [x] Live CLI tests for Redis Streams/PubSub, Kafka, AMQP, and NATS messaging workflows.
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
- [x] Live-test `mariadb://` SQL lifecycle, typed values, and limiting against MariaDB.
- [x] Live-test `tidb://` SQL lifecycle, typed values, limiting, safety confirmation, and schema-qualified table names against TiDB.
- [x] Live-test `valkey://`, `keydb://`, and `dragonfly://` KV lifecycle, raw write guard, and TTL against their real services.

### T4: TUI After Core Stability

Goal: build a TUI that consumes the same verified core/CLI behavior.

- [ ] Connection picker backed by core config resolution.
- [ ] Capability-aware SQL/KV/Document views.
- [ ] Read limits and write-confirmation prompts.
- [ ] TUI smoke tests for navigation and command dispatch.

### T5: Extended Backends

Goal: add new families only after the core behavior remains stable under integration tests.

- [ ] Search backend adapter.
- [ ] Time-series HTTP adapter.
- [ ] SQL Server adapter.
- [ ] Cassandra adapter.
