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
- [ ] Redis raw command validation and typed result conversion.
- [ ] Redis Streams/PubSub capability split.
- [ ] MongoDB filter/update/aggregate implementation.
- [ ] Remove adapter-side `unwrap`.

## P4: TUI

Goal: ratatui frontend built on core `ConnectionManager`.

- [x] Minimal TUI shell.
- [ ] Defer detailed TUI until core/CLI are stable.

## P5: Self-Contained Messaging

Goal: bounded message operations with no external runtime dependencies.

- [x] Kafka pure/native feature boundary scaffold.
- [x] AMQP/NATS adapter shells.
- [ ] Kafka pure backend real ping/list/produce/consume.
- [ ] AMQP real producer/consumer/admin.
- [ ] NATS real producer/consumer/admin.
- [ ] Redis Streams/PubSub support in Redis adapter.

## P6: Distribution And Extended Backends

Goal: release-quality packages and optional advanced backends.

- [x] CI/release workflow scaffold.
- [ ] Make workflows reuse build artifacts and avoid duplicate builds.
- [ ] npm/pip/uv/mise packaging.
- [ ] Optional native Kafka implementation.
- [ ] Future adapters: search, time-series HTTP, SQL Server, Cassandra.
