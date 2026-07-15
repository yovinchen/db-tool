# dbtool Implementation Tasks

This task plan maps the design document milestones into commit-sized work.

## Execution Guardrails

These rules are active for every future task until explicitly changed by the
project owner.

- [x] Start each new feature or fix from a fresh feature branch, normally
      `codex/<short-scope>`, before editing implementation files.
- [x] Do not edit or push directly on `master` for feature work. Finish the
      feature branch first, verify locally, then merge the completed branch back
      into `master`.
- [x] Use local verification as the default completion gate. Run the smallest
      meaningful local test set for the changed scope, then record any known
      gaps in the commit message.
- [x] GitHub Actions budget freeze: do not manually dispatch workflows, do not
      trigger avoidable CI runs, and cancel already-running Actions when the
      owner declares CI budget exhausted.
- [x] While the CI budget freeze is active, do not push branch or `master`
      updates that would start GitHub CI. Keep completed merges local until the
      owner explicitly allows remote pushes/CI again.
- [x] Each feature cycle must be independent: create branch -> implement ->
      local test -> commit -> merge to `master` -> start the next feature from a
      new branch.

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
- [x] Service-free embedded/library smoke for registry composition, `ConnectionManager`, `SafetyGuard`, and `FlowControl` without spawning the CLI.

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

- [x] `ping`, `caps`, `conn list/add/remove`, `sql`, `kv`, `doc`, `mq`, `ts`, `search` command shell.
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
- [x] Negotiated atomic KV value/absolute-expiry snapshot and restore contract.
- [x] KV artifact v3 preserves per-key lifetime, rejects v2 offline, and skips expired entries without reviving them.
- [x] Redis Docker v3 lifecycle coverage for persistent/binary/empty/long/expired keys, confirmation binding, and cleanup.
- [x] Refresh KV artifact v3 lifecycle against real Valkey, KeyDB, and Dragonfly before closing IF-T64.
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
- [x] Cassandra/ScyllaDB CQL adapter through a dedicated CQL command surface plus the SQL-compatible path, with real Cassandra live coverage.

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
- [x] Docker Compose OpenSearch security-plugin TLS profile with generated local CA/node certs.
- [x] Docker Compose product-native Elasticsearch profile for search compatibility drift checks.
- [x] Custom project name, database names, credentials, and host ports through environment variables.
- [x] CPU/memory/resource limits for integration services.
- [x] Integration scripts for up/down/test lifecycle.
- [x] Local DB suite runner for phase-based Compose config, service-free, base, fixture, roundtrip, compatibility, TiDB, and opt-in heavy profile automation.
- [x] Service-free smoke script for core SQLite CRUD, safety, result limiting, and configured timeout behavior.
- [x] Docker-backed flow-control smoke script for live request timeout, rate/admission flags, SQL/KV/document limits, and disposable test data cleanup.
- [x] Docker-backed server-side SQL timeout smoke script for PostgreSQL `statement_timeout`, `idle_in_transaction_session_timeout`, `lock_timeout`, and MySQL `innodb_lock_wait_timeout`.
- [x] Docker-backed connection-config smoke script for live `connections.toml` named SQL/KV/document connections and connection-level timeout behavior.
- [x] Docker-backed custom environment smoke script for custom project name, database names, credentials, and host ports.
- [x] Reusable file-backed base fixture data for PostgreSQL, MySQL, Redis, and MongoDB with a Docker-backed CLI verification script.
- [x] Dockerfile-backed fixture database images for PostgreSQL, MySQL, Redis, and MongoDB with a Docker-backed CLI verification script.
- [x] dbtool-mediated logical data roundtrip smoke for PostgreSQL, MySQL, Redis, and MongoDB export/restore verification.
- [x] Docker image smoke script that runs the same core SQLite flow inside the containerized dbtool CLI.
- [x] Live CLI tests for SQL, KV, and document workflows.
- [x] Live CLI tests for MySQL protocol aliases and Redis-compatible protocol aliases.
- [x] Live CLI tests for real MariaDB, Valkey, KeyDB, and Dragonfly compatibility services.
- [x] Live CLI tests for real TiDB compatibility service.
- [x] Env-gated external Redshift compatibility smoke for supplied Redshift endpoints.
- [x] Live CLI tests for TiDB auth, SQL TLS, component TLS, X509, and local HA topology.
- [x] TiDB secure HA SQL-node failover drill script with CI manual entry and documented pass/fail criteria.
- [x] TiDB secure HA PD leader outage drill with TLS PD API leader discovery and documented pass/fail criteria.
- [x] TiDB secure HA TiKV outage boundary drill with dbtool request/deadline bounded probe behavior.
- [x] TiDB secure HA certificate regeneration cold-restart drill with documented pass/fail criteria.
- [x] TiDB secure HA logical data roundtrip smoke with TLS cross-node export/restore/readback.
- [x] Live CLI test entry for SQL Server SQL lifecycle, typed values, limiting, tables, and schema.
- [x] File-backed Cassandra/CQL fixture data with Docker-backed dbtool readback, table listing, and schema verification.
- [x] Live CLI tests for Redis Streams/PubSub, Kafka, AMQP, and NATS messaging workflows.
- [x] Live CLI tests for AMQPS and NATS TLS messaging workflows.
- [x] Env-gated vendor Kafka-compatible smoke for externally supplied AutoMQ, WarpStream, and Confluent endpoints.
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
- [x] Bounded query command history with Up/Down recall and write-confirmation-aware recording.
- [x] Full-screen operational status header/footer with wrapped input and result panels.
- [x] Richer per-capability command forms for SQL, KV, document, search, and time-series operations.

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
| T12 TUI workflow depth | P2 | Done | Added command history, full-screen status polish, and per-capability command forms while reusing the existing query execution, write confirmation, and limit behavior. | `cargo test -p dbtool-tui`; covered by `./scripts/verify.sh`. |
| T13 SQL Server adapter gate | P3 | Done | Added `adapter-sqlserver`, `sqlserver://` and `mssql://` registration, Docker profile, integration scripts, and CLI live lifecycle coverage. The heavyweight Docker profile passed on GitHub Actions x86_64 runner via workflow_dispatch run `27592553564`. | `cargo test -p adapter-sqlserver`; `docker compose -f docker-compose.integration.yml --profile sqlserver config`; `./scripts/integration-sqlserver-test.sh`. |
| T14 Cassandra/CQL adapter gate | P3 | Done | Accepted a constrained CQL-over-`SqlEngine` surface, added `adapter-cassandra`, `cassandra://` and `scylla://` registration, Docker profile, integration scripts, and CLI live lifecycle coverage. The local Docker profile passed with address translation enabled for host-port mapping. | `cargo test -p adapter-cassandra`; `docker compose -f docker-compose.integration.yml --profile cassandra config`; `./scripts/integration-cassandra-test.sh`. |
| T15 Prometheus remote write | P3 | Done | Added Prometheus remote write without new dependencies by hand-encoding the minimal protobuf WriteRequest and Snappy literal block. CLI and TUI expose explicit time-series writes behind the existing `--allow-write`/pending-write gate. | `cargo test -p adapter-timeseries`; `cargo test -p dbtool-cli ts::tests`; `cargo test -p dbtool-tui`; covered by `./scripts/verify.sh`. |
| T16 Production TiDB HA drills | P3 | Done | Added a secure HA SQL-node failover drill, a PD single-node outage drill, a PD leader outage drill, a TiKV outage boundary drill, a certificate regeneration cold-restart drill, a logical data roundtrip smoke, and a TiProxy drill. Added `testdata/tidb-ha-drills.manifest` plus a service-free validator that proves each drill script, db-suite phase, and documented pass/fail section remains wired together. Production TiKV failover, online certificate rotation, product-native backup/restore, upgrade, and existing-session migration remain explicit boundaries outside the local dbtool HA harness. | `./scripts/integration-tidb-ha-drill.sh`; `./scripts/integration-tidb-pd-drill.sh`; `./scripts/integration-tidb-pd-leader-drill.sh`; `./scripts/integration-tidb-tikv-outage-boundary.sh`; `./scripts/integration-tidb-cert-regeneration-test.sh`; `./scripts/integration-tidb-logical-roundtrip-test.sh`; `./scripts/integration-tidb-tiproxy-test.sh`; `./scripts/validate-tidb-ha-drills.sh`; covered by `./scripts/verify.sh`; local-only while CI budget is frozen. |
| T17 Database fixture Dockerfiles | P2 | Done | Added Dockerfile-backed fixture images for PostgreSQL, MySQL, Redis, and MongoDB so database containers can start with reusable test data already loaded, then verified dbtool readback against the `fixture-images` Compose profile. | `./scripts/integration-fixture-images-test.sh`; `./scripts/validate-compose-configs.sh`; local-only while CI budget is frozen. |
| T18 Base database logical roundtrip | P2 | Done | Added a dbtool-mediated logical export/restore smoke for PostgreSQL, MySQL, Redis, and MongoDB. The script exports shared fixture data to `.tmp/`, restores it into independent tables/key prefixes/collections, and reads it back through dbtool. | `./scripts/integration-data-roundtrip-test.sh`; local-only while CI budget is frozen. |
| T19 TiDB secure logical roundtrip | P2 | Done | Added a TiDB secure HA logical export/restore smoke. The script exports rows through one TLS SQL node, restores into an independent table through the second SQL node, and verifies cross-node readback through dbtool. | `./scripts/integration-tidb-logical-roundtrip-test.sh`; local-only while CI budget is frozen. |
| T20 Local DB suite automation | P2 | Done | Added a phase-based local suite runner so the database verification flow can be run from one entrypoint. The default set covers Compose config validation, service-free checks, base DB workflows, flow-control, server timeout, live connection config, custom env, fixture data/images, logical roundtrip, compatibility profiles, and TiDB; heavy phases, including the dbtool Docker image smoke, remain explicit through `DBTOOL_IT_DB_SUITE_PHASES=heavy` or `all`. | `./scripts/integration-db-suite.sh`; `DBTOOL_IT_DB_SUITE_DRY_RUN=1 DBTOOL_IT_DB_SUITE_PHASES=all ./scripts/integration-db-suite.sh`; local-only while CI budget is frozen. |
| T21 Live connection config smoke | P2 | Done | Added a Docker-backed temporary `connections.toml` smoke for PostgreSQL, MySQL, Redis, and MongoDB. The script uses named connections for pings and SQL/KV/document operations, and verifies connection-level timeout limits against a live PostgreSQL `pg_sleep` query. | `./scripts/integration-connection-config-test.sh`; included in the default `./scripts/integration-db-suite.sh`; local-only while CI budget is frozen. |
| T22 Custom DB environment smoke | P2 | Done | Added Docker-backed custom env smoke for PostgreSQL, MySQL, Redis, and MongoDB. The script starts the base services with custom project name, database names, credentials, and host ports, then verifies generated DSNs plus dbtool read/write behavior. | `./scripts/integration-custom-env-test.sh`; included in the default `./scripts/integration-db-suite.sh`; local-only while CI budget is frozen. |
| T23 Server-side SQL timeout smoke | P2 | Done | Added Docker-backed database-side timeout coverage for PostgreSQL and MySQL. The script keeps dbtool client deadlines larger than DB-side timeout budgets, then proves PostgreSQL `statement_timeout` cancels slow statements, `idle_in_transaction_session_timeout` terminates idle transactions, `lock_timeout` cancels blocked writes, and MySQL `innodb_lock_wait_timeout` cancels blocked dbtool writes at the server layer. | `./scripts/integration-server-timeout-test.sh`; included in the default `./scripts/integration-db-suite.sh`; local-only while CI budget is frozen. |
| T24 Cassandra fixture data smoke | P2 | Done | Added reusable CQL fixture data and a Docker-backed Cassandra smoke. The script loads `testdata/base-cassandra-seed.cql` through dbtool, verifies seeded row readback, table listing, and schema inspection, and keeps the slower JVM-backed check in the heavy suite. | `./scripts/integration-cassandra-fixture-data-test.sh`; `DBTOOL_IT_DB_SUITE_PHASES=cassandra-fixture ./scripts/integration-db-suite.sh`; local-only while CI budget is frozen. |
| T25 Embedded library smoke | P1 | Done | Added a service-free Rust integration test proving consumers can use `dbtool-core` and `dbtool-registry` directly: build the registry, reuse a SQLite connector through `ConnectionManager`, apply `SafetyGuard`, and run SQL under `FlowControl` without spawning the CLI. | `cargo test -p dbtool-registry --test embedded_library`; covered by `./scripts/verify.sh`. |
| T26 TUI command history | P2 | Done | Added bounded in-memory command history for the TUI query panel. Up/Down recalls prior commands without losing the current draft, adjacent duplicates are skipped, history is capped, read commands record after execution, and pending writes record only after confirmation. | `cargo test -p dbtool-tui`; covered by `./scripts/verify.sh`. |
| T27 TUI full-screen status polish | P2 | Done | Added a full-width operational status header and footer to the TUI, including selected connection, read/write mode, limit, command-history count, active panel, pending-write status, result size, and wrapped query/result panels. | `cargo test -p dbtool-tui`; covered by `./scripts/verify.sh`. |
| T28 TUI capability forms | P2 | Done | Added a form palette for SQL, KV, document, search, and time-series commands. Forms expose editable fields, generate the existing command syntax, and keep execution on the same shared safety and dispatch path. | `cargo test -p dbtool-tui`; covered by `./scripts/verify.sh`. |
| T29 TiDB HA drill manifest | P3 | Done | Added a service-free manifest and validator for the TiDB secure HA drill chain so scripts, suite phases, documentation headings, and known production boundaries cannot drift silently. | `./scripts/validate-tidb-ha-drills.sh`; covered by `./scripts/verify.sh`. |
| T30 Prometheus remote write | P3 | Done | Implemented no-dependency Prometheus remote write support for `TimeSeriesStore::write_points`, exposed `ts write` in CLI, and added TUI write parsing/forms that reuse the existing write-confirmation path. | `cargo test -p adapter-timeseries`; `cargo test -p dbtool-cli ts::tests`; `cargo test -p dbtool-tui`; covered by `./scripts/verify.sh`. |
| T31 CLI discoverability polish | P3 | Done | Added user-facing help text for KV and document subcommands so `dbtool kv --help` and `dbtool doc --help` describe read/write behavior, JSON inputs, scan bounds, and raw command arguments. | `cargo test -p dbtool-cli --test cli_json cli_help_documents_core_command_families`; covered by targeted CLI help assertions. |
| T32 Shell completions and manpage artifacts | P3 | Done | Added a hidden clap-metadata artifact generator, release archive packaging for bash/zsh/fish completions and `dbtool.1`, and npm/Python wrapper package inclusion for the same generated files. | `cargo test -p dbtool-cli --test cli_json cli_generate_artifacts_writes_completion_and_manpage_files`; `./scripts/smoke-release-artifacts.sh` checks archive contents. |
| T33 Full CLI help coverage | P3 | Done | Expanded root, SQL, search, time-series, messaging, and connection help text with safety boundaries, JSON input expectations, bounded consume/query behavior, and concrete examples without changing command execution. | `cargo test -p dbtool-cli --test cli_json cli_help_documents_core_command_families`; covered by `./scripts/verify.sh`. |
| T34 Dedicated CQL command surface | P2 | Done | Added `CqlEngine`, a `cql` capability bit, Cassandra/ScyllaDB `as_cql` support, and `dbtool cql query/exec/keyspaces/tables/schema` while preserving the existing SQL-compatible CQL path. | `cargo test -p adapter-cassandra`; `cargo test -p dbtool-cli --test cli_json cql_exec_requires_write_flag_before_connecting`; `cargo test -p dbtool-cli --test live_services cassandra_live_cql_lifecycle_and_typed_values`; covered by `./scripts/verify.sh`. |
| T35 Generic export/import CLI | P2 | Done | Added public read-only `export sql/kv/doc` commands and write-gated `import sql/kv/doc` commands using versioned JSON artifacts. The base database roundtrip smoke now uses only these public commands for PostgreSQL, MySQL, Redis, and MongoDB restores. | `cargo test -p dbtool-cli --test cli_json export_import_sql_round_trips_sqlite_rows`; `cargo test -p dbtool-cli transfer::tests`; `./scripts/integration-data-roundtrip-test.sh`; local-only while CI budget is frozen. |
| T36 Product-native Elasticsearch profile | P2 | Done | Added an opt-in `elasticsearch` Compose profile, up/test scripts, manual CI input, db-suite heavy phase, and a live test that exercises `elasticsearch://` ping, write guard, document indexing, search, and index listing against the real Elasticsearch image. | `docker compose -f docker-compose.integration.yml --profile elasticsearch config`; `DBTOOL_IT_DB_SUITE_DRY_RUN=1 DBTOOL_IT_DB_SUITE_PHASES=elasticsearch ./scripts/integration-db-suite.sh`; optional `./scripts/integration-elasticsearch-test.sh`. |
| T37 Vendor Kafka-compatible smoke profiles | P2 | Done | Added an env-gated vendor Kafka smoke script for AutoMQ, WarpStream, and Confluent endpoints. The script runs the native Kafka backend, maps DSN username/password plus selected SASL/TLS query params into librdkafka config, and skips safely when no external DSNs are supplied. | `cargo test -p dbtool-cli --no-default-features --features full-native --test live_messaging vendor_kafka_compatible_smoke_profiles`; `DBTOOL_IT_DB_SUITE_DRY_RUN=1 DBTOOL_IT_DB_SUITE_PHASES=kafka-vendors ./scripts/integration-db-suite.sh`; optional `./scripts/integration-kafka-vendor-test.sh`. |
| T38 OpenSearch security-plugin TLS profile | P2 | Done | Added an opt-in `opensearch-security` Compose profile with generated local CA/node certificates, HTTPS transport, basic auth, up/test scripts, manual CI input, db-suite heavy phase, and a live test for `opensearch+https://admin:...?...tls-ca=...` ping, write guard, index, search, and index listing. | `docker compose -f docker-compose.integration.yml --profile opensearch-security config`; `DBTOOL_IT_DB_SUITE_DRY_RUN=1 DBTOOL_IT_DB_SUITE_PHASES=opensearch-security ./scripts/integration-db-suite.sh`; optional `./scripts/integration-opensearch-security-test.sh`. |
| T39 External Redshift compatibility smoke | P2 | Done | Added an env-gated Redshift smoke script and manual CI input. The smoke uses a supplied `DBTOOL_IT_REDSHIFT_DSN` to verify the `redshift://` alias through ping, caps, typed query, result limiting, create/insert/query/schema/drop lifecycle, and skips safely when no DSN is supplied. | `cargo test -p dbtool-cli --test live_services redshift_external_sql_lifecycle_and_typed_values`; `DBTOOL_IT_DB_SUITE_DRY_RUN=1 DBTOOL_IT_DB_SUITE_PHASES=redshift ./scripts/integration-db-suite.sh`; optional `./scripts/integration-redshift-test.sh`. |
| T40 IBM Db2 adapter | P3 | Done | Added `adapter-db2` using ODBC, DSN scheme `db2://`, aliases `ibmdb2://`/`as400://`, full `SqlEngine` surface, `db2` Docker Compose profile, up/test integration scripts, and a live CLI lifecycle test. The live Docker profile requires IBM Data Server Driver for ODBC at the OS level; this is an explicit runtime boundary — service-free adapter tests pass everywhere, and the Docker profile plus integration script are available when the IBM ODBC runtime is installed. | `cargo test -p adapter-db2`; `docker compose -f docker-compose.integration.yml --profile db2 config`; optional `./scripts/integration-db2-test.sh` with `DBTOOL_RUN_DB2_INTEGRATION=1`. |
| T41 Per-backend completeness ledger | P1 | Done | Added a machine-readable manifest, human task table, evidence contract, and service-free validator that separates harness-ready, live-pass, blocked, partial, and external states. | `./scripts/validate-db-completeness.sh`; covered by `./scripts/verify.sh`. |
| T42 Real backend completeness campaign | P1 | Done | Every locally available registered backend completed its family checklist. The final campaign state is 22 COMPLETE, 2 BLOCKED, 2 EXTERNAL, and 1 PARTIAL; unavailable architecture/runtime/credential boundaries remain explicit rather than being treated as passes. | `docs/db-completeness-tasks.md`; `testdata/db-completeness.manifest`; `./scripts/validate-db-completeness.sh`. |
| IF-T43–IF-T66 Interface completion campaign | P0 | In progress | Close the remaining declared-interface gaps, feature/release mismatches, TUI safety holes, CLI usage and capability negotiation; synchronize the current design and rebuild/package the project. Redis exact SCAN and binary/raw safety, SQL transactional import, messaging resource lifecycle plus typed stateful-consume contract, RabbitMQ fail-closed management detail, atomic named-connection CRUD, explicit Document one/many mutation semantics, target-faithful release packaging, KV expiry preservation, and bounded metadata catalogs are included; protocol-specific stateful consumption remains in progress and external prerequisites stay separately blocked. | `docs/interface-completion-tasks.zh-CN.md`; one independently verifiable feature per Lore commit. |
