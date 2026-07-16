# dbtool Final Goal Audit

Last updated: 2026-07-16

This audit maps the final objective to current, repo-verifiable evidence. It is
not a replacement for optional live Docker drills, but it proves the codebase has
the required architecture, packaging, safety gates, and protocol coverage wired
together.

## Verdict

The repo satisfies the original dbtool baseline objective at the project level:

- one shared Rust core supports CLI/Claude Skill, TUI, and embedded-library use;
- adapters are registered by protocol family and reuse aliases for compatible
  products;
- release packaging targets Linux, macOS, and Windows on x64 and arm64;
- npm, pip/uv, and mise install paths are present;
- connection resolution supports raw DSNs, named connections, and
  `DBTOOL_CONN_*`;
- default safety is read-first with explicit write gates, destructive confirm
  tokens, flow-control limits, timeouts, and retry budgets.

Product-specific production-readiness exercises remain explicit boundaries in
[implementation-status.md](implementation-status.md), not missing pieces of this
objective.

The real-product validation campaign is reported separately: 22 tasks are
`COMPLETE`, 2 are `BLOCKED`, 2 are `EXTERNAL`, and 1 is `PARTIAL`.
Those five non-complete products are not silently counted as passes.

## Active Interface Completion Campaign

The baseline verdict does not claim that every newly identified interface and
release-hardening enhancement is already complete. The active Chinese task
board is [interface-completion-tasks.zh-CN.md](interface-completion-tasks.zh-CN.md).
It tracks declared-interface gaps, Cargo feature correctness, TUI safety, CLI
strictness, packaging/install smoke, and explicit external boundaries.

Current interface result: IF-T43–50, IF-T53–67, and IF-T69–79 are complete
(34 tasks). The declared read, write, catalog, configuration, CLI/TUI, and
legacy-API contracts are implemented. The remaining three tasks are release or
external evidence gates rather than hidden adapter fallbacks: IF-T51 requires
the final local gate plus six-platform CI, IF-T52 requires unavailable product
runtimes/DSNs, and IF-T68 requires current-SHA Windows x64 runtime plus arm64
compile/link evidence.

## Requirement Evidence

| Requirement | Evidence |
| --- | --- |
| Single shared core | `crates/dbtool-core` defines models, connector traits, DSN parsing, formatter, safety, limiter, and flow-control services. |
| CLI / Claude Skill shape | `crates/dbtool-cli` exposes `ping`, `caps`, `conn`, `sql`, `cql`, `kv`, `doc`, `export`, `import`, `mq`, `search`, and `ts` with help text for safety boundaries and JSON inputs; `SKILL.md` documents machine-readable usage. |
| TUI shape | `crates/dbtool-tui` uses the same registry/connection manager, write confirmation, command history, status layout, and capability forms. |
| Embedded-library shape | `crates/dbtool-registry/tests/embedded_library.rs` builds the registry directly and uses `ConnectionManager`, `SafetyGuard`, and `FlowControl` without spawning the CLI. |
| Protocol-family reuse | `crates/dbtool-core/src/registry/alias.rs` maps compatible schemes to canonical families including MySQL/MariaDB/TiDB, PostgreSQL/Cockroach/Timescale/Redshift, Redis/Valkey/KeyDB/Dragonfly, Kafka/AutoMQ/Redpanda/WarpStream/Confluent, and OpenSearch/Elasticsearch; external Redshift and Kafka vendor smokes verify supplied non-local endpoints without committing secrets. |
| SQL, CQL, NoSQL, search, time-series coverage | SQL, Cassandra/ScyllaDB CQL, Redis-compatible KV, MongoDB documents, OpenSearch/Elasticsearch search, and Prometheus time-series adapters are implemented and listed in `docs/implementation-status.md`; public export/import commands cover logical SQL row, KV, and document transfers, while OpenSearch security-plugin TLS and product-native Elasticsearch have opt-in live profiles. |
| Messaging coverage | Kafka/Redpanda-compatible, env-gated AutoMQ/WarpStream/Confluent vendor smoke, AMQP/RabbitMQ, Redis Streams/PubSub, RabbitMQ management, and NATS/JetStream coverage are implemented and documented. |
| Cross-platform release | `.github/workflows/release.yml`, `scripts/package-release.sh`, `scripts/package-npm.mjs`, `scripts/package-python-wheel.py`, and `dist/mise/README.md` all list Linux musl, macOS, and Windows targets for x64 and arm64. |
| Single binary artifact | Release archives contain `dbtool` or `dbtool.exe`; `scripts/smoke-binary.sh` and `scripts/smoke-release-artifacts.sh` validate packaged binaries. |
| Completion and manpage artifacts | `dbtool generate-artifacts` emits bash/zsh/fish completions and `dbtool.1` from clap metadata; release, npm, and Python package scripts include those files. |
| npm / pip / uv / mise install paths | `dist/npm`, `scripts/package-npm.mjs`, `dist/python`, `scripts/package-python-wheel.py`, and `dist/mise/README.md` provide wrapper packages and mise/ubi metadata. |
| Environment connections | `ConnectionResolver` handles raw DSNs, named config, and `DBTOOL_CONN_*`; `docs/connections.example.toml` documents named connection config. |
| Read-only default | CLI tests cover write refusal before connection for search and time-series writes; SQL safety tests cover read/write/destructive classification. |
| Destructive confirmation | `SafetyGuard` issues target-bound confirm tokens for destructive SQL; CLI JSON tests cover two-step confirmation. |
| Flow control / no hang | `FlowControl` covers concurrency, rate limiting, acquire timeout, request timeout, overall deadline, and retry budget; service-free and Docker-backed smoke scripts exercise bounded behavior. |
| Local integration automation | `scripts/integration-db-suite.sh`, `scripts/validate-compose-configs.sh`, and the TiDB HA manifest validator keep the live verification matrix discoverable and bounded. |

## Repo-Level Completion Gate

Run:

```bash
./scripts/validate-final-goal.sh
./scripts/verify.sh
```

`validate-final-goal.sh` is service-free. It checks that target matrices,
package wrappers, safety evidence, protocol aliases, task status, and this audit
remain synchronized. It reports the baseline result and the active interface
campaign status separately instead of treating an active enhancement queue as a
failure of the already-proven baseline.
