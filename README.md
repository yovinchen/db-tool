# dbtool

dbtool is a Rust workspace for a unified data and message connection tool. The current implementation follows the design in [dbtool-design.md](dbtool-design.md): a small core crate defines DSN parsing, capability traits, safety guards, result limits, format envelopes, and a registry mechanism; adapters and frontends build on top of that core.

## Current Focus

The project is being completed core-first.

- `dbtool-core` is the stable foundation: domain models, ports, DSN parsing, registry, config loading, safety, limiting, and formatting.
- `dbtool-cli` is the first frontend that exercises the core.
- `dbtool-tui` is intentionally a lightweight shell for now.
- Message adapters are staged. Kafka pure/native feature selection is wired, with pure Rust as the default and native librdkafka available through `full-native`.

## Workspace Layout

```text
.
├── Cargo.toml
├── README.md
├── dbtool-design.md
├── crates
│   ├── dbtool-core       # Domain model, ports, DSN/config, registry, services
│   ├── dbtool-registry   # Composition root, feature-gated adapter registration
│   ├── dbtool-cli        # Machine-friendly CLI for direct use and Claude Code skills
│   ├── dbtool-tui        # Future interactive terminal UI
│   ├── adapter-sql       # MySQL/Postgres/SQLite protocol-family adapter
│   ├── adapter-redis     # Redis-compatible key-value adapter
│   ├── adapter-mongo     # MongoDB document adapter
│   ├── adapter-search    # OpenSearch/Elasticsearch HTTP search adapter
│   ├── adapter-timeseries # Prometheus HTTP time-series adapter
│   ├── adapter-kafka     # Kafka-compatible message adapter shell
│   ├── adapter-amqp      # AMQP plus RabbitMQ management adapter
│   └── adapter-nats      # NATS adapter shell
├── docs
├── tests
└── .github/workflows
```

## Core Concepts

`dbtool-core` owns the stable contracts:

- `model`: shared result, value, document, message, metadata, and time-series structs.
- `port`: capability traits such as `SqlEngine`, `KeyValueStore`, `DocumentStore`, `SearchEngine`, `MessageProducer`, and `AdminInspect`.
- `dsn`: DSN parsing, environment expansion, and safe redaction.
- `registry`: scheme-to-factory lookup plus protocol-family alias handling.
- `config`: named connection loading from `connections.toml` and `DBTOOL_CONN_*` environment variables.
- `service`: safety guard, result limiter, formatter, flow control, resolver, and long-lived connection manager.

Protocol aliases are centralized in `dbtool-core/src/registry/alias.rs`. For example, `postgresql`, `cockroach`, `timescale`, and `redshift` all resolve through the PostgreSQL protocol family.

## CLI Shape

All CLI responses are JSON envelopes by default:

```json
{
  "ok": true,
  "kind": "postgres",
  "data": {},
  "meta": {
    "elapsed_ms": 0,
    "truncated": false
  }
}
```

Examples:

```bash
cargo run -p dbtool-cli -- --dsn sqlite::memory: ping
cargo run -p dbtool-cli -- conn list
cargo run -p dbtool-cli -- --dsn sqlite::memory: sql query "select 1"
cargo run -p dbtool-cli -- --dsn sqlite::memory: --format table sql query "select 1 as id"
cargo run -p dbtool-cli -- --dsn sqlite::memory: --format ndjson sql query "select 1 as id"
cargo run -p dbtool-cli -- --conn redis-local kv get my-key
cargo run -p dbtool-cli -- --dsn opensearch://127.0.0.1:9200 search indices
cargo run -p dbtool-cli -- --dsn opensearch://127.0.0.1:9200 --limit 10 search search users --q '{"match_all":{}}'
cargo run -p dbtool-cli -- --dsn opensearch://127.0.0.1:9200 --allow-write search index users '{"name":"alice"}'
cargo run -p dbtool-cli -- --dsn prometheus://127.0.0.1:9090 ts measurements
cargo run -p dbtool-cli -- --dsn prometheus://127.0.0.1:9090 ts query up --last-minutes 10
```

Named connections resolve in this order:

1. `--dsn` raw DSN
2. `DBTOOL_CONN_<NAME>` environment variable
3. `connections.toml`

The default config path is:

- Unix/macOS: `$XDG_CONFIG_HOME/dbtool/connections.toml`, or `$HOME/.config/dbtool/connections.toml`
- Windows: `%APPDATA%\dbtool\connections.toml`

Example:

```toml
[connections.local-sqlite]
dsn = "sqlite::memory:"
readonly = true

[connections.prod-readonly]
dsn = "postgres://user:${DB_PASSWORD}@db.example.com/app"
readonly = true
```

## Safety Defaults

SQL execution is guarded before it reaches adapters:

- Read statements such as `SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`, and `WITH` are allowed.
- Non-destructive writes require `--allow-write`.
- Destructive operations such as `DROP`, `TRUNCATE`, `ALTER`, `CREATE`, and `DELETE` without `WHERE` require a two-step `--confirm <token>` flow.

This keeps the CLI non-interactive and machine-readable, which is important for Claude Code skill usage.

## Claude Skill Usage

The project-level [SKILL.md](SKILL.md) documents the automation contract for Claude-style callers. JSON remains the default machine-readable output, and `--format table` / `--format ndjson` are available for human inspection or pipeline use.

## Verification

Manifest-level validation that does not need dependency downloads:

```bash
cargo metadata --no-deps --format-version 1
```

Full checks require crates.io access the first time dependencies are downloaded:

```bash
./scripts/verify.sh
```

The script runs:

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Live database integration tests can start local Docker services with resource limits:

```bash
./scripts/integration-test.sh
```

Compatible database integration tests start real MariaDB and Valkey services by default:

```bash
./scripts/integration-compat-test.sh
```

Run the optional Redis-compatible matrix with KeyDB and Dragonfly as well:

```bash
DBTOOL_IT_COMPAT_EXTRA=1 ./scripts/integration-compat-test.sh
```

PostgreSQL-family compatibility tests start real CockroachDB and TimescaleDB services:

```bash
./scripts/integration-pg-compat-test.sh
```

TiDB compatibility uses a heavier PD/TiKV/TiDB profile:

```bash
./scripts/integration-tidb-test.sh
```

TiDB secure HA integration starts 3 PD, 2 TiKV, and 2 TiDB SQL nodes with component TLS, SQL TLS, `REQUIRE SSL`, and `REQUIRE X509` checks:

```bash
./scripts/integration-tidb-secure-test.sh
```

Live messaging integration tests start Redis, Redpanda, RabbitMQ, and NATS:

```bash
./scripts/integration-mq-test.sh
```

Kafka native/librdkafka can be tested against the same Docker services:

```bash
./scripts/integration-mq-native-test.sh
```

Search and time-series integration tests start OpenSearch, an
OpenSearch-compatible HTTPS harness, and Prometheus:

```bash
./scripts/integration-observability-test.sh
```

See [docs/integration-testing.md](docs/integration-testing.md) for custom project names, database names, ports, credentials, resource limits, and cleanup.

Messaging metadata has protocol-specific boundaries. See [docs/messaging-boundaries.md](docs/messaging-boundaries.md) for why AMQP queue listing needs RabbitMQ's management API, why NATS admin is JetStream-scoped, and why Redis Pub/Sub is live-only.

Heavyweight future adapters such as SQL Server and Cassandra are tracked in [docs/extended-backends.md](docs/extended-backends.md); they intentionally remain unregistered until their protocol dependencies and Docker resource costs are accepted.

OpenSearch and Elasticsearch-compatible endpoints can use plain HTTP or TLS:

```bash
dbtool --dsn opensearch://127.0.0.1:9200 search indices
dbtool --dsn opensearch+https://search.example.com:9200 search indices
dbtool --dsn elasticsearch+https://elastic.example.com:9200 search search logs '{"match_all":{}}'
```

## Distribution

Release builds compile each target once, upload raw binary artifacts, and reuse those artifacts for GitHub Release archives plus npm and pip/uv wrappers.

- GitHub Release assets: `dbtool-<tag>-<target>.tar.gz`
- npm wrapper: `@yovinchen/dbtool`
- pip/uv wrapper: `dbtool-bin`
- mise/ubi: consumes the GitHub Release asset names documented in [dist/mise/README.md](dist/mise/README.md)

## Implementation Status

- Core contracts and services: implemented as the main foundation.
- SQL/Redis/Mongo adapters: implemented and covered by service-free plus live-test paths, including real MariaDB, TiDB, TiDB auth/TLS/HA, Valkey, KeyDB, and Dragonfly compatibility profiles.
- OpenSearch/Elasticsearch HTTP/HTTPS search adapter: implemented for index listing, search, and single-document indexing; covered by service-free HTTP/TLS mapping tests, plain HTTP OpenSearch live tests, and HTTPS live transport tests.
- Prometheus HTTP time-series adapter: implemented for metric listing and range queries, with read-only semantics.
- Kafka adapter: pure Rust ping/list/detail/produce/consume implemented behind `full`; native librdkafka backend implemented behind `full-native`.
- Redis Streams/PubSub, AMQP, and NATS adapters: real bounded producer/consumer paths implemented; NATS JetStream admin and RabbitMQ management-backed queue discovery are implemented.
- TUI: basic connection picker, capability-aware command dispatch, read limits, and write confirmation are implemented; richer forms remain future work.
- Release packaging: GitHub Release archive, npm, pip/uv, and mise/ubi metadata are wired; signing/notarization is still future work.
