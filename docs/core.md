# Core Implementation Notes

`dbtool-core` is the dependency-inversion boundary for the workspace. Adapters implement traits from core; frontends call core services and registry APIs.

## DSN Handling

`Dsn::parse` expands `${VAR}` placeholders before storing `Dsn::raw`, so adapters receive the actual connection string. Use `Dsn::redacted()` or `redact_dsn` for logs and command output.

## Protocol Families

Aliases live in `registry::alias::PROTOCOL_ALIASES`. Register adapters through `Registry::register_family` when a backend covers a whole protocol family.

Examples:

- `mysql`: `mariadb`, `tidb`
- `postgres`: `postgresql`, `cockroach`, `timescale`, `redshift`
- `redis`: `valkey`, `keydb`, `dragonfly`
- `kafka`: `automq`, `redpanda`, `warpstream`, `confluent`

## Named Connections

Use `ConnectionResolver` instead of duplicating lookup logic. The resolver accepts either a raw DSN or a connection name, then checks environment variables and `ConnectionConfig`.

## Safety And Limits

`SafetyGuard` classifies SQL before execution. `ResultLimiter` applies output-size constraints after adapter execution when pushdown is unavailable.

These services are intentionally adapter-agnostic so the CLI, TUI, and embedded library paths can share the same behavior.
