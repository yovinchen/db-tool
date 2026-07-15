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

## Capability Negotiation

`Capabilities` booleans are a backward-compatible family summary only.
Method dispatch uses `Connector::operations()` and the stable
`CapabilityOperation` names. The required order for CLI, TUI, and embedded
callers is:

```text
exact operation -> capability accessor -> method invocation
```

Missing operations fail with `UNSUPPORTED_CAPABILITY`; callers must not fall
back to an unbounded or weaker method. Optional groups include SQL atomic
import, KV expiry preservation, Document cardinality/lifecycle, bounded
catalogs, stateful messaging, and each partial-admin method. Typed consumers
reject operation names that their `CapabilityOperation` version does not know;
they must never grant support from an unknown string. An absent `operations`
field in a legacy report means an empty method-level set, not implicit support.

`CapabilityReport` keeps legacy booleans flattened and adds a sorted,
deduplicated `operations` array. See
[`interface-usage.zh-CN.md`](interface-usage.zh-CN.md) for the embedded calling
pattern and
[`test-evidence/capability-negotiation.md`](test-evidence/capability-negotiation.md)
for verification.

## Named Connections

Use `ConnectionResolver` instead of duplicating lookup logic. The resolver accepts either a raw DSN or a connection name, then checks environment variables and `ConnectionConfig`.

## Safety And Limits

`SafetyGuard` classifies SQL before execution. `ResultLimiter` and `ListLimiter`
validate caller budgets and N+1 probes; public user-query and top-level name
catalog paths push the bound into adapters or enforce an explicit protocol
response-size ceiling. Nested metadata collections remain tracked by IF-T67.

These services are intentionally adapter-agnostic so the CLI, TUI, and embedded library paths can share the same behavior.

The service-free embedded smoke lives in
`crates/dbtool-registry/tests/embedded_library.rs`. It builds the adapter
registry directly, reuses a SQLite connector through `ConnectionManager`,
checks `SafetyGuard` confirmation behavior, and runs a query under
`FlowControl` without spawning the CLI binary.
