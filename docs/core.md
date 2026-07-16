# Core Implementation Notes

`dbtool-core` is the dependency-inversion boundary for the workspace. Adapters implement traits from core; frontends call core services and registry APIs.

## DSN Handling

`Dsn::parse` expands `${VAR}` placeholders before storing `Dsn::raw`, so adapters receive the actual connection string. Use `Dsn::redacted()` or `redact_dsn` for logs and command output.

Redacted DSNs are display labels, not unique connection identities. Destructive
callers use `SafetyGuard::bind_target_scope(display, resolved_dsn)` so token
calculation includes a hidden digest of the complete expanded target while the
public impact object retains only the display label. Never persist the bound
internal string in logs or transfer artifacts.

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
validate legacy item budgets and N+1 probes. New first-party row/document paths use
`ReadBudget`; complete schema, DDL, topic detail, and lag paths use `MetadataBudget`.
Both pair item accounting with a caller byte ceiling and exact negotiated operations.
Top-level catalogs now use exact `*_budgeted` siblings so every complete scalar/object,
the returned envelope, and the N+1 probe are caller-byte-accounted. Legacy item-only
methods remain only for the IF-T75 compatibility lifecycle.

These services are intentionally adapter-agnostic so the CLI, TUI, and embedded library paths can share the same behavior.

`SafetyGuard` strips only its validated internal hidden-scope suffix when it
builds `impact.target`, but uses the complete bound target for token calculation.
This preserves deterministic two-process confirmation without publishing raw
credentials or a public DSN fingerprint.

The service-free embedded smoke lives in
`crates/dbtool-registry/tests/embedded_library.rs`. It builds the adapter
registry directly, reuses a SQLite connector through `ConnectionManager`,
checks `SafetyGuard` confirmation behavior, and runs a query under
`FlowControl` without spawning the CLI binary.
