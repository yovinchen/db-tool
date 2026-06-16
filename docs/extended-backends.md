# Extended Backend Plan

This document captures the remaining heavyweight protocol work. These backends
should not be registered as fake adapters: until a protocol client exists,
`UNSUPPORTED_SCHEME` is clearer than a connector that cannot actually connect.

## SQL Server

Recommended adapter shape:

- Crate: `adapter-sqlserver`
- Scheme: `sqlserver://`
- Capability: `SqlEngine`
- Client dependency: `tiberius`
- Docker image: `mcr.microsoft.com/mssql/server`
- Resource note: SQL Server images are large and usually need at least 2 GiB
  memory; keep it in an opt-in profile separate from default integration tests.

Acceptance tasks:

- [x] Add the adapter crate and feature gate.
- [x] Parse `sqlserver://user:pass@host:port/database` DSNs and preserve
      caller-facing kind.
- [x] Implement `ping`, `query`, `execute`, `list_tables`, and `describe_table`.
- [x] Map SQL Server scalar types into core `Value`.
- [x] Add SQL safety and limiter coverage through existing CLI paths.
- [x] Add `sqlserver` Docker profile, up/test scripts, CI workflow input, and
      live CLI lifecycle tests.
- [ ] Run the heavyweight live profile in an amd64-capable Docker environment
      and record the result in `docs/tasks.md`.

## Cassandra

Accepted adapter shape for the first implementation:

- Crate: `adapter-cassandra`
- Scheme: `cassandra://`
- Alias: `scylla://`
- Capability: constrained `SqlEngine` surface carrying CQL strings. This keeps
  existing CLI safety, output formatting, limits, and table/schema commands
  usable now. A future `CqlEngine` can be added if prepared values, paging,
  protocol-specific commands, or richer TUI forms need a cleaner boundary.
- Client dependency: `scylla`
- Docker image: `cassandra`
- Resource note: Cassandra/Scylla containers are memory-heavy and slower to
  become healthy; keep them opt-in and isolated from default verification.

Acceptance tasks:

- [x] Decide whether Cassandra belongs behind a new `CqlEngine` trait instead
      of `SqlEngine`.
- [x] Add the adapter crate and feature gate after the trait decision.
- [x] Parse `cassandra://user:pass@host:port/keyspace` DSNs.
- [x] Implement ping, bounded query, keyspace/table discovery, and schema
      inspection.
- [x] Map CQL primitive, collection, timestamp, UUID, and null values into core
      `Value`.
- [x] Add Cassandra or Scylla Docker profile, up/test scripts, CI workflow input,
      and live CLI lifecycle tests.
- [x] Run the heavyweight live profile and record the result in `docs/tasks.md`.

## Dependency Gate

SQL Server now carries its explicit TDS dependency and isolated Docker profile.
Cassandra now carries its explicit CQL dependency and isolated Docker profile.
Keep future heavyweight protocol dependencies behind explicit adapter crates,
Cargo features, and opt-in Compose profiles so default verification remains
service-free.
