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

Recommended adapter shape:

- Crate: `adapter-cassandra`
- Scheme: `cassandra://`
- Capability: most likely a future CQL-specific capability, or a constrained
  SQL-like query surface only if the design explicitly accepts that mismatch.
- Likely client dependency: a CQL client such as `scylla`
- Docker image: `cassandra` or `scylladb/scylla`
- Resource note: Cassandra/Scylla containers are memory-heavy and slower to
  become healthy; keep them opt-in and isolated from default verification.

Acceptance tasks:

- [ ] Decide whether Cassandra belongs behind a new `CqlEngine` trait instead
      of `SqlEngine`.
- [ ] Add the adapter crate and feature gate after the trait decision.
- [ ] Parse `cassandra://user:pass@host:port/keyspace` DSNs.
- [ ] Implement ping, bounded query, keyspace/table discovery, and schema
      inspection.
- [ ] Map CQL primitive, collection, timestamp, UUID, and null values into core
      `Value`.
- [ ] Add Cassandra or Scylla Docker profile, up/test scripts, CI workflow input,
      and live CLI lifecycle tests.

## Dependency Gate

SQL Server now carries its explicit TDS dependency and isolated Docker profile.
Cassandra still requires a new protocol dependency and heavier Docker image. Add
the Cassandra scheme only when a dedicated implementation task explicitly
accepts the CQL trait and resource costs. Until then, keep the scheme
unregistered so unsupported Cassandra targets fail early and honestly.
