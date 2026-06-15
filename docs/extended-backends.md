# Extended Backend Plan

This document captures the remaining heavyweight protocol work. These backends
should not be registered as fake adapters: until a protocol client exists,
`UNSUPPORTED_SCHEME` is clearer than a connector that cannot actually connect.

## SQL Server

Recommended adapter shape:

- Crate: `adapter-sqlserver`
- Scheme: `sqlserver://`
- Capability: `SqlEngine`
- Likely client dependency: a TDS client such as `tiberius`
- Docker image: `mcr.microsoft.com/mssql/server`
- Resource note: SQL Server images are large and usually need at least 2 GiB
  memory; keep it in an opt-in profile separate from default integration tests.

Acceptance tasks:

- [ ] Add the adapter crate and feature gate.
- [ ] Parse `sqlserver://user:pass@host:port/database` DSNs and preserve
      caller-facing kind.
- [ ] Implement `ping`, `query`, `execute`, `list_tables`, and `describe_table`.
- [ ] Map SQL Server scalar types into core `Value`.
- [ ] Add SQL safety and limiter coverage through existing CLI paths.
- [ ] Add `sqlserver` Docker profile, up/test scripts, CI workflow input, and
      live CLI lifecycle tests.

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

Both adapters require new protocol dependencies and heavier Docker images. Add
them only when a dedicated implementation task explicitly accepts those costs.
Until then, keep the schemes unregistered so unsupported targets fail early and
honestly.
