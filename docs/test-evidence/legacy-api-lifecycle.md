# Legacy API Lifecycle Evidence

Task: IF-T75

Result: COMPLETE

Run date: 2026-07-16

## Compatibility contract

The core retains every public unbounded or item-only trait method throughout
the `1.x` line so existing embedded implementors are not broken by a patch
release. All 64 methods now have a Rust `#[deprecated]` attribute whose note
names the exact replacement and states that removal is allowed no earlier than
`2.0.0`. Exact methods continue to default to `UNSUPPORTED_CAPABILITY`; none of
them calls a legacy method when an adapter has not implemented the exact
contract.

| Trait | Deprecated methods | Exact migration family |
| --- | ---: | --- |
| `SqlEngine` | 9 | budgeted query/execute/import/catalog/schema |
| `CqlEngine` | 8 | budgeted query/execute/catalog/schema |
| `KeyValueStore` | 7 | bounded reads plus budgeted writes/raw I/O |
| `DocumentStore` | 13 | budgeted catalog/find/CRUD/aggregate/drop |
| `TimeSeriesStore` | 4 | budgeted catalog/write/range query |
| `SearchEngine` | 9 | budgeted catalog/search/get/mutations |
| `MessageProducer` | 1 | budgeted production |
| `AdminInspect` | 4 | budgeted topic catalog/detail/lag |
| `Db2Engine` | 9 | budgeted catalogs/detail/DDL |
| **Total** | **64** | no first-party exact-to-legacy fallback |

`DocumentStore::update_many -> update` and `delete_many -> delete` are the only
production legacy-to-legacy default bridges. Both carry a method-local
allowance and exist solely to preserve the historical `1.x` bulk contract.
They are not used by exact default methods. Other intentional legacy calls are
confined to locally annotated compatibility assertions.

## First-party migration proof

The following command was run with the current workspace source:

```text
RUSTFLAGS='-D deprecated' cargo check --workspace --lib --bins
```

Result: PASS (exit 0, 13m24s). This compiled core, registry, CLI, TUI, and every
adapter production target while promoting any unapproved legacy trait call to
a hard error. Same-named calls into database drivers were classified separately
because they are not dbtool capability-trait calls.

Additional gates:

| Check | Result |
| --- | --- |
| `cargo test -p dbtool-core` | 151 unit + 1 doctest PASS |
| `cargo clippy -p dbtool-core --all-targets -- -D warnings` | PASS |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p dbtool-core --no-deps` | PASS |
| Kafka pure/native and NATS exact-path regression | 21+1 / 30 / 20+3 PASS |
| Deprecation count and `2.0.0` migration-note count | 64 / 64 PASS |

## Migration commits

- `7c5bab6` freezes the 64-method compatibility window and exact replacement
  notes.
- `c24c7b5`, `096d7cc`, and `d6801a5` keep CLI, TUI, transfer, and embedded
  examples on exact mutation paths.
- `8a91418`, `fd0ff0c`, and `2b0a6a0` remove first-party validation and adapter
  exact-to-legacy coupling.
- `bf6985a` keeps Kafka/NATS fixtures exact while retaining only explicit
  compatibility probes.

## Boundary

Third-party crates outside this workspace were not compiled. They remain source
compatible through `1.x` but will receive actionable deprecation warnings.
Removal or a signature-breaking change requires the `2.0.0` boundary and a
separate migration decision.
