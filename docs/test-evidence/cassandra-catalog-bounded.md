# Cassandra CQL Bounded Catalog Evidence

Task ID: IF-T66-CQL / IF-T74-CQL

Result: LIVE_PASS

Run at (UTC): 2026-07-15T22:03:58Z

Environment: Docker on macOS arm64; Rust 1.96.0; Cassandra 5.0

## Contract

- `cql.list_keyspaces_bounded` and `cql.list_tables_bounded` are independent
  method-level capabilities. The legacy `cql=true`, `sql=true`, and unbounded
  list methods never imply them.
- The Cassandra adapter validates `ListLimiter` and computes N+1 before the
  first session request. Zero and `usize::MAX` therefore fail before DSN
  connection or catalog access.
- Catalog queries use Scylla driver's async pager with a server page size of
  `min(N+1, 256)`. The adapter retains at most N+1 accepted items and drops the
  unread pager as soon as the probe item is observed.
- An explicit keyspace table request is restricted by
  `WHERE keyspace_name = ...` at the server. An all-user-table request streams
  bounded pages and excludes system keyspaces while collecting; it may scan
  more than one page to prove completeness, but never materializes the raw
  system catalog or more than N+1 returned table identities.
- Cassandra does not support a portable global catalog `ORDER BY`. Bounded
  results therefore retain backend scan order instead of claiming that a
  truncated subset is the global lexicographic prefix. Every returned table
  keeps its keyspace-qualified identity.
- Both the CQL wording and the legacy SQL wording exposed by this adapter
  advertise their own bounded operations. CLI `cql keyspaces` and
  `cql tables` require the CQL operations and never fall back.

## Verification

| Check | Result |
| --- | --- |
| Adapter unit and operation contract | 9/9 PASS |
| Existing Cassandra CQL CRUD and typed values | PASS |
| Existing bounded row pager N+1 | PASS |
| Three-table exact limit N=3 | 3 rows, `truncated=false` PASS |
| Three-table probe limit N=2 | 2 rows, `truncated=true` PASS |
| Keyspace exact N and N-1 probe | exact false / probe true PASS |
| Invalid zero/overflow limit before unreachable DSN | `CONFIG_ERROR` PASS |
| Method operations in public `caps` | four SQL/CQL bounded operations PASS |
| Cleanup | isolated bounded keyspace absent; shared test keyspace tables `[]` PASS |

Commands:

```text
DBTOOL_IT_KEEP_SERVICES=1 ./scripts/integration-cassandra-test.sh
cargo test -p adapter-cassandra --lib
cargo clippy -p adapter-cassandra --all-targets -- -D warnings
```

The live service was Apache Cassandra 5.0. The `scylla://` alias was exercised
against the same Cassandra-compatible protocol endpoint; a native ScyllaDB
product image was not started in this slice and is not represented as a
separate vendor LIVE_PASS here. A later product-native run is recorded in
[`scylladb.md`](scylladb.md).

## IF-T74 scalar-byte extension（2026-07-16）

- SQL alias 与 CQL surface 现在都独立广告并调用
  `list_schemas/list_keyspaces_budgeted`、`list_tables_budgeted`；CQL exact
  方法直接委托同一个 SQL exact 实现，避免两个入口产生不同计费语义。
- `ReadLimiter` 在 retention 前对完整 String/TableInfo 计费，最终再对
  `BoundedList` envelope 与唯一 N+1 probe 计费；任一 byte 超限都不返回部分值。
- page size 仍为 `min(N+1,256)`。无指定 keyspace 时，为过滤 system keyspace，
  驱动可能跨越更多原始系统行，但 caller-visible item 最多观察 N+1。

本轮 adapter 12/12 与 all-target Clippy PASS。Cassandra 5 Docker 同时比较 SQL/CQL
两入口：table exact bytes=103、keyspace exact bytes=160，N 成功、N-1 返回
`READ_BUDGET_EXCEEDED`、N+1 `truncated=true`；临时 keyspace 最终残留 0。
