# KV / Redis 读取信封证据

Task ID: IF-T70

Result: LIVE_PASS

Run at (UTC): 2026-07-16

Environment: macOS arm64; Rust 1.96.0; Docker Redis 7.4.9、Valkey 8.1.8、KeyDB 6.3.4、Dragonfly 1.39.0

## 完成合同

公共 `KeyValueStore` 新增并独立协商以下 operation：

- `kv.exists`
- `kv.get_bounded`
- `kv.get_with_expiry_bounded`
- `kv.scan_bounded`
- `kv.raw_command_bounded`

旧 `key_value=true`、`kv.get`、`kv.scan` 或 `kv.raw_command` 均不能授权这些方法。
GET/expiry 在 Lua 内先 `EXISTS/STRLEN` 再返回 value；SCAN 在 Lua 内限制实际页的唯一 key
数量和原始字节；只读 RAW 在 Lua 内按命令语义限制逻辑 items、bulk bytes 和递归节点。
`ReadLimiter` 最后对 portable `Option<Bytes>`、`KeyValueSnapshot`、`BoundedList<String>` 或
typed `Value` 做精确 JSON 信封计费。

`GETDEL/LPOP/RPOP/SPOP/SET ... GET` 在 adapter 与 CLI 两层、远端 mutation 之前拒绝。
KV replace preflight 使用 `EXISTS`；export 对 scan、每键 snapshot、累计 entry 与最终 artifact
执行 caller-owned 字节预算，错误时不发布部分文件，也不覆盖旧文件。

## 验证矩阵

| 层级/产品 | 验证结果 |
| --- | --- |
| Core contract | 114 unit + 1 doctest；五个 exact operation、默认 fail-closed、optional/single envelope 字节 N/N-1 |
| Redis adapter | 43/43 unit；Clippy `-D warnings`；Lua 顺序、SCAN 去重/probe、RAW logical item/recursive hard cap |
| Redis Docker | bounded missing/empty/large GET、TTL snapshot、SCAN N/N+1/长 key、RAW MGET item/bytes、危险 mutation 不改变 string/list/set、DEL 零残留 |
| Redis 7.4.9 | 新 bounded 矩阵 PASS；完整 KV SET/GET/overwrite/NX+TTL/RAW/DEL/absence 生命周期 PASS |
| Valkey 8.1.8 | 新 bounded 矩阵 PASS；完整 KV 生命周期 PASS |
| KeyDB 6.3.4 | 新 bounded 矩阵 PASS；完整 KV 生命周期 PASS |
| Dragonfly 1.39.0 | 新 bounded 矩阵 PASS；完整 KV 生命周期 PASS |
| Transfer | Redis v3 TTL/bytes roundtrip、累计 byte failure 保留旧 artifact、replace `EXISTS` 预检不读取 4 KiB 旧值、清理 PASS |
| CLI/TUI | CLI 96 unit、KV integration 4、transfer 10、TUI 32、CLI/TUI Clippy `-D warnings` PASS |

代表性 live 命令（必须设置 gate；未设置时测试会 skip，不能计为 PASS）：

```bash
source scripts/integration-env.sh
DBTOOL_RUN_INTEGRATION=1 cargo test -p adapter-redis --lib \
  tests::redis_live_bounded_kv_envelopes_and_raw_mutation_safety -- --exact
DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test kv_binary_raw \
  redis_compatible_live_bounded_read_envelopes_and_cleanup -- --exact
DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test live_services \
  kv_lifecycle_and_raw_safety
DBTOOL_RUN_INTEGRATION=1 cargo test -p dbtool-cli --test transfer_artifacts \
  redis_artifact_v3_preserves_lifetimes_skips_expired_and_binds_replacement -- --exact
```

Cleanup: PASS。每个产品使用唯一前缀，显式 DEL 后再次 bounded SCAN 得到空集合；Redis
adapter 的危险 mutation fixture 也核对 string/list/set 未改变并全部删除。产品级全部 CRUD
内容证据继续保存在 `redis.md`、`valkey.md`、`keydb.md`、`dragonfly.md`，本文件只记录 IF-T70
新增读取信封的增量证明。

Known boundary: Redis Lua/redis-rs 仍需物化一个已通过服务端 transport gate 的合法响应；
caller JSON envelope 与 RESP payload 编码不同，因此服务端使用保守原始上限，客户端保留第二次
精确 portable 序列化校验。

Commits: `9525719`, `7f7044b`, IF-T70 caller/evidence follow-up
