# dbtool 接口使用与安全范式

更新时间：2026-07-16

本文件记录已经实现并验证的公共接口用法。所有示例都遵循同一约定：

- 读取默认允许；写入必须显式传入 `--allow-write`。
- 删除资源等破坏性操作先返回 `CONFIRM_REQUIRED`，再次调用时同时传入
  `--allow-write --confirm <token>`；公开 `impact.target` 只含脱敏标签，令牌内部同时绑定
  完整 resolved DSN、操作和资源，不能跨 credential、tenant/query、命名连接内容或目标复用。
- `--limit` 必须大于零。`--max-bytes` 默认 8 MiB、必须在 `1..=16 MiB`；SQL/CQL、
  KV、Document、Search、TimeSeries、消息批次和所有顶层名称目录使用它限制一个完整调用方响应。已经声明 exact budgeted operation 的用户查询和顶层名称目录，在可分页
  协议的适配器边界读取到 `limit + 1` 即停止；无法按条目分页的协议使用独立响应字节上限
  并只保留 N+1 个可移植结果。输出最多保留 `limit` 条，`meta.truncated` 只在实际观察到
  探针项或后端明确报告不完整时为真。二级元数据集合由 IF-T67 单独收口。
- JSON 参数必须是合法 JSON；对象型参数不会接受数组或标量代替。
- 错误通过稳定的 `error.code` 返回。接口不适用时返回
  `UNSUPPORTED_CAPABILITY`，不得用空数组或成功响应伪装。

## 方法级能力协商规范

任何数据命令或嵌入式调用都先检查具体 method-level operation，不能只看
`sql=true`、`document=true`、`admin=true` 等能力族布尔值：

```bash
dbtool --conn prod caps
```

响应保持旧字段兼容，同时增加稳定、排序去重的 `data.operations`：

```json
{
  "ok": true,
  "kind": "sqlite",
  "data": {
    "sql": true,
    "cql": false,
    "db2": false,
    "key_value": false,
    "document": false,
    "time_series": false,
    "search": false,
    "producer": false,
    "consumer": false,
    "admin": false,
    "operations": [
      "sql.describe_table",
      "sql.describe_table_bounded",
      "sql.execute",
      "sql.insert_rows_atomic",
      "sql.list_schemas_bounded",
      "sql.list_schemas_budgeted",
      "sql.list_tables_bounded",
      "sql.list_tables_budgeted",
      "sql.query_bounded",
      "sql.query_budgeted"
    ]
  }
}
```

示例省略了同一 connector 的其它基础 operation；调用方必须按实际数组判断，不能把示例
当作固定全集。`caps.data.operations` 与 Rust `CapabilityOperation` 序列化名是机器合同；
`error.needed` 只用于诊断，不能替代 operation 集做版本协商。

嵌入式调用固定使用 exact operation → accessor → invoke：

```rust,no_run
use std::sync::Arc;

use dbtool_core::{
    error::Error,
    model::ReadBudget,
    port::CapabilityOperation,
    service::ConnectionManager,
};
use dbtool_registry::build_registry;

# async fn example() -> dbtool_core::Result<()> {
let manager = ConnectionManager::new(Arc::new(build_registry()));
let connector = manager.get_or_connect("sqlite::memory:").await?;
let needed = CapabilityOperation::SqlQueryBudgeted;

if !connector.operations().contains(&needed) {
    return Err(Error::UnsupportedCapability {
        kind: connector.kind().0,
        needed: needed.as_str(),
    });
}
let sql = connector.as_sql().ok_or_else(|| Error::UnsupportedCapability {
    kind: connector.kind().0,
    needed: "SqlEngine",
})?;
let rows = sql
    .query_budgeted("select 1 as id", &[], ReadBudget::new(100, 8 * 1024 * 1024)?)
    .await?;
assert!(!rows.truncated);
# Ok(())
# }
```

`Capabilities` 的布尔字段只用于旧调用方的族发现。以下扩展永远不能由粗粒度字段猜测：
SQL/CQL/Document budgeted read、SQL 原子导入、KV expiry、Document one/many 与 drop、所有 budgeted catalog、Messaging
group/durable/ack，以及 list/detail/lag/delete 各自独立的 partial admin 方法。读取旧版、不含
`operations` 的 `CapabilityReport` 时，operation 集按空处理，即“不声明任何新扩展”。

## 顶层目录 items + bytes 范式

所有一方 CLI/TUI 目录路径统一构造 `ReadBudget(max_items,max_bytes)`，先在 DSN 解析和
连接之前校验，再协商 exact `*_budgeted` operation。旧 `*_bounded` 只控制条目数，保留给
嵌入式兼容调用方；一方路径不会回退。

| 接口族 | CLI | exact operation |
| --- | --- | --- |
| SQL | `sql schemas`、`sql tables` | `sql.list_schemas_budgeted`、`sql.list_tables_budgeted` |
| CQL | `cql keyspaces`、`cql tables` | `cql.list_keyspaces_budgeted`、`cql.list_tables_budgeted` |
| Db2 | `db2 schemas/tables/sequences/routines/tablespaces/foreign-keys` | 对应 `sql.*_budgeted` 或 `db2.*_budgeted` |
| Document | `doc collections` | `document.list_collections_budgeted` |
| Search | `search indices` | `search.list_indices_budgeted` |
| TimeSeries | `ts measurements` | `time_series.list_measurements_budgeted` |
| Messaging admin | `mq topics` | `message.admin.list_topics_budgeted` |

适配器必须在保留前计费每个完整标量或结构体，并在返回前计费完整
`BoundedList { items, truncated }` 和唯一 N+1 探针。`items.len()` 永远不超过
`max_items`；只有真实观察到第 N+1 项时才标记截断。可分页后端把 N+1 下推到 SQL
`LIMIT`、Mongo cursor、Prometheus label-values 或管理分页；不能分页的 Kafka metadata、
Search CAT 等接口使用独立协议响应硬上限，再只构造和观察 N+1 个 portable 对象。调用方
字节不足时统一返回 `READ_BUDGET_EXCEEDED`，不返回部分目录。

## 命名连接配置 CRUD

`conn` 只修改默认 `connections.toml`；`DBTOOL_CONN_*` 环境连接由进程环境管理，CLI
会列出但拒绝覆盖或删除。新增普通条目要求显式写权限：

```bash
dbtool --allow-write conn add local-pg \
  'postgres://app:${DB_PASSWORD}@127.0.0.1:5432/app' --readonly

dbtool conn list
```

`${ENV_NAME}` 在校验时替换为不含 secret 的哨兵，确认 scheme 已由当前 feature 集注册后，
原模板逐字保存；不会把当前环境变量值写入文件或响应。name 固定为 1..=64 个小写 ASCII
字母、数字和内部连字符，避免文件 key 与 `DBTOOL_CONN_<NAME>` 映射歧义。

重复 name 默认返回 `CONFIG_ERROR`。覆盖和删除是两段式破坏性操作：

```bash
# 第一次分别返回 CONFIRM_REQUIRED
dbtool --allow-write conn add local-pg 'postgres://new-host/app' --replace
dbtool --allow-write conn remove local-pg

# 第二次必须重放完全相同的 action、name 和条目内容
dbtool --allow-write --confirm '<token>' \
  conn add local-pg 'postgres://new-host/app' --replace
dbtool --allow-write --confirm '<token>' conn remove local-pg
```

replace token 绑定配置绝对目标、name、原条目和新条目；remove token 绑定目标、name 和当前
条目，因此不能跨动作、条目或内容复用。replace 不暴露 limits 编辑面，会保留原条目的
限流策略；`readonly` 以本次参数为准。

写入先在同目录创建独占临时文件，Unix mode 为 `0600`，完整写入并同步后才原子替换目标；
Unix 再同步父目录，Windows 使用 replace-existing + write-through。替换前失败会删除 temp
并保留旧文件。当前 typed TOML model 会保留 defaults、connections 和 limits，但无法保留
注释/排版；成功响应以 `serialization.comments_preserved=false` 明确这一边界。所有 DSN
输出均脱敏，配置解析错误也不会回显含凭证的源行。

确认目标使用 display/binding 分层：artifact 和 `impact.target` 只保存 `conn:<name>` 或
整段隐藏 userinfo/query/fragment 的 DSN；SafetyGuard 另对完整、已展开的 resolved DSN 生成
不公开的 hidden scope。该 digest 不写入 JSON、TUI 或 transfer artifact，既避免低熵凭据的
离线猜测面，也保证两个可见标签相同但凭据/tenant 不同的目标不能共享 token。`--dsn` 与
`--conn` 同时存在时，display 和 hidden scope 都服从 `--dsn` 优先级。

本地配置的资源边界是接口合同，不是建议值：配置文件最多 1 MiB、最多 1024 个文件条目，
配置 key 最多 256 bytes，raw DSN 与每轮环境展开后的 DSN 都最多 16 KiB，单个文本限流设置
最多 256 bytes。`DBTOOL_CONN_*` 最多收集 256 项、总 name+DSN 最多 512 KiB；非 UTF-8、
控制字符名称和规范化后重名都 fail closed，错误不会包含环境变量值。

`conn list` 先验证全局 `--limit/--max-bytes`，再分别取 caller 值与 512 items、256 KiB
内部硬上限的较小值。输出末尾换行计入 byte envelope；如果连固定 metadata 都放不下，返回
`READ_BUDGET_EXCEEDED` 且 stdout 为空。JSON、Table、NDJSON 均保持 N 完整、N+1
`truncated=true` 和 byte N/N-1 语义。配置路径先转为绝对词法路径；普通 UTF-8 路径保持可直接
使用，控制字符仅作安全可见转义，确认 scope 另用不公开且无碰撞的 OS path identity。

## KV 二进制值与 Redis RAW 安全边界

`kv set` 接受且只接受一种值来源：位置参数是 UTF-8 文本，`--value-base64` 是严格的
canonical RFC 4648 字节编码。缺值、同时给两种值、缺 padding、多余 padding、空白、
非法字符或非零尾比特都会在解析 DSN/连接前返回 `CONFIG_ERROR`：

```bash
dbtool --dsn "$REDIS_DSN" --allow-write kv set binary-key \
  --value-base64 'AP9oZWxsbw=='
dbtool --dsn "$REDIS_DSN" kv get binary-key
```

`kv get` 的响应同时保留兼容字段和无损字段：

- `data.value`：合法 UTF-8 仍是原字符串；二进制与不存在都为 `null`。
- `data.value_bytes`：key 存在时始终是 `dbtool-value-v2` 的 `bytes` tagged value；
  key 不存在时为 `null`，因此 empty bytes 与 missing 不再混淆。
- `data.encoding`：`utf8`、`binary` 或 missing 时的 `null`。

读取调用必须先协商 `kv.get_bounded`，并把单值 `ReadBudget(1,--max-bytes)` 传给
adapter。Redis/Valkey/KeyDB/Dragonfly 在 Lua 内先执行 `EXISTS` 与 `STRLEN`；只有原始值
未超过门禁时才执行并返回 `GET`。客户端随后对完整 typed value 再做紧凑 JSON 字节计费，
超限统一返回 `READ_BUDGET_EXCEEDED`，不会把超大 bulk 先交给 redis-rs 解码。

`kv raw` 不是任意 Redis shell。CLI 与 adapter 各自使用 fail-closed 白名单：

1. 单值/标量读取直接允许；MGET/HMGET、索引范围、随机成员和 Stream range 必须能在
   连接前证明不超过全局 `--limit`。
2. `KEYS`、raw `SCAN`、HGETALL/HKEYS/HVALS、SMEMBERS、XREAD/XINFO 等无法由该参数面
   证明有界的读取被拒绝；使用专用 `kv scan` 或消息/管理接口。
3. SET/DEL/INCR/过期/集合局部变更等目标明确的写命令必须先有 `--allow-write`，再执行
   两阶段确认。令牌绑定脱敏连接目标、规范化命令、全部参数与写入目标；改变 key/value/
   命令或连接均不可复用。
4. ACL/CONFIG/CLIENT/CLUSTER、FLUSH、SELECT/SWAPDB、MIGRATE/RESTORE、脚本/函数、
   事务、复制运行时及 Pub/Sub 连接态命令始终拒绝；未知命令也不向服务端透传。
5. `GETDEL`、`LPOP`、`RPOP`、`SPOP` 与 `SET ... GET` 会在修改后返回旧值或被移除值，
   因而 CLI 与 adapter 都在发往服务端前拒绝，避免“远端已变更但本地因预算失败”的状态。
6. raw 请求限制为单参数 1 MiB、合计 8 MiB；只读 RAW 必须协商
   `kv.raw_command_bounded`。Lua 在返回前按命令语义计算逻辑 item、累计 bulk bytes 与独立
   100,000 节点递归硬上限；客户端再校验完整 portable `Value`。二进制 bulk string 保持
   `Value::Bytes`；无法映射到 portable
   Value 的 RESP set/push/verbatim/big-number、非 UTF-8/非字符串/重复 map key 均显式
   返回序列化错误，不生成占位值。

## KV / Redis SCAN

```bash
dbtool --dsn "$REDIS_DSN" --limit 100 kv scan 'app:*'
```

CLI 会把 `ReadBudget(--limit,--max-bytes)` 传给 `KeyValueStore::scan_bounded`。Redis
adapter 不使用会隐藏后续分页错误的高层迭代器，而是在只读 Lua 中执行
`SCAN cursor MATCH pattern COUNT n`；`COUNT` 只作为提示，每一页都在 RESP 编码前限制实际
唯一 key 数与原始 key bytes。客户端跨页去重并只观察 N+1 个唯一 key，直到取得探针、
服务端 cursor 返回 `0`，或任一页失败。返回值遵循以下固定合同：

1. 只有找到第 N+1 个不同 key 时，输出才设置 `meta.truncated=true`；刚好 N 个为
   `false`。
2. Redis 在遍历期间可能重复 key；adapter 跨页去重，重复项不占调用方预算。
3. 非零 cursor 在归零前重复视为服务端分页环，返回 `QUERY_ERROR`，不得返回已收集的
   部分结果。
4. portable KV key 接口当前要求 UTF-8；遇到二进制 key 返回
   `SERIALIZATION_ERROR`，不会用替换字符篡改身份。
5. SCAN 是并发修改下的 best-effort 遍历，不是 Redis 数据库快照；需要恢复用途时还要
   检查 artifact 的 `complete/truncated/source_changed` 字段。

## Document / MongoDB

### 完整查询选项

```bash
dbtool \
  --dsn "$MONGO_DSN" \
  --limit 20 \
  doc find users \
  --filter '{"active":true}' \
  --skip 40 \
  --sort '{"created_at":-1,"_id":1}' \
  --projection '{"name":1,"created_at":1,"_id":0}'
```

参数映射：

| CLI | `FindOptions` | MongoDB 语义 |
| --- | --- | --- |
| `--limit` | `ReadBudget.max_items` | 最多输出 N 条，adapter 最多观察 N+1 条判断下一页 |
| `--max-bytes` | `ReadBudget.max_bytes` | 原始 BSON 累计值和完整 portable 响应的共同硬上限 |
| `--skip` | `skip` | 跳过匹配结果 |
| `--sort` | `sort` | JSON 排序对象，`1` 升序、`-1` 降序 |
| `--projection` | `projection` | JSON 字段投影对象 |

当实际剩余结果刚好等于 `--limit` 时，`meta.truncated=false`；只有探测到额外
一条时才是 `true`。

### 有界聚合

```bash
dbtool --dsn "$MONGO_DSN" --limit 100 \
  doc aggregate events '[{"$match":{"level":"error"}},{"$sort":{"ts":-1}}]'
```

CLI 调用 `DocumentStore.aggregate_budgeted`；MongoDB 使用单文档 batch，在转换前累计原始
BSON，并在返回前再次校验完整 portable `BoundedList<Document>`。字节超限返回
`READ_BUDGET_EXCEEDED`，不会返回部分结果。
含 `$out` 或 `$merge` 的 pipeline 属于写操作，仍要求 `--allow-write`。

### 更新与删除保护

```bash
dbtool --dsn "$MONGO_DSN" --allow-write \
  doc update users --filter '{"id":42}' --update '{"active":false}'

dbtool --dsn "$MONGO_DSN" --allow-write \
  doc delete users --filter '{"id":42}'
```

CLI 默认协商并调用 `document.update_one_budgeted` /
`document.delete_one_budgeted`，即使过滤器匹配多条也只
变更一条。需要对全部匹配项执行变更时，必须显式增加 `--many`，先取得确认令牌，再以
完全相同的连接、集合、操作、过滤器和更新内容执行第二次命令：

```bash
dbtool --dsn "$MONGO_DSN" --allow-write \
  doc update users --filter '{"tenant":"acme"}' \
  --update '{"active":false}' --many

dbtool --dsn "$MONGO_DSN" --allow-write --confirm '<confirm_token>' \
  doc update users --filter '{"tenant":"acme"}' \
  --update '{"active":false}' --many
```

令牌摘要绑定脱敏连接目标、集合、操作类型、many 模式、规范化后的顶层 JSON 与完整
嵌套内容；修改过滤器/更新内容、切换 update/delete、改变连接或集合均会拒绝旧令牌。
单条模式不接受无关的 `--confirm`，避免调用者误以为令牌授权了不同语义。

`update` 和 `delete` 的过滤器都必须是非空 JSON 对象；更新内容也必须是 JSON 对象。
CLI 会在连接前拒绝 `{}`、数组、`null`、空集合名和 NUL 字符，MongoDB adapter 在协议
边界再次拒绝空过滤器。

嵌入式兼容说明：历史 `DocumentStore::update/delete` 继续保持批量语义，不做破坏性
改名；新代码应先从 `Connector::operations()` 协商四个 exact operation，再调用
`update_one_budgeted/update_many_budgeted/delete_one_budgeted/delete_many_budgeted`。
粗粒度 `document=true` 不代表这些
可选方法已实现，未声明时必须得到 `UNSUPPORTED_CAPABILITY`。

### 删除集合

第一次调用获取令牌：

```bash
dbtool --dsn "$MONGO_DSN" --allow-write doc drop archived_events
```

响应的 `error.code` 为 `CONFIRM_REQUIRED`，并包含 `confirm_token`、连接目标和
集合资源。确认后执行：

```bash
dbtool --dsn "$MONGO_DSN" --allow-write \
  --confirm '<confirm_token>' \
  doc drop archived_events
```

嵌入式调用方可使用 `DocumentStore::drop_collection_budgeted`。不支持集合生命周期的
connector 使用 trait 默认实现，返回 `UNSUPPORTED_CAPABILITY`。调用前必须先检查
`document.drop_collection_budgeted`；粗粒度 `document=true` 不授权集合生命周期。

## TUI 写操作与终端恢复

启动：

```bash
cargo run -p dbtool-tui
```

TUI 对 SQL 命令采用语句内容分类，而不是依赖命令前缀。以下四种输入都会先交给
同一个 `SafetyGuard`：

```text
sql query DELETE FROM users WHERE id = 42
sql DELETE FROM users WHERE id = 42
sql exec UPDATE users SET active = false WHERE id = 42
exec DROP TABLE archived_users
```

处理顺序固定为：

1. 解析 SQL 并区分只读、写入、破坏性语句；
2. 如果连接配置为 `readonly = true`，在建连前拒绝所有写入；
3. 可写连接把写命令放入一次性 pending 状态，只有当前命令按 `y` 才执行；
4. query/fallback 和最终 adapter 调用点会再次执行安全校验，不能通过命令别名绕过；
5. `SELECT` 即使写成 `sql exec SELECT ...` 仍按只读语句处理。

终端 raw mode 与 alternate screen 由 `TerminalSession` 管理。正常退出、运行时创建
失败、draw/poll/read 错误、提前返回和 panic unwind 都会尝试先离开 alternate
screen、再关闭 raw mode；即使第一步恢复失败，也仍继续执行第二步。

## Cargo feature 与发布范式

| 构建 | 命令 | 用途 |
| --- | --- | --- |
| 最小核心 | `cargo build -p dbtool-cli --no-default-features` | 不编译、不注册任何 adapter，用于嵌入式最小依赖验证 |
| 默认 | `cargo build -p dbtool-cli` | 常用 SQL/KV/Document/Search/Timeseries 能力 |
| Apple Silicon 发布 | `cargo build --release --target aarch64-apple-darwin -p dbtool-cli --no-default-features --features portable` | macOS ARM64；完整自包含 adapter 集；pure Kafka；不含需要宿主 ODBC 的 Db2 |
| 全功能 pure Kafka | `cargo build -p dbtool-cli --no-default-features --features full` | 在 portable 基础上增加 Db2 ODBC |
| 全功能 native Kafka | `cargo build -p dbtool-cli --no-default-features --features full-native` | Db2 ODBC + librdkafka；不会同时编译 pure Kafka backend |

`./scripts/validate-feature-matrix.sh` 同时检查编译、adapter 依赖树、Kafka backend
互斥和支持 scheme。正式 tag 发布先执行：

```bash
./scripts/validate-release-version.sh v0.1.0
```

tag 必须严格等于 workspace 中 `dbtool-cli` 的版本。正式 release workflow 在真实
Apple Silicon `macos-latest` runner 上只生成 `aarch64-apple-darwin` portable
二进制，执行 SQLite 核心烟测，并附加一个包含 binary、bash/zsh/fish 补全和
manpage 的 `.tar.gz` 以及对应 SHA-256 sidecar。

Apple Silicon Mac 本机可直接运行：

```bash
./scripts/package-macos-arm64.sh
```

默认输出到 `release-dist/macos-arm64/`，同时生成 `.tar.gz` 和
`.tar.gz.sha256`。脚本要求真实 Darwin arm64 主机，验证 tag/workspace 版本、构建
portable release、解包并执行核心 SQLite 流程。底层 archive/npm/Python 打包器仍
支持显式 `DBTOOL_PACKAGE_TARGETS`，但不属于当前正式 GitHub Release 的上传面。

`--format` 由 Clap 枚举解析，只接受 `json`、`table`、`ndjson`。未知值在连接
数据库之前直接以非零状态退出，不再回退成 JSON。

## SQL 参数绑定

`sql query` 与 `sql exec` 都接受一个 JSON 数组：

```bash
dbtool --dsn "$POSTGRES_DSN" --allow-write \
  sql exec \
  'insert into events(id,note,payload,occurred_at,metadata) values ($1,$2,$3,$4,$5)' \
  --params '[7,"O'"'"'Reilly",{"$bytes":[0,127,255]},{"$timestamp":1700000000123},{"$json":{"source":"api"}}]'
```

MySQL 与 SQLite 使用 `?`，PostgreSQL 使用 `$1`、`$2`。参数不会插入 SQL 字符串，
包含单引号或看似 SQL 的文本仍按普通数据发送。

| JSON 写法 | `Value` | 绑定语义 |
| --- | --- | --- |
| `null` | `Null` | SQL NULL；PostgreSQL 使用 unknown OID 由语句上下文推断类型 |
| `true` / `false` | `Bool` | 布尔参数 |
| `-42` | `Int` | 必须在 i64 范围内 |
| `3.5` | `Float` | 有限浮点数 |
| `"text"` | `Text` | UTF-8 文本 |
| `{"$bytes":[0,127,255]}` | `Bytes` | 每个元素必须是 0..255 的整数 |
| `{"$timestamp":1700000000123}` | `Timestamp` | UTC Unix epoch 毫秒；PG=`timestamptz`、MySQL=`datetime`、SQLite=chrono 文本绑定 |
| `{"$json":{...}}` | `Json` | 原生 PostgreSQL json/jsonb、MySQL JSON；SQLite JSON 文本语义 |

嵌入式调用方传入 `Value::Array` 或 `Value::Map` 时也按 JSON 参数绑定；无效时间范围
和无法序列化的结构返回显式错误。SQL Server、Db2 和 Cassandra 当前不支持动态参数
时继续返回机器可读的 `UNSUPPORTED_CAPABILITY`/query error，绝不忽略参数。

`sql query --schema` 没有跨方言的执行语义，现已从 CLI 删除，避免一个被接受却不参与
查询的死参数。表列表继续通过 `sql tables --schema <name>` 过滤，表结构使用
`sql schema <schema.table>` 的可往返限定名。

## SQL / CQL 递归字节预算范式

CLI、SQL export 和嵌入式交互路径只调用 exact budgeted operation；CLI/export 把全局
`--limit` 与 `--max-bytes` 组成一个 `ReadBudget` 传入 adapter。TUI 在其支持的 SQL 与
Document 路径使用同一 exact operation，并固定采用默认 8 MiB 字节预算：

```bash
dbtool --dsn "$POSTGRES_DSN" --limit 100 --max-bytes 8388608 \
  sql query 'select * from events order by id'

dbtool --dsn "$CASSANDRA_DSN" --limit 100 --max-bytes 8388608 \
  cql query 'select * from app.events'
```

`SqlEngine::query_budgeted` 与 `CqlEngine::query_cql_budgeted` 是独立协商的 exact
method；旧族布尔值和仅按条数的 operation 不授权它们，trait 默认实现也不会调用 legacy
方法。实现合同固定为：

1. `max_rows` 必须大于零，且 `max_rows + 1` 不能溢出；
2. adapter 最多观察一条探测行；发现该行时只返回 `max_items` 条并标记
   `ResultSet.truncated=true`；
3. 结果恰好等于上限但没有探测行时必须为 false；
4. 列元数据、每个完整 row、嵌套 `Value::Array/Map/Json/Bytes`、完整 `ResultSet` 包络及
   N+1 探针均计入同一个 `max_bytes`；超过上限整体返回 `READ_BUDGET_EXCEEDED`；
5. SQLx 使用 row stream，Cassandra 使用 `page_size=1`，Tiberius 使用一次性 disposable
   result stream，Db2 ODBC 使用单行 rowset，并对 driver 报告的截断 fail closed；
6. CLI 会把相同值同步到响应 `meta.truncated`，export 的完整 artifact 还必须落在同一
   `--max-bytes` 内。

### 旧 API 生命周期与迁移

所有已经存在 exact sibling 的无界、仅按条数或无输入信封方法都只作为 `0.1.x`
嵌入式兼容面保留，并带有 Rust `#[deprecated]` 迁移提示；最早只能在 breaking
release `0.2.0` 删除。旧 `CapabilityOperation` 名仍保留序列化兼容，但它们只说明旧
实现存在，不能授权 exact 调用，新调用方也不得在 exact operation 缺失时回退：

| Trait | `0.1.x` legacy 方法 | 新调用方必须协商并调用 |
| --- | --- | --- |
| `SqlEngine` | `query`, `query_bounded`, `execute`, `insert_rows_atomic`, `list_schemas`, `list_schemas_bounded`, `list_tables`, `list_tables_bounded`, `describe_table` | `query_budgeted`, `execute_budgeted`, `insert_rows_atomic_budgeted`, `list_schemas_budgeted`, `list_tables_budgeted`, `describe_table_bounded` |
| `CqlEngine` | `query_cql`, `query_cql_bounded`, `execute_cql`, `list_keyspaces`, `list_keyspaces_bounded`, `list_cql_tables`, `list_cql_tables_bounded`, `describe_cql_table` | `query_cql_budgeted`, `execute_cql_budgeted`, `list_keyspaces_budgeted`, `list_cql_tables_budgeted`, `describe_cql_table_bounded` |
| `KeyValueStore` | `get`, `get_with_expiry`, `set`, `restore_with_expiry`, `delete`, `scan`, `raw_command` | `get_bounded`, `get_with_expiry_bounded`, `set_budgeted`, `restore_with_expiry_budgeted`, `delete_budgeted`, `scan_bounded`; RAW 只读用 `raw_command_bounded`，写入用 `raw_command_io_budgeted` |
| `DocumentStore` | `list_collections`, `list_collections_bounded`, `find`, `insert`, `update`, `delete`, `update_one`, `update_many`, `delete_one`, `delete_many`, `aggregate`, `aggregate_bounded`, `drop_collection` | `list_collections_budgeted`, `find_budgeted`, `insert_budgeted`；`update`/`update_many` 用 `update_many_budgeted`，`delete`/`delete_many` 用 `delete_many_budgeted`，单条用 `update_one_budgeted`/`delete_one_budgeted`；aggregate 只读用 `aggregate_budgeted`，`$out/$merge` 用 `aggregate_write_budgeted`；集合删除用 `drop_collection_budgeted` |
| `TimeSeriesStore` | `list_measurements`, `list_measurements_bounded`, `write_points`, `query_range` | `list_measurements_budgeted`, `write_points_budgeted`, `query_range_bounded` |
| `SearchEngine` | `list_indices`, `list_indices_bounded`, `search`, `index_doc`, `put_doc`, `get_doc`, `update_doc`, `delete_doc`, `delete_index` | 对应 `*_budgeted` exact 方法 |
| `MessageProducer` | `produce` | `produce_budgeted` |
| `AdminInspect` | `list_topics`, `list_topics_bounded`, `topic_detail`, `consumer_lag` | `list_topics_budgeted`, `topic_detail_bounded`, `consumer_lag_bounded` |
| `Db2Engine` | `list_sequences`, `list_sequences_bounded`, `list_routines`, `list_routines_bounded`, `list_tablespaces`, `list_tablespaces_bounded`, `list_foreign_keys`, `list_foreign_keys_bounded`, `generate_ddl` | 对应 `*_budgeted`，DDL 使用 `generate_ddl_bounded` |

`exists`、带完整 `ConsumeOptions` 的 `consume`、目标绑定的 `delete_resource` 本身已经是
有限/精确合同，因此不属于本轮弃用。CLI、TUI、transfer 和 embedded 推荐范式均遵循
`exact operation -> accessor -> exact method`。production 中只有 `DocumentStore`
`update_many -> update` 和 `delete_many -> delete` 两个带局部说明的
`0.1.x` legacy-to-legacy 默认桥接；它们不是 exact 默认实现。其余保留调用
只出现在带局部 `#[allow(deprecated)]` 的 `0.1.x compatibility` 测试中。

`export sql` 只接受只读语句。新生成的 `sql-rows` v3 artifact 使用
`dbtool-value-v2` typed wire codec，并必须包含 `truncated` 完整性字段；部分 artifact
拒绝导入，调用方必须提高 `--limit` 重新导出，不能把不完整数据静默恢复成完整表。
历史 v1/v2 artifact 既缺少无歧义 typed codec，也可能缺少完整性标记，因此统一 fail
closed；保留的 `--accept-legacy-unmarked` 仅用于兼容旧命令行脚本，不会绕过版本、类型或
截断检查，旧数据必须从源重新导出。

### SQL 参数化事务导入

```bash
dbtool --dsn "$POSTGRES_DSN" --limit 1000 --allow-write \
  import sql --table archive.events --input events.json
```

导入在连接目标前完成 artifact 版本、完整性、项目预算、table/column 标识符、重复列和
行宽校验。连接后还必须协商到 `sql.insert_rows_atomic_budgeted`；当前只有
SQLite、PostgreSQL 与
MySQL 声明该可选 operation。适配器不会信任 CLI 的预检，还会独立执行同一组结构校验，
然后遵循以下合同：

1. 每个 `Value` 都通过驱动参数绑定，不生成 SQL literal；引号文本、bytes、timestamp、
   JSON、array/map 都沿用 SQL 参数类型合同。
2. 完整 row batch 只使用一个数据库事务；任何绑定错误、约束错误、驱动错误或单行
   `rows_affected != 1` 都显式回滚。
3. MySQL 在开始事务前查询目标 base table 的 storage engine；`TRANSACTIONS != YES`
   （例如 MyISAM）时，空批次和非空批次都在零写入状态拒绝。
4. 只有事务成功提交才返回 `atomic=true`。SQL Server、Db2、Cassandra/Scylla 等未声明
   此 operation 的 connector 返回 `UNSUPPORTED_CAPABILITY`，不会偷偷退回逐行导入。
5. 表必须已经存在；此接口不负责建表、迁移 schema 或跨数据库事务。

## KV / Document artifact 完整性范式

`export kv` 使用 `kv.scan_bounded` 探测额外 key，再以
`kv.get_with_expiry_bounded` 原子读取每个快照；所有 retained entry 由累计 `ReadLimiter`
计费，最终 artifact 也不得超过调用方 `--max-bytes`，失败时旧目标文件保持不变。
`export doc` 使用与 `doc find` 相同的 `ReadBudget(--limit,--max-bytes)`。两者都只把前
`--limit` 项写入 artifact。当前格式分别为
`kv-pairs` v3、`documents` v3，并固定包含：

- `source.connector`、脱敏后的 `source.connection`、资源与 typed selector；
- `integrity.value_codec=dbtool-value-v2`；
- `complete`、`truncated`、`source_changed`、导出/选中数量和执行上限；
- `consistency=best-effort`，明确说明遍历本身不是跨并发写入的事务快照。

KV 二进制值按 `Value::Bytes` 的 base64 tagged wire format 保存，不再使用无类型的
JSON 数字数组；每个 v3 KV entry 还必须带显式 `expiry`：`persistent`，或
`expires-at-unix-ms` 加绝对 Unix 毫秒。导出只调用已协商的
`kv.get_with_expiry_bounded`，由 adapter 在一个后端操作内读取 value、Redis `TIME` 与 `PTTL`；
若 SCAN 选中了 key、原子读取时 key 已消失，则 `source_changed=true`、`complete=false`，
artifact 只能诊断，不能导入。已成功读取后 key 的正常过期不会反向破坏 artifact 完整性，
因为绝对 deadline 会随 artifact 保存时间自然流逝。

`import kv` 不再提供统一 `--ttl`。导入只调用
`kv.restore_with_expiry_budgeted`，由目标 Redis 的
`TIME` 在同一个 Lua 操作中判断 deadline：已过期或刚好到期返回 `expired_skipped` 且完全
不执行 SET；未来 deadline 使用剩余毫秒执行 `SET PX [NX]`；persistent 使用
`SET [NX]`。因此不会重新开始旧 PTTL，也不会短暂复活已经过期的 key。跨 Redis 实例迁移
绝对时间要求源/目标服务器时钟基本同步；时钟偏差属于部署边界，不能通过客户端重置 TTL
掩盖。
artifact 先以 Unix `0600` 权限写同目录独占临时文件并同步文件内容，再通过统一原子发布
primitive 替换目标。Unix 使用同目录 rename 并在成功后同步父目录；Windows 使用
`MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)`，因此已有普通文件可被替换且返回前请求
write-through。替换前的写入、同步或发布失败会删除临时文件并保留旧目标；Unix rename 已经
成功后若父目录同步失败，函数会返回错误，但新目标已经发布，不能声称旧目标仍在。单个
artifact 的导入读写硬上限固定为 256 MiB；SQL/Document bounded export 还受调用方
`--max-bytes` 限制。
导入还会把全局 `--limit` 作为项目总预算，在解析 DSN 前拒绝更多 rows/keys/documents；
超限时必须有意识地提高 `--limit` 或拆分资源。
旧 KV v1/v2、缺失或伪造 expiry、Document v1/v2、计数不一致、未知 codec、标记矛盾以及任何
`complete=false` artifact 都会在连接目标数据库之前拒绝，且没有跳过类型/版本校验的
兼容开关。KV v2 无法区分 persistent/expiring key，必须从源重新导出，不能静默恢复为
persistent，也不能把旧 PTTL 当成新的相对时长。

```bash
dbtool --dsn "$REDIS_DSN" --limit 1000 \
  export kv --pattern 'app:*' --out app-kv.json

dbtool --dsn "$MONGO_DSN" --limit 1000 \
  export doc users --filter '{"active":true}' --out active-users.json

dbtool --dsn "$REDIS_DSN" --limit 1000 --allow-write \
  import kv --input app-kv.json --key-prefix 'restore:'

dbtool --dsn "$MONGO_DSN" --limit 1000 --allow-write \
  import doc users_restore --input active-users.json --drop-id
```

KV import 默认使用条件创建，目标 key 已存在时拒绝；replace preflight 只协商并调用
`kv.exists`，不会为了判断存在性读取旧的大值。确需覆盖时必须显式增加
`--replace-existing`，再使用首次返回的、绑定目标 DSN、完整转换后 target key、exact bytes
与 persistent/absolute-expiry
摘要的全局 `--confirm` token。Document import 在写前拒绝 artifact 内重复 `_id`，并以
best-effort 方式检查目标中已存在的 `_id`；也可由调用方显式 `--drop-id` 生成新身份。
`_id` 预检保留 bytes/timestamp/ObjectId
等 backend 类型，但它不是锁定快照；预检与插入之间的并发写入仍由目标唯一约束作最终裁决。
通用 KV/Document port 没有跨所有项目的事务保证，因此这两类 import 的成功响应仍明确
返回 `atomic=false`。KV 另外返回 `per_entry_atomic=true`、`expiry_preserved=true`、实际
写入的 `restored`、目标预检存在且实际写入的 `replaced` 与完全未写入的
`expired_skipped`；NX 条件冲突明确失败，不会伪装成过期跳过。预检可避免已知冲突，但后端
约束、竞态或运行时网络故障仍可能造成部分写入。SQL import 则仅在 connector 声明并兑现上述整批事务合同后执行和返回
`atomic=true`。KV/Document 调用方应使用独立目标资源，并通过读回校验后再切换流量。

## TimeSeries / Prometheus 查询范围

`ts query` 有两种互斥的时间范围：

```bash
# 相对窗口；未写时默认最近 60 分钟
dbtool --dsn "$PROMETHEUS_DSN" ts query 'rate(http_requests_total[5m])' \
  --last-minutes 30

# 精确闭区间；单位为 Unix epoch 毫秒，两个端点必须同时提供
dbtool --dsn "$PROMETHEUS_DSN" ts query 'up' \
  --start-ms 1710000000000 --end-ms 1710000060000

# 三维读取预算：最多 20 个 series、5000 个累计 sample、8 MiB 完整响应
dbtool --dsn "$PROMETHEUS_DSN" --limit 5000 --max-bytes 8388608 \
  ts query 'up' --last-minutes 30 --max-series 20
```

规范如下：

- `--last-minutes` 必须大于零，且不能与显式端点混用；
- `--start-ms` 与 `--end-ms` 必须成对出现，`start <= end`；
- 全局 `--limit` 对所有 series 的样本总数生效，不是每个 series 各用一次；
- `--max-series` 独立限制 series 数；未写时沿用全局 `--limit`，不会隐式放大查询；
- 全局 `--max-bytes` 覆盖返回 `SeriesSet` 的 series name、columns、label/sample JSON、完整外层结构及唯一 N+1 probe；超限返回
  `READ_BUDGET_EXCEEDED`，不返回一份字节不完整的响应；
- TS 查询的 series/sample 预算均为 `1..=1,000,000`，字节预算还受 core 全局硬上限约束；零值、超上限和时间运算溢出在解析 DSN/建立连接前返回 `CONFIG_ERROR`；
- CLI/TUI 只调用 `time_series.query_range_bounded`；仅声明粗粒度 `time_series=true` 或旧 `query_range` 的第三方 adapter 会得到
  `UNSUPPORTED_CAPABILITY`，不回退到无界读取；
- Prometheus HTTP response 另有独立 16 MiB transport ceiling；该上限在 JSON 解析前防止无界 body，caller `--max-bytes` 则在 portable 结构级精确计费，两者不互相伪装；
- 成功响应保持 `data.series`、`data.truncated` 与 `meta.truncated`，显式范围不会改变
  JSON 契约。

CLI mock 服务测试会核对发给 Prometheus 的秒级 `start/end`；Prometheus 2.55.1 Docker
实测会写入两个带标签和精确时间戳的样本，再用相对窗口和显式 epoch-ms 窗口逐值读回；另外分别验证 series 截断、sample 截断和小字节预算失败。当前 registry 的公共 `TimeSeriesStore` 实现只有 Prometheus；TimescaleDB 走 SQL 协议，InfluxDB/VictoriaMetrics/QuestDB 尚未注册，因此不将它们写成本接口的实测 PASS。Prometheus 不提供通用 series delete API；测试以唯一 metric namespace 和一次性 Docker volume/retention 完成隔离与清理。

## 通用 mutation 输入信封与结果范式

SQL/CQL execute、KV 写入、Document 写入、Search 写入和 Prometheus remote
write 只由独立 exact operation 授权。CLI 复用三个全局参数构造同一份
`InputBudget`：

- `--limit` → `max_items`，限制一个批次的逻辑行、文档、键、参数或点数量；
- `--max-item-bytes` → `max_item_bytes`，限制每个完整逻辑项；
- `--max-bytes` → `max_batch_bytes`，限制包含 target、字段名和分隔符的完整请求。

CLI 默认分别为 100、8 MiB 和 8 MiB；嵌入式 `InputBudget::default()` 的默认 item
上限为 1,000。core 绝对上限为 100,000 items、16 MiB
单项和 16 MiB 完整请求。零值或超过硬上限在解析 DSN/创建连接前返回
`CONFIG_ERROR`。恰好 N bytes 可通过，N-1 返回稳定
`INPUT_BUDGET_EXCEEDED`，并保证还没有开始远端 mutation：

```bash
dbtool --dsn "$POSTGRES_DSN" --allow-write \
  --limit 100 --max-item-bytes 1048576 --max-bytes 8388608 \
  sql exec 'UPDATE jobs SET state=$1 WHERE id=$2' \
  --params '["done",42]'

dbtool --dsn "$MONGO_DSN" --allow-write \
  --limit 100 --max-item-bytes 1048576 --max-bytes 8388608 \
  doc insert jobs '{"_id":42,"state":"created"}'
```

CLI 必须在连接前用 `InputLimiter` 计入完整请求，adapter 在首个写入前重复
校验并追加协议固定边界；不能只检查 payload 而漏掉 table/key/collection/index/
document ID/filter/update/options。嵌入式调用方采用相同顺序：

```rust
let budget = InputBudget::new(100, 1024 * 1024, 8 * 1024 * 1024)?;
InputLimiter::new(budget, "application document insert")?
    .validate_items_with_request(&documents, &serde_json::json!({
        "collection": collection,
        "documents": &documents,
    }))?;
require_operation(&connector, CapabilityOperation::DocumentInsertBudgeted)?;
let result = connector
    .as_document()
    .ok_or(/* UNSUPPORTED_CAPABILITY */)?
    .insert_budgeted(collection, documents, budget)
    .await?;
```

exact operation 完整集合为：

| 协议族 | exact mutation |
| --- | --- |
| SQL | `sql.execute_budgeted`, `sql.insert_rows_atomic_budgeted` |
| CQL | `cql.execute_budgeted` |
| KV | `kv.set_budgeted`, `kv.restore_with_expiry_budgeted`, `kv.delete_budgeted`, `kv.raw_command_io_budgeted` |
| Document | `document.insert_budgeted`, `document.update_one_budgeted`, `document.update_many_budgeted`, `document.delete_one_budgeted`, `document.delete_many_budgeted`, `document.aggregate_write_budgeted`, `document.drop_collection_budgeted` |
| Search | `search.index_doc_budgeted`, `search.put_doc_budgeted`, `search.update_doc_budgeted`, `search.delete_doc_budgeted`, `search.delete_index_budgeted` |
| TimeSeries | `time_series.write_points_budgeted` |

SQL atomic import 在一次事务成功 commit 后才报告 `atomic=true`；Mongo ordered
insert、KV 多键导入、Search、Prometheus 等没有跨请求公共事务，必须按实际语义报告
`atomic=false` 或 per-entry atomic，不能把“全部输入已预检”误写成“远端批次原子”。
一旦首次写入、commit、broker/HTTP/driver dispatch 可能到达远端，后续连接、超时、
响应大小、状态码或解析失败统一返回非重试 `OUTCOME_INDETERMINATE`。CLI 的 outer
request/deadline timeout 对 SQL/CQL、KV mutation、Document mutation、import、Search、
TS write、MQ produce/delete 以及会 ACK/commit 的 consume 使用相同语义；纯只读路径仍返回
普通 `TIMEOUT`。调用方必须查询目标状态或使用业务幂等键后再决定补偿。

每个真实产品的 N/N-1、完整 CRUD/readback 和零残留记录见
[`mutation-input-budgets.md`](test-evidence/mutation-input-budgets.md)。

## Messaging / Kafka 字段范式

Kafka-compatible producer 可显式给出已有公共 `Message` 模型中的字段：

```bash
dbtool --dsn "$KAFKA_DSN" --allow-write \
  --max-bytes 8388608 \
  mq produce orders 'created' \
  --key order-42 \
  --header trace=abc \
  --header content-type=text/plain \
  --partition 0 \
  --timestamp-ms 1710000000123 \
  --max-message-bytes 8388608
```

`payload` 与 `--key` 都按原始 UTF-8 bytes 编码，不会把 JSON 文本解析或重写；
`--header` 可重复，每项必须是 `KEY=VALUE`，key 不能为空、不能带首尾空白且不能重复。
partition 必须非负，offset 由 broker 分配，成功响应的 `placements` 会返回真实
partition/offset。

### MessageProducer 输入预算与安全范式

`mq produce` 只协商 exact `message.produce_budgeted`，不会因为旧 connector 仍有
`producer=true` 或实现了 legacy `produce` 就退回无界调用。CLI 一次只提交一条消息，因此
构建的 `ProduceBudget` 固定 `max_messages=1`；`--max-message-bytes` 限制完整 portable
`Message` 的 compact JSON 大小，全局 `--max-bytes` 独立限制完整 `Vec<Message>` 包络。
两项默认均为 8 MiB、硬上限均为 16 MiB，零值和超出硬上限在解析 DSN 或连接 broker 前
返回 `CONFIG_ERROR`。嵌入式调用方可以显式选择批量数量，但不能超过 100,000 条：

```rust
let budget = ProduceBudget::new(100, 1024 * 1024, 8 * 1024 * 1024)?;
if !connector
    .operations()
    .contains(&CapabilityOperation::MessageProduceBudgeted)
{
    return Err(Error::UnsupportedCapability {
        kind: connector.kind().0,
        needed: CapabilityOperation::MessageProduceBudgeted.as_str(),
    });
}
let outcome = connector
    .as_producer()
    .ok_or_else(|| Error::UnsupportedCapability {
        kind: connector.kind().0,
        needed: "MessageProducer",
    })?
    .produce_budgeted(target, messages, budget)
    .await?;
```

强制合同是：预算自身、非空/数量、每条完整 Message、完整 batch、target 和全部协议字段
必须在建资源、创建 client/channel 或首个发送之前完成校验；这些纯校验之间的先后顺序不构成
公共合同。任一步前置检查失败都不得产生远端副作用；一个批次不能边校验边发送。恰好 N
bytes 必须成功，N-1 必须以稳定的
`INPUT_BUDGET_EXCEEDED` 失败，并且不返回部分成功结果。该错误表示“尚未开始远端 mutation”，
`is_retryable=false`；调用方可以在修正输入或提高合法预算后重新发起，但不应原样自动重放。

空批次是迁移时必须显式处理的兼容差异：exact `produce_budgeted` 始终以
`CONFIG_ERROR` 拒绝空批。四协议 legacy `produce` 为 0.x embedded 兼容仍保留“先校验有效
target，然后 `produced=0` 的 no-op”；legacy 非空批次也会应用有限的默认预算，而不是继续
无界发送。新代码必须使用 exact operation，并在调用前决定空集合是上游错误还是由调用方
自行跳过；不能依赖 legacy no-op 作为业务分支。

一旦队列/topic/stream 创建、publish/append 或 broker confirm 可能已经到达远端，后续连接、
delivery、flush、解析 placement 或 CLI request/deadline 超时统一返回非重试
`OUTCOME_INDETERMINATE`。调用方必须先以业务幂等键、broker offset/Stream ID、队列详情或
目标资源状态核对，再决定是否补偿；绝不能按普通 `CONNECTION_ERROR`/`TIMEOUT` 自动重试。
rate/concurrency admission 在 mutation 尚未开始前仍分别返回 `RATE_LIMITED`/
`OVERLOADED`，不会被误标为结果不确定。

各协议还保留自己的更严格预检边界：

- Kafka：全部 topic、partition、key/header/timestamp 和 broker-assigned offset 约束在建 topic
  前检查；broker/topic 的 `message.max.bytes` 可能低于 dbtool 16 MiB 硬上限，发送后拒绝属于
  结果不确定，不能被写成“零写入”。
- AMQP：完整 properties/field-table、header 固定宽度与队列名在建 channel/queue 前检查；
  body 由 lapin 分帧，broker 更低策略不能预知。队列声明本身是 mutation，之后的
  declaration/publisher-confirm 错误保守返回结果不确定。AMQP 0.9.1 没有 confirm 的
  confirm，也不承诺 exactly-once。
- NATS：先构造并验证完整 PUB/HPUB body、subject、header 和 server `INFO.max_payload`；
  Core NATS 的 flush 不是逐消息 JetStream publish ACK，dispatch 后错误必须人工核对。
- Redis Streams：仅接受可无损映射到 XADD 的字段，offset/timestamp 等 broker-assigned 字段
  在首个 XADD 前拒绝；Redis Pub/Sub 只接受 payload，且 channel 是瞬时路由，无法用持久资源
  目录证明无人收到。任一 XADD/PUBLISH 后错误均按结果不确定处理。

可从已知 Kafka placement 精确开始有界读取：

```bash
dbtool --dsn "$KAFKA_DSN" mq consume orders \
  --partition 0 --offset 42 --max 100 --timeout 5 \
  --max-message-bytes 1048576 --max-bytes 8388608
```

`--max` 和秒级 `--timeout` 必须大于零，partition/offset 必须非负；
`--max-message-bytes` 限制一个包含 payload/key/headers/cursor/metadata 的完整 Message，
全局 `--max-bytes` 限制完整 `Vec<Message>` 包络，两者均不能超过 16 MiB。无效值和当前平台
无法表示的超大 timeout 会在解析 DSN 或连接 broker 前返回 `CONFIG_ERROR`。当响应恰好
达到 `--max` 时，`meta.truncated=true` 只表示本次预算已用完，不证明 broker 中还有
下一条消息。

状态消费使用显式 typed 参数，不从 DSN 或固定默认 group 猜测：

```bash
# native Kafka consumer group
dbtool --dsn "$BROKER_DSN" --allow-write mq consume orders \
  --group billing --ack on-success --max 10 --timeout 5

# durable consumer
dbtool --dsn "$NATS_DSN" --allow-write mq consume ORDERS \
  --durable billing --ack none --max 10 --timeout 5
```

`--group` 与 `--durable` 互斥；`--consumer` 必须依附 `--group`；stateful identity 必须
显式给出 `--ack none|on-success`。即使选择 `none`，group membership、PEL 或 delivery
状态仍可能变化，因此统一要求 `--allow-write`。stateful 模式当前不与 partition/offset/
cursor 混用。CLI 连接后还会检查 `message.consume_group`、
`message.consume_durable`、`message.consume_ack`；粗粒度 `consumer=true` 不会冒充这些
扩展能力。不提供跨进程通用 `mq ack <token>`，因为 Kafka commit、Redis XACK、NATS
reply subject 和 AMQP channel-scoped delivery tag 不是同一种可移植句柄。

native Kafka 只声明 `message.consume_group` 与 `message.consume_ack`。它通过真正的
group subscription 从 broker committed offset（无提交时为 earliest）开始；
`--ack none` 不提交，下一次同组调用可重放；`--ack on-success` 等整个返回批次全部转换
成功后，按每个 partition 观察到的最大 offset + 1 同步提交。pure `rskafka` 不声明这两项
能力。Kafka 的 static member 需要长生命周期并且关闭时不会主动离组，与一次性 CLI 调用
不兼容，因此 native Kafka 明确拒绝 `--consumer`，不会把它偷换成 `client.id` 或静默忽略。

所有协议先完成转换、逐消息计费和完整 batch 包络计费，随后才允许 AMQP multiple ACK、
Redis XACK、Kafka offset commit 或 JetStream double-ack。超限统一返回
`READ_BUDGET_EXCEEDED`：AMQP requeue、Redis 保留 PEL、Kafka 不提交 offset、JetStream
不推进 ACK floor。pure/native Kafka 的接收 frame 在 DSN 参数应用后重新冻结到 16 MiB，
调用方不能通过 DSN 放大。ACK/commit 只能保证 broker 接受进度更新；进度提交后、CLI 输出前仍存在进程崩溃窗口，
不得描述为 exactly-once。

当前 pure `rskafka` 与 native `librdkafka` 均已在 Redpanda 上逐字段验证
payload/key/headers/partition/offset/timestamp。Kafka、Redis Streams 与 NATS JetStream
还会返回无损 `cursor`，可原样作为 inclusive 起点重放同一条仍被保留的消息：

```bash
dbtool --dsn "$KAFKA_DSN" mq consume orders --cursor 'kafka:0:42' --max 1
dbtool --dsn "$REDIS_DSN" mq consume stream:orders \
  --cursor 'redis-stream:1710000000000-3' --max 1
dbtool --dsn "$NATS_DSN" mq consume ORDERS \
  --cursor 'nats-jetstream:42' --max 1
```

协议不接受的字段会返回 `CONFIG_ERROR`，不会静默丢弃。AMQP 消费必须显式选择
`--ack on-success` 并提供写权限；返回 delivery tag、
redelivered、exchange、routing key 等已 ACK 的诊断 metadata；它不是可跨进程复用的
cursor。native Kafka 的 `mq lag <group>` 使用真实 committed offset 和 high watermark；
pure Kafka 明确不支持。RabbitMQ queue depth 只属于 `mq detail`，不得伪装成
consumer-group lag。

外部 Kafka producer 可以写入 tombstone、null header value、重复 header key 或非 UTF-8
header value，而公共 `Message` 模型不能无损表达这些形态。native consumer 会在提交 group
offset 之前返回明确的序列化错误；它不会把 tombstone 变成空 payload、把 null 变成空串、
覆盖重复 header，或做有损 UTF-8 转换。

持久消息资源删除统一使用两段式确认：

```bash
# 第一次返回 CONFIRM_REQUIRED 和 confirm_token
dbtool --dsn "$RABBIT_MANAGEMENT_DSN" --allow-write \
  mq delete --kind amqp-queue jobs --if-empty --if-unused

# 第二次的 kind/name/if-empty/if-unused 必须与第一次完全一致
dbtool --dsn "$RABBIT_MANAGEMENT_DSN" --allow-write --confirm '<token>' \
  mq delete --kind amqp-queue jobs --if-empty --if-unused
```

可删除类型为 Kafka topic、AMQP queue、Redis Stream、NATS JetStream；Core NATS subject
和 Redis Pub/Sub channel 没有持久资源删除语义。确认摘要绑定连接目标、resource kind/name
以及完整删除选项，任何条件变化都会拒绝旧 token。RabbitMQ 管理 detail 只接受合法
`messages`，或在该字段缺失时以 checked `messages_ready + messages_unacknowledged` 重建；
快照尚未完整、值非法或溢出时返回错误，不以零代替。

Redis Streams、NATS JetStream 与 AMQP 的 stateful ACK 已完成协议级实现与 Docker 实测。
只有声明并实现 `message.consume_group`、`message.consume_durable`、
`message.consume_ack` 中实际所需组合的 adapter 才能推进 broker 进度；转换失败发生在
ACK/commit 前，结果不确定时返回不可自动重试的 `OUTCOME_INDETERMINATE`。无状态 cursor
读取仍不能被描述为已经提交 consumer-group 进度，也不提供 exactly-once 承诺。

## Search / OpenSearch / Elasticsearch

完整文档生命周期使用稳定 ID；自动 ID 写入也会返回后端生成的 ID：

```bash
dbtool --dsn "$SEARCH_DSN" --allow-write \
  search put users user-42 '{"name":"Alice","role":"reader"}'

dbtool --dsn "$SEARCH_DSN" search get users user-42

dbtool --dsn "$SEARCH_DSN" --allow-write \
  search update users user-42 '{"role":"editor","revision":2}'

dbtool --dsn "$SEARCH_DSN" --allow-write \
  search delete users user-42
```

`index`（自动 ID）、`put`、`update`、`delete` 的结果均保留稳定字段
`index/id/result/version`，同时把 `_seq_no`、`_primary_term`、shard 信息等后端字段
保留下来。`get` 对缺失文档返回 `data: null`；其他 HTTP 404/409/5xx 不会伪装成功，
错误消息包含 HTTP 状态和后端 JSON。

搜索请求可直接包含完整 body，CLI 的 `--limit` 会覆盖更大的 body `size`，但不会放大 body 中更小的 `size`；显式 `--from` 会覆盖 body offset，`--source` 会覆盖 body 中的 `_source:false`。CLI/TUI 只调用 `search.search_budgeted`，`search get` 只调用 `search.get_doc_budgeted`；旧 `search.search/get_doc` 和粗粒度 `search=true` 不授权完整读取。响应返回：

- `total` 与 `total_relation`；
- `hits`、`took_ms`、`timed_out`；
- `aggregations`；
- 未被统一模型识别的顶层与 hits-container 元数据。

```bash
dbtool --dsn "$SEARCH_DSN" --limit 20 --max-bytes 8388608 search search users \
  --q '{"query":{"match_all":{}},"aggs":{"roles":{"terms":{"field":"role.keyword"}}}}' \
  --source
```

全局 `--limit` 是 search hit 数量预算；全局 `--max-bytes` 覆盖完整 hit、`_source`、aggregations、hits-container metadata、后端扩展字段和最终 `SearchHits` envelope。`get` 固定为一项预算，但存在文档与缺失文档的 optional envelope 都需完整计费。超限稳定返回 `READ_BUDGET_EXCEEDED`，不返回一份被字节截断的 JSON。

Adapter 在 JSON 解析前对 Content-Length、chunked decoded body 和无长度 body 应用 `min(caller max_bytes,16 MiB)` transport ceiling，解析后再对 portable 结构精确计费。两层都通过才返回成功。

删索引属于破坏性资源操作。第一次调用只获取目标绑定令牌，第二次才执行：

```bash
dbtool --dsn "$SEARCH_DSN" --allow-write search delete-index users
dbtool --dsn "$SEARCH_DSN" --allow-write --confirm '<confirm_token>' \
  search delete-index users
```

令牌绑定 DSN/命名连接、操作和精确索引名；同一令牌改成另一个索引会在连接前被拒绝。
OpenSearch 2.17.1 与 Elasticsearch 8.15.5 的完整生命周期已通过 Docker 实测，
分别覆盖 exact capability、完整 source/aggregation/get、body size clamp、1-byte 失败、目标绑定删索引与零残留。TLS 兼容 fixture 只作 transport/CA/种子数据证据；因不提供 get/aggregation/delete-index 完整后端面，不写成产品级零残留 PASS。
