# dbtool 接口使用与安全范式

更新时间：2026-07-16

本文件记录已经实现并验证的公共接口用法。所有示例都遵循同一约定：

- 读取默认允许；写入必须显式传入 `--allow-write`。
- 删除资源等破坏性操作先返回 `CONFIRM_REQUIRED`，再次调用时同时传入
  `--allow-write --confirm <token>`；令牌绑定连接目标、操作和资源，不能跨目标复用。
- `--limit` 必须大于零。需要判断是否还有下一页的读取会在适配器内部最多探测
  `limit + 1` 条，输出只保留 `limit` 条，并通过 `meta.truncated` 精确标记。
- JSON 参数必须是合法 JSON；对象型参数不会接受数组或标量代替。
- 错误通过稳定的 `error.code` 返回。接口不适用时返回
  `UNSUPPORTED_CAPABILITY`，不得用空数组或成功响应伪装。

## Document / MongoDB

### 完整查询选项

```bash
dbtool \
  --dsn 'mongodb://user:pass@127.0.0.1:27017/app?authSource=admin' \
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
| `--limit` | `limit` | 最多输出 N 条，内部读取 N+1 条判断下一页 |
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

CLI 调用 `DocumentStore.aggregate_bounded`，适配器停止保留超过探测上限的文档。
含 `$out` 或 `$merge` 的 pipeline 属于写操作，仍要求 `--allow-write`。

### 更新与删除保护

```bash
dbtool --dsn "$MONGO_DSN" --allow-write \
  doc update users --filter '{"id":42}' --update '{"active":false}'

dbtool --dsn "$MONGO_DSN" --allow-write \
  doc delete users --filter '{"id":42}'
```

`update` 和 `delete` 都拒绝空过滤器 `{}`。如需全量变更，应使用显式、可审计的
匹配条件，不能依赖空条件绕过安全边界。

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

嵌入式调用方可使用 `DocumentStore::drop_collection`。不支持集合生命周期的
connector 使用 trait 默认实现，返回 `DocumentStore.drop_collection` 对应的
`UNSUPPORTED_CAPABILITY`。

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
| 六平台自包含发布 | `cargo build -p dbtool-cli --no-default-features --features portable` | 完整自包含 adapter 集；pure Kafka；不含需要宿主 ODBC 的 Db2 |
| 全功能 pure Kafka | `cargo build -p dbtool-cli --no-default-features --features full` | 在 portable 基础上增加 Db2 ODBC |
| 全功能 native Kafka | `cargo build -p dbtool-cli --no-default-features --features full-native` | Db2 ODBC + librdkafka；不会同时编译 pure Kafka backend |

`./scripts/validate-feature-matrix.sh` 同时检查编译、adapter 依赖树、Kafka backend
互斥和支持 scheme。正式 tag 发布先执行：

```bash
./scripts/validate-release-version.sh v0.1.0
```

tag 必须严格等于 workspace 中 `dbtool-cli` 的版本。release workflow 只使用
`portable` 生成六平台二进制；npm 的 Unix 子包在复制后强制设置 0755，npm 主包和
Linux x64 子包会实际安装并执行 `dbtool --version`；Python musllinux wheel 在
Alpine 容器中实际安装并执行。archives、`.tgz` 和 `.whl` 都附加到同一个
GitHub Release。

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

`sql query --schema` 当前没有跨方言的执行语义，因此在连接前返回
`SqlQuerySchema` 的 `UNSUPPORTED_CAPABILITY`。表列表仍通过
`sql tables --schema <name>` 使用已实现的 schema 过滤。

## TimeSeries / Prometheus 查询范围

`ts query` 有两种互斥的时间范围：

```bash
# 相对窗口；未写时默认最近 60 分钟
dbtool --dsn "$PROMETHEUS_DSN" ts query 'rate(http_requests_total[5m])' \
  --last-minutes 30

# 精确闭区间；单位为 Unix epoch 毫秒，两个端点必须同时提供
dbtool --dsn "$PROMETHEUS_DSN" ts query 'up' \
  --start-ms 1710000000000 --end-ms 1710000060000
```

规范如下：

- `--last-minutes` 必须大于零，且不能与显式端点混用；
- `--start-ms` 与 `--end-ms` 必须成对出现，`start <= end`；
- 全局 `--limit` 对所有 series 的样本总数生效，不是每个 series 各用一次；
- TS 查询的样本预算为 `1..=1,000,000`，零值、超上限和时间运算溢出在连接前返回
  `CONFIG_ERROR`；
- 成功响应保持 `data.series`、`data.truncated` 与 `meta.truncated`，显式范围不会改变
  JSON 契约。

CLI mock 服务测试会核对发给 Prometheus 的秒级 `start/end`；Prometheus 2.55.1 Docker
实测会写入两个带标签和精确时间戳的样本，再用相对窗口和显式 epoch-ms 窗口逐值读回。

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

搜索请求可直接包含完整 body，CLI 的 `--limit` 会覆盖更大的 body `size`，显式
`--from` 会覆盖 body offset，`--source` 会覆盖 body 中的 `_source:false`。响应返回：

- `total` 与 `total_relation`；
- `hits`、`took_ms`、`timed_out`；
- `aggregations`；
- 未被统一模型识别的顶层与 hits-container 元数据。

```bash
dbtool --dsn "$SEARCH_DSN" --limit 20 search search users \
  --q '{"query":{"match_all":{}},"aggs":{"roles":{"terms":{"field":"role.keyword"}}}}' \
  --source
```

删索引属于破坏性资源操作。第一次调用只获取目标绑定令牌，第二次才执行：

```bash
dbtool --dsn "$SEARCH_DSN" --allow-write search delete-index users
dbtool --dsn "$SEARCH_DSN" --allow-write --confirm '<confirm_token>' \
  search delete-index users
```

令牌绑定 DSN/命名连接、操作和精确索引名；同一令牌改成另一个索引会在连接前被拒绝。
OpenSearch 2.17.1 与 Elasticsearch 8.15.5 的完整生命周期已通过 Docker 实测，
测试后没有 `dbtool_it_*` 索引残留。
