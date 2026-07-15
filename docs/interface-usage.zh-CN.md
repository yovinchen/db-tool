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
5. raw 请求限制为单参数 1 MiB、合计 8 MiB；adapter 响应限制为 8 MiB 和最多
   10,000 个 RESP 节点。二进制 bulk string 保持 `Value::Bytes`；无法映射到 portable
   Value 的 RESP set/push/verbatim/big-number、非 UTF-8/非字符串/重复 map key 均显式
   返回序列化错误，不生成占位值。

## KV / Redis SCAN

```bash
dbtool --dsn "$REDIS_DSN" --limit 100 kv scan 'app:*'
```

CLI 会向 `KeyValueStore::scan` 传入 `limit + 1`。Redis adapter 不使用会隐藏后续分页
错误的高层迭代器，而是显式执行 `SCAN cursor MATCH pattern COUNT n`，直到取得探测
key、服务端 cursor 返回 `0`，或任一页失败。返回值遵循以下固定合同：

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

CLI 默认调用 `document.update_one` / `document.delete_one`，即使过滤器匹配多条也只
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
改名；新代码应先从 `Connector::operations()` 协商四个显式 operation，再调用
`update_one/update_many/delete_one/delete_many`。粗粒度 `document=true` 不代表这些
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

`sql query --schema` 没有跨方言的执行语义，现已从 CLI 删除，避免一个被接受却不参与
查询的死参数。表列表继续通过 `sql tables --schema <name>` 过滤，表结构使用
`sql schema <schema.table>` 的可往返限定名。

## SQL / CQL 有界读取范式

CLI、TUI、SQL export 和嵌入式交互路径不再调用全量查询后截断，而是把全局
`--limit` 传入 adapter：

```bash
dbtool --dsn "$POSTGRES_DSN" --limit 100 \
  sql query 'select * from events order by id'

dbtool --dsn "$CASSANDRA_DSN" --limit 100 \
  cql query 'select * from app.events'
```

`SqlEngine::query_bounded` 与 `CqlEngine::query_cql_bounded` 是 required method，新的
adapter 不能通过默认实现退回“全量读取再 truncate”。实现合同固定为：

1. `max_rows` 必须大于零，且 `max_rows + 1` 不能溢出；
2. adapter 最多观察一条探测行；发现该行时只返回 `max_rows` 条并标记
   `ResultSet.truncated=true`；
3. 结果恰好等于上限但没有探测行时必须为 false；
4. SQLx 使用 row stream，Cassandra 使用分页 row stream，Tiberius 只读取第一结果集，
   Db2 ODBC 的有界路径使用单行 rowset，避免预取整批；
5. CLI 会把相同值同步到响应 `meta.truncated`。

公共 `query/query_cql` 保留为显式无界兼容接口，供已知规模的内部 metadata 查询和
受控嵌入式调用使用；任何用户输入、导出或交互式调用必须优先选择 bounded 方法。

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
行宽校验。连接后还必须协商到 `sql.insert_rows_atomic`；当前只有 SQLite、PostgreSQL 与
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

`export kv` 与 `export doc` 使用 `--limit + 1` 探测是否还有额外项目，只把前
`--limit` 项写入 artifact。当前格式分别为 `kv-pairs` v2、`documents` v3，并固定包含：

- `source.connector`、脱敏后的 `source.connection`、资源与 typed selector；
- `integrity.value_codec=dbtool-value-v2`；
- `complete`、`truncated`、`source_changed`、导出/选中数量和执行上限；
- `consistency=best-effort`，明确说明遍历本身不是跨并发写入的事务快照。

KV 二进制值按 `Value::Bytes` 的 base64 tagged wire format 保存，不再使用无类型的
JSON 数字数组；Document 内的 bytes、timestamp、JSON、array、map 同样保持类型。
当前 KV 完整性标记只覆盖本次选择到的 key/value 集合，不代表 Redis 原生 TTL 已备份；
现有 `import kv --ttl` 是调用方显式指定的统一新 TTL，未指定时恢复为持久 key。逐 key
保留剩余 TTL 属于后续 KV artifact 版本，当前不能把该格式描述为完整 Redis 备份。
artifact 先以 Unix `0600` 权限写同目录临时文件并 `sync`，再通过 rename 发布并同步
父目录，避免进程中断留下看似完整的目标文件；单个 artifact 的读写上限固定为 256 MiB。
导入还会把全局 `--limit` 作为项目总预算，在解析 DSN 前拒绝更多 rows/keys/documents；
超限时必须有意识地提高 `--limit` 或拆分资源。
旧 KV v1、Document v1/v2、计数不一致、未知 codec、标记矛盾以及任何
`complete=false` artifact 都会在连接目标数据库之前拒绝，且没有跳过类型/版本校验的
兼容开关。

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

KV import 默认使用条件创建，目标 key 已存在时拒绝；确需覆盖时必须显式增加
`--replace-existing`，再使用首次返回的、绑定目标 DSN、完整转换后 key/value 集合与 TTL
摘要的全局 `--confirm` token。Document import 在写前拒绝 artifact 内重复 `_id`，并以
best-effort 方式检查目标中已存在的 `_id`；也可由调用方显式 `--drop-id` 生成新身份。
`_id` 预检保留 bytes/timestamp/ObjectId
等 backend 类型，但它不是锁定快照；预检与插入之间的并发写入仍由目标唯一约束作最终裁决。
通用 KV/Document port 没有跨所有项目的事务保证，因此这两类 import 的成功响应仍明确
返回 `atomic=false`；预检可避免已知冲突，但后端约束、竞态或运行时网络故障仍可能造成
部分写入。SQL import 则仅在 connector 声明并兑现上述整批事务合同后执行和返回
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

## Messaging / Kafka 字段范式

Kafka-compatible producer 可显式给出已有公共 `Message` 模型中的字段：

```bash
dbtool --dsn "$KAFKA_DSN" --allow-write \
  mq produce orders 'created' \
  --key order-42 \
  --header trace=abc \
  --header content-type=text/plain \
  --partition 0 \
  --timestamp-ms 1710000000123
```

`payload` 与 `--key` 都按原始 UTF-8 bytes 编码，不会把 JSON 文本解析或重写；
`--header` 可重复，每项必须是 `KEY=VALUE`，key 不能为空、不能带首尾空白且不能重复。
partition 必须非负，offset 由 broker 分配，成功响应的 `placements` 会返回真实
partition/offset。

可从已知 Kafka placement 精确开始有界读取：

```bash
dbtool --dsn "$KAFKA_DSN" mq consume orders \
  --partition 0 --offset 42 --max 100 --timeout 5
```

`--max` 和秒级 `--timeout` 必须大于零，partition/offset 必须非负；无效值和当前平台
无法表示的超大 timeout 会在解析 DSN 或连接 broker 前返回 `CONFIG_ERROR`。当响应恰好
达到 `--max` 时，`meta.truncated=true` 只表示本次预算已用完，不证明 broker 中还有
下一条消息。

状态消费使用显式 typed 参数，不从 DSN 或固定默认 group 猜测：

```bash
# consumer group；member 只允许与 group 同时出现
dbtool --dsn "$BROKER_DSN" --allow-write mq consume orders \
  --group billing --consumer worker-1 --ack on-success --max 10 --timeout 5

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

ACK/commit 只能保证 broker 接受进度更新；进度提交后、CLI 输出前仍存在进程崩溃窗口，
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

设计中声明的 durable/group 消费与显式 ACK 选择仍由 IF-T48 跟踪；现有无状态 cursor
读取不能被描述为已经提交 consumer-group 进度。

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
