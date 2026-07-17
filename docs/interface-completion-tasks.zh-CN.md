# dbtool 接口完整性收口任务表

更新时间：2026-07-18

本表只追踪当前已经注册、已经公开或已经在能力 trait 中声明的接口。
Oracle、etcd、InfluxDB、VictoriaMetrics、Pulsar、MQTT、RocketMQ 等设计候选
不属于“补齐现有接口”，除非后续先完成产品立项和独立适配器设计。

## 状态定义

- `待实施`：已确认是真实接口缺口，尚未开始修改。
- `实施中`：测试契约或代码正在修改。
- `完成`：实现、测试、文档、CLI 和提交均已同步。
- `明确边界`：协议模型本身不提供可移植语义，必须返回机器可读的不支持结果。
- `外部阻塞`：需要本机之外的运行时、架构或凭证，不能伪造通过。

## 当前接口收口队列

| Task | 优先级 | 状态 | 接口缺口 | 完成标准 |
| --- | --- | --- | --- | --- |
| IF-T43 SQL 参数绑定 | P0 | 完成 | `SqlEngine` 已声明 `params`，但 MySQL/PostgreSQL/SQLite 原先静默忽略，CLI 固定传空数组；SQL Server/Db2/Cassandra 直接拒绝。 | CLI 已提供 JSON 数组与 bytes/timestamp/json 标签；MySQL/PostgreSQL/SQLite 原生绑定 Null/Bool/Int/Float/Text/Bytes/Timestamp/JSON；结构化嵌入式值按 JSON 绑定；其余后端继续明确拒绝；SQLite 单测/CLI 及 PostgreSQL/MySQL Docker 全类型生命周期通过。 |
| IF-T44 有界 SQL/CQL 读取 | P0 | 完成 | SQL 原先使用 `fetch_all`、Cassandra 使用 `query_unpaged`，CLI 在全量取回后才截断；export 还可能写出未标记的部分 artifact。 | `query_bounded/query_cql_bounded` 为 required trait；SQLx、Cassandra、Tiberius、ODBC 均最多观察 `limit + 1` 条；CLI/TUI/export/embedded 已迁移；artifact 记录并拒绝导入部分结果；SQLite 10,000 行回归及 PostgreSQL 16.14、MySQL 8.4.9、Cassandra 5.0.8 Docker 精确截断实测通过。 |
| IF-T45 Search 文档 CRUD | P0 | 完成 | Search 原先只支持自动 ID 写入、搜索和索引列表，且丢弃写入 ID/版本与聚合元数据。 | 公共 trait、CLI 和适配器现支持自动 ID index、稳定 ID put/get/update/delete 及 delete-index；返回 ID/result/version、聚合/took/timed_out/total relation 和未知后端元数据；写操作受 `--allow-write` 保护，删索引使用目标绑定确认；OpenSearch 2.17.1 与 Elasticsearch 8.15.5 完整生命周期实测且零测试索引残留。 |
| IF-T46 Document 集合生命周期与查询选项 | P1 | 完成 | MongoDB CLI 原先未暴露 `skip/sort/projection`，公共 API 不能 drop collection。 | 已增加完整 `doc find` 选项、`aggregate_bounded`、空过滤器更新保护和 `drop_collection`；集合删除使用目标绑定确认；MongoDB 7 Docker 精确查询、限制与删除生命周期实测通过。 |
| IF-T47 Messaging 精细能力与资源删除 | P1 | 完成 | `admin=true` 原先不能表达方法级支持；Kafka lag 未实现；公共 API 不能删除 topic/queue/stream。 | capability operation 已逐方法声明；native Kafka 读取真实 group committed offset/high watermark；独立 `AdminMutate` 对 Kafka topic、AMQP queue、Redis Stream、NATS JetStream 执行受写保护和目标/选项绑定确认的删除；pure Kafka lag、AMQP/Rabbit queue-depth lag 等不适用项明确返回不支持。 |
| IF-T48 CLI 参数面完整性 | P1 | 完成 | Search 聚合及完整响应、TS start/end、MQ key/header/partition/timestamp/offset、lossless cursor 与非 Kafka metadata 已补齐；group/durable/ack 已冻结为 typed identity/ack 与独立 operation。native Kafka 完成 group subscription/commit；Redis 完成显式 group+member、PEL 优先重放、整批转换后 XACK 与运行时 lag 协商；Core NATS queue group 与 JetStream durable/verified ACK/精确 lag 已分层；AMQP 要求显式 `--ack on-success`，整批转换后仅提交一个 multiple ACK。 | 四族状态消费均通过 adapter/CLI 能力协商和 Docker 实测；转换错误发生在 ACK/commit 前，ACK 结果不确定时返回不可自动重试的 `OUTCOME_INDETERMINATE`；AMQP 的 ACK 到可见 stdout 间隙及各协议非事务边界已在规范中明确，不伪装 exactly-once。 |
| IF-T49 能力协商规范 | P1 | 完成 | 旧 `Capabilities` 只有粗粒度布尔值，调用方可能在 downcast 后才发现某个方法不支持。 | `CapabilityReport` 保持旧布尔字段兼容并输出排序去重的稳定 operation；CLI/TUI/transfer/embedded 均按 exact operation → accessor → invoke；Document lifecycle、partial admin 与 stateful messaging 不再由粗粒度字段猜测；旧 JSON 读取兼容、Docker Mongo 与各层回归通过。证据：[`capability-negotiation.md`](test-evidence/capability-negotiation.md)。 |
| IF-T50 设计与状态文档同步 | P1 | 完成 | `dbtool-design.md`、README、implementation status 的 feature、命令和更新时间曾落后于实际代码。 | 设计文档现只列已注册 feature/scheme、真实 CLI/TUI/embedded 能力和发布边界；已解决问题归档；README、Skill、中文使用范式、实现状态与证据索引同步到 2026-07-16。 |
| IF-T51 编译与打包闭环 | P0 | 完成（单一 macOS ARM64 发布目标） | 接口收口后已完整执行 fmt/workspace check/strict Clippy/全仓测试、deprecated production gate、minimal/default/portable/full/full-native feature 矩阵、portable release 与 Docker 冷构建。正式发布范围按产品决策收缩为 `aarch64-apple-darwin`。 | 本机一键脚本生成 binary、completions、manpage、archive 与 SHA-256，解包运行和 SQLite 核心流程 PASS；GitHub Release 使用真实 ARM64 `macos-latest` runner 构建/执行并只上传一个 `.tar.gz`。证据：[`release-packaging.md`](test-evidence/release-packaging.md)。 |
| IF-T52 外部产品验证 | P2 | 外部阻塞 | SQL Server 本地 arm64、Db2 ODBC、Redshift、AutoMQ/WarpStream/Confluent、Elasticsearch 原生 HTTPS 依赖额外环境。 | 保留独立 runner 和解除条件；获得相应运行时/DSN 后逐产品生成 LIVE_PASS 证据；没有真实运行不得改成 COMPLETE。ScyllaDB 已由 IF-T80 解除。 |
| IF-T53 Cargo feature 与发布能力一致性 | P0 | 完成 | CLI/TUI 原先无法真正关闭 registry 默认 feature；`full-native` 同时启用 pure/native Kafka；release 只构建 default。 | `--no-default-features` 现在得到空 registry；`full`/`full-native` 互斥选择 Kafka backend；macOS ARM64 release 使用不含 host ODBC/native Kafka 的 `portable` 完整自包含能力集；feature matrix 校验 scheme 与依赖树。 |
| IF-T54 TUI 安全与终端恢复 | P0 | 完成 | TUI 原先依赖字符串前缀识别写操作，SQL fallback 可绕过共享 `SafetyGuard`；异常退出可能残留 raw mode/alternate screen。 | 所有 SQL 输入现统一使用 AST 分类、readonly 与一次性确认检查；query/fallback 绕过回归通过；RAII `TerminalSession` 已覆盖正常、draw/poll/read 错误、初始化失败和 panic unwind 恢复。 |
| IF-T55 CLI/发布严格性 | P0 | 完成 | 非法 `--format` 原先静默降级；tag 与 Cargo 版本无一致性门禁。 | Clap `ValueEnum` 严格拒绝非法格式；release 先校验 tag=workspace 版本；正式 macOS ARM64 binary 在打包前和解包后执行，GitHub Release 只接受目标专属 archive。可选 npm/Python 生成器保留自身严格目标校验但不进入正式发布。 |
| IF-T56 SQL 行返回写操作安全 | P0 | 完成 | 共享分类器原先把所有 parsed Query 与解析失败的 `WITH` 当作只读，data-modifying CTE、`SELECT INTO` 和 locking SELECT 可穿过 query/export。 | Query AST 递归检查 CTE/set body；`SELECT INTO` 为 destructive，locking SELECT 为 write，无法检查的 WITH fail closed；破坏性 SQL 在发令牌前要求 `--allow-write`；core 与 CLI 连接前回归通过。 |
| IF-T57 Redis SCAN 精确分页 | P0 | 完成 | 原实现依赖 `AsyncIter`，分页中的请求/解码错误可能被隐藏；CLI 用 `len >= limit` 会把刚好 N 条误报为截断。 | 适配器显式循环 `SCAN cursor MATCH COUNT`，传播每页错误、严格拒绝非 UTF-8 key、跨页去重并检测 cursor 环；CLI 读取 N+1 后仅在存在探测 key 时标记截断；Redis Docker 的 N、N+1、25-key 多页、非 UTF-8 错误与全量清理均通过。 |
| IF-T58 SQL 参数化事务导入 | P0 | 完成 | SQL artifact 原先把值拼成方言相关 literal 并逐行独立提交；中途约束失败会留下部分数据，却统一报告 `atomic=false`。 | 新增不由 `sql=true` 推导的可选 `sql.insert_rows_atomic`；SQLite/PostgreSQL/MySQL 二次校验标识符和行宽、逐值绑定参数并在一个事务提交，任一错误全部回滚且成功报告 `atomic=true`；MySQL 在写前拒绝 MyISAM 等非事务引擎；其他 SQL connector 明确返回不支持，绝不退回 literal 拼接。 |
| IF-T59 RabbitMQ 管理计数与删除确认 | P0 | 完成 | RabbitMQ 新队列的管理响应可能暂时缺 `messages`，旧实现要么把可重建计数当失败，要么有被缺字段误报为零的风险；删除 token 只绑定 kind/name，未绑定 `if-empty/if-unused`。 | queue detail 优先使用合法 `messages`，字段缺失时只以 checked `messages_ready + messages_unacknowledged` 重建；缺失、非法和溢出均 fail closed；删除确认摘要绑定完整 resource/options，不能跨条件复用；RabbitMQ 3.13 Docker 的生产、详情、ACK、条件删除、删除后不存在及零残留通过。 |
| IF-T60 命名连接配置 CRUD | P0 | 完成 | 设计和 CLI help 声明 `conn list/add/remove`，实际只有 list；调用方只能手工编辑含凭证的 TOML，缺少原子写、覆盖确认和脱敏合同。 | `conn add` 要求写权限并默认拒绝覆盖；`--replace` 与 `conn remove` 使用绑定配置路径、name、旧/新条目内容的确认；环境管理项不可修改；DSN template 离线校验且不展开 secret；同目录 0600 temp、file sync、跨平台原子替换和 parent/write-through 后发布；失败前旧文件保持完整，所有输出脱敏并明确 typed serialization 不保留注释。 |
| IF-T61 Document 单条/批量变更语义 | P0 | 完成 | 公共 `update/delete` 沿用批量语义，而 CLI 未让调用者明确选择单条或批量，非空但宽泛的过滤器可能一次影响多条。 | 保留旧方法的批量兼容合同，新增且独立协商 `document.update_one/update_many/delete_one/delete_many`；CLI 默认只变更一条，`--many` 必须使用绑定连接、集合、操作、规范化过滤器及更新内容的确认令牌；MongoDB 7 Docker 精确验证单条各 1 条、批量更新 3 条、批量删除剩余 2 条，跨内容/操作复用令牌被拒且最终零集合残留。 |
| IF-T62 KV 二进制与 RAW 安全 | P0 | 完成 | `kv get` 原先用有损 UTF-8 转换，binary/empty/missing 无法可靠区分；`kv set` 只有文本输入；raw 几乎可透传任意命令，写操作只有粗粒度权限且集合响应可能无界或丢失 RESP 类型。 | get 保留 UTF-8 兼容字段并新增 typed bytes/encoding；set 支持严格 canonical base64；CLI 与 Redis adapter 使用独立白名单、参数/请求/响应预算和不可移植 RESP 显式错误；所有 raw mutation 需要参数/目标/连接绑定确认，管理/脚本/连接态/未知/无界命令 fail closed；Redis 7 Docker 覆盖 binary/empty/text/missing、令牌防复用、危险命令拒绝及零前缀残留。 |
| IF-T63 Messaging 状态消费 typed 合同 | P0 | 完成 | `ConsumeOptions` 和 CLI 无法表达 consumer group、durable 与 ACK/commit 意图；粗粒度 `consumer=true` 可能被误当成会推进 broker 进度。 | 新增 `ConsumerIdentity` 与 `AckMode`、`--group/--consumer/--durable/--ack` 和三项稳定 operation；stateful identity 必须显式选择 ACK 且要求写权限，身份/互斥/position 冲突在连接前拒绝；legacy consumer 不自动声明扩展能力，只有逐协议 adapter 明确声明后 CLI 才调用。 |
| IF-T64 KV 过期时间无损迁移 | P0 | 完成 | KV v2 artifact 只保存 value，导入时统一 `--ttl` 会把 persistent 与 expiring key 混为一谈，并可能延长或复活源 key。 | 以独立协商的 `kv.get_with_expiry/kv.restore_with_expiry` 原子合同生成 v3 artifact，v2/缺失 expiry 在 DSN 解析前拒绝，过期 entry 在目标服务端原子判定并跳过；Redis 7.4.9、Valkey 8.1.8、KeyDB 6.3.4、Dragonfly 1.39.0 Docker 均覆盖 persistent/binary/empty/长短 TTL、短期过期不复活、长 TTL 不延寿、expiry 绑定确认与零残留；Dragonfly `TIME` 的整数数组响应也按 typed value 精确兼容。 |
| IF-T65 发布制品目标身份完整性 | P0 | 完成 | archive/npm/wheel 打包器原先允许从 artifact 根目录复用一个通用 `dbtool`，本机文件可能被复制成其它平台的“制品”。 | 所有打包器只接受 target 专属路径并在写输出前预检选择目标；正式 workflow 固定 `DBTOOL_PACKAGE_TARGETS=aarch64-apple-darwin`，并在真实 ARM64 runner 上执行目标 binary。其它目标仍可由通用工具显式生成，但不会进入正式 Release。 |
| IF-T66 元数据与目录读取有界化 | P0 | 完成 | SQL/CQL/Db2 schema/table/catalog、Document collection、Search index、TS measurement、MQ topic 等顶层 list 接口曾可能由 adapter 全量取回后才在 CLI 截断。 | 各族已冻结 N+1、continuation 或硬响应预算；CLI/TUI 只调用显式 bounded operation，精确区分完整、截断与不支持；SQLite/PostgreSQL/MySQL/MariaDB/Cassandra/Mongo/OpenSearch/Prometheus/Redis-compatible/Kafka/RabbitMQ/NATS 实测或明确外部边界均有独立证据。 |
| IF-T67 二级元数据有界化 | P0 | 完成 | `MetadataBudget(max_items,max_bytes)` 已覆盖 SQL/CQL/Db2 完整表结构与 DDL、AMQP/Kafka/Redis/NATS detail 和 lag；columns、index membership、config、watermark 与 lag 候选均在保留前计费。CLI/TUI 只协商 exact bounded operation，`--limit/--max-bytes` 在 DSN 前校验；超限稳定返回 `METADATA_BUDGET_EXCEEDED`，不返回部分 schema/detail/DDL。 | core、SQLx、SQL Server、Cassandra、Db2 和四类消息 adapter 的 N/N+1、字节 N/N-1、协议 frame/分页边界均通过；外部 SQL Server/Db2 live 仍归 IF-T52。证据：[`nested-metadata-budgets.md`](test-evidence/nested-metadata-budgets.md)。 |
| IF-T68 Transfer artifact 跨平台原子替换 | P1 | 实施完成，等待当前 SHA 的 Windows CI 运行证据 | transfer 与连接配置已复用同一原子发布 primitive；Windows 使用 replace-existing + write-through，Unix rename 后同步父目录；替换前失败删除临时文件且旧目标保持完整。PR 门禁已显式运行 core 原子测试、CLI transfer 替换测试，并对 Windows x64/arm64 portable release 完整编译链接；本机不是 Windows，不能把门禁配置冒充为本 SHA 的 Windows PASS。 | 本机 core 3/3、CLI transfer 1/1、portable check 和 YAML 解析通过；合并/关闭前需记录当前 SHA 的 Windows x64 runtime 与 arm64 compile/link run URL。证据：[`transfer-artifact-atomic-publication.md`](test-evidence/transfer-artifact-atomic-publication.md)。 |
| IF-T69 SQL/CQL 结果递归字节预算 | P0 | 完成 | 新增 `ReadBudget`、`sql.query_budgeted/cql.query_budgeted` 与稳定 `READ_BUDGET_EXCEEDED`；列元数据、完整 row、递归 Value、最终 ResultSet 和 N+1 probe 共用一个字节信封。SQLx、Cassandra、Tiberius、Db2 均在协议读取边界限制观察/预取；CLI/TUI/export 只调用 exact operation，并暴露全局 `--max-bytes`。 | core/五类 adapter 单测与 Clippy 通过；SQLite 字节边界、Docker PostgreSQL 16/MySQL 8/Cassandra 5 的 N/N+1 与小字节预算均由 CLI 实测；SQL Server/Db2 live 归 IF-T52。证据：[`read-budget-envelopes.md`](test-evidence/read-budget-envelopes.md)。 |
| IF-T70 KV/Redis 读取信封 | P0 | 完成 | 已新增 `kv.exists`、`kv.get_bounded`、`kv.get_with_expiry_bounded`、`kv.scan_bounded`、`kv.raw_command_bounded`；GET/TTL、SCAN 页和只读 RAW 在 Redis Lua 内先执行值、页和递归响应门禁，再由 `ReadLimiter` 精确校验 portable 信封。 | CLI/TUI/export 只协商 exact bounded operation；KV export 对 scan、每键快照、累计 entries 与最终 artifact 共用调用方上限；replace 预检只调用 `EXISTS`；`GETDEL/LPOP/RPOP/SPOP/SET ... GET` 在 mutation 前拒绝。Redis、Valkey、KeyDB、Dragonfly 的 bounded 读取及完整 KV CRUD/RAW 清理矩阵通过。证据：[`kv-read-envelopes.md`](test-evidence/kv-read-envelopes.md)。 |
| IF-T71 Document 累计字节预算 | P0 | 完成 | 新增 `document.find_budgeted/aggregate_budgeted`；Mongo 游标使用单文档 batch，原始 BSON 在转换前累计计费，portable Document、完整响应与 N+1 probe 再次计费。CLI、TUI、export 与 import preflight 均使用 exact caller-owned budget；TUI 固定默认 8 MiB，import 对每个精确 `_id` probe 使用同一上限值。旧 document 能力不授权新方法。 | core 111、Mongo 13、默认 feature CLI/TUI 回归与 MongoDB 7 Docker find/aggregate N+1、小字节失败和集合清理通过。driver 在 adapter 计费前物化一个合法单文档/wire response 的边界已记录。证据：[`read-budget-envelopes.md`](test-evidence/read-budget-envelopes.md)。 |
| IF-T72 Messaging 消费字节与 ACK 语义 | P0 | 完成 | `ConsumeOptions` 已增加单消息/整批硬预算；CLI 以 `--max-message-bytes` 和全局 `--max-bytes` 暴露。AMQP、Redis Streams、pure/native Kafka、Core NATS/JetStream 在确认前完成完整 Message 与 Vec 包络计费；Kafka 的 receive/fetch frame 在 DSN 后强制冻结。 | 默认/native 测试与 Clippy 通过；RabbitMQ 超限 requeue、Redis PEL 保留重放、Kafka 不提交 offset、JetStream ACK floor 不推进均由 Docker 实测。Core NATS/Redis PubSub 无 ACK/replay 的协议边界明确。证据：[`message-consume-byte-budgets.md`](test-evidence/message-consume-byte-budgets.md)。 |
| IF-T73 Time-series 结构化预算 | P1 | 完成 | 已新增 `TimeSeriesReadBudget(max_series,max_samples,max_bytes)`、`query_range_bounded` 与独立 `time_series.query_range_bounded` operation；series header/label/column、跨全部 series 的累计 sample、完整 `SeriesSet` 和唯一 N+1 probe 共用调用方字节信封，不返回字节超限的部分结果。 | Prometheus adapter 保留独立 16 MiB HTTP transport ceiling；CLI 以 `--max-series`、全局 `--limit` 和 `--max-bytes` 在连接前构建三维预算，TUI 只协商 exact operation。Core/adapter/CLI/TUI 测试、Prometheus adapter 与公共 CLI Docker 读回均通过。证据：[`time-series-read-envelopes.md`](test-evidence/time-series-read-envelopes.md)。 |
| IF-T74 目录标量字节预算 | P1 | 完成 | 12 类 exact `*_budgeted` sibling 已覆盖 SQL/CQL/Db2/Document/Search/TimeSeries/Messaging 顶层目录；所有完整名称或 catalog object 在 retention 前经 `ReadLimiter` 计费，最终 `BoundedList` 与唯一 probe 共用调用方 item+byte 信封。CLI/TUI 只协商 exact operation，预算在 DSN 前校验。 | SQLite、PostgreSQL、MySQL、MariaDB、TiDB、Cassandra、MongoDB、OpenSearch、Elasticsearch、Prometheus、Redpanda、RabbitMQ、Redis、NATS 的 item N/N+1、byte N/N-1 与清理通过；SQL Server/Db2 service-free 通过，live 仍归 IF-T52。证据：[`catalog-scalar-byte-budgets.md`](test-evidence/catalog-scalar-byte-budgets.md)。 |
| IF-T75 旧无界 API 生命周期 | P2 | 完成 | Core 9 个 trait 的 64 个无界/仅 item-bound 方法已统一标记 Rust `#[deprecated]`，每个 note 指向 exact replacement；兼容面在整个 `1.x` 保留，最早只能在 `2.0.0` 删除。 | CLI/TUI/transfer/embedded/全部 adapter production target 以 `RUSTFLAGS='-D deprecated' cargo check --workspace --lib --bins` 通过；exact 默认实现不回退 legacy，仅保留两个明确的 `1.x` legacy-to-legacy Document 默认桥接和局部兼容测试。证据：[`legacy-api-lifecycle.md`](test-evidence/legacy-api-lifecycle.md)。 |
| IF-T76 Search 完整读取信封 | P0 | 完成 | 已新增 `search_budgeted/get_doc_budgeted`、独立 exact operation 和 `SearchReadLimiter`；search 对完整 hit/_source、aggregations、hits metadata、unknown `extra` 与最终 `SearchHits` 计费，get 对存在/缺失的完整 optional envelope 计费。 | HTTP Content-Length/chunked/无长度 body 在 JSON 解析前受 `min(caller max_bytes,16 MiB)` 限制；请求 `size` 不能超过 caller max_items。CLI/TUI 只协商 exact operation；OpenSearch 2.17.1 与 Elasticsearch 8.15.5 各自通过 adapter 和公共 CLI 完整生命周期、删索引与零残留。证据：[`search-read-envelopes.md`](test-evidence/search-read-envelopes.md)。 |
| IF-T77 MessageProducer 输入预算与结果不确定性 | P0 | 完成 | 已新增 `ProduceBudget(max_messages,max_message_bytes,max_batch_bytes)`、`MessageWriteLimiter`、`produce_budgeted` 与 exact `message.produce_budgeted`；CLI 在 DSN 前校验完整 Message/Vec 包络，执行开始后的超时不再返回可重试 `TIMEOUT`。Kafka、RabbitMQ、NATS、Redis 均在建资源/首个发送前完成全批预算和协议字段预检；首次可能写入后的失败统一为非重试 `OUTCOME_INDETERMINATE`。exact 空批 fail closed；0.x legacy 仅保留有效 target 的空批 no-op，非空调用也使用有限默认预算。 | RabbitMQ Docker 4/4、NATS 3/3、pure Kafka 1/1、native Kafka 1/1、Redis 7 1/1 均证明超限零写或不建资源、精确 N/N-1、成功读回与本次资源清理；NATS 最终 `streams=consumers=messages=0`，Redis `dbtool_it_produce_*` 为空。Redpanda 的一个旧测试 topic 不是本次资源且未被越权删除。证据：[`message-produce-input-budgets.md`](test-evidence/message-produce-input-budgets.md)。 |
| IF-T78 通用 mutation/request 输入信封 | P1 | 完成 | 新增通用 `InputBudget/InputLimiter`，以 20 个 exact mutation operation 覆盖 SQL 2、CQL 1、KV 4、Document 7、Search 5、TimeSeries 1；CLI/TUI/transfer/embedded 及 adapter 使用同一 caller-owned 输入信封。 | 所有数量、完整 item、递归字节、target 和协议固定上限在连接/首个写入前重复验证；N/N-1、原子/非原子语义、`OUTCOME_INDETERMINATE`、Docker 完整 CRUD/读回与零残留已按资源记录。证据：[`mutation-input-budgets.md`](test-evidence/mutation-input-budgets.md)。 |
| IF-T79 本地连接配置有界化 | P2 | 完成 | 配置文件、模型字段、环境目录以及 raw/expanded DSN 均有固定数量/字节上限；load 拒绝 symlink/device/增长或替换中的文件，save 在分配完整 TOML 和原子发布前计算转义后上界。`conn list` 使用调用方 `--limit/--max-bytes` 与 512 items/256 KiB 硬上限的较小值。 | JSON/Table/NDJSON 的 item N/N+1、byte N/N-1、固定元数据不足、配置 1 MiB、1024 entries、环境 256 entries/512 KiB、DSN 16 KiB、相对配置路径跨 cwd 绑定、控制/non-UTF-8 路径及凭据不回显均通过；Core 139、conn 15+8、cli_json 35 和 Clippy PASS。Windows drive-relative cfg 测试已实现但当前 SHA 的真实 Windows 运行仍归 IF-T68 门禁。证据：[`bounded-connection-config.md`](test-evidence/bounded-connection-config.md)、[`confirmation-target-binding.md`](test-evidence/confirmation-target-binding.md)。 |
| IF-T80 ScyllaDB 真实产品验证 | P1 | 完成 | `scylla://` 原先只在 Cassandra 节点上验证别名路由，不能证明命名产品兼容。 | 新增官方 ScyllaDB 2026.1.8 ARM64 Docker profile、独立 DSN/up/test/db-suite/CI 入口；真实节点完成 SQL-compatible 与原生 CQL 表 CRUD、12 类 typed 值、目录及结果 N/N+1/字节预算、exact mutation 输入预算和全部 keyspace/table/container 清理。证据：[`scylladb.md`](test-evidence/scylladb.md)，实现提交 `336f4bd`。 |

## 顶层有界目录完成证据

IF-T66 的协议族证据分开保存，避免用一个“全部通过”掩盖不同协议的分页边界：

- [SQL / PostgreSQL / MySQL / MariaDB](test-evidence/sql-catalog-bounded.md)
- [SQL Server](test-evidence/sqlserver-catalog-bounded.md)
- [Cassandra / Scylla CQL](test-evidence/cassandra-catalog-bounded.md)
- [Db2](test-evidence/db2-catalog-bounded.md)
- [Document / Search / TimeSeries](test-evidence/bounded-document-search-timeseries.md)
- [Redis / Kafka / RabbitMQ / NATS Messaging](test-evidence/messaging-bounded-catalogs.md)
- [IF-T74 items + bytes 总矩阵](test-evidence/catalog-scalar-byte-budgets.md)
- [IF-T75 旧 API 弃用与一方 exact-path 总矩阵](test-evidence/legacy-api-lifecycle.md)
- [IF-T77 MessageProducer 输入预算与结果不确定性](test-evidence/message-produce-input-budgets.md)
- [IF-T78 通用 mutation 输入信封与真实后端矩阵](test-evidence/mutation-input-budgets.md)

这些证据只关闭“顶层名称目录”。表结构列/索引、partition/lag 明细和完整 DDL 的二级增长
边界由 IF-T67 单独跟踪，不能因为 IF-T66 完成而自动视为有界。

## 明确协议边界

以下项目不是代码缺陷，不应通过返回空集合伪装成功：

- AMQP 0.9.1 没有可移植的全局队列列表和 consumer-group lag；RabbitMQ 使用
  `rabbitmq+http://` 管理接口。
- Redis Pub/Sub 和 Core NATS subject 没有持久目录；持久管理分别使用 Redis
  Streams 和 NATS JetStream。
- Prometheus 是 append/query 模型，不提供通用行式 update/delete。
- 无状态 CLI 的跨进程全局 QPS 不属于当前进程内 `FlowControl` 合同。

## 每项完成流程

1. 先增加失败测试，锁定接口和安全行为。
2. 实现 core trait、adapter、CLI/TUI 或 embedded 调用路径。
3. 更新中文使用规范、帮助文本、任务状态和完整性证据。
4. 运行最小针对性测试及 `git diff --check`。
5. 以仓库现有 Lore 格式独立提交，再进入下一项。
6. 最终运行 `./scripts/verify.sh`、全 feature 编译和打包烟测。
