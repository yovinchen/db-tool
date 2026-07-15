# dbtool 接口完整性收口任务表

更新时间：2026-07-16

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
| IF-T48 CLI 参数面完整性 | P1 | 实施中（native Kafka group commit 完成；Redis/NATS/AMQP 待完成） | Search 聚合及完整响应、TS start/end、MQ key/header/partition/timestamp/offset、lossless cursor 与非 Kafka metadata 已补齐；group/durable/ack 已冻结为 typed identity/ack 与独立 operation。native Kafka 已实现真正 group subscription、ack-none 重放和 whole-batch 成功后 commit；其它 broker 的 PEL/durable/ACK 仍需分别兑现。 | Search、TS 和 native Kafka 已完成；Kafka/Redis Streams/NATS JetStream 的无状态 cursor 继续兼容；完成 Redis XREADGROUP/XACK、NATS queue/durable ACK 和 AMQP 批量成功后 ACK 后整体标记完成。 |
| IF-T49 能力协商规范 | P1 | 待实施 | `Capabilities` 只有粗粒度布尔值，调用方仍可能在运行时命中某个方法的 `UNSUPPORTED_CAPABILITY`。 | `caps` 保持兼容并补充稳定的 operation 列表；每个 connector 明确声明支持与边界；文档提供嵌入式调用范式。 |
| IF-T50 设计与状态文档同步 | P1 | 待实施 | `dbtool-design.md`、README、implementation status 的 feature、命令和更新时间落后于实际代码。 | 设计文档只列真实 feature/scheme；已解决开放问题归档；中文 CLI/嵌入式/安全范式完整；状态文档与任务表一致。 |
| IF-T51 编译与打包闭环 | P0 | 实施中（宿主机真实打包完成，等待全部接口完成后的全仓终验） | 六平台发布链已经严格绑定 target artifact；本机需在新接口全部完成后再执行一次最终 fmt/check/clippy/test/full-native。 | 本机 `portable` release binary、completions、manpage、archive、npm tarball、wheel 已真实生成、隔离安装并执行；最终全仓验证及六平台 CI 矩阵仍作为关闭条件。 |
| IF-T52 外部产品验证 | P2 | 外部阻塞 | SQL Server 本地 arm64、Db2 ODBC、Redshift、AutoMQ/WarpStream/Confluent、真实 Scylla、Elasticsearch 原生 HTTPS 依赖额外环境。 | 保留独立 runner 和解除条件；获得相应运行时/DSN 后逐产品生成 LIVE_PASS 证据；没有真实运行不得改成 COMPLETE。 |
| IF-T53 Cargo feature 与发布能力一致性 | P0 | 完成 | CLI/TUI 原先无法真正关闭 registry 默认 feature；`full-native` 同时启用 pure/native Kafka；release 只构建 default。 | `--no-default-features` 现在得到空 registry；`full`/`full-native` 互斥选择 Kafka backend；六平台 release 使用不含 host ODBC/native Kafka 的 `portable` 完整自包含能力集；feature matrix 校验 scheme 与依赖树。 |
| IF-T54 TUI 安全与终端恢复 | P0 | 完成 | TUI 原先依赖字符串前缀识别写操作，SQL fallback 可绕过共享 `SafetyGuard`；异常退出可能残留 raw mode/alternate screen。 | 所有 SQL 输入现统一使用 AST 分类、readonly 与一次性确认检查；query/fallback 绕过回归通过；RAII `TerminalSession` 已覆盖正常、draw/poll/read 错误、初始化失败和 panic unwind 恢复。 |
| IF-T55 CLI/发布严格性 | P0 | 完成 | 非法 `--format` 原先静默降级；tag 与 Cargo 版本无一致性门禁；npm/Python 包缺少安装后执行烟测，npm binary 权限未显式设置。 | Clap `ValueEnum` 严格拒绝非法格式；release 先校验 tag=workspace 版本；npm Unix binary 强制 0755；npm 与 musllinux wheel 在发布工作流安装后执行并核对版本；npm/Python 制品附加到 GitHub Release。 |
| IF-T56 SQL 行返回写操作安全 | P0 | 完成 | 共享分类器原先把所有 parsed Query 与解析失败的 `WITH` 当作只读，data-modifying CTE、`SELECT INTO` 和 locking SELECT 可穿过 query/export。 | Query AST 递归检查 CTE/set body；`SELECT INTO` 为 destructive，locking SELECT 为 write，无法检查的 WITH fail closed；破坏性 SQL 在发令牌前要求 `--allow-write`；core 与 CLI 连接前回归通过。 |
| IF-T57 Redis SCAN 精确分页 | P0 | 完成 | 原实现依赖 `AsyncIter`，分页中的请求/解码错误可能被隐藏；CLI 用 `len >= limit` 会把刚好 N 条误报为截断。 | 适配器显式循环 `SCAN cursor MATCH COUNT`，传播每页错误、严格拒绝非 UTF-8 key、跨页去重并检测 cursor 环；CLI 读取 N+1 后仅在存在探测 key 时标记截断；Redis Docker 的 N、N+1、25-key 多页、非 UTF-8 错误与全量清理均通过。 |
| IF-T58 SQL 参数化事务导入 | P0 | 完成 | SQL artifact 原先把值拼成方言相关 literal 并逐行独立提交；中途约束失败会留下部分数据，却统一报告 `atomic=false`。 | 新增不由 `sql=true` 推导的可选 `sql.insert_rows_atomic`；SQLite/PostgreSQL/MySQL 二次校验标识符和行宽、逐值绑定参数并在一个事务提交，任一错误全部回滚且成功报告 `atomic=true`；MySQL 在写前拒绝 MyISAM 等非事务引擎；其他 SQL connector 明确返回不支持，绝不退回 literal 拼接。 |
| IF-T59 RabbitMQ 管理计数与删除确认 | P0 | 完成 | RabbitMQ 新队列的管理响应可能暂时缺 `messages`，旧实现要么把可重建计数当失败，要么有被缺字段误报为零的风险；删除 token 只绑定 kind/name，未绑定 `if-empty/if-unused`。 | queue detail 优先使用合法 `messages`，字段缺失时只以 checked `messages_ready + messages_unacknowledged` 重建；缺失、非法和溢出均 fail closed；删除确认摘要绑定完整 resource/options，不能跨条件复用；RabbitMQ 3.13 Docker 的生产、详情、ACK、条件删除、删除后不存在及零残留通过。 |
| IF-T60 命名连接配置 CRUD | P0 | 完成 | 设计和 CLI help 声明 `conn list/add/remove`，实际只有 list；调用方只能手工编辑含凭证的 TOML，缺少原子写、覆盖确认和脱敏合同。 | `conn add` 要求写权限并默认拒绝覆盖；`--replace` 与 `conn remove` 使用绑定配置路径、name、旧/新条目内容的确认；环境管理项不可修改；DSN template 离线校验且不展开 secret；同目录 0600 temp、file sync、跨平台原子替换和 parent/write-through 后发布；失败前旧文件保持完整，所有输出脱敏并明确 typed serialization 不保留注释。 |
| IF-T61 Document 单条/批量变更语义 | P0 | 完成 | 公共 `update/delete` 沿用批量语义，而 CLI 未让调用者明确选择单条或批量，非空但宽泛的过滤器可能一次影响多条。 | 保留旧方法的批量兼容合同，新增且独立协商 `document.update_one/update_many/delete_one/delete_many`；CLI 默认只变更一条，`--many` 必须使用绑定连接、集合、操作、规范化过滤器及更新内容的确认令牌；MongoDB 7 Docker 精确验证单条各 1 条、批量更新 3 条、批量删除剩余 2 条，跨内容/操作复用令牌被拒且最终零集合残留。 |
| IF-T62 KV 二进制与 RAW 安全 | P0 | 完成 | `kv get` 原先用有损 UTF-8 转换，binary/empty/missing 无法可靠区分；`kv set` 只有文本输入；raw 几乎可透传任意命令，写操作只有粗粒度权限且集合响应可能无界或丢失 RESP 类型。 | get 保留 UTF-8 兼容字段并新增 typed bytes/encoding；set 支持严格 canonical base64；CLI 与 Redis adapter 使用独立白名单、参数/请求/响应预算和不可移植 RESP 显式错误；所有 raw mutation 需要参数/目标/连接绑定确认，管理/脚本/连接态/未知/无界命令 fail closed；Redis 7 Docker 覆盖 binary/empty/text/missing、令牌防复用、危险命令拒绝及零前缀残留。 |
| IF-T63 Messaging 状态消费 typed 合同 | P0 | 完成 | `ConsumeOptions` 和 CLI 无法表达 consumer group、durable 与 ACK/commit 意图；粗粒度 `consumer=true` 可能被误当成会推进 broker 进度。 | 新增 `ConsumerIdentity` 与 `AckMode`、`--group/--consumer/--durable/--ack` 和三项稳定 operation；stateful identity 必须显式选择 ACK 且要求写权限，身份/互斥/position 冲突在连接前拒绝；legacy consumer 不自动声明扩展能力，只有逐协议 adapter 明确声明后 CLI 才调用。 |
| IF-T64 KV 过期时间无损迁移 | P0 | 实施中（core、CLI v3 与 Redis 实测完成；兼容产品待跑） | KV v2 artifact 只保存 value，导入时统一 `--ttl` 会把 persistent 与 expiring key 混为一谈，并可能延长或复活源 key。 | 已以独立协商的 `kv.get_with_expiry/kv.restore_with_expiry` 原子合同生成 v3 artifact，v2/缺失 expiry 在 DSN 解析前拒绝，过期 entry 在目标 Redis 内原子判定并跳过；Redis 7.4.9 Docker 已覆盖 persistent/binary/empty/长短 TTL、短期过期不复活、长 TTL 不延寿、expiry 绑定确认与零残留。Valkey/KeyDB/Dragonfly 的 v3 生命周期矩阵尚未重跑，不得标记本项完成。 |
| IF-T65 发布制品目标身份完整性 | P0 | 完成 | archive/npm/wheel 打包器原先允许从 artifact 根目录复用一个通用 `dbtool`，本机文件可能被复制成其它五个平台的“制品”。 | 所有打包器只接受 target 专属路径并在写输出前预检全部选择目标；默认仍要求六目标齐全，`DBTOOL_PACKAGE_TARGETS` 只用于显式本机烟测且拒绝未知、空项、重复项；macOS arm64 archive/npm/wheel 均由真实 release binary 安装执行。 |
| IF-T66 元数据与目录读取有界化 | P0 | 待实施 | SQL schema/table、Document collection、Search index、TS measurement、MQ topic 等 list 接口仍可能由 adapter 全量取回后才在 CLI 截断。 | 为每族冻结 adapter 侧 N+1/continuation 合同；CLI、TUI、embedded 不得先全量加载；精确区分完整、截断与不支持，并增加大目录回归。 |

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
