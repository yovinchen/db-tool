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
| IF-T47 Messaging 精细能力与资源删除 | P1 | 待实施 | `admin=true` 不能表达方法级支持；Kafka lag 未实现；公共 API 不能删除 topic/queue/stream。 | 能力报告列出具体操作；native Kafka 支持 group offset/lag；新增独立 `AdminMutate`，可支持的 Kafka/RabbitMQ/Redis/NATS 资源删除受写保护与确认；协议不适用项保持明确边界。 |
| IF-T48 CLI 参数面完整性 | P1 | 实施中（TS/Search/Kafka 字段已完成） | Search 聚合及完整响应、TS start/end 已补齐；MQ 已暴露 key/header/partition/timestamp/offset 并完成 Kafka 双后端保真验证，精确 cursor/group 与非 Kafka 元数据边界仍在收口。 | Search 已返回聚合与后端元数据；TS 已支持互斥的最近窗口和成对 epoch-ms 范围；Kafka 通过 pure/native 实测逐字段回读；待完成 cursor/group 和各消息协议的拒绝/映射规范后整体标记完成。 |
| IF-T49 能力协商规范 | P1 | 待实施 | `Capabilities` 只有粗粒度布尔值，调用方仍可能在运行时命中某个方法的 `UNSUPPORTED_CAPABILITY`。 | `caps` 保持兼容并补充稳定的 operation 列表；每个 connector 明确声明支持与边界；文档提供嵌入式调用范式。 |
| IF-T50 设计与状态文档同步 | P1 | 待实施 | `dbtool-design.md`、README、implementation status 的 feature、命令和更新时间落后于实际代码。 | 设计文档只列真实 feature/scheme；已解决开放问题归档；中文 CLI/嵌入式/安全范式完整；状态文档与任务表一致。 |
| IF-T51 编译与打包闭环 | P0 | 待实施 | 现有脚本已覆盖六平台制品，但需要在新接口完成后重新生成 CLI artifact、release archive、npm 包和 Python wheel 并做安装烟测。 | fmt/check/clippy/test/full-native 通过；本机 release binary、completions、manpage、archive、npm tarball、wheel 生成并校验；跨平台矩阵配置验证通过。 |
| IF-T52 外部产品验证 | P2 | 外部阻塞 | SQL Server 本地 arm64、Db2 ODBC、Redshift、AutoMQ/WarpStream/Confluent、真实 Scylla、Elasticsearch 原生 HTTPS 依赖额外环境。 | 保留独立 runner 和解除条件；获得相应运行时/DSN 后逐产品生成 LIVE_PASS 证据；没有真实运行不得改成 COMPLETE。 |
| IF-T53 Cargo feature 与发布能力一致性 | P0 | 完成 | CLI/TUI 原先无法真正关闭 registry 默认 feature；`full-native` 同时启用 pure/native Kafka；release 只构建 default。 | `--no-default-features` 现在得到空 registry；`full`/`full-native` 互斥选择 Kafka backend；六平台 release 使用不含 host ODBC/native Kafka 的 `portable` 完整自包含能力集；feature matrix 校验 scheme 与依赖树。 |
| IF-T54 TUI 安全与终端恢复 | P0 | 完成 | TUI 原先依赖字符串前缀识别写操作，SQL fallback 可绕过共享 `SafetyGuard`；异常退出可能残留 raw mode/alternate screen。 | 所有 SQL 输入现统一使用 AST 分类、readonly 与一次性确认检查；query/fallback 绕过回归通过；RAII `TerminalSession` 已覆盖正常、draw/poll/read 错误、初始化失败和 panic unwind 恢复。 |
| IF-T55 CLI/发布严格性 | P0 | 完成 | 非法 `--format` 原先静默降级；tag 与 Cargo 版本无一致性门禁；npm/Python 包缺少安装后执行烟测，npm binary 权限未显式设置。 | Clap `ValueEnum` 严格拒绝非法格式；release 先校验 tag=workspace 版本；npm Unix binary 强制 0755；npm 与 musllinux wheel 在发布工作流安装后执行并核对版本；npm/Python 制品附加到 GitHub Release。 |
| IF-T56 SQL 行返回写操作安全 | P0 | 完成 | 共享分类器原先把所有 parsed Query 与解析失败的 `WITH` 当作只读，data-modifying CTE、`SELECT INTO` 和 locking SELECT 可穿过 query/export。 | Query AST 递归检查 CTE/set body；`SELECT INTO` 为 destructive，locking SELECT 为 write，无法检查的 WITH fail closed；破坏性 SQL 在发令牌前要求 `--allow-write`；core 与 CLI 连接前回归通过。 |

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
