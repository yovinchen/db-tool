# 方法级能力协商测试证据

状态：PASS（2026-07-16）

Task：IF-T49

## 冻结合同

dbtool 的能力发现分为三层：

1. `Capabilities` 的十个布尔字段只用于旧调用方的能力族发现；
2. `Connector::operations()` 是具体方法能否调用的权威合同；
3. `as_sql()`、`as_document()` 等 accessor 只在精确 operation 已通过后取得。

调用顺序固定为：

```text
connect -> operations contains exact method -> capability accessor -> invoke
```

缺少 operation 时返回 `UNSUPPORTED_CAPABILITY`，不得先 downcast、不得回退到语义更弱或
无界的方法、不得用空集合伪装成功。`caps.data.operations` 与 Rust
`CapabilityOperation` 的稳定序列化名是机器合同；`error.needed` 只作为诊断字段，不应用来
替代 operation 集做版本协商。

`CapabilityReport` 由 CLI 与 TUI 共用：旧布尔字段继续扁平输出，`operations` 按稳定名称
排序并去重。反序列化旧版、不含 `operations` 的报告时，该字段默认为空，因此旧报告不会
被误解为拥有任一新方法。

## 不允许由粗粒度字段推导的扩展

| 扩展族 | 必须显式声明的 operation |
| --- | --- |
| SQL 原子导入 | `sql.insert_rows_atomic` |
| KV expiry 无损迁移 | `kv.get_with_expiry`, `kv.restore_with_expiry` |
| Document 精确 cardinality | `document.update_one`, `document.update_many`, `document.delete_one`, `document.delete_many` |
| Document 生命周期 | `document.drop_collection` |
| 有界目录 | 所有 `*.list_*_bounded` operation |
| item + byte 目录 | 所有 `*.list_*_budgeted` operation；不能由旧 bounded operation 推导 |
| Stateful messaging | `message.consume_group`, `message.consume_durable`, `message.consume_ack` |
| Messaging admin | list-bounded、detail、lag、delete 四项分别声明 |

尤其是 `admin=true` 不授权任何具体管理方法；`document=true` 不授权 drop；
`consumer=true` 不授权 group/durable/ack。

## 当前 partial connector 矩阵

| Connector | 显式扩展能力 |
| --- | --- |
| MySQL/PostgreSQL/SQLite | SQL atomic import；SQL schema/table bounded catalog |
| SQL Server | SQL schema/table bounded catalog |
| Cassandra/Scylla | SQL schema/table bounded catalog；CQL keyspace/table bounded catalog |
| Db2 | SQL schema/table bounded catalog；四类 Db2 bounded catalog |
| MongoDB | bounded collections；one/many update/delete；drop collection |
| Redis family | KV expiry；group/ack；admin list-bounded/detail/delete；lag 由运行时版本探测 |
| Kafka pure | admin list-bounded/detail/delete；不声明 group/ack/lag |
| Kafka native | group/ack；admin list-bounded/detail/lag/delete |
| Direct AMQP | consume ack；admin detail/delete；不声明 list/lag |
| RabbitMQ management | admin list-bounded/detail/delete；不声明 lag |
| NATS/JetStream | group/durable/ack；admin list-bounded/detail/lag/delete |
| Search/TimeSeries | 各自 bounded catalog |

## 前端与嵌入式闭环

- CLI 的 SQL、CQL、Db2、KV、Document、Search、TimeSeries 与 Messaging 每个 action 都在
  accessor 前选择并校验 exact operation。
- `export`/`import` 按准备后的真实子步骤协商多个 operation。例如 KV export 同时要求
  scan 与 expiry snapshot；Document import 只有保留 `_id` 时才要求 find，非空批次才要求
  insert。
- TUI 中所有会取得 capability accessor 的 data action 同样先协商 exact operation；
  connector-level `ping` 不需要 operation，`ping.capabilities` 与 `caps` 输出同一
  `CapabilityReport`。
- Embedded registry smoke 在 `as_sql()` 前检查 `sql.execute` 与
  `sql.query_budgeted`，并传入显式 `ReadBudget`，展示可复制的调用范式。

## 自动化结果

| 验证 | 结果 |
| --- | --- |
| `dbtool-core` full unit suite, including capability/legacy/serde coverage | 96/96 PASS |
| Mongo adapter full unit suite, including explicit operation declaration | 10/10 PASS |
| CLI command unit tests | 92/92 PASS |
| TUI tests | 32/32 PASS |
| Transfer unit tests | 19/19 PASS |
| Transfer artifact integrations | 10/10 PASS |
| CLI caps integration | PASS |
| Embedded registry integration | PASS |
| CLI/TUI targeted Clippy `-D warnings` | PASS |
| SQLite core end-to-end smoke | PASS |

Docker MongoDB 7 真实用例重新运行了完整 find options、bounded aggregate、one/many
update/delete 与 drop collection，并断言 `caps.data.operations` 包含
`document.drop_collection`；结果 PASS，测试 collection 清理完成。

Messaging 的 partial-admin 负路径已经在 Redis、Kafka pure/native、direct AMQP、RabbitMQ
management 与 NATS Docker 用例中覆盖：未声明 lag/list/detail/delete 的 connector 返回
`UNSUPPORTED_CAPABILITY`，不会因为 `admin=true` 或 accessor 存在而分发到方法。

## 对应提交

- `5a4d2d0`：共享 capability report、Document lifecycle 与 MQ 方法门禁；
- `d6b34a7`：旧版 capability JSON 反序列化兼容；
- `249e266`：所有 CLI/TUI 方法统一 exact operation 调度；
- `dddff00`：transfer 按准备后的真实子步骤协商。

本证据本身不把未提供的外部服务写为 live pass；Db2 运行环境边界仍由其产品任务管理。
SQL Server 后续已在 x86_64 官方产品容器完成独立生命周期，见 `sqlserver.md`，但不把该
结果倒写成这里的 exact capability 单测。
