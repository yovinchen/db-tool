# IF-T74 顶层目录标量字节预算证据

状态：PASS（2026-07-16）

## 统一合同

所有已注册、可增长的顶层目录均提供独立 exact `*_budgeted` operation，并接收
`ReadBudget(max_items,max_bytes)`：

1. item/byte 预算在 DSN 解析或任何后端请求前验证；
2. 每个完整名称或 portable catalog object 在 retention 前计费；
3. 最多观察 N+1 项，输出最多 N 项，只有真实 probe 存在才标记 `truncated=true`；
4. 返回前校验完整 `BoundedList { items, truncated }` 与 probe 字节；
5. 字节不足返回稳定 `READ_BUDGET_EXCEEDED`，不返回部分目录；
6. CLI/TUI 只协商 exact budgeted operation，不回退到 legacy 全量或 item-only 方法。

Legacy `list_*` 与 `list_*_bounded` 仍为 0.x 嵌入式兼容面；其生命周期由 IF-T75
单独处理，不能因为本项一方路径已迁移就直接删除。

## 协议族矩阵

| 协议族 | exact operation | 后端读取边界 | 验证结果 |
| --- | --- | --- | --- |
| SQLite / PostgreSQL / MySQL / MariaDB / TiDB | `sql.list_schemas_budgeted`、`sql.list_tables_budgeted` | SQL `LIMIT N+1` 或等价 catalog query；完整 `String` / `TableInfo` retention 前计费 | SQLite 本地、PostgreSQL 16、MySQL 8.4、MariaDB 11.4、TiDB 8.5.6 item/byte PASS |
| SQL Server | 同 SQL operations | checked `TOP (N+1)`；TDS row 转换前受固定边界 | service-free 8/8 PASS；arm64 live 归 IF-T52 |
| Cassandra / CQL | SQL operations + `cql.list_keyspaces_budgeted`、`cql.list_tables_budgeted` | portable CQL query 只观察 N+1 caller-visible identity | Cassandra 5 SQL/CQL exact 等价与 item/byte PASS |
| Db2 | SQL operations + 四个 `db2.list_*_budgeted` | fetch/row 上限与完整 sequence/routine/tablespace/FK 计费 | service-free 22/22 PASS；ODBC live 归 IF-T52 |
| MongoDB | `document.list_collections_budgeted` | `listCollections batchSize=N+1`，driver 单项解码后立即计费 | MongoDB 7 item/byte、drop、零前缀残留 PASS |
| OpenSearch / Elasticsearch | `search.list_indices_budgeted` | raw CAT JSON `min(caller,1 MiB)`；只构造/观察 N+1 `IndexInfo` | OpenSearch 2.17.1、Elasticsearch 8.15.5 item/byte 与删索引清理 PASS |
| Prometheus | `time_series.list_measurements_budgeted` | label-values `limit=N+1`；独立 raw HTTP 16 MiB ceiling | Prometheus 2.55.1 item/byte PASS；一次性 TSDB volume 清理 |
| Kafka pure/native | `message.admin.list_topics_budgeted` | dedicated metadata frame 16 MiB；只构造 N+1 `TopicInfo` | pure 18、native 26；Redpanda live item/byte 与 run-owned topic 清理 PASS |
| RabbitMQ management | 同 Messaging operation | 稳定分页、最终页只转换剩余 N+1、每页 1 MiB | 24 unit + 3 integration；RabbitMQ 3.13 队列清空 PASS |
| Redis Streams | 同 Messaging operation | capped read-only Lua SCAN page + visitor | 47/47；Redis 7 DB14 item/byte、`DBSIZE=0` PASS |
| NATS JetStream | 同 Messaging operation | stream iterator到 N+1 即停止 | 17 unit + 2 integration；NATS 2.10 state 全零 PASS |

Direct AMQP、Redis Pub/Sub 与 Core NATS subjects 没有可移植持久目录，继续明确不广告
该 exact operation；这不是用空列表伪装成功。Redshift、SQL Server、Db2、真实 Scylla、
外部 Kafka vendors 与 Elasticsearch 原生 HTTPS 的额外产品运行仍由 IF-T52 跟踪。

## 一方调用链验证

CLI 的 SQL、CQL、Db2、Document、Search、TimeSeries、Messaging 目录命令，以及 TUI
暴露的 SQL/Document/Search/TimeSeries 目录命令，均在连接前构造 item+byte budget，并
只调用 exact sibling。

```text
cargo test -p dbtool-cli --bin dbtool       # 106/106 PASS
cargo test -p dbtool-tui                    # 33/33 PASS
cargo clippy -p dbtool-cli -p dbtool-tui --all-targets -- -D warnings
```

各 adapter 的定向单测和严格 Clippy 均通过；真实 Docker 运行、资源名、版本、字段级
CRUD/查询和清理结果保存在各产品证据文件及以下分族证据中：

- [`sql-catalog-bounded.md`](sql-catalog-bounded.md)
- [`sqlserver-catalog-bounded.md`](sqlserver-catalog-bounded.md)
- [`cassandra-catalog-bounded.md`](cassandra-catalog-bounded.md)
- [`db2-catalog-bounded.md`](db2-catalog-bounded.md)
- [`bounded-document-search-timeseries.md`](bounded-document-search-timeseries.md)
- [`messaging-bounded-catalogs.md`](messaging-bounded-catalogs.md)

## 原子提交

接口、协议族和一方调用链按独立验证边界提交：`504646b`、`a252f41`、`9e9344e`、
`26a586e`、`6465d8c`、`7fa937e`、`d8b4737`、`a1d6ecb`、`2519684`、`7ebe273`、
`aa062a1`、`4f6de80`、`70a12aa`。本证据只关闭 IF-T74，不把 IF-T75/77/78 或
外部 IF-T52 运行自动标记为完成。
