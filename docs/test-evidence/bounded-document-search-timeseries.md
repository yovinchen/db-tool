# Document / Search / Time-Series 有界目录证据

状态：PASS（2026-07-16）

范围：IF-T66 顶层目录条目边界与 IF-T74 标量字节信封的 Document、Search、TimeSeries
分支。本证据只覆盖 collection、index、measurement 名称目录；表结构中的列/索引等二级
元数据不在本分支范围内。

## 公共合同

三个目录都使用 `ReadBudget(max_items,max_bytes)`、`BoundedList<T> { items, truncated }`
和严格 N+1 探针：

- `max_items=0`、`max_items=usize::MAX`、`max_bytes=0` 和超出 16 MiB 的字节预算在解析
  DSN、建立连接之前返回 `CONFIG_ERROR`。
- CLI 和 TUI 必须先协商各自的显式 operation；只有旧的粗粒度 capability 时返回 `UNSUPPORTED_CAPABILITY`，不会回退到无界的 legacy list。
- `items.len()` 永远不超过调用方的 `--limit`；只有实际观察到第 N+1 项时，`meta.truncated=true`。
- 每个完整名称/`IndexInfo` 在 retention 前计费；返回前再计费完整 `BoundedList` 与探针。
  exact byte N 成功、N-1 返回 `READ_BUDGET_EXCEEDED`，不返回部分目录。

| 接口 | 显式 operation | Adapter 侧边界 | 已知限制 |
| --- | --- | --- | --- |
| MongoDB collections | `document.list_collections_budgeted` | `listCollections` cursor 的 server `batchSize=N+1`；完整 name、envelope、probe 由 `ReadLimiter` 计费 | MongoDB 协议没有该命令的总结果 `limit`；driver 先解码单个 `CollectionSpecification`，`batchSize` 限制协议 cursor 批次 |
| OpenSearch / Elasticsearch indices | `search.list_indices_budgeted` | CAT JSON 受 `min(caller max_bytes,1 MiB)` 限制；只构造/观察 N+1 个完整 `IndexInfo` | CAT indices 在兼容版本间没有可靠分页/总量 limit，因此不能声称服务端只扫描 N+1 |
| Prometheus measurements | `time_series.list_measurements_budgeted` | label-values 请求带 `limit=N+1`；完整 name、envelope、probe 计费 | raw HTTP JSON wrapper 仍受独立 16 MiB transport ceiling；它不冒充 portable caller budget |

CLI 的 `doc collections`、`search indices`、`ts measurements` 和 TUI 对应命令统一输出
`data` 加 `meta.truncated`。Legacy `list_*` 与 item-only `list_*_bounded` 仅为嵌入式兼容
接口，新的 CLI/TUI 路径只调用 exact `list_*_budgeted`。

## 自动化验证

离线/单元验证：

```text
cargo test -p adapter-mongo -p adapter-search -p adapter-timeseries -p dbtool-cli -p dbtool-tui --lib
cargo test -p dbtool-cli --bin dbtool
cargo test -p dbtool-tui --bin dbtool-tui
cargo test -p dbtool-cli --test bounded_document_search_ts invalid_catalog_limits_fail_before_connection_for_all_three_surfaces -- --exact --nocapture
```

结果：Mongo 15/15、Search 37/37、TimeSeries 26/26；均覆盖 item N/N+1、byte
N/N-1、无效预算、exact operation 与协议 transport cap。CLI 106/106、TUI 33/33，目录
命令在连接前同时校验 item/byte，且拒绝 legacy fallback；严格 Clippy 与 diff check 通过。

Docker 真实验证：

```text
DBTOOL_RUN_INTEGRATION=1 \
DBTOOL_IT_MONGO_DSN='mongodb://dbtool:***@127.0.0.1:17017/dbtool_it_mongo?authSource=admin' \
cargo test -p dbtool-cli --test bounded_document_search_ts \
  mongo_live_collection_catalog_distinguishes_n_from_n_plus_one_and_cleans_up \
  -- --exact --nocapture

DBTOOL_RUN_OBSERVABILITY_INTEGRATION=1 \
DBTOOL_IT_OPENSEARCH_DSN='opensearch://127.0.0.1:19200' \
cargo test -p dbtool-cli --test bounded_document_search_ts \
  opensearch_live_index_catalog_distinguishes_n_from_n_plus_one_and_cleans_up \
  -- --exact --nocapture

DBTOOL_RUN_OBSERVABILITY_INTEGRATION=1 \
DBTOOL_IT_PROMETHEUS_DSN='prometheus://127.0.0.1:19090' \
cargo test -p dbtool-cli --test bounded_document_search_ts \
  prometheus_live_measurement_catalog_distinguishes_n_from_n_plus_one_without_writes \
  -- --exact --nocapture
```

| 后端 | N 完整结果 | N+1 截断结果 | operation | 清理 |
| --- | --- | --- | --- | --- |
| MongoDB 7 | 隔离数据库 3 collections，完整 item/byte N PASS | item N+1 截断、byte N-1 稳定失败 PASS | PASS | 三个 collection 全部 drop；测试前缀复查无残留 PASS |
| OpenSearch 2.17.1 | 公共目录 3-index 生命周期及 adapter 两个隔离 index 的 exact byte N PASS | item N+1 截断、byte N-1 稳定失败 PASS | PASS | 所有临时 index 删除；只剩原系统 index PASS |
| Elasticsearch 8.15.5 | 两个隔离 index 的完整目录和 exact byte N PASS | item N+1 截断、byte N-1 稳定失败 PASS | PASS | 两个 index 删除后 `_cat/indices` 为 `[]` PASS |
| Prometheus 2.55.1 | unique remote-write metric 的完整 measurement item/byte N PASS | item N+1 截断、byte N-1 稳定失败 PASS | PASS | 产品无通用即时 series delete；一次性 volume 与 1h retention 清理 PASS |

测试后的 OpenSearch 目录只剩已有系统 index；所有 `dbtool_it_bounded_search_*` 资源均不存在。MongoDB 隔离数据库在最后一个 collection 删除后无可见目录内容。
