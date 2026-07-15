# Document / Search / Time-Series 有界目录证据

状态：PASS（2026-07-16）

范围：IF-T66 顶层目录读取的 Document、Search、TimeSeries 分支。本证据只覆盖 collection、index、measurement 名称目录；表结构中的列/索引等二级元数据不在本分支范围内。

## 公共合同

三个目录都使用 `BoundedList<T> { items, truncated }` 和严格 N+1 探针：

- `max_items=0` 和 `max_items=usize::MAX` 在解析 DSN、建立连接之前返回 `CONFIG_ERROR`。
- CLI 和 TUI 必须先协商各自的显式 operation；只有旧的粗粒度 capability 时返回 `UNSUPPORTED_CAPABILITY`，不会回退到无界的 legacy list。
- `items.len()` 永远不超过调用方的 `--limit`；只有实际观察到第 N+1 项时，`meta.truncated=true`。

| 接口 | 显式 operation | Adapter 侧边界 | 已知限制 |
| --- | --- | --- | --- |
| MongoDB collections | `document.list_collections_bounded` | `listCollections` cursor 的 server `batchSize=N+1`，客户端读取到 N+1 后立即停止 | MongoDB 协议没有该命令的总结果 `limit`；`batchSize` 限制单批传输，dbtool 不再请求超出探针所需的项 |
| OpenSearch / Elasticsearch indices | `search.list_indices_bounded` | CAT indices 只请求 `index` 字段并按名称排序；响应体硬上限 1 MiB；解析和保留最多 N+1 | CAT indices 在兼容版本间没有可靠分页/总量 limit，因此不能声称服务端 N+1；超出 1 MiB 时失败关闭，而不是读取完整无界目录后伪装成有界结果 |
| Prometheus measurements | `time_series.list_measurements_bounded` | label-values 请求带 `limit=N+1`，解析和保留最多 N+1 | 依赖 Prometheus-compatible 后端遵守 label-values `limit`；即使兼容实现忽略参数，通用 HTTP 16 MiB 上限仍会失败关闭 |

CLI 的 `doc collections`、`search indices`、`ts measurements` 和 TUI 对应命令统一输出 `data` 加 `meta.truncated`。Legacy `list_collections`、`list_indices`、`list_measurements` 仅为嵌入式兼容接口，新的 CLI/TUI 路径不调用它们。

## 自动化验证

离线/单元验证：

```text
cargo test -p adapter-mongo -p adapter-search -p adapter-timeseries -p dbtool-cli -p dbtool-tui --lib
cargo test -p dbtool-cli --bin dbtool
cargo test -p dbtool-tui --bin dbtool-tui
cargo test -p dbtool-cli --test bounded_document_search_ts invalid_catalog_limits_fail_before_connection_for_all_three_surfaces -- --exact --nocapture
```

结果：三个 adapter 的 bounded parser、N/N+1、operation 声明和 HTTP body cap 单测通过；CLI 82/82 通过；三种无效 limit 的公共 CLI 测试通过。TUI 新增的显式能力门禁与连接前校验通过。TUI 全量中另有一个既存断言仍要求错误消息回显未知配置字段 `readonli`，与当前“配置诊断不泄露原文”的核心策略冲突；它不是本分支引入的目录错误，需独立修正测试期望。

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
| MongoDB 7 | 隔离数据库 3 collections、`limit=3`、`truncated=false` PASS | 同一目录 `limit=2`、返回 2、`truncated=true` PASS | PASS | 三个 collection 全部经公共确认式 drop 删除；空目录复查 PASS |
| OpenSearch 2.17.1 | 当前基线加 3 个临时 index、`limit=N`、`truncated=false` PASS | `limit=N-1`、精确返回 N-1、`truncated=true` PASS | PASS | 三个临时 index 全部经公共确认式 delete-index 删除；前缀复查无残留 PASS |
| Prometheus 2.55.1 | 对现有 measurement 总数使用 `limit=N`、`truncated=false` PASS | 同一快照使用 `limit=N-1`、`truncated=true` PASS | PASS | 测试只读，不创建 metric 或其他资源 |

测试后的 OpenSearch 目录只剩已有系统 index；所有 `dbtool_it_bounded_search_*` 资源均不存在。MongoDB 隔离数据库在最后一个 collection 删除后无可见目录内容。
