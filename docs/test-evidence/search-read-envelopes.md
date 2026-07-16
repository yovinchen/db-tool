# Search 完整读取信封证据

状态：PASS（2026-07-16）

范围：IF-T76。本项覆盖 search/get 完整响应；index 目录的标量字节预算仍属于
IF-T74，mutation input 预算属于 IF-T78，不互相冒充完成。

## 契约和能力协商

| 接口 | Exact operation | item 边界 | byte 边界 |
| --- | --- | --- | --- |
| `SearchEngine::search_budgeted` | `search.search_budgeted` | request `size <= max_items`；后端返回超量 hit 时整体失败 | 完整 hit/_source、aggregations、hits metadata、top-level extra 和 `SearchHits` envelope |
| `SearchEngine::get_doc_budgeted` | `search.get_doc_budgeted` | 存在文档消耗 1 item，缺失消耗 0 | `_source`、extra 与存在/缺失的完整 optional envelope |

Legacy `search=true`、`search.search`、`search.get_doc` 和旧 trait method 都不授权上述新契约。
CLI/TUI 在解析 DSN 与连接前构建 `ReadBudget`；CLI search 使用全局
`--limit/--max-bytes`，get 固定 1 item 并使用全局 `--max-bytes`。

## 两层字节边界

1. HTTP Content-Length、chunked decoded body 和无长度 body 在 JSON 解析前应用
   `min(caller max_bytes, 16 MiB)`；caller 限制命中时稳定映射为
   `READ_BUDGET_EXCEEDED`，固定 transport ceiling 命中仍是连接错误。
2. JSON 转成 portable model 后，`SearchReadLimiter` 再对每个完整 hit 和最终响应计费。
   任一层超限都不返回部分 JSON。

## 自动化和产品实测

| 层级 | 结果 |
| --- | --- |
| Core | Search complete limiter/item/byte/aggregation/get optional 测试、exact operation 与 legacy 拒绝全通过 |
| Adapter | 35/35 + all-target Clippy PASS |
| CLI/TUI | Search unit 12/12、CLI help 1/1、TUI 32/32、CLI/TUI all-target Clippy PASS |
| OpenSearch 2.17.1 adapter lifecycle | 3 documents、size clamp、aggregation-only、get/missing/small byte、delete-index/zero residual：1/1 PASS |
| Elasticsearch 8.15.5 adapter lifecycle | 同上：1/1 PASS |
| OpenSearch 2.17.1 public CLI lifecycle | exact caps、source/aggregation/get、limit/one-byte failure、confirmed delete/zero residual：1/1 PASS |
| Elasticsearch 8.15.5 public CLI lifecycle | 同上：1/1 PASS |

关键命令：

```text
cargo test -p dbtool-core
cargo test -p adapter-search
cargo test -p dbtool-cli cmd::search::tests
cargo test -p dbtool-cli --test cli_json cli_help_documents_core_command_families
cargo test -p dbtool-tui
cargo clippy -p adapter-search -p dbtool-cli -p dbtool-tui --all-targets -- -D warnings
DBTOOL_RUN_OBSERVABILITY_INTEGRATION=1 cargo test -p adapter-search \
  tests::opensearch_live_budgeted_search_get_aggregation_and_cleanup -- --exact --nocapture
DBTOOL_RUN_ELASTICSEARCH_INTEGRATION=1 cargo test -p adapter-search \
  tests::elasticsearch_live_budgeted_search_get_aggregation_and_cleanup -- --exact --nocapture
DBTOOL_RUN_OBSERVABILITY_INTEGRATION=1 cargo test -p dbtool-cli --test live_observability \
  opensearch_live_index_search_and_list -- --exact --nocapture
DBTOOL_RUN_ELASTICSEARCH_INTEGRATION=1 cargo test -p dbtool-cli --test live_observability \
  elasticsearch_native_live_index_search_and_list -- --exact --nocapture
```

## 清理与 TLS 边界

两个真实产品测试使用每次唯一 index，正常路径通过目标绑定确认删除，失败
unwind 还有 best-effort cleanup guard，最后通过 index 目录断言零残留。Dockerfile TLS
兼容 fixture 不实现 get/aggregation/delete-index 完整面，只作 HTTPS/CA/种子传输证据，
不写成产品 CRUD 或零残留 PASS。
