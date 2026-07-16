# Time-series 结构化读取信封证据

状态：PASS（2026-07-16）

范围：IF-T73。本证据覆盖公共 `TimeSeriesStore` 的 range query，不把顶层
measurement 目录（IF-T66）或 write input 信封（IF-T78）合并计为本项通过。

## 公共契约

- `TimeSeriesReadBudget { max_series, max_samples, max_bytes }` 是调用方所有的完整信封；
- `max_samples` 在所有 series 之间累计，不会对每个 series 重置；
- limiter 在保留前对 series name/columns 和每个 sample row 计费，并对完整
  `SeriesSet` 再计费；唯一 N+1 series/sample probe 也占用字节预算；
- series 或 sample 规模达到预算时返回前 N 项且 `truncated=true`；完整结果加
  probe 超过字节预算时返回 `READ_BUDGET_EXCEEDED`，不返回部分 JSON；
- 三个维度的零值、溢出或超硬上限值在 DSN 解析/建立连接前失败；
- 新契约使用独立 `time_series.query_range_bounded` operation。CLI/TUI 不用旧
  `query_range` 或粗粒度 `time_series=true` 作为授权回退。

Prometheus adapter 在上述 portable 结构计费之外保留 16 MiB HTTP response body
transport ceiling。这是 JSON 解析前的固定防护；caller `max_bytes` 是解析后对完整
portable 响应的精确契约，不把两者混写成同一个边界。

## 调用面闭环

| 调用面 | series 预算 | sample 预算 | byte 预算 | 能力协商 |
| --- | --- | --- | --- | --- |
| CLI | `ts query --max-series`，默认等于全局 `--limit` | 全局 `--limit` | 全局 `--max-bytes` | exact operation |
| TUI | 命令 `limit` | 同一 `limit` | core 默认 8 MiB | exact operation |
| Embedded | 显式 `TimeSeriesReadBudget` | 显式累计上限 | 显式完整响应上限 | `query_range_bounded` |

## 自动化与真实后端

| 层级 | 验证 | 结果 |
| --- | --- | --- |
| Core | 预算校验、series N/N+1、跨 series sample N/N+1、完整字节 N/N-1、exact operation | PASS |
| Prometheus adapter | parser/limiter 绑定、传输上限、三维预算与 live range query | 24 tests + Docker 1/1 PASS |
| CLI | help/连接前失败/exact operation/mock range、三维参数传递 | unit 8/8 + integration 3/3 PASS |
| TUI | exact operation、连接前预算、legacy 能力拒绝 | 32/32 PASS |
| 公共 CLI + Prometheus 2.55.1 | 两个 tagged/timestamped series 读回；`max-series=1` 截断；`max-bytes=1` 稳定失败 | Docker 1/1 PASS |

定向验证命令：

```text
cargo test -p dbtool-core
cargo test -p adapter-timeseries
cargo test -p dbtool-cli ts::tests
cargo test -p dbtool-cli --test ts_cli
cargo test -p dbtool-tui
cargo clippy -p adapter-timeseries -p dbtool-cli -p dbtool-tui --all-targets -- -D warnings
DBTOOL_RUN_OBSERVABILITY_INTEGRATION=1 cargo test -p adapter-timeseries \
  prometheus_live_query_range_enforces_structured_budget -- --exact --nocapture
DBTOOL_RUN_OBSERVABILITY_INTEGRATION=1 cargo test -p dbtool-cli --test live_observability \
  prometheus_live_measurements_and_query -- --exact --nocapture
```

## 产品范围与清理

当前 registry 中唯一注册为 `TimeSeriesStore` 的产品是 Prometheus。TimescaleDB 使用
PostgreSQL/SQL 接口；InfluxDB、VictoriaMetrics 和 QuestDB 尚未注册，不在证据中伪装成
PASS。Prometheus 的 append/query 模型没有通用行式 update/delete API。测试以每次唯一
metric namespace 隔离，并由一次性 Docker TSDB volume 和 retention 回收数据；这个清理边界不写成一个不存在的 public delete 能力。
