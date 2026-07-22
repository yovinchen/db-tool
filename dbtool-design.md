# dbtool 统一数据与消息连接工具——当前实现设计

> 状态：2026-07-16 实现基线，不再是立项草案<br>
> Workspace 版本：0.1.0；Rust 2021；MSRV 1.80<br>
> 适用对象：CLI、独立 TUI、在 Rust 程序中嵌入的 <code>dbtool-core</code> / <code>dbtool-registry</code><br>
> 事实优先级：可编译源码与测试 > Cargo feature 与发布工作流 > 本文档

本文档描述仓库当前已经存在的架构、公共接口和发布边界。历史 P0–P7 里程碑、尚未注册的候选后端以及已经解决的开放问题不再混入“已支持”范围。接口使用细节另见 <code>docs/interface-usage.zh-CN.md</code>，产品级验证状态另见 <code>docs/db-completeness-tasks.md</code> 与 <code>testdata/db-completeness.manifest</code>。

---

## 0. 范围与术语

### 0.1 当前目标

1. 以稳定的能力端口统一 SQL、CQL、Db2 扩展、KV、文档、搜索、时序和消息系统。
2. 以协议族注册表复用兼容产品，不为每个产品复制 adapter。
3. CLI、TUI、嵌入式调用共享同一套模型、能力协商、安全护栏、结果限制和连接解析。
4. 用户查询、导出、顶层目录和消息消费有明确预算；写入默认拒绝；破坏性操作使用目标与内容绑定的两阶段确认。单资源内部的可增长元数据集合由 IF-T67 单独收口。
5. 通过 Cargo feature 裁剪后端，并以 <code>portable</code> 组合生成六平台 CLI 发布物。
6. 用服务无关测试、Docker 实测和外部端点实测分别记录“接口存在”“协议可运行”“真实产品已验证”，不把三者混为一谈。

### 0.2 已确定的术语

- TDB/TSDB 在本项目中指时序数据能力；TiDB 是 MySQL 协议兼容产品。
- “协议兼容”只表示复用 wire protocol adapter，不自动证明某个供应商的认证、管理面或运维语义完全相同。
- “自包含”指发布的 CLI 不需要 Python、Node、JVM 等语言运行时。SQLite 会在构建期编译并链接 C 代码；Db2 仍需要宿主 IBM ODBC runtime；native Kafka 存在 C/CMake/librdkafka 构建与平台边界。
- <code>rabbitmq+http</code> 是独立的 RabbitMQ Management HTTP connector，不是 <code>amqp</code> 的别名。

### 0.3 非目标

- 不提供 MCP server、常驻代理或跨进程协调服务。
- 不提供 GUI、ORM、schema migration、CDC、通用备份恢复或生产数据库 HA 编排。
- 不强行把不同范式压成一个万能 <code>query</code>；公共模型之外保留能力专用接口。
- 不承诺跨多个 CLI 进程的全局 QPS。
- Oracle、etcd、InfluxDB、VictoriaMetrics 原生 API、Pulsar、MQTT、RocketMQ 当前没有 factory，也不是“未完成的现有接口”；若纳入须另立范围。

---

## 1. 设计原则

| 原则 | 当前实现 |
| --- | --- |
| 能力而非产品类型 | <code>Connector</code> 只暴露其实际支持的能力 accessor；方法级能力由 <code>operations</code> 精确声明。 |
| 协议族复用 | <code>register_family</code> 同时注册 canonical scheme 与权威 alias 表。 |
| 依赖倒置 | core 定义模型、端口与服务；adapter 依赖 core；front-end 通过 registry 装配。 |
| Fail closed | 未注册 scheme、未声明 operation、缺少 accessor、非法参数、未授权写入均返回显式错误，不用空结果伪装成功。 |
| 有界优先 | 用户查询、导出、顶层目录列表和消息消费必须在 adapter 边界限制工作量；调用方不得先全量加载再截断；二级元数据继续由 IF-T67 跟踪。 |
| 兼容但不猜测 | 粗粒度 <code>Capabilities</code> 为兼容保留；可选方法和 partial admin 绝不由布尔值猜出。 |
| 安全默认只读 | 普通写要求 <code>--allow-write</code>；破坏性写还要求绑定目标和内容的 <code>--confirm</code>。 |
| 可裁剪发布 | registry 通过 feature 决定编译和注册哪些 adapter；正式六平台发布固定使用 <code>portable</code>。 |

---

## 2. 架构与实际 crate

### 2.1 六边形架构与组合根

~~~text
CLI(dbtool)        TUI(dbtool-tui)        embedded Rust caller
        |                       |                       |
       +---------- dbtool-core ----------------+
       | model / port / dsn / service / config |
       +------------------+---------------------+
                          ^
                          | implements ports
     adapter-sql / redis / mongo / search / timeseries
     adapter-kafka / amqp / nats / sqlserver / cassandra / db2
                          ^
                          |
                  dbtool-registry
             scheme + feature composition root
~~~

依赖方向保持为：

- <code>dbtool-core</code> 不依赖具体数据库驱动。
- 每个 adapter 只依赖 core 和自身驱动。
- <code>dbtool-registry</code> 是仓库内唯一集中感知全部 adapter 的组合根。
- CLI/TUI 依赖 core 与 registry，不直接按产品做中心化大枚举分发。

动态加载共享库没有采用：Rust ABI、跨平台装载和单文件分发均不适合当前目标。扩展通过编译期 crate + registry 完成。

### 2.2 Workspace crate 清单

| crate | 责任 |
| --- | --- |
| <code>dbtool-core</code> | 统一模型、错误、DSN、配置、能力 trait、registry 基础结构、连接解析、安全、限流、格式化、连接管理。 |
| <code>adapter-sql</code> | MySQL/MariaDB/TiDB、PostgreSQL 族、SQLite。 |
| <code>adapter-sqlserver</code> | SQL Server/TDS。 |
| <code>adapter-cassandra</code> | Cassandra/Scylla CQL，并提供 SQL 风格兼容检查面。 |
| <code>adapter-db2</code> | Db2 SQL + Db2 专有 catalog/DDL；宿主 ODBC 边界。 |
| <code>adapter-redis</code> | Redis 协议 KV、Streams/PubSub 消息与部分管理能力。 |
| <code>adapter-mongo</code> | MongoDB 文档能力。 |
| <code>adapter-search</code> | OpenSearch/Elasticsearch 兼容 HTTP/HTTPS 搜索能力。 |
| <code>adapter-timeseries</code> | Prometheus HTTP query 与 remote write。 |
| <code>adapter-kafka</code> | 同一 adapter 下的 pure <code>rskafka</code> 与 native <code>rdkafka</code> backend。 |
| <code>adapter-amqp</code> | AMQP/AMQPS 直连与 RabbitMQ Management HTTP。 |
| <code>adapter-nats</code> | NATS core + JetStream。 |
| <code>dbtool-registry</code> | 按 feature 组装 factory，导出 <code>build_registry()</code>。 |
| <code>dbtool-cli</code> | <code>dbtool</code> 命令、JSON/table/ndjson 输出、artifact 与生成发布辅助文件。 |
| <code>dbtool-tui</code> | 独立 ratatui 程序；复用 core/registry，但不是 CLI 子命令。 |

### 2.3 core 的实际边界

<code>dbtool-core/src</code> 当前由以下模块组成：

- <code>model/</code>：<code>Value</code>、<code>ResultSet</code>、<code>BoundedList</code>、<code>Document</code>、<code>Message</code>、<code>SeriesSet</code>、metadata 与各种 outcome。
- <code>port/connector.rs</code>：<code>Connector</code>、<code>Capabilities</code>、<code>CapabilityOperation</code>、<code>CapabilityReport</code>。
- <code>port/capability.rs</code>：所有能力 trait。
- <code>registry/</code>：<code>Registry</code>、<code>Factory</code> 与 alias 表。
- <code>dsn/</code>：解析与脱敏。
- <code>service/</code>：resolver、safety、throttle、limiter、formatter、manager。
- <code>config/</code>：环境连接与 <code>connections.toml</code>。
- <code>error.rs</code>：统一错误与机器码。

Factory 的真实类型为：

~~~rust
pub type Factory =
    fn(Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>>;
~~~

Factory 取得 <code>Dsn</code> 所有权，避免异步 future 借用调用栈。<code>Registry::register_family</code> 依据统一 alias 表注册整个协议族；<code>Registry::connect</code> 解析 DSN、canonicalize scheme、选择 factory 并返回 trait object。

CLI 每次调用直接从 registry 建连。TUI/长会话使用 <code>ConnectionManager</code>，其当前合同是按 DSN 缓存 <code>Arc&lt;Box&lt;dyn Connector&gt;&gt;</code>；不要把它描述成跨后端统一连接池或跨进程服务。

---

## 3. 已注册 scheme、alias 与后端能力

以下是 <code>PROTOCOL_ALIASES</code> 与 <code>build_registry()</code> 的完整当前清单。只有对应 feature 开启时才会实际注册；未知或未编译的 scheme 返回 <code>UNSUPPORTED_SCHEME</code>。

| Canonical scheme | 全部 alias | adapter / feature | 主要端口与边界 |
| --- | --- | --- | --- |
| <code>mysql</code> | <code>mariadb</code>, <code>tidb</code> | adapter-sql / <code>sql</code> | <code>SqlEngine</code>；整批事务导入与有界 catalog。 |
| <code>postgres</code> | <code>postgresql</code>, <code>cockroach</code>, <code>timescale</code>, <code>redshift</code> | adapter-sql / <code>sql</code> | <code>SqlEngine</code>；兼容产品仍需各自真实端点验证。 |
| <code>sqlite</code> | 无 | adapter-sql / <code>sql</code> | <code>SqlEngine</code>；支持 <code>sqlite::memory:</code> 与文件 DSN。 |
| <code>sqlserver</code> | <code>mssql</code> | adapter-sqlserver / <code>sqlserver</code> | <code>SqlEngine</code>；TDS。 |
| <code>cassandra</code> | <code>scylla</code> | adapter-cassandra / <code>cassandra</code> | <code>CqlEngine</code> + SQL 风格兼容面。 |
| <code>db2</code> | <code>ibmdb2</code>, <code>as400</code> | adapter-db2 / <code>db2</code> | <code>SqlEngine</code> + <code>Db2Engine</code>；需要宿主 IBM ODBC runtime。 |
| <code>redis</code> | <code>valkey</code>, <code>keydb</code>, <code>dragonfly</code> | adapter-redis / <code>redis</code> | KV + producer/consumer + partial admin；lag 由运行时版本探测决定。 |
| <code>mongodb</code> | 无 | adapter-mongo / <code>mongodb</code> | <code>DocumentStore</code>。 |
| <code>opensearch</code> | <code>elasticsearch</code>, <code>opensearch+https</code>, <code>elasticsearch+https</code> | adapter-search / <code>search</code> | <code>SearchEngine</code>；HTTP/HTTPS。 |
| <code>prometheus</code> | <code>prometheus+http</code> | adapter-timeseries / <code>timeseries</code> | <code>TimeSeriesStore</code>；range query + remote write。 |
| <code>kafka</code> | <code>automq</code>, <code>redpanda</code>, <code>warpstream</code>, <code>confluent</code> | adapter-kafka / <code>kafka</code> 或 <code>kafka-native</code> | producer/consumer + partial admin；backend 决定 stateful/lag 能力。 |
| <code>amqp</code> | <code>amqps</code> | adapter-amqp / <code>amqp</code> | producer/consumer；直连 AMQP 只有 detail/delete 管理能力。 |
| <code>rabbitmq+http</code> | 无，独立 scheme | adapter-amqp / <code>amqp</code> | 只提供 RabbitMQ Management list/detail/delete，不提供消息收发或 group lag。 |
| <code>nats</code> | <code>nats+tls</code> | adapter-nats / <code>nats</code> | NATS/JetStream producer、stateful consumer 与 admin。 |

重要约束：

- alias 是可读性便利，不是产品认证。兼容系统也可直接使用 canonical scheme。
- <code>mysql</code>、<code>postgres</code>、<code>sqlite</code> 当前由同一个 <code>sql</code> feature 一起编译；registry 中的 <code>mysql/postgres/sqlite</code> feature 只是指向 <code>sql</code> 的兼容别名，并不能单独裁剪驱动。
- AMQP 0.9.1 没有可移植的全局队列列表；队列发现必须使用 <code>rabbitmq+http</code>。
- Redis Pub/Sub channel 与 NATS core subject 是实时路由名，不应伪装成持久 topic catalog；持久管理语义分别落在 Redis Streams 与 JetStream。

---

## 4. Cargo feature 组合

### 4.1 dbtool-registry 的精确 feature

~~~toml
default = ["sql", "redis", "mongodb", "search", "timeseries"]

mysql     = ["sql"]
postgres  = ["sql"]
sqlite    = ["sql"]
sql       = ["dep:adapter-sql"]
sqlserver = ["dep:adapter-sqlserver"]
cassandra = ["dep:adapter-cassandra"]
db2       = ["dep:adapter-db2"]
redis     = ["dep:adapter-redis"]
mongodb   = ["dep:adapter-mongo"]
search    = ["dep:adapter-search"]
timeseries = ["dep:adapter-timeseries"]
amqp      = ["dep:adapter-amqp"]
nats      = ["dep:adapter-nats"]
mq        = ["nats", "amqp", "redis"]

kafka        = ["dep:adapter-kafka", "adapter-kafka/backend-pure"]
kafka-native = ["dep:adapter-kafka", "adapter-kafka/backend-native"]

full-common = [
  "sql", "sqlserver", "cassandra", "redis", "mongodb",
  "search", "timeseries", "mq"
]
portable    = ["full-common", "kafka"]
full        = ["full-common", "db2", "kafka"]
full-native = ["full-common", "db2", "kafka-native"]
~~~

语义必须按下表理解：

| 组合 | 实际内容 | 用途 |
| --- | --- | --- |
| <code>--no-default-features</code> | 空 registry，不编译或注册 adapter | 最小依赖、feature 门禁与 embedded 自定义组合验证。 |
| default | SQL、Redis（含 Streams/PubSub）、MongoDB、Search、Prometheus | 日常开发默认，不含 SQL Server、Cassandra、AMQP、NATS、Kafka、Db2。 |
| <code>full-common</code> | SQL、SQL Server、Cassandra、Redis、MongoDB、Search、Prometheus、AMQP、NATS | 两种 Kafka preset 的公共基础；不是 CLI 对外 feature。 |
| <code>portable</code> | <code>full-common</code> + pure Kafka | 六平台正式 CLI 发布组合；不含 Db2 ODBC 与 native Kafka。 |
| <code>full</code> | <code>full-common</code> + Db2 + pure Kafka | 含 Db2 的完整 pure-Kafka 构建。 |
| <code>full-native</code> | <code>full-common</code> + Db2 + native Kafka | 只选择 native backend，不同时选择 pure backend。 |

<code>adapter-kafka</code> 自身 default 为 <code>backend-pure</code>；出现 <code>backend-native</code> 时源码选择 rdkafka，否则选择 rskafka。仓库 preset 与 feature-matrix 校验保证 <code>full</code>/<code>full-native</code> 不把两个 backend 同时带入。手工同时启用 <code>kafka</code> 和 <code>kafka-native</code> 不属于受支持构建范式。

### 4.2 front-end 转发范围

- <code>dbtool-cli</code> 对外 feature：default、<code>portable</code>、<code>full</code>、<code>full-native</code>、<code>sqlserver</code>、<code>cassandra</code>、<code>db2</code>。
- <code>dbtool-tui</code> 对外 feature：default、<code>full</code>、<code>full-native</code>。
- 想精确组合 registry leaf feature 的嵌入方，应直接依赖 <code>dbtool-registry</code>；不能假设 CLI crate 转发了每个 leaf。

### 4.3 构建边界

- SQLite 的 bundled C 代码在构建时进入二进制，运行时不要求单独安装 SQLite。
- Db2 adapter 使用 ODBC，真实运行需要宿主 IBM Data Server Driver；因此不进入 <code>portable</code>。
- native Kafka 使用 rdkafka/librdkafka，并有 CMake/C 工具链与平台验证成本；因此不进入六平台 portable release。
- TLS 路径优先使用 rustls；具体产品证书、认证和协议差异仍由对应 adapter 与集成测试约束。

---

## 5. 能力模型与精确协商

### 5.1 三层合同

能力协商不是只看一个布尔值，而是三层都要成立：

1. <code>Capabilities</code>：兼容旧调用方的能力族摘要。
2. <code>Connector::operations()</code>：稳定、方法级、权威的可调用 operation 清单。
3. <code>as_sql/as_kv/...</code>：取得对应 trait object 后才能调用。

<code>Capabilities</code> 当前字段完整为：

~~~rust
pub struct Capabilities {
    pub sql: bool,
    pub cql: bool,
    pub db2: bool,
    pub key_value: bool,
    pub document: bool,
    pub time_series: bool,
    pub search: bool,
    pub producer: bool,
    pub consumer: bool,
    pub admin: bool,
}
~~~

<code>CapabilityReport</code> 兼容序列化合同为：

~~~rust
pub struct CapabilityReport {
    #[serde(flatten)]
    pub legacy: Capabilities,
    #[serde(default)]
    pub operations: Vec<CapabilityOperation>,
}
~~~

因此 <code>caps</code> JSON 继续在顶层输出 <code>sql</code> 等旧字段，没有额外的 <code>legacy</code> 对象；同时新增 <code>operations</code>。<code>CapabilityReport::new</code> 按 operation 的稳定字符串排序并去重，使 CLI、TUI 和 embedded 输出一致。

### 5.2 全部稳定 operation 名

这些字符串是公共协商协议，不应随 Rust enum variant 改名。

| 族 | 全部 operation |
| --- | --- |
| SQL | <code>sql.query</code>, <code>sql.query_bounded</code>, <code>sql.execute</code>, <code>sql.insert_rows_atomic</code>, <code>sql.list_schemas</code>, <code>sql.list_schemas_bounded</code>, <code>sql.list_tables</code>, <code>sql.list_tables_bounded</code>, <code>sql.describe_table</code> |
| CQL | <code>cql.query</code>, <code>cql.query_bounded</code>, <code>cql.execute</code>, <code>cql.list_keyspaces</code>, <code>cql.list_keyspaces_bounded</code>, <code>cql.list_tables</code>, <code>cql.list_tables_bounded</code>, <code>cql.describe_table</code> |
| Db2 | <code>db2.list_sequences</code>, <code>db2.list_sequences_bounded</code>, <code>db2.list_routines</code>, <code>db2.list_routines_bounded</code>, <code>db2.list_tablespaces</code>, <code>db2.list_tablespaces_bounded</code>, <code>db2.list_foreign_keys</code>, <code>db2.list_foreign_keys_bounded</code>, <code>db2.generate_ddl</code> |
| KV | <code>kv.get</code>, <code>kv.get_with_expiry</code>, <code>kv.set</code>, <code>kv.restore_with_expiry</code>, <code>kv.delete</code>, <code>kv.scan</code>, <code>kv.raw_command</code> |
| Document | <code>document.list_collections</code>, <code>document.list_collections_bounded</code>, <code>document.find</code>, <code>document.insert</code>, <code>document.update</code>, <code>document.delete</code>, <code>document.update_one</code>, <code>document.update_many</code>, <code>document.delete_one</code>, <code>document.delete_many</code>, <code>document.aggregate</code>, <code>document.aggregate_bounded</code>, <code>document.drop_collection</code> |
| Time series | <code>time_series.list_measurements</code>, <code>time_series.list_measurements_bounded</code>, <code>time_series.write_points</code>, <code>time_series.query_range</code> |
| Search | <code>search.list_indices</code>, <code>search.list_indices_bounded</code>, <code>search.search</code>, <code>search.index_doc</code>, <code>search.put_doc</code>, <code>search.get_doc</code>, <code>search.update_doc</code>, <code>search.delete_doc</code>, <code>search.delete_index</code> |
| Message | <code>message.produce</code>, <code>message.consume</code>, <code>message.consume_group</code>, <code>message.consume_durable</code>, <code>message.consume_ack</code>, <code>message.admin.list_topics</code>, <code>message.admin.list_topics_bounded</code>, <code>message.admin.topic_detail</code>, <code>message.admin.consumer_lag</code>, <code>message.admin.delete</code> |

### 5.3 哪些能力可从旧布尔值推导

<code>Connector::operations()</code> 的默认实现只展开已冻结的基础方法：

| 粗粒度字段 | 默认展开 | 绝不自动推导 |
| --- | --- | --- |
| <code>sql</code> | query、query_bounded、execute、list_schemas、list_tables、describe_table | atomic import；两个 bounded catalog |
| <code>cql</code> | query、query_bounded、execute、list_keyspaces、list_tables、describe_table | 两个 bounded catalog |
| <code>db2</code> | sequences、routines、tablespaces、foreign_keys、generate_ddl | 四个 bounded catalog |
| <code>key_value</code> | get、set、delete、scan、raw_command | 原子读取/恢复 expiry |
| <code>document</code> | list、find、insert、兼容 bulk update/delete、aggregate、aggregate_bounded | bounded collection、显式 one/many、drop |
| <code>time_series</code> | list、write、query_range | bounded measurement list |
| <code>search</code> | list、search、完整 document/index CRUD | bounded index list |
| <code>producer</code> | produce | 无 |
| <code>consumer</code> | stateless consume | group、durable、ack |
| <code>admin</code> | 什么都不展开 | 所有 list/bounded-list/detail/lag/delete 均须 connector 显式声明 |

其中 <code>admin=true</code> 只表示“存在某种管理面”，绝不表示四类管理方法齐全。调用方必须以具体 operation 为准，不能因为 <code>admin</code> 为真就调用全部 <code>AdminInspect</code>/<code>AdminMutate</code> 方法。

### 5.4 嵌入式调用的唯一正确范式

顺序固定为“精确 operation → accessor → invoke”；缺任何一步都返回 <code>UNSUPPORTED_CAPABILITY</code>，不得 fallback 到语义更弱的方法，也不得 panic：

~~~rust
use dbtool_core::{
    Error, Result,
    port::{CapabilityOperation, CapabilityReport, Connector},
};

async fn read_exact_kv_lifetime(conn: &dyn Connector, key: &str) -> Result<()> {
    let report = CapabilityReport::new(
        conn.capabilities(),
        conn.operations(),
    );
    let needed = CapabilityOperation::KeyValueGetWithExpiry;

    if !report.operations.contains(&needed) {
        return Err(Error::UnsupportedCapability {
            kind: conn.kind().0,
            needed: needed.as_str(),
        });
    }

    let kv = conn.as_kv().ok_or_else(|| Error::UnsupportedCapability {
        kind: conn.kind().0,
        needed: "KeyValueStore",
    })?;

    let _snapshot = kv.get_with_expiry(key).await?;
    Ok(())
}
~~~

adapter 的发布前不变量是：声明某 operation 就必须提供对应 accessor 与真实实现；不支持的可选方法保留 trait 的 fail-closed 默认实现。CLI 和 TUI 同样遵循这一顺序。

### 5.5 当前 partial/optional operation 矩阵

| connector | 显式增加的 operation |
| --- | --- |
| MySQL/PostgreSQL/SQLite | <code>sql.insert_rows_atomic</code>、两个 SQL bounded catalog |
| SQL Server | 两个 SQL bounded catalog；不声明 atomic import |
| Cassandra/Scylla | 两个 SQL bounded catalog + 两个 CQL bounded catalog |
| Db2 | 两个 SQL bounded catalog + 四个 Db2 bounded catalog；不声明 atomic import |
| MongoDB | bounded collection list、update one/many、delete one/many、drop collection |
| Search | bounded index list |
| Prometheus | bounded measurement list |
| Redis 族 | KV expiry 原子操作、group、ack、admin list/bounded-list/detail/delete；<code>consumer_lag</code> 仅在运行时探测到 Redis 7+ 兼容能力时声明，KeyDB 6 不声明；不支持 durable |
| pure Kafka | admin list/bounded-list/detail/delete；不声明 group、ack、lag、durable |
| native Kafka | group、ack、admin list/bounded-list/detail/lag/delete；不声明 durable |
| AMQP/AMQPS 直连 | ack、admin detail/delete；不声明 admin list/lag 或 group/durable |
| RabbitMQ Management HTTP | admin list/bounded-list/detail/delete；无 producer/consumer/lag |
| NATS/JetStream | group、durable、ack、admin list/bounded-list/detail/lag/delete |

---

## 6. 实际 trait 面

| trait | 当前方法 | 关键合同 |
| --- | --- | --- |
| <code>Connector</code> | kind、capabilities、operations、ping、close；<code>as_sql/as_cql/as_db2/as_kv/as_document/as_timeseries/as_search/as_producer/as_consumer/as_admin/as_admin_mutate</code> | accessor 默认 <code>None</code>；adapter 只覆盖实际能力。 |
| <code>SqlEngine</code> | query、query_bounded、execute、insert_rows_atomic、list_schemas、list_schemas_bounded、list_tables、list_tables_bounded、describe_table | query_bounded 为 required；atomic import 和 bounded catalog 为可选、显式协商。 |
| <code>CqlEngine</code> | query_cql、query_cql_bounded、execute_cql、list_keyspaces、list_keyspaces_bounded、list_cql_tables、list_cql_tables_bounded、describe_cql_table | bounded query 为 required；bounded catalog 可选。 |
| <code>Db2Engine</code> | list_sequences、list_routines、list_tablespaces、list_foreign_keys、generate_ddl 及前四者 bounded 版本 | Db2 专有 metadata，不塞入通用 SQL trait。 |
| <code>KeyValueStore</code> | get、get_with_expiry、set、restore_with_expiry、delete、scan、raw_command | expiry 两个方法要求单次后端原子快照/恢复；不能用 GET + TTL 模拟。 |
| <code>DocumentStore</code> | collections、find、insert、兼容 bulk update/delete、显式 one/many、aggregate、aggregate_bounded、drop_collection | 新调用方必须显式协商 one/many；aggregate_bounded required；drop 可选。 |
| <code>TimeSeriesStore</code> | list_measurements、bounded list、write_points、query_range | Prometheus 语义是 append/query，不提供通用 row update/delete。 |
| <code>SearchEngine</code> | list_indices、bounded list、search、index_doc、put_doc、get_doc、update_doc、delete_doc、delete_index | 覆盖文档和 index 生命周期；delete-index 仍受外层确认保护。 |
| <code>MessageProducer</code> | produce | <code>Message</code> 保留 key、headers、partition、timestamp、cursor/metadata 等 typed 字段；各协议拒绝不适用字段。 |
| <code>MessageConsumer</code> | consume | <code>ConsumeOptions</code> 强制 max/timeout；identity、ack、cursor 的可用性由 operation 协商。 |
| <code>AdminInspect</code> | list_topics、bounded list、topic_detail、consumer_lag | partial admin；没有任何方法可仅凭 <code>admin=true</code> 调用。 |
| <code>AdminMutate</code> | delete_resource | 与只读 admin 分离；外层必须执行写授权和目标绑定确认。 |

历史 <code>DocumentStore::update/delete</code> 保持 bulk 语义只为嵌入式兼容。CLI 的默认更新/删除是 one；<code>--many</code> 使用显式 operation 和确认流程。不要把兼容方法当作新接口的 cardinality 依据。

---

## 7. 统一调用与安全范式

### 7.1 有界读取

1. <code>--limit</code>/<code>max_items</code> 必须大于零，计算探测项时使用 checked add，溢出在访问后端前拒绝。
2. <code>SqlEngine::query_bounded</code> 与 <code>CqlEngine::query_cql_bounded</code> 是 required method；CLI、TUI、export 和用户可控 embedded 路径不得调用无界 query 后再截断。
3. catalog bounded 方法是可选 operation。调用方缺少声明时返回 <code>UNSUPPORTED_CAPABILITY</code>，不得退回 <code>list_*</code> 全量加载。
4. 标准结果最多保留 N 项，只有观察到额外一项时 <code>truncated=true</code>；刚好 N 项且无额外项必须为 false。
5. “N+1”是完整性语义，不等于所有协议都发一次 N+1 请求：SQL、TDS、CQL paging、Redis SCAN、Kafka metadata、HTTP CAT、NATS paging 各自使用协议可实现的上限与 continuation。
6. 消息 consume 达到 <code>--max</code> 时只表示预算耗尽，不证明 broker 中一定还有下一条。

### 7.2 写入与破坏性操作

- 默认只读；普通写需要全局 <code>--allow-write</code>。
- 命名连接的 <code>readonly=true</code> 一票否决写入。
- SQL 由 parser 优先分类 read/write/destructive；无 WHERE 的 UPDATE/DELETE、DROP、TRUNCATE 等进入 destructive。
- 非 SQL 的 collection/index/topic/queue/stream/config 删除使用同一 <code>SafetyGuard</code> 目标绑定能力。
- 第一次 destructive 调用返回 <code>CONFIRM_REQUIRED</code> 与 token，不执行；第二次必须重放相同连接、操作、资源和内容并提供 <code>--confirm &lt;token&gt;</code>。
- 不存在跳过两阶段确认的 <code>--yes-i-understand-destructive</code> 或通用 force 开关。
- 远端是否已执行无法判定时返回 <code>OUTCOME_INDETERMINATE</code>；调用方必须检查远端状态，不得自动重放。

### 7.3 参数、值与输出

- SQL query/exec 使用 JSON array 参数并交给驱动绑定；不拼接 SQL literal。
- 公共 <code>Value</code> 支持 null、bool、int、float、text、bytes、timestamp、JSON/array/map；bytes 走 typed/base64 wire 表达。
- CLI 格式只接受 <code>json</code>、<code>table</code>、<code>ndjson</code>；非法格式由 clap 在建连前拒绝。
- JSON 成功响应保留 data/meta，错误响应保留稳定 <code>error.code</code>；不支持的能力不能返回空数组冒充成功。
- 当前错误码包括：<code>CONFIG_ERROR</code>、<code>INVALID_DSN</code>、<code>UNSUPPORTED_SCHEME</code>、<code>UNSUPPORTED_CAPABILITY</code>、<code>CONNECTION_ERROR</code>、<code>AUTH_ERROR</code>、<code>QUERY_ERROR</code>、<code>OUTCOME_INDETERMINATE</code>、<code>CONFIRM_REQUIRED</code>、<code>READ_ONLY</code>、<code>WRITE_NOT_ALLOWED</code>、<code>RATE_LIMITED</code>、<code>OVERLOADED</code>、<code>TIMEOUT</code>、<code>DEADLINE_EXCEEDED</code>、<code>SERIALIZATION_ERROR</code>、<code>INTERNAL_ERROR</code>。

### 7.4 export/import 完整性

- SQL、KV、Document 当前 artifact 为带版本、typed codec 与完整性 metadata 的 v3 结构。
- 部分、legacy、计数矛盾、未知 codec、超过 256 MiB 或项目数超过全局 limit 的 artifact 在连接目标前拒绝。
- artifact 用同目录临时文件、Unix 0600、sync + rename 发布；Unix 可原子替换已有目标，Windows 对已有目标的 replace 语义仍由 IF-T68 收口，当前失败时删除临时文件并保留原目标。
- SQL import 只有 MySQL、PostgreSQL、SQLite 在协商到 <code>sql.insert_rows_atomic</code> 后执行整批单事务；SQL Server、Db2、Cassandra 不做逐行降级。
- KV v3 通过 <code>get_with_expiry</code>/<code>restore_with_expiry</code> 保存 exact bytes 与 persistent/absolute expiry；已过期项不短暂复活。Redis 返回 per-entry atomic，不宣称整批事务。
- Document import 是 best effort，成功仍明确 <code>atomic=false</code>；调用方应导入独立目标并读回校验。

### 7.5 命名连接与凭证

解析顺序为：

1. 任何合法 URI scheme 的原始 DSN；
2. <code>DBTOOL_CONN_&lt;UPPER_NAME&gt;</code> 环境变量；
3. <code>connections.toml</code> 中的命名连接。

CLI 的 <code>conn list/add/remove</code> 已实现。add 要求写权限；replace/remove 需要与配置绝对路径、name 和条目内容绑定的确认。环境连接只读展示，CLI 不覆盖或删除。配置更新使用同目录原子替换；typed TOML 会保留已建模字段，但不保留注释/排版。所有输出和错误路径必须脱敏 DSN。

### 7.6 进程内流量控制

<code>ThrottleConfig</code> 默认值为并发 8、无速率上限、获取许可 2 秒、单次请求 10 秒、整体 deadline 15 秒、最多重试 3 次。

- rate token、并发 semaphore、请求执行都受 timeout 约束。
- <code>FlowControl::run</code> 可重试并让尝试与退避共享同一个 overall deadline。
- <code>FlowControl::run_single</code> 不重试，CLI 一次性操作用它避免写入被重放。
- <code>overall_deadline=None</code> 表示没有统一总预算，admission 与 request 各用自身预算；不是自动退化成 request timeout。
- 当前 <code>Error::is_retryable()</code> 只把 connection error 与 timeout 视为可重试；不要宣称已按数据库错误码自动重试死锁。
- 该层只在单进程内生效，不提供跨多次 CLI 调用的全局 QPS。

---

## 8. CLI 当前接口

### 8.1 全局参数

<code>dbtool</code> 使用 <code>--dsn</code> 或 <code>--conn</code> 选择连接；公共参数包括 <code>--format</code>、正数 <code>--limit</code>、<code>--max-concurrency</code>、<code>--rate</code>、<code>--acquire-timeout</code>、<code>--request-timeout</code>、<code>--deadline</code>、<code>--max-retries</code>、<code>--allow-write</code>、<code>--confirm</code>、<code>--verbose</code>。

### 8.2 完整命令树

| 族 | 已实现子命令 |
| --- | --- |
| 通用 | <code>ping</code>, <code>caps</code> |
| 连接配置 | <code>conn list</code>, <code>conn add</code>, <code>conn remove</code> |
| SQL | <code>sql query</code>, <code>sql exec</code>, <code>sql tables</code>, <code>sql schema</code>, <code>sql schemas</code> |
| CQL | <code>cql query</code>, <code>cql exec</code>, <code>cql keyspaces</code>, <code>cql tables</code>, <code>cql schema</code> |
| Db2 | <code>db2 sequences</code>, <code>db2 routines</code>, <code>db2 tablespaces</code>, <code>db2 foreign-keys</code>, <code>db2 ddl</code>, <code>db2 schemas</code>, <code>db2 tables</code>, <code>db2 schema</code> |
| KV | <code>kv get</code>, <code>kv set</code>, <code>kv scan</code>, <code>kv del</code>, <code>kv raw</code> |
| Document | <code>doc collections</code>, <code>doc find</code>, <code>doc insert</code>, <code>doc update</code>, <code>doc delete</code>, <code>doc drop</code>, <code>doc aggregate</code> |
| Transfer | <code>export sql</code>, <code>export kv</code>, <code>export doc</code>, <code>import sql</code>, <code>import kv</code>, <code>import doc</code> |
| Time series | <code>ts measurements</code>, <code>ts query</code>, <code>ts write</code> |
| Search | <code>search indices</code>, <code>search search</code>, <code>search index</code>, <code>search put</code>, <code>search get</code>, <code>search update</code>, <code>search delete</code>, <code>search delete-index</code> |
| Messaging | <code>mq produce</code>, <code>mq consume</code>, <code>mq topics</code>, <code>mq detail</code>, <code>mq lag</code>, <code>mq delete</code> |
| 发布辅助 | 隐藏命令 <code>generate-artifacts</code> 生成 bash/zsh/fish completion 与 manpage |

CLI 没有 <code>tui</code> 子命令；TUI 是独立 binary。

### 8.3 关键使用规范

- SQL/CQL query 使用 adapter-bounded 路径；SQL/CQL catalog 与 Db2 catalog 必须协商 bounded operation。
- <code>kv raw</code> 是 fail-closed allowlist，不是任意 Redis shell；危险管理命令、未知命令和无法证明有界的读取拒绝透传。
- Document find 支持 filter、skip、sort、projection；update/delete 默认 one，<code>--many</code> 要求确认；drop 必须确认。
- Search 已覆盖自动 ID index、指定 ID put/get/update/delete 与 delete-index。
- Prometheus query 支持相对窗口或显式 epoch-ms 区间；样本总预算 1..=1,000,000；remote write 要求写权限。
- MQ produce/consume 使用 typed key/header/partition/timestamp/cursor；group/durable/ack 只在 operation 声明时允许。stateful identity 与 ACK 会改变 broker 状态，因此受写权限保护。
- MQ delete 必须携带明确 resource kind，并经过目标绑定确认；不同协议的 if-empty/if-unused 等选项不能互相套用。

---

## 9. TUI 当前实现

<code>dbtool-tui</code> 是独立 ratatui 程序，当前已经具备：

- 从配置文件与环境变量发现连接并选择连接；
- <code>--help</code>、<code>--smoke</code> 与完整交互事件循环；没有配置时回退到 scratch SQLite 内存连接；
- <code>ping</code>/<code>caps</code> 与方法级 operation 展示；
- operation-aware dispatch；固定 16 个表单会轮转展示，不会按当前 connector 动态隐藏；
- SQL query/exec/schemas/tables/schema，KV get/scan/set/del，Document collections/find，Search indices/index/query，Time-series measurements/query/write；
- 正数读取 limit、adapter-bounded catalog、结果 truncation；
- parser-based SQL 写分类、readonly 拒绝、一次性 pending-write 确认；
- 有界命令历史、全屏状态信息；
- <code>TerminalSession</code> RAII 恢复 raw mode 与 alternate screen，覆盖正常退出、I/O 错误、初始化失败和 panic unwind。

当前边界：

- TUI 不是 CLI 全命令镜像；CQL、Db2、MQ、transfer、connection CRUD 没有对应 TUI 面板。
- Document 在 TUI 中只有 collections/find；Search 查询输入语法为 <code>search &lt;index&gt; &lt;json&gt;</code>，不是 CLI 的 <code>search search</code> 形式。
- 历史草案中的 MQ topic/lag 面板没有实现，不能写成当前能力。
- TUI 是独立应用，不是承诺稳定 API 的可嵌入组件库。
- 当前 release workflow 只打包 <code>dbtool-cli</code> 的 <code>dbtool</code>，不发布 <code>dbtool-tui</code>。

---

## 10. 发布与打包现状

### 10.1 六平台矩阵

tag <code>v*</code> 触发 <code>.github/workflows/release.yml</code>。tag 必须严格匹配 workspace/CLI 版本，然后每个 target 只编译一次：

| 平台 | target |
| --- | --- |
| Linux x64 | <code>x86_64-unknown-linux-musl</code> |
| Linux arm64 | <code>aarch64-unknown-linux-musl</code> |
| macOS x64 | <code>x86_64-apple-darwin</code> |
| macOS arm64 | <code>aarch64-apple-darwin</code> |
| Windows x64 | <code>x86_64-pc-windows-msvc</code> |
| Windows arm64 | <code>aarch64-pc-windows-msvc</code> |

构建命令固定为：

~~~text
cargo/cross build --release --target <target> -p dbtool-cli --no-default-features --features portable
~~~

### 10.2 产物

同一批 target-specific binary 被复用于：

- GitHub Release archive：<code>dbtool-&lt;tag&gt;-&lt;target&gt;.tar.gz</code>；
- npm 主包 <code>@yovinchen/dbtool</code> 与六个平台包；
- pip/uv wheel <code>dbtool-bin</code>；
- bash/zsh/fish completions 与 <code>dbtool.1</code>；
- mise/ubi 按约定消费 GitHub Release asset。

打包器先检查所有所选 target 的专属路径，禁止用一个宿主 binary 冒充其它平台。npm Linux x64 与 Python musllinux wheel 会在发布工作流中隔离安装并执行版本烟测；archives、npm tarball、wheel 一并附加到 Release。

当前 workflow 只创建 GitHub Release 附件，不执行 npm publish 或 PyPI upload；Python wheel 由仓库脚本直接组装，不使用 maturin。mise/ubi 没有独立二进制 job，复用相同 tarball 命名；Release 也不包含 TUI binary、Docker image 或项目 <code>SKILL.md</code>。

### 10.3 已验证与未验证

- 发布工作流、tag/version 门禁、target 身份预检、archive/npm/wheel 生成与安装烟测已经实现。
- macOS arm64 的 portable release binary、archive、npm tarball、wheel 已在宿主机真实生成并执行。
- 最终接口收口后的全仓 fmt/check/clippy/test/full-native 终验，以及一次真实 tag 触发的六平台矩阵，仍是发布关闭条件。
- 当前工作流没有 codesign、Apple notarization 或 Windows signing；这些只能写为后续分发加固项。
- <code>full</code>/<code>full-native</code> 是开发、测试或特定部署构建，不是六平台官方 release preset。

---

## 11. 验证分层与完成判据

存在 adapter、通过 mock test、通过兼容协议容器、通过真实供应商产品，代表不同证据等级：

1. service-free/unit：接口、operation、边界和错误可验证，不需要外部服务。
2. Docker integration：对仓库声明的具体镜像完成真实读写与清理。
3. external endpoint：Redshift、AutoMQ、WarpStream、Confluent 等需要调用方提供 DSN/凭证；未提供时 skip 不是 pass。
4. release matrix：每个 target 的 binary 和包装物必须来自该 target 自身。

产品完整性以 <code>docs/db-completeness-tasks.md</code> 和 manifest 为准；不能因 alias 可解析就把产品标为 CRUD 完成。接口补全任务以 <code>docs/interface-completion-tasks.zh-CN.md</code> 为准；仓库级验收以 <code>scripts/verify.sh</code>、<code>scripts/validate-feature-matrix.sh</code>、<code>scripts/validate-final-goal.sh</code> 及相应集成脚本为准。

旧的阶段式里程碑已归档。当前完成判据是：

- 公开 trait 的 required/optional 合同与 adapter operations 一致；
- CLI/TUI/embedded 路径先协商 operation，不出现语义降级；
- 用户查询、导出、顶层目录与消息消费有界，写保护与确认不可绕过；二级元数据完成度以 IF-T67 为准；
- feature 矩阵编译且 scheme 集准确；
- 全仓静态检查/测试通过；
- portable 六平台发布物在真实 workflow 中完成构建、安装和烟测；
- 外部依赖或未提供凭证的项明确记录为 boundary，不伪装为通过。

---

## 12. 可扩展性

### 12.1 已有协议的新兼容产品

若新产品确实兼容现有协议：

1. 在 <code>PROTOCOL_ALIASES</code> 添加 alias；
2. 增加 canonicalization、registry 与真实产品验证；
3. 不创建重复 adapter。

alias 增加仍是公共 DSN 合同变更，不能只改文档。

### 12.2 新协议、已有能力

增加独立 adapter crate，实现：

- <code>Connector</code>；
- 一个或多个现有能力 trait；
- 对应 accessor；
- 精确 <code>operations()</code>，尤其是 optional/bounded/admin；
- registry feature、factory 与 scheme；
- service-free contract test、CLI 负路径和可用时的 live test。

core 不需要知道具体驱动。

### 12.3 全新数据范式

只有现有端口确实无法表达时，才在 core 增加：

1. 新模型与 trait；
2. <code>Connector</code> 默认 <code>None</code> accessor；
3. 稳定 operation 名与 capability report；
4. CLI/TUI/embedded 协商路径；
5. formatter、安全、bounded 与测试合同。

这是加法扩展；已有 adapter 依靠默认 accessor 不必被迫实现新范式。

---

## 13. 已解决决策归档

| 历史问题 | 当前决策 |
| --- | --- |
| TDB 是 TiDB 还是时序库 | TDB/TSDB 指时序；TiDB 归 MySQL 协议族。 |
| 默认 Kafka backend | <code>portable</code>/<code>full</code> 使用 pure；<code>full-native</code> 使用 native。 |
| 首批 MQ 范围 | Kafka、AMQP/RabbitMQ、Redis、NATS 已实现；其余不是当前未完成接口。 |
| Oracle 是否进入当前实现 | 不进入；无 feature、无 factory、无 scheme。 |
| adapter 何时拆 crate | 已全部按协议/驱动拆分，registry 为组合根。 |
| TUI 是组件还是应用 | 独立应用；没有稳定组件库承诺。 |
| destructive 如何自动化确认 | 非交互两阶段 token，绑定目标/操作/资源/内容；没有通用 bypass。 |
| feature 如何分 portable/native | <code>portable</code>、<code>full</code>、<code>full-native</code> 已冻结并有矩阵校验。 |
| 凭证必须 keyring 吗 | 当前使用环境变量、DSN template 与脱敏配置 CRUD；keyring 不是当前完成条件。 |
| 发布是否重复编译 | matrix 每 target 编译一次，archive/npm/wheel 复用对应 artifact。 |

---

## 14. 真实未决事项与不可伪装边界

以下项目没有阻塞当前架构，但必须继续显式记录：

1. Db2 live test 需要宿主 IBM ODBC runtime；没有 runtime 时只能证明 service-free adapter 合同。
2. Redshift、AutoMQ、WarpStream、Confluent 等外部产品需要真实端点和凭证，env-gated skip 不是通过。
3. SQL Server 本机 arm64 Docker 不能代表 x86_64 服务路径；当前真实覆盖依赖 amd64 环境/CI。
4. 产品原生 Elasticsearch HTTPS 等仍需补外部证据；ScyllaDB 已通过命名产品原生 ARM64/x86_64 门禁，不能再用旧的 Cassandra alias-only 状态描述。
5. TiDB 本地安全 HA、PD/SQL 节点演练、TiProxy 与证书冷重启不等于生产备份、升级、在线证书轮换和全量容灾认证。
6. FlowControl 没有跨进程全局限流。
7. AMQP 无可移植 queue list；RabbitMQ queue depth 不是 consumer-group lag。Redis Pub/Sub 与 NATS core 没有持久 catalog 语义。
8. Prometheus 是 append/query 能力，不承诺通用行级 update/delete。
9. Kafka metadata 受响应 frame/协议能力约束，不应虚构统一 page count；Search CAT 与 NATS/JetStream catalog 也有协议特定 continuation 边界。
10. codesign/notarization/Windows signing 尚未实现。
11. 六平台 workflow 已定义，但仍需最终真实 tag 执行作为发布闭环证据。
12. Oracle、etcd、InfluxDB、VictoriaMetrics 原生 API、Pulsar、MQTT、RocketMQ 若要支持，必须新增 adapter/feature/operation/测试范围；不得列成当前接口欠账。
13. 表列/索引、完整 DDL 输入、topic partition watermarks 与 consumer lag 明细等二级集合仍由 IF-T67 追踪；顶层目录有界不能自动证明单资源内部对象有界。
14. transfer artifact 当前使用同目录 rename；Windows 已存在目标时缺少与配置 CRUD 相同的 replace-existing + write-through 路径，由 IF-T68 跟踪。

当前没有需要“确认后才能开始 P0”的架构开放问题；项目已经处于接口收口、真实产品验证和发布闭环阶段。

---

## 15. 相关权威资料

- <code>crates/dbtool-core/src/port/connector.rs</code>：能力名、粗粒度兼容与 report。
- <code>crates/dbtool-core/src/port/capability.rs</code>：trait 方法与 bounded/optional 合同。
- <code>crates/dbtool-core/src/registry/alias.rs</code>：scheme/alias 唯一清单。
- <code>crates/dbtool-registry/Cargo.toml</code> 与 <code>src/lib.rs</code>：feature 和组合根。
- <code>docs/interface-usage.zh-CN.md</code>：CLI/embedded 安全使用范式。
- <code>docs/implementation-status.md</code>：实现面摘要。
- <code>docs/db-completeness-tasks.md</code> 与 <code>docs/test-evidence/</code>：逐产品完成证据。
- <code>.github/workflows/release.yml</code>：正式发布矩阵。

本文档的后续修改必须同时核对上述源码；不能再把候选设计、历史计划或兼容性假设写成当前已实现能力。
