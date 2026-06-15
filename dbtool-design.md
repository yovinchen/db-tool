# dbtool 统一数据 / 消息系统连接工具 — 设计文档

> 版本: v0.4 (评审修正)
> 定位: 一个跨平台、**无外部运行时依赖**(单文件静态二进制)的统一**数据 + 消息**连接工具,可作为库嵌入、作为 TUI 独立运行,并作为 Claude Code Skill 被调用。
> v0.2 变更: 加入 AutoMQ/Redpanda 等协议兼容系统;以"协议族"重组适配器;新增架构风格选型、Crate/模块划分、能力动态分发机制、端到端数据流、三层扩展模型。
> v0.3 变更: 新增**流量控制(throttle)层**——并发/速率限制、单次与整体超时、获取许可超时、重试上限与退避、DB 服务端超时,以及防过载/防死锁的统一执行算法(第 8.1 节)。
> v0.4 变更(评审修正): ①术语从"纯 Rust"改为"无外部运行时依赖",澄清 SQLite/可选项的 C 编译边界(§2.1);②`Factory` 改为取所有权 `Dsn`,消除生命周期隐患(§6.2);③重试预算闭环——`deadline_at` 在重试外层建立一次,尝试与退避共享同一预算(§8.1.4);④Kafka backend 改为单 adapter + 互斥 backend feature(§12.2);⑤补齐协议族别名并明确"别名非必须,可用基础 scheme"(§3.8/§6.3);⑥破坏性操作确认改为非交互的 `--confirm <token>` 两段式(§15.1);⑦P0 拆为 P0a/P0b(§17);⑧示例代码修正(`consume` 的 `partial`)。

---

## 0. 术语与范围澄清

- **TDB / TSDB** → 按 **时序数据库** 处理(InfluxDB、Prometheus、VictoriaMetrics、TimescaleDB)。
- **协议兼容系统** → 多个产品共用同一 wire protocol,因此**共用同一适配器**,不重复实现:
  - **AutoMQ / Redpanda / WarpStream / Confluent** 兼容 **Kafka 协议** → 复用 Kafka 适配器。
  - **TiDB** 兼容 **MySQL 协议**;**CockroachDB / Redshift** 兼容 **PostgreSQL 协议**。
  - **Valkey / KeyDB / DragonflyDB** 兼容 **Redis 协议**。
- 本工具是**统一连接层**,横跨两大语义域:
  - **存储域**(取数语义:query / get / find / scan):关系型、NewSQL、文档、键值、宽列、搜索、时序。
  - **消息域**(收发语义:produce / consume / subscribe):Kafka 系、AMQP、NATS、Pulsar、Redis Streams、MQTT、RocketMQ。

---

## 1. 目标与非目标

### 1.1 目标
1. **一个核心库 `dbtool-core`**:用稳定的"能力抽象"屏蔽所有后端差异。
2. **单一静态二进制**:Linux / macOS / Windows × x64 / arm64 开箱即用,默认构建零外部运行时依赖(无 C 库、无 Docker、无 JVM)。
3. **三种使用形态共享同一核心**:CLI(Skill)、TUI、可嵌入库。
4. **连接信息走环境变量**,凭证不落明文。
5. **多渠道分发**:npm / pip / uv / mise,共享同一套编译产物。
6. **高可读、高可扩展**:新增一个后端的代价随其"是否协议兼容 / 是否复用已有能力"而分级递增(见第 16 节)。

### 1.2 非目标
- 不做 MCP server(避免常驻进程、协议开销、上下文占用)。
- 不做 GUI、不做 ORM / 迁移框架 / 数据同步管道。
- 首版不追求覆盖所有系统,分阶段交付。

---

## 2. 设计原则

| 原则 | 说明 |
|---|---|
| **能力而非类型** | 按"能力"(SQL/KV/文档/时序/搜索/生产/消费/管理)建模,而非按产品类型。一个适配器实现它支持的能力子集。 |
| **协议族复用** | 适配器按 **wire protocol** 而非产品实现;协议兼容的产品通过"scheme 别名"复用同一适配器,零新增代码。 |
| **无外部运行时依赖优先** | 适配器优先选无运行时依赖的实现;真正引入**外部运行时依赖**或显著抬高构建复杂度者(完整 Kafka via librdkafka、Oracle via Instant Client)隔离为可选 feature,不进默认构建。注意区分"构建期 C 工具链"与"运行期外部依赖"——见 §2.1。 |
| **依赖倒置** | 核心定义抽象(端口),适配器实现抽象,组合根负责装配。依赖箭头一律指向核心。 |
| **开闭原则** | 新增后端 = 加一个适配器 + 注册,不修改核心与其他适配器。 |
| **静态链接** | 默认 target 用 musl / native-static,产出零依赖单文件。 |
| **CLI 无状态、TUI 有状态** | CLI 每次建连即用即走;TUI 维护连接管理器(池/长连接)。 |
| **安全默认只读** | 写操作需显式开关;破坏性操作拦截,确认走非交互式 token(§15.1)。 |

### 2.1 "无外部运行时依赖" 的准确口径(重要澄清)

本项目的承诺是 **"单文件、无外部运行时依赖"**,而**不是字面意义的 "100% 纯 Rust、零 C 代码"**。两者必须区分:

| 概念 | 含义 | 是否影响用户 |
|---|---|---|
| **构建期 C 工具链** | 某些 crate 在编译时需要 C 编译器,把 C 源码**静态编入**最终二进制 | 仅影响"谁来构建"(CI/我们),用户拿到的仍是单文件 |
| **运行期外部依赖** | 二进制运行时需要系统已安装某动态库 / 客户端(如 Oracle Instant Client、系统 librdkafka) | 直接影响用户,违背"开箱即用" |

据此分三类:
- **真·纯 Rust(无 C)**:MySQL/Postgres 走 `sqlx`(rustls)、Redis、MongoDB、NATS、AMQP 等。
- **构建期编 C、运行期自包含**:**SQLite** 经 `sqlx` + `libsqlite3-sys`(bundled)会从 SQLite 的 C 源码静态编译链接——它**不是纯 Rust**,但产出的二进制**无运行时依赖**,仍满足"单文件开箱即用"。故 SQLite 保留在默认集,但术语上不再宣称"纯 Rust"。
- **运行期外部依赖(必须 opt-in)**:**Oracle**(Instant Client)、**完整 Kafka**(若选 librdkafka 后端)会引入运行时/重型构建依赖,一律隔离为可选 feature,不进默认。

> 因此后文系统矩阵的列名由"纯 Rust"改为 **"无运行时依赖"**;✅ = 单文件自包含(可能含构建期 C),⚠️ = 自包含但能力受限,❌ = 需运行期外部依赖。

---

## 3. 支持系统矩阵

> "无运行时依赖" 列(口径见 §2.1):✅ 单文件自包含(可能含构建期 C 编译,如 SQLite);⚠️ 自包含但能力受限;❌ 需运行期外部依赖(库/客户端)。

### 3.1 关系型 / NewSQL — 能力 `SqlEngine`
| 系统 | crate | 无运行时依赖 | 阶段 |
|---|---|---|---|
| MySQL / MariaDB / **TiDB** | `sqlx`(rustls) | ✅ | P1 |
| PostgreSQL / **CockroachDB** | `sqlx` | ✅ | P1 |
| SQLite | `sqlx` + `libsqlite3-sys`(bundled,构建期编 C) | ✅¹ | P1 |
| SQL Server | `tiberius` | ✅ | P6 |
| Oracle | `oracle`(需 Instant Client) | ❌ | 待定 |

> ¹ SQLite(bundled)在**构建期**编译 SQLite 的 C 源码并**静态链接**,运行期仍是自包含单文件、无外部依赖(口径见 §2.1)。它不是字面"纯 Rust",但满足"开箱即用"。

### 3.2 键值 — `KeyValueStore`(+ 消息能力)
| 系统 | crate | 无运行时依赖 | 阶段 |
|---|---|---|---|
| Redis / **Valkey / KeyDB / Dragonfly** | `redis`(rustls) | ✅ | P3 |
| etcd | `etcd-client` | ✅ | P6 |

### 3.3 文档 — `DocumentStore`
| 系统 | crate | 无运行时依赖 | 阶段 |
|---|---|---|---|
| MongoDB | `mongodb`(官方) | ✅ | P3 |

### 3.4 宽列 — `SqlEngine` 风格 / 自定义
| 系统 | crate | 无运行时依赖 | 阶段 |
|---|---|---|---|
| Cassandra / ScyllaDB | `scylla` | ✅ | P6 |

### 3.5 搜索 — `SearchEngine`
| 系统 | crate | 无运行时依赖 | 阶段 |
|---|---|---|---|
| Elasticsearch / OpenSearch | `elasticsearch` / `reqwest` | ✅ | P6 |

### 3.6 时序 — `TimeSeriesStore`
| 系统 | crate | 无运行时依赖 | 阶段 |
|---|---|---|---|
| InfluxDB | `influxdb` / HTTP | ✅ | P6 |
| TimescaleDB | 复用 PostgreSQL | ✅ | P1 |
| Prometheus / VictoriaMetrics | HTTP API | ✅ | P6 |

### 3.7 消息 / 流 — `MessageProducer` / `MessageConsumer` / `AdminInspect`
| 系统 | crate | 无运行时依赖 | 阶段 |
|---|---|---|---|
| **Kafka / AutoMQ / Redpanda / WarpStream**(自包含) | `rskafka` | ⚠️ 受限 | P5 |
| **Kafka / AutoMQ / Redpanda**(完整) | `rdkafka`(librdkafka) | ❌ C 依赖 | P6 (opt-in) |
| RabbitMQ (AMQP) | `lapin` | ✅ | P5 |
| NATS / JetStream | `async-nats` | ✅ | P5 |
| Redis Streams / PubSub | `redis` | ✅ | P5 |
| Pulsar | `pulsar` | ✅ | P6 |
| MQTT | `rumqttc` | ✅ | P6 |
| RocketMQ | `rocketmq-client-rust` | ⚠️ 待评估 | 待定 |

### 3.8 协议族 → 兼容产品(适配器复用关系,核心去重机制)

> **要实现的适配器数量,按"协议族"算,而非按"产品"算。** 一个协议族适配器自动覆盖一批兼容产品。

| 协议族(=一个适配器) | 自动覆盖的产品 | 规范 scheme + 别名(权威清单) |
|---|---|---|
| MySQL 协议 | MySQL、MariaDB、TiDB | `mysql://`(规范);别名 `mariadb://`、`tidb://` |
| PostgreSQL 协议 | PostgreSQL、CockroachDB、TimescaleDB、Redshift | `postgres://`(规范);别名 `postgresql://`、`cockroach://`、`timescale://`、`redshift://` |
| Redis 协议 | Redis、Valkey、KeyDB、DragonflyDB | `redis://`(规范);别名 `valkey://`、`keydb://`、`dragonfly://` |
| **Kafka 协议** | **Kafka、AutoMQ、Redpanda、WarpStream、Confluent** | `kafka://`(规范);别名 `automq://`、`redpanda://`、`warpstream://`、`confluent://` |
| AMQP | RabbitMQ | `amqp://` |
| MongoDB 协议 | MongoDB、兼容实现 | `mongodb://` |

> **别名是便利,不是必须**:任何协议兼容产品都可直接用规范 scheme 连接(AutoMQ 用 `kafka://` 一样工作)。别名仅提升 DSN 可读性;本表与 §6.3 注册代码保持一致,是别名的唯一权威来源。新增别名只需在 §6.3 的列表里加一项,零新增逻辑(见 16.1)。

---

## 4. 架构总览与风格选型

### 4.1 架构风格:端口与适配器(六边形) + 插件注册表

整体采用 **Hexagonal Architecture(Ports & Adapters)** 叠加 **Plugin Registry**:

```
                ┌───────────────────── 驱动侧(使用方) ─────────────────────┐
                │   CLI (Skill)        TUI            第三方程序(嵌入)      │
                └───────────────┬───────────────┬───────────────┬───────────┘
                                │ 仅依赖端口+服务 │               │
                                ▼               ▼               ▼
        ┌───────────────────────────────────────────────────────────────────┐
        │                          dbtool-core (核心域)                       │
        │                                                                     │
        │   应用服务(Services): 连接解析 / 安全护栏 / 结果限制 / 格式化         │
        │   ───────────────────────────────────────────────────────────────  │
        │   端口(Ports / 能力 trait): Connector + SqlEngine / KeyValueStore   │
        │                         / DocumentStore / TimeSeriesStore /         │
        │                         SearchEngine / MessageProducer /            │
        │                         MessageConsumer / AdminInspect              │
        │   ───────────────────────────────────────────────────────────────  │
        │   域模型(Model): Value / ResultSet / Message / Document / Error     │
        │   注册表机制(Registry): scheme → Factory(数据结构,不含具体实现)    │
        └───────────────────────────────────────────────────────────────────┘
                                ▲               ▲               ▲
                                │ 实现端口        │               │
        ┌───────────────────────┴───────┬───────┴───────┬───────┴───────────┐
        │  adapter-sql(sqlx)            │ adapter-redis │ adapter-kafka     │ ...
        │  mysql/postgres/sqlite/tidb   │ redis/valkey  │ kafka/automq/...  │
        └───────────────────────────────┴───────────────┴───────────────────┘
                                            被驱动侧(适配器)
```

- **端口(Ports)** = 能力 trait,定义"能做什么"。稳定、不依赖任何具体驱动。
- **适配器(Adapters)** = 用 `sqlx`/`redis`/`rskafka` 等真实驱动实现端口,负责"把驱动的原生类型翻译成核心的统一模型"。
- **注册表(Registry)** = 组合根,把 `scheme` 映射到适配器工厂;受 Cargo feature 门控。
- **使用方(CLI/TUI/库)** = 只依赖端口与应用服务,**完全不感知具体后端**。

### 4.2 为什么选这套架构(选型论证)

| 候选 | 优点 | 缺点 | 结论 |
|---|---|---|---|
| **单体 + 大 match/enum 分发** | 实现直接 | 每加一个后端都要改中心枚举,违反开闭;无法支持第三方/feature 裁剪 | ✗ |
| **六边形 + 插件注册(本方案)** | 后端可插拔、可裁剪、可第三方扩展;核心稳定;使用方与后端解耦 | 每个适配器有少量样板(可用宏消除) | ✓ |
| **纯动态加载(.so 插件)** | 运行时热插拔 | Rust ABI 不稳定、跨平台复杂、违背"单文件零依赖" | ✗ |

关键判断:本项目的**易变点是"后端种类"**(会不断增加),而**稳定点是"能力语义"**。六边形架构正是把易变的适配器隔离在稳定端口之后,使"增加后端"成为纯加法操作。插件注册表则让这种加法在编译期通过 feature 精确裁剪,既支持"最小核心"也支持"全功能",还为第三方适配器预留了扩展点。

### 4.3 分层模型(自内向外,依赖只能向内)

| 层 | 职责 | 是否含 I/O | 依赖 |
|---|---|---|---|
| **L1 域模型** | `Value`/`ResultSet`/`Message`/`Document`/`Error`/`Dsn` | 否 | 无 |
| **L2 端口** | `Connector` + 各能力 trait | 否(仅签名) | L1 |
| **L3 应用服务** | 连接解析、安全护栏、结果限制、格式化、连接管理 | 编排级 | L1, L2 |
| **L4 注册表** | `scheme → Factory` 映射、能力协商 | 否 | L1, L2 |
| **L5 适配器** | 各后端实现端口,驱动类型 ↔ 域模型映射 | 是 | L1, L2(+各驱动 crate) |
| **L6 使用方** | CLI(clap)、TUI(ratatui)、库 API | 是 | L1–L4 |

> L5 适配器虽然实现 L2 端口,但**使用方(L6)不直接依赖 L5**;两者在组合根(注册表)处汇合。这是依赖倒置的体现。

### 4.4 Crate 划分与依赖方向

```
dbtool-core                 # L1–L4:域模型、端口、服务、注册表机制、错误
   ▲        ▲        ▲        ▲
   │        │        │        │        (每个适配器 crate 只依赖 core + 自己的驱动)
adapter-sql adapter-redis adapter-mongo adapter-kafka  adapter-amqp  adapter-nats ...
   ▲        ▲        ▲        ▲              ▲              ▲
   └────────┴────────┴────────┴──────────────┴──────────────┘
                              │
                       dbtool-registry            # 组合根:按 feature 装配所有适配器,导出 build_registry()
                              ▲
                  ┌───────────┴───────────┐
            dbtool-cli                dbtool-tui   # 仅依赖 core + registry
```

设计要点:
- **依赖箭头全部指向 `dbtool-core`**;core 不依赖任何后端驱动,可独立编译与单元测试。
- **适配器按"驱动/协议族"分 crate**(而非按产品),减少 crate 数量:`adapter-sql` 一个 crate 内含 MySQL/Postgres/SQLite(都用 sqlx)。
- **`dbtool-registry` 是唯一感知"全部适配器"的地方**,用 `#[cfg(feature)]` 决定编译进哪些;CLI/TUI 只调 `build_registry()`,自身保持干净。
- 第三方可发布自己的 `dbtool-adapter-xxx`(只依赖 `dbtool-core`),在自己的组合根注册,无需改动本仓库。

> 早期阶段(P0–P3)可先把适配器作为 `dbtool-core` 的 feature 门控模块,降低多 crate 协调成本;待适配器变多再按上图拆分。架构上两种形态等价,因为依赖方向一致。

### 4.5 `dbtool-core` 内部模块划分

```
dbtool-core/src/
├── model/                 # L1 域模型
│   ├── value.rs           # 统一值类型 Value
│   ├── result.rs          # ResultSet / ColumnMeta / ExecOutcome
│   ├── message.rs         # Message / ProduceOutcome / ConsumeOptions
│   ├── document.rs        # Document / Find/Update Outcome
│   ├── series.rs          # Point / SeriesSet / TimeRange
│   └── meta.rs            # TableInfo / TopicInfo / LagInfo ...
├── port/                  # L2 端口(能力 trait)
│   ├── connector.rs       # 基础 trait + 能力 accessor
│   └── capability.rs      # Sql/KV/Document/TimeSeries/Search/Producer/Consumer/Admin
├── dsn/                   # 连接串
│   ├── parse.rs           # URL 风格解析
│   └── redact.rs          # 凭证脱敏
├── registry/              # L4 注册表机制
│   ├── registry.rs        # Registry / Factory 类型
│   └── alias.rs           # 协议族别名表
├── service/               # L3 应用服务(CLI/TUI 共享)
│   ├── resolver.rs        # 连接解析:name/dsn/env/config → Dsn,展开 ${ENV}
│   ├── safety.rs          # 危险操作识别(基于 sqlparser)
│   ├── throttle.rs        # 流量控制:并发/速率限制、超时、重试、防死锁
│   ├── limiter.rs         # 结果行数限制 / 截断标记(注意:与 throttle 不同)
│   ├── formatter.rs       # Value → JSON / table / ndjson
│   └── manager.rs         # ConnectionManager(TUI 用连接池/长连接)
├── config/                # 配置
│   ├── file.rs            # connections.toml
│   └── env.rs             # 环境变量解析
└── error.rs               # 统一 Error(thiserror)
```

每个模块单一职责、可独立测试;`service/` 是 CLI 与 TUI 之间避免逻辑重复的关键。

---

## 5. 能力抽象与动态分发(核心机制)

不同范式无法用单一 `query()` 统一。采用**基础 trait + 能力 accessor** 模式,既类型安全又可扩展。

### 5.1 基础 trait:能力 accessor 模式

`Connector` 提供一组返回 `Option<&dyn 能力>` 的访问器,默认全为 `None`,适配器只覆盖自己支持的能力:

```rust
#[async_trait]
pub trait Connector: Send + Sync {
    fn kind(&self) -> ConnectorKind;
    fn capabilities(&self) -> Capabilities;          // bitflags,用于展示/校验
    async fn ping(&self) -> Result<()>;
    async fn close(self: Box<Self>) -> Result<()>;

    // —— 能力访问器:默认 None,适配器按需覆盖 ——
    fn as_sql(&self)        -> Option<&dyn SqlEngine>       { None }
    fn as_kv(&self)         -> Option<&dyn KeyValueStore>   { None }
    fn as_document(&self)   -> Option<&dyn DocumentStore>   { None }
    fn as_timeseries(&self) -> Option<&dyn TimeSeriesStore> { None }
    fn as_search(&self)     -> Option<&dyn SearchEngine>    { None }
    fn as_producer(&self)   -> Option<&dyn MessageProducer> { None }
    fn as_consumer(&self)   -> Option<&dyn MessageConsumer> { None }
    fn as_admin(&self)      -> Option<&dyn AdminInspect>    { None }
}
```

> 选用此模式而非 `Any` 向下转型:**编译期类型安全、无运行时反射、调用点零成本**。新增能力时给 `Connector` 加一个带默认 `None` 的访问器即可,**所有现存适配器自动继承默认值、无需改动**(开闭原则)。每个适配器的样板可用 `impl_capabilities!` 派生宏消除。

### 5.2 能力 trait 清单

```rust
#[async_trait] pub trait SqlEngine: Connector {
    async fn query(&self, sql: &str, p: &[Value]) -> Result<ResultSet>;
    async fn execute(&self, sql: &str, p: &[Value]) -> Result<ExecOutcome>;
    async fn list_schemas(&self) -> Result<Vec<String>>;
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>>;
    async fn describe_table(&self, table: &str) -> Result<TableSchema>;
}
#[async_trait] pub trait KeyValueStore: Connector {
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    async fn set(&self, key: &str, val: &[u8], o: SetOptions) -> Result<()>;
    async fn delete(&self, keys: &[String]) -> Result<u64>;
    async fn scan(&self, pattern: &str, limit: usize) -> Result<Vec<String>>;
    async fn raw_command(&self, args: &[String]) -> Result<Value>;   // 逃生通道
}
#[async_trait] pub trait DocumentStore: Connector {
    async fn list_collections(&self) -> Result<Vec<String>>;
    async fn find(&self, c: &str, filter: Value, o: FindOptions) -> Result<Vec<Document>>;
    async fn insert(&self, c: &str, docs: Vec<Document>) -> Result<InsertOutcome>;
    async fn update(&self, c: &str, filter: Value, upd: Value) -> Result<UpdateOutcome>;
    async fn delete(&self, c: &str, filter: Value) -> Result<u64>;
    async fn aggregate(&self, c: &str, pipeline: Vec<Value>) -> Result<Vec<Document>>;
}
#[async_trait] pub trait TimeSeriesStore: Connector {
    async fn list_measurements(&self) -> Result<Vec<String>>;
    async fn write_points(&self, pts: Vec<Point>) -> Result<()>;
    async fn query_range(&self, q: &str, r: TimeRange) -> Result<SeriesSet>;
}
#[async_trait] pub trait SearchEngine: Connector {
    async fn list_indices(&self) -> Result<Vec<IndexInfo>>;
    async fn search(&self, idx: &str, q: Value, o: SearchOptions) -> Result<SearchHits>;
    async fn index_doc(&self, idx: &str, doc: Value) -> Result<()>;
}
#[async_trait] pub trait MessageProducer: Connector {
    async fn produce(&self, target: &str, msgs: Vec<Message>) -> Result<ProduceOutcome>;
}
#[async_trait] pub trait MessageConsumer: Connector {
    async fn consume(&self, source: &str, o: ConsumeOptions) -> Result<Vec<Message>>;  // 必须有界
}
#[async_trait] pub trait AdminInspect: Connector {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>>;
    async fn topic_detail(&self, name: &str) -> Result<TopicDetail>;
    async fn consumer_lag(&self, group: &str) -> Result<Vec<LagInfo>>;
}
```

### 5.3 适配器能力组合表

| 适配器 | Sql | KV | Doc | TS | Search | Producer | Consumer | Admin |
|---|---|---|---|---|---|---|---|---|
| sql (mysql/pg/sqlite/tidb) | ✓ | | | (timescale) | | | | |
| redis (valkey/keydb) | | ✓ | | | | ✓(streams) | ✓(streams) | |
| mongo | | | ✓ | | | | | |
| kafka (automq/redpanda) | | | | | | ✓ | ✓ | ✓ |
| amqp (rabbitmq) | | | | | | ✓ | ✓ | ✓ |
| nats | | | | | | ✓ | ✓ | ✓ |
| influx | | | | ✓ | | | | |
| elasticsearch | | | | | ✓ | | | |
| cassandra | ✓(CQL) | | | | | | | |

### 5.4 运行时能力协商(分发逻辑)

使用方拿到 `Box<dyn Connector>` 后,通过访问器拿具体能力;不支持则给出明确错误:

```rust
// CLI: dbtool sql query --conn prod "SELECT ..."
let conn = registry.connect(&dsn).await?;                 // Box<dyn Connector>
let sql  = conn.as_sql()
    .ok_or_else(|| Error::UnsupportedCapability {
        kind: conn.kind(), needed: "SqlEngine",
    })?;
let rs = sql.query(stmt, &params).await?;                 // 真正执行
```

`dbtool caps --conn prod` 则直接读 `conn.capabilities()` 输出该连接支持的能力清单,供 Claude 在操作前先探测。

### 5.5 统一值模型

```rust
pub enum Value {
    Null, Bool(bool), Int(i64), Float(f64), Text(String),
    Bytes(Vec<u8>),         // JSON 序列化为 base64
    Timestamp(i64),         // epoch millis, UTC
    Json(serde_json::Value),
    Array(Vec<Value>), Map(BTreeMap<String, Value>),
}
pub struct ResultSet {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<Value>>,
    pub truncated: bool,
}
```

适配器负责"驱动原生类型 ↔ `Value`"双向映射,使所有后端的输出在 CLI 层统一为 JSON。

---

## 6. 注册表、DSN 与协议族复用

### 6.1 DSN(scheme 决定走哪个适配器)
```
mysql://user:pass@host:3306/db        tidb://...            (→ sql 适配器)
postgres://user:pass@host:5432/db     cockroach://...       (→ sql 适配器)
sqlite:///abs/path.db
redis://:pass@host:6379/0             valkey://...          (→ redis 适配器)
mongodb://user:pass@host:27017/db
kafka://b1:9092,b2:9092               automq://...  redpanda://...   (→ kafka 适配器)
amqp://user:pass@host:5672/vhost      nats://host:4222
es+https://host:9200                  influxdb://host:8086?org=...&bucket=...
cassandra://host:9042
```

### 6.2 注册表机制

> **关于 `Factory` 的生命周期**:工厂接收**所有权** `Dsn`(而非 `&Dsn`),从而 async future 可安全持有它、返回 `'static`,避免"future 借用局部 `Dsn` 却要求 `'static`"的冲突。`Dsn` 解析一次后按值移交给工厂。

```rust
// 取所有权:future 拥有 Dsn,可满足 'static
pub type Factory =
    fn(Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>>;

pub struct Registry { map: HashMap<&'static str, Factory> }

impl Registry {
    pub fn register(&mut self, scheme: &'static str, f: Factory) {
        self.map.insert(scheme, f);
    }
    pub async fn connect(&self, dsn_str: &str) -> Result<Box<dyn Connector>> {
        let dsn = Dsn::parse(dsn_str)?;
        let f = *self.map.get(dsn.scheme())
            .ok_or_else(|| Error::UnsupportedScheme(dsn.scheme().to_owned()))?;
        f(dsn).await                 // 按值移交;若工厂内部需保留原始串,Dsn 自带 raw 字段
    }
}
```

> 备选方案:`for<'a> fn(&'a Dsn) -> BoxFuture<'a, ...>`(借用且生命周期对齐)。本设计选"取所有权",因为工厂常需把连接参数搬进长生命周期的连接对象,按值最省心。

### 6.3 协议族别名(AutoMQ / TiDB 复用的落地)

**别名策略**:每个协议族注册一个**规范 scheme** + 若干**便利别名**;别名只是为了 DSN 可读性,**并非必须**——任何协议兼容产品都可以直接用基础 scheme 连接(如 AutoMQ 用 `kafka://`、Redshift 用 `postgres://` 一样能跑)。下表别名与 §3.8 保持一致,是别名的**唯一权威清单**。

```rust
// dbtool-registry/src/lib.rs
pub fn build_registry() -> Registry {
    let mut r = Registry::new();

    #[cfg(feature = "sql")] {
        // MySQL 协议族
        for s in ["mysql", "mariadb", "tidb"] { r.register(s, adapter_sql::mysql_factory); }
        // PostgreSQL 协议族
        for s in ["postgres", "postgresql", "cockroach", "redshift", "timescale"] {
            r.register(s, adapter_sql::postgres_factory);
        }
        r.register("sqlite", adapter_sql::sqlite_factory);
    }
    #[cfg(feature = "redis")] {
        for s in ["redis", "valkey", "keydb", "dragonfly"] { r.register(s, adapter_redis::factory); }
    }
    #[cfg(feature = "kafka")] {           // 单一 feature;backend(pure/native)在 adapter 内按 cfg 选择
        for s in ["kafka", "automq", "redpanda", "warpstream", "confluent"] {
            r.register(s, adapter_kafka::factory);
        }
    }
    #[cfg(feature = "mongodb")] r.register("mongodb", adapter_mongo::factory);
    #[cfg(feature = "amqp")]    r.register("amqp",    adapter_amqp::factory);
    #[cfg(feature = "nats")]    r.register("nats",    adapter_nats::factory);
    // ...
    r
}
```

> 注意:Kafka 只有**一个** `kafka` feature 决定"是否编入 Kafka 适配器";用 `rskafka` 还是 librdkafka 由 adapter 内部的互斥 backend feature 决定(§12.2),注册处不感知 backend。

### 6.4 Feature 门控与组合根
`dbtool-registry` 的 `[features]` 透传到各适配器 crate,`build_registry()` 用 `#[cfg]` 决定注册谁。未启用的适配器**既不编译也不注册**,运行时对其 scheme 报"未在当前构建启用"。

---

## 7. 端到端数据流

### 7.1 CLI 一次性调用全链路
```
argv ──► clap 解析 ──► ConnectionResolver
                          │  name/dsn/env/config → Dsn,展开 ${ENV},脱敏副本用于日志
                          ▼
                     Registry.connect(dsn) ──► Box<dyn Connector>   [适配器建连]
                          │
                          ▼
                     能力协商: conn.as_sql()/as_kv()/as_consumer()...
                          │  不支持 → Error::UnsupportedCapability(JSON 返回)
                          ▼
              ┌── 写/改/删路径 ──► SafetyGuard.check(stmt, allow_write, force)
              │                       │  命中破坏性模式 → 拒绝/要求确认
              ▼                       ▼
        ResultLimiter(注入/截断)  FlowControl.run(op):
                          │            ① 取速率令牌(带 acquire_timeout)
                          │            ② 取并发许可(带 acquire_timeout)→ 拿不到报 Overloaded,不挂起
                          │            ③ budget = min(剩余总预算, 单次超时)
                          │            ④ timeout(budget, adapter 执行) → 超时即中止
                          │            ⑤ 可重试错误/DB死锁 → 退避重试(有上限,受预算约束)
                          ▼
                     Formatter: ResultSet/Outcome → JSON 信封 ──► stdout
                          │
                          ▼
                     conn.close()(一次性,即用即走)
```

### 7.2 关键流程逻辑

- **连接解析(resolver)**:优先级 `--dsn` > `--conn`(先查环境变量 `DBTOOL_CONN_<NAME>`,再查 `connections.toml`);统一展开 `${ENV}`;生成脱敏 DSN 供日志/错误使用。
- **安全护栏(safety)**:对 SQL 用 **`sqlparser`(纯 Rust)** 解析出语句类型,可靠识别 `DROP/TRUNCATE/ALTER`、无 `WHERE` 的 `DELETE/UPDATE`;对 KV/MQ 识别 `FLUSHALL`、整 topic 删除等。默认拒写;`--allow-write` 放行普通写;破坏性操作走**非交互两段式 token 确认**(`--confirm <token>`,见 §15.1);连接级 `readonly=true` 一票否决。
- **消费有界(consume)**:CLI 永不无限阻塞。在一个带总 `deadline` 的循环里累积消息,收满 `max` 或到期即返回**已收部分**(用显式缓冲,避免丢失):
  ```rust
  async fn consume_bounded(src: &Source, max: usize, timeout: Duration)
      -> Result<Vec<Message>>
  {
      let deadline = Instant::now() + timeout;
      let mut buf = Vec::with_capacity(max);
      while buf.len() < max {
          let remaining = deadline.saturating_duration_since(Instant::now());
          if remaining.is_zero() { break; }                 // 到期:返回已收部分
          match tokio::time::timeout(remaining, src.next()).await {
              Ok(Ok(Some(msg))) => buf.push(msg),
              Ok(Ok(None))      => break,                    // 流结束
              Ok(Err(e))        => return Err(e),
              Err(_)            => break,                    // 到期
          }
      }
      Ok(buf)
  }
  ```
- **结果限制(ResultLimiter)**:限"返回多少行"。默认 `--limit`(如 100),能下推则注入 `LIMIT`/`size`,否则收集后截断并置 `truncated=true`,防止撑爆 Claude 上下文。**注意与"流量控制"区分**:前者限返回行数,后者(见 8.1)限请求速率/并发/时长。
- **流量控制(FlowControl)**:限"发多少请求、跑多久",防止拖垮 DB、防止进程挂死。详见第 8.1 节。
- **格式化(formatter)**:默认 JSON 信封;`--format table` 给人看;`--format ndjson` 适合大结果流式处理。

JSON 信封统一为:
```json
{ "ok": true, "kind": "kafka",
  "data": { ... },
  "meta": { "elapsed_ms": 12, "truncated": false } }
```
```json
{ "ok": false, "error": { "code": "UNSUPPORTED_CAPABILITY", "message": "redis 不支持 SQL 查询" } }
```

---

## 8. 横切关注点

### 8.1 流量控制与防过载(限流 / 并发 / 超时 / 防死锁)

> **先区分两个"limit"**:第 7.2 的 **结果限制(ResultLimiter)** 限的是"返回多少行";本节的 **流量控制(FlowControl)** 限的是"发出多少请求、跑多久"。二者正交互补,都必须有。

#### 8.1.1 目标(逐条对应需求)
| 需求 | 机制 |
|---|---|
| 每次请求数不超过上限、可设置 | 并发信号量 `max_concurrency` + 令牌桶速率 `rate`(均可配置) |
| 不能拖垮 DB | 并发上限 + 速率上限 + 有界连接池;按目标独立计额 |
| "增加延时"(平滑请求节奏) | 令牌桶天然把请求按速率铺开,峰值被削平 |
| 数据请求的(总)时间不超过限制 | 单次 `request_timeout` + 整体 `overall_deadline`;**排队、执行、重试、退避全部消耗同一 `deadline_at`**(§8.1.4) |
| 防止进入死锁(进程卡死) | **获取许可/令牌也有超时**(拿不到即报错而非死等)+ **重试有上限**(不死循环)+ DB 服务端 `lock_timeout` |

#### 8.1.2 模块与配置
位于 `service/throttle.rs`,核心类型 `FlowControl`。**按连接名(目标)各持一个实例**,而非全局共享——拖垮 A 库不应连累 B 库。

```rust
pub struct ThrottleConfig {
    pub max_concurrency: usize,             // 在途请求上限(信号量),默认如 8
    pub rate: Option<Rate>,                 // QPS/QPM 上限(令牌桶),可选
    pub acquire_timeout: Duration,          // 获取许可/令牌的最长等待 → 防排队死等
    pub request_timeout: Duration,          // 单次操作超时
    pub overall_deadline: Option<Duration>, // 排队 + 执行 的总预算
    pub max_retries: u32,                   // 瞬时错误 / DB 死锁的重试上限 → 防死循环
    pub backoff: Backoff,                   // 指数退避 + 抖动
}
pub enum Rate { PerSecond(u32), PerMinute(u32) }
```

#### 8.1.3 核心执行算法(防过载 + 防死等,一体)
并发用 `tokio::sync::Semaphore`(纯 Rust),速率用 `governor` crate 的令牌桶(纯 Rust)。**关键在于:令牌与许可的获取都用 `timeout` 包裹——拿不到不是无限阻塞,而是返回明确错误**,这是防"卡死"的根本。

```rust
// 剩余预算辅助:到 deadline 还剩多久;已过则 DeadlineExceeded
#[inline]
fn remaining(deadline_at: Instant) -> Result<Duration> {
    let now = Instant::now();
    if now >= deadline_at { Err(Error::DeadlineExceeded) } else { Ok(deadline_at - now) }
}

impl FlowControl {
    // 单次尝试:接收**外层传入的共享 deadline_at**(不在内部重新计时)
    async fn run_once<F, T>(&self, deadline_at: Instant, op: F) -> Result<T>
    where F: Future<Output = Result<T>> {
        // ① 速率令牌:等待上限 = min(acquire_timeout, 到 deadline 的剩余)
        if let Some(rl) = &self.rate {
            let wait = self.acquire_timeout.min(remaining(deadline_at)?);
            timeout(wait, rl.until_ready()).await.map_err(|_| Error::RateLimited)?;
        }
        // ② 并发许可:同样受 deadline 约束,拿不到 → Overloaded(不挂起)
        let wait = self.acquire_timeout.min(remaining(deadline_at)?);
        let _permit = timeout(wait, self.sem.clone().acquire_owned())
            .await.map_err(|_| Error::Overloaded)??;
        // ③ 执行预算 = min(到 deadline 的剩余, 单次超时)
        let budget = remaining(deadline_at)?.min(self.request_timeout);
        // ④ 带预算执行真正的 I/O,超时即中止
        timeout(budget, op).await.map_err(|_| Error::Timeout)?
    }
}
```

要点:
- **`deadline_at` 由重试外层建立一次**(见 8.1.4),`run_once` 不重新计时,因此排队、执行、退避全部消耗**同一个总预算**。
- 所有等待点(令牌、许可、执行)都有超时上界 → `FlowControl` 自身**永不无限阻塞**。

#### 8.1.4 重试与退避(共享同一预算,真正闭环)

> 修正点:`deadline_at` 在进入重试循环前建立一次;**每次尝试 + 每次退避 sleep 都从这同一个 deadline 扣减**,退避睡眠也不得越过 deadline。这样 `overall_deadline` 才真正约束"尝试 + 退避"的总和。

```rust
async fn with_retry<F, Fut, T>(fc: &FlowControl, mk: F) -> Result<T>
where F: Fn() -> Fut, Fut: Future<Output = Result<T>> {
    // 总预算:overall_deadline 优先,否则退化为单次 request_timeout
    let deadline_at = Instant::now() + fc.overall_deadline.unwrap_or(fc.request_timeout);
    let mut attempt = 0;
    loop {
        match fc.run_once(deadline_at, mk()).await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_retryable() && attempt < fc.max_retries => {
                attempt += 1;
                // 退避也消耗同一预算:睡眠 = min(退避时长, 剩余预算);剩余为 0 即停
                let rem = remaining(deadline_at)?;               // 预算耗尽 → DeadlineExceeded
                let nap = fc.backoff.delay_with_jitter(attempt).min(rem);
                if nap.is_zero() { return Err(Error::DeadlineExceeded); }
                sleep(nap).await;
            }
            Err(e) => return Err(e),  // 不可重试 / 超出次数 / 预算耗尽 → 失败,绝不死循环
        }
    }
}
```
`is_retryable()` 覆盖:连接重置、`too many connections`(瞬时)、**DB 死锁检测(MySQL `1213` / PostgreSQL `40P01`)**、序列化失败(`40001`)。DB 检出死锁会回滚一个牺牲者,重试牺牲者通常即成功。重试**受次数上限与总预算双重约束**——两者任一触顶即停,退避睡眠也算在预算内。

#### 8.1.5 DB 服务端超时(与客户端超时双保险)
连接建立时自动下发(可配置),防服务端挂起 / 锁等待:
- **PostgreSQL**:`statement_timeout`、`lock_timeout`、`idle_in_transaction_session_timeout`
- **MySQL**:`SESSION max_execution_time`(SELECT)、`innodb_lock_wait_timeout`

客户端 `request_timeout` 兜底网络层,服务端超时兜底 DB 层,任意一层都不会无限挂死。

#### 8.1.6 死锁防护小结
1. 默认只读 + 不鼓励长事务 → 结构上规避多数应用级死锁。
2. 写操作设 `lock_timeout` → 锁等待快速失败而非挂起。
3. DB 死锁错误自动重试(有上限)。
4. 客户端令牌/许可/执行全部有超时 → 工具本身永不死等。

#### 8.1.7 作用范围(诚实边界)
`FlowControl` 在**单个进程内**生效:单次 CLI 调用内的扇出(如多语句批处理、`caps` 探测)、TUI 会话、嵌入式长驻使用。**跨多次 CLI 调用的全局 QPS 不在范围内**——无状态 CLI 每次新进程无法共享计数器;且 Claude 驱动时调用本就串行(等结果再发下一条),有效 QPS 天然很低。若确需跨进程全局限流,可作为后续可选项(本地共享计数 / 协调器),非默认。

### 8.2 其他横切关注点

| 关注点 | 设计 |
|---|---|
| **错误模型** | core 统一 `Error`(`thiserror`):`Config/Dsn/UnsupportedScheme/UnsupportedCapability/Connection/Auth/Query/Timeout/Safety/Serialization` + 流控相关 `RateLimited/Overloaded/DeadlineExceeded`。适配器把驱动错误映射进来;CLI 序列化为错误信封。 |
| **异步运行时** | 统一 `tokio`。CLI 一次性用 `current_thread` 运行时(更轻);TUI 用多线程运行时 + 连接池。 |
| **连接管理** | CLI 无池(建连→用→drop);TUI 用 `ConnectionManager`(按连接名持有 sqlx pool / 长连接 client,并挂载该连接的 `FlowControl`),供长会话与多面板复用。`ConnectionManager` 在 core,嵌入方亦可用。连接池本身的 `max_connections` 是并发上限的第二道闸。 |
| **凭证与脱敏** | 默认从环境变量取密码;日志/错误中 DSN 密码段自动打码为 `***`;可选 `keyring`(纯 Rust)作本地加密后端(非默认 feature)。 |
| **可观测性** | `tracing` 分级日志;`--verbose` 打开;敏感信息一律走脱敏层;流控的拒绝/重试/超时均打点。 |

---

## 9. 配置与凭证

三种连接来源(优先级从高到低):`--dsn` 直给 → `--conn <name>`(环境变量 `DBTOOL_CONN_<NAME>` → 配置文件)。

```toml
# ~/.config/dbtool/connections.toml(凭证用 ${ENV} 引用,文件可安全入库)
[defaults.limits]                          # 全局默认流控,可被单连接覆盖
max_concurrency = 8
rate = "50/s"
acquire_timeout = "2s"
request_timeout = "10s"
overall_deadline = "15s"
max_retries = 3

[connections.prod_mysql]
dsn = "mysql://app:${MYSQL_PWD}@db.internal:3306/orders"
readonly = true
[connections.prod_mysql.limits]            # 该连接收紧,保护生产库
max_concurrency = 4
rate = "20/s"
request_timeout = "5s"

[connections.events]
dsn = "automq://kafka1:9092,kafka2:9092"   # AutoMQ 直接走 kafka 适配器
```

---

## 10. CLI 设计(Skill 调用面)

动词映射能力(节选):
```
sql query|exec|tables|schema   --conn <n> [--allow-write] [--limit N] [--schema s]
kv  get|set|scan|raw           --conn <n> ...        # raw 为原生命令逃生通道
doc find|insert|collections|aggregate --conn <n> --filter '{...}' --limit N
ts  query|measurements         --conn <n> --from -1h --to now
search query|indices           --conn <n> --q '{...}'
mq  produce|consume|topics|lag  --conn <n> --topic t --group g --max N --timeout 5s
ping | caps | conn list/add/remove | tui            # caps 先探能力
```
输出默认 JSON;`--format table|ndjson`;一律带默认 `--limit`(限行数);`consume` 强制 `--max`/`--timeout`。

**流控参数(所有子命令通用,覆盖配置文件默认值):**
```
--max-concurrency N   # 在途上限      --rate 50/s        # 速率上限
--request-timeout 5s  # 单次超时      --deadline 15s     # 整体预算(含排队)
--acquire-timeout 2s  # 取许可超时    --max-retries 3    # 重试上限
```
被流控拒绝时返回明确错误码(`RATE_LIMITED` / `OVERLOADED` / `DEADLINE_EXCEEDED` / `TIMEOUT`),便于 Claude 据此退避或减载,而不是傻等。

**破坏性操作确认(非交互)**:`--allow-write` 放行普通写;破坏性操作先空跑得到 `confirm_token`,再 `--confirm <token>` 重放执行(详见 §15.1),全程 JSON、不卡自动化。

---

## 11. TUI 设计
基于 `ratatui`:连接列表 / 能力自适应面板(SQL 表浏览、KV 浏览、MQ topic 与消息流、消费滞后视图)。复用 `dbtool-core` 的 `ConnectionManager`(连接池)与 services。可作为库被第三方 TUI 嵌入(组件化目标见开放问题)。

---

## 12. 跨平台编译与"无运行时依赖"边界

### 12.1 目标矩阵
| 平台 | target | 链接 |
|---|---|---|
| Linux x64 | `x86_64-unknown-linux-musl` | 静态 |
| Linux arm64 | `aarch64-unknown-linux-musl` | 静态 |
| macOS x64 | `x86_64-apple-darwin` | 动态(libSystem) |
| macOS arm64 | `aarch64-apple-darwin` | 动态 |
| Windows x64 | `x86_64-pc-windows-msvc` | 静态 CRT |
| Windows arm64 | `aarch64-pc-windows-msvc` | 静态 CRT |

- TLS 一律 **rustls**(纯 Rust),不用 OpenSSL。
- Linux musl 产出无运行时依赖的静态二进制(含 SQLite 时,SQLite 的 C 在构建期静态编入)。
- 工具链用 **mise** 统一 Rust 版本与 targets,并作 task runner;**无 C 依赖**的 target 直接 `cargo build --target`;含 C 编译(SQLite/可选 native)的 target 用 `cross`(容器内置 C 工具链)。

### 12.2 Feature 策略(默认自包含,需运行时外部依赖者 opt-in)

设计修正:**Kafka 只有一个 `kafka` feature**(决定"是否编入 Kafka 适配器");用哪个 backend 由 adapter 内部的**互斥** backend feature 决定,默认 `rskafka`(自包含),`kafka-native` 切换为 librdkafka。`full` 不再直接写后端,避免与 `full-native` 冲突。

```toml
[features]
default = ["sql", "redis", "mongodb"]        # 自包含;SQLite 构建期编 C,运行期无依赖

# —— 能力 feature ——
sql = ["mysql","postgres","sqlite"]          # 同一 adapter-sql crate
redis = []  ; mongodb = []  ; search = []  ; timeseries = []  ; cassandra = []  ; mssql = []
nats = []   ; amqp = []     ; pulsar = []    ; mqtt = []
mq = ["nats","amqp","redis"]                 # 自包含消息组(不含 kafka)
kafka = ["dep:rskafka"]                      # 编入 Kafka 适配器,默认 rskafka backend(自包含,功能受限)

# —— backend 切换(adapter 内 cfg 互斥;仅在已启用 kafka 时有意义)——
kafka-native = ["kafka", "dep:rdkafka"]      # 切换为 librdkafka:完整功能,但需运行期/重型构建 C 依赖
oracle = ["dep:oracle"]                      # Oracle:需 Instant Client(运行期外部依赖)

# —— 预设组合 ——
full        = ["sql","redis","mongodb","search","timeseries","cassandra","mssql","mq","kafka","pulsar","mqtt"]
full-native = ["full","kafka-native","oracle"]   # 在 full 基础上,把 kafka 切到 native 并加 Oracle
```

adapter-kafka 内部按 backend 选择(互斥,二者择一):
```rust
#[cfg(feature = "kafka-native")]
mod backend { /* 用 rdkafka(librdkafka) */ }
#[cfg(not(feature = "kafka-native"))]   // 默认
mod backend { /* 用 rskafka(自包含,功能受限) */ }
```

> `full` 与 `full-native` 不再冲突:`full` 用默认 rskafka backend;`full-native` 叠加 `kafka-native` 后,cfg 优先选中 rdkafka backend(同一 `adapter_kafka::factory`,注册处无感)。

对外:npm/pip/uv/mise 默认分发 **`full`(自包含单文件)**;另提供 **`full-native`** 包给需要完整 Kafka(事务/精确一次)或 Oracle 的用户——该包含 C 运行期依赖、平台覆盖略窄。

---

## 13. 分发(共享同一套编译产物)
```
GitHub Actions matrix 编译 → artifacts
   ├─ GitHub Release (dbtool-<triple>.tar.gz/.zip) ── 服务 mise(ubi) 与 install 脚本
   ├─ npm   : 主包 + 6 平台子包(os/cpu + optionalDependencies),bin 入口 exec 对应二进制
   ├─ pip/uv: maturin(bindings="bin")各平台 wheel
   └─ mise  : ubi:owner/dbtool 直接消费 Release asset
```
CI 关键:**先 build matrix 出 artifacts,后续打包 job 复用,绝不重复 `cargo build`。** macOS 需 codesign + notarize;Windows 建议签名。

---

## 14. Claude Code Skill 集成
`SKILL.md` 要点:触发场景(查库/看结构/调线上数据/看 Redis key/看 Kafka 消息/看消费滞后);**操作前先 `dbtool caps` 探能力**;给足每个子命令示例与输出格式;安全行为说明(只读默认、`--allow-write`、破坏性拦截、凭证走环境变量);JSON 结构与 `truncated`/`--limit` 说明。典型流程:`caps → schema → query → 解析 JSON 回答`。

---

## 15. 安全设计

只读默认;`sqlparser` 识别并拦截破坏性操作;默认 `--limit`(限行数);DSN 密码脱敏;连接级 `readonly` 一票否决;消费强制 `--max`/`--timeout`。**流控防过载(第 8.1)**:并发/速率上限保护 DB,获取许可与执行全程有超时上界、重试有上限,工具自身永不死等;DB 服务端 `statement_timeout`/`lock_timeout` 双保险防服务端挂起与锁等待。

### 15.1 破坏性操作确认(非交互,适配 Skill 自动化)

> 修正点:**不使用交互式 y/N 确认**——那会卡住 Claude 等自动化调用。改为**两段式 token 确认**,全程非交互、输出稳定 JSON。

分级:
- **普通写**(`INSERT`/`UPDATE … WHERE`/`SET`):`--allow-write` 即可放行。
- **破坏性操作**(`DROP`/`TRUNCATE`/无 `WHERE` 的 `DELETE`/`UPDATE`、`FLUSHALL`、删 topic 等):需**两段式确认**。

两段式流程(对 Claude 友好):
1. **第一次调用**(只带 `--allow-write`,不带 token):工具**不执行**,而是返回一个 `confirm_token`——它是 `hash(规范化语句 + 目标连接 + 影响面摘要)`,并附带"将影响什么"的预估:
   ```json
   { "ok": false,
     "error": { "code": "CONFIRM_REQUIRED",
       "message": "destructive op requires confirmation",
       "confirm_token": "9f2c…",
       "impact": { "op": "TRUNCATE", "target": "orders", "est_rows": 1200000 } } }
   ```
2. **第二次调用**:带 `--confirm 9f2c…` 重放同一语句。工具重新计算 token 比对一致才执行;不一致(语句/目标被改动)即拒绝。

要点:
- `confirm_token` 与具体语句、目标、影响面**绑定**,不能复用到别的破坏性操作,避免"一个 `--force` 放行一切"。
- 全程非交互、机器可解析,Claude 可在 SKILL.md 指引下完成"先看影响 → 再确认"的安全节奏。
- 仍可选 `--yes-i-understand-destructive` 用于明确的脚本场景(等价于跳过第一段),但默认推荐 token 两段式。
- 连接级 `readonly=true` 对以上一切一票否决。

---

## 16. 可扩展性:三层扩展模型(核心卖点)

新增一个系统的代价,按其性质分三档递增:

### 16.1 一层:协议兼容产品 —— 零代码
目标系统兼容已有协议(**AutoMQ/Redpanda → Kafka,TiDB → MySQL,Valkey → Redis**)。
**做法**:在组合根加一行 `r.register("automq", adapter_kafka::factory);`。
**改动范围**:仅注册表别名表。无新逻辑、无新依赖、不碰核心与其他适配器。

### 16.2 二层:新协议、已有能力 —— 加一个适配器 crate
目标是全新协议,但语义落在已有能力(如又一个文档库 → `DocumentStore`)。
**做法**:新建 `dbtool-adapter-xxx`(只依赖 `dbtool-core`),实现 `Connector` + 对应能力 trait + `as_xxx()` 访问器,注册 scheme。
**改动范围**:新增一个 crate + 一行注册。核心与其他适配器**零改动**。

### 16.3 三层:全新范式 —— 加一个能力 trait(加法)
出现现有能力无法表达的范式。
**做法**:在 `port/` 加一个新能力 trait,在 `Connector` 加一个默认 `None` 的 `as_newcap()` 访问器,CLI 加对应子命令。
**改动范围**:核心做**加法**;所有现存适配器因继承默认 `None` 而**无需改动**(开闭原则保持)。

> 这三层正是"模块清晰 + 可扩展"的具体兑现:**绝大多数新增落在一层/二层,核心几乎不动。**

---

## 17. 里程碑规划

| 阶段 | 目标 | 交付 |
|---|---|---|
| **P0a** | 可编译骨架 | workspace、L1 域模型、L2 端口(`Connector`+能力 trait)、`Registry`/`Dsn`、**一个 mock adapter** + **契约测试**(对端口的行为约定测试)、错误模型骨架。目标:`cargo build` 通过、契约测试跑通 |
| **P0b** | 服务能力 | 在 P0a 之上补 `resolver`(连接/环境变量解析)、`safety`(sqlparser + token 确认)、`throttle`(FlowControl + 重试预算)、`formatter`(JSON 信封)。目标:无真实 DB 也可单测全链路 |
| **P1** | SQL 族(最大价值) | adapter-sql:MySQL/Postgres/SQLite(+TiDB/Cockroach/Timescale 别名);`sql query/exec/tables/schema` |
| **P2** | CLI 打磨 + Skill | JSON 信封、安全护栏、命名连接、`caps`/`ping`;**SKILL.md + Claude Code 实测** |
| **P3** | KV + 文档 | adapter-redis(+Valkey 别名)、adapter-mongo |
| **P4** | TUI | ratatui + ConnectionManager(连接池) |
| **P5** | 消息(自包含) | NATS / RabbitMQ / Redis Streams;adapter-kafka(`rskafka`,覆盖 Kafka/**AutoMQ**/Redpanda,功能受限) |
| **P6** | 扩展 + 发布 | kafka-native(opt-in 完整 Kafka)、ES、InfluxDB、Cassandra、SQL Server;跨平台 CI + npm/pip/uv/mise |
| **P7** | 加固 | keyring、签名/公证、文档、性能与稳定性 |

排序原则:先跑通 SQL + Claude 集成拿核心价值,再按"自包含易做 → 需外部依赖难做"铺开,完整 Kafka 放靠后。

---

## 18. 关键风险与取舍

1. **运行时依赖 vs 完整 Kafka / Oracle(最重要)**:`rdkafka`(librdkafka)与 `oracle`(Instant Client)引入运行期外部依赖,违背"开箱即用"。对策:默认自包含(Kafka 走受限 `rskafka`,但**已覆盖 AutoMQ/Redpanda**),完整能力作 `full-native` 独立包(§12.2)。需拍板:默认是否接受 Kafka 功能受限。注:SQLite 虽编 C,但运行期自包含,不在此列(§2.1)。
2. **抽象边界泄漏**:各范式无法 100% 统一。对策:能力 trait 分治 + 每类保留原生逃生通道(`kv raw`、任意 SQL、`doc aggregate` 任意 pipeline)。
3. **一次性 CLI 语义**:消费必须有界;建连有延迟(交互式可接受),TUI 才用池。
4. **二进制体积**:全开变大。对策:feature 裁剪,提供最小核心与全功能两档。
5. **维护负担**:适配器多。对策:协议族复用 + 能力 trait 让新增多为加法 + 第三方适配器模板。
6. **平台细节**:macOS 公证、Windows arm64 工具链、musl 兼容性,需 CI 早期验证。

---

## 19. 开放问题(需确认)

1. **默认构建的 Kafka**:接受 `rskafka` 功能受限作默认(AutoMQ/Redpanda 同样受此影响),还是默认就要完整 Kafka(引入 C 依赖)?
2. **"TDB" 确指**:时序数据库(本文档假设)还是 TiDB(已被 MySQL 适配器覆盖)?
3. **Oracle 是否纳入**:纳入必依赖 Instant Client,建议 opt-in 或暂不支持。
4. **凭证存储**:仅环境变量/配置引用,还是需要 keyring 本地加密?
5. **首批 MQ 范围**:是否先做 Kafka 系(含 AutoMQ)+ RabbitMQ + NATS + Redis Streams,其余后置?
6. **TUI 是否需作为可嵌入组件库**(供第三方 TUI 内嵌),还是仅独立程序?
7. **适配器拆分时机**:P0–P3 用 core 内 feature 模块,何时拆为独立 adapter crate?
8. **跨进程全局限流**:是否需要跨多次 CLI 调用的全局 QPS(需本地共享计数/协调器),还是单进程内限流 + 串行调用已足够?

---

*文档结束 — 确认开放问题后即可进入 P0 骨架:`dbtool-core` 的 `Connector`/能力 trait 接口定稿、`Registry`/`Dsn`/`resolver`/`safety` 的实现,以及 Cargo 依赖与 feature 锁定(届时需核对各 crate 最新版本)。*
