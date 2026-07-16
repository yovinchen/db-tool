# Messaging 顶层目录有界化测试证据

状态：PASS（2026-07-16）

范围：IF-T66 顶层目录 item 边界与 IF-T74 完整标量/结构字节信封。

## 合同

- CLI `mq topics` 同时使用全局 `--limit` 与 `--max-bytes`，在解析 DSN 或建立连接前拒绝无效 item/byte 预算。
- 连接建立后必须协商 `message.admin.list_topics_budgeted`；缺少该 operation 时返回 `UNSUPPORTED_CAPABILITY`，不得回退到旧的全量 `list_topics` 或 item-only `list_topics_bounded`。
- 成功响应最多返回 N 项。只有后端观察到第 N+1 项时，`meta.truncated` 才为 `true`。
- 每个完整 `TopicInfo` 在 retention 前计费；返回前再校验完整 `BoundedList` 和唯一 probe。exact byte N 成功，N-1 返回 `READ_BUDGET_EXCEEDED`。
- `caps.data.operations` 必须明确列出 `message.admin.list_topics_budgeted`；旧的 `admin=true`、`message.admin.list_topics` 或 `message.admin.list_topics_bounded` 不会隐式授予该能力。

## 后端边界

| 后端 | 有界读取方法 | 完整性说明 |
| --- | --- | --- |
| Redis / Valkey / KeyDB / Dragonfly | 只读 Lua `SCAN TYPE stream` 页 + visitor；跨页去重，达到 N+1 即停止，每个完整 `TopicInfo` 先计费 | `COUNT` 是提示；Lua 在传输前强制最多 4096 个 key、key 总字节最多 896 KiB；只枚举 Streams，不枚举 Pub/Sub |
| NATS JetStream | `streams()` 分页迭代器只拉到 N+1，逐项计费后停止 | `async-nats` 可能整页解码；只枚举 JetStream，不枚举 Core subject；Core-only 服务端运行时明确报 unavailable |
| RabbitMQ management | 稳定名称排序分页，最终页只转换剩余 N+1 `TopicInfo` | 每页 1 MiB HTTP 上限；页号、页大小、页数、重复队列及非末页短页严格校验；Direct AMQP 不广告目录 |
| Kafka pure (`rskafka`) | 独立 catalog client 将 metadata frame 限为 16 MiB，lazy map 只构造 N+1 个 `TopicInfo` | 普通生产/消费 client 保持原有 100 MiB 默认；Metadata API 无分页，不能声称 broker 只扫描 N+1 |
| Kafka native (`librdkafka`) | 独立 catalog consumer 强制 `receive.message.max.bytes=16777216` / `fetch.max.bytes=16776704`，只构造 N+1 | 普通 producer/consumer 不受影响；DSN 不能放大目录 consumer 的预算 |

Kafka 的 16 MiB 是独立目录客户端的协议解码内存上限，不是服务端分页。Kafka Metadata API 不能按 topic 数量请求第一页，因此该后端的保证是“有界响应帧 + N+1 可移植结果”，不是“broker 只扫描 N+1 个 topic”。Redis 的 `COUNT` 同样不是服务器硬上限，所以用 Lua 在服务端将单页结果变为硬限额响应；NATS 客户端仍按服务器页解码。三者都在可移植结果达到 N+1 后停止，但只有 Kafka/Redis 在适配器侧建立了明确字节预算，NATS 不能声称网络层严格只接收 N+1 项。

## 自动测试

静态与单元测试：

```text
cargo check -p adapter-redis -p adapter-nats -p adapter-amqp -p adapter-kafka -p dbtool-cli
cargo check -p adapter-kafka --no-default-features --features backend-native
cargo test -p dbtool-cli --test mq_cli
cargo test -p adapter-amqp -p adapter-redis -p adapter-nats -p adapter-kafka --lib
cargo test -p adapter-kafka --no-default-features --features backend-native --lib
```

结果：

- CLI 前置校验：10/10；覆盖 `--limit 0`、`usize::MAX`，均在不可达 DSN 连接前返回配置错误。
- RabbitMQ adapter：24 unit + 3 integration。
- Redis adapter：47/47。
- NATS adapter：17 unit + 2 integration。
- Kafka pure adapter：18 unit + 1 integration。
- Kafka native adapter：26/26。

Docker N/N+1 测试命令使用独立构建目录与仅 Messaging 功能组合，避免其他功能分支影响二进制：

```text
CARGO_TARGET_DIR=/tmp/dbtool-bounded-mq-target \
DBTOOL_RUN_MQ_INTEGRATION=1 \
DBTOOL_IT_REDIS_DSN=redis://127.0.0.1:16379/0 \
DBTOOL_IT_KAFKA_DSN=kafka://127.0.0.1:19092 \
DBTOOL_IT_AMQP_DSN='amqp://dbtool:***@127.0.0.1:15672/dbtool_it' \
DBTOOL_IT_RABBITMQ_MANAGEMENT_DSN='rabbitmq+http://dbtool:***@127.0.0.1:15673/dbtool_it' \
DBTOOL_IT_NATS_DSN=nats://127.0.0.1:14222 \
cargo test -p dbtool-cli --no-default-features \
  --features dbtool-registry/mq,dbtool-registry/kafka \
  --test bounded_messaging -- --test-threads=1 --nocapture
```

结果：pure Kafka + Redis + NATS + RabbitMQ 的 item-only 公共 CLI 生命周期仍为 4/4；同一 Kafka 用例切换 `dbtool-registry/kafka-native` 后为 1/1。本轮 IF-T74 adapter live 另行验证所有四族 exact item N/N+1 与 byte N/N-1：Redpanda pure、Redis 7、RabbitMQ 3.13、NATS 2.10 均 PASS；native Kafka 的同一 limiter/operation 与 feature-specific 单测、Clippy PASS。

Redis 协议兼容复核：同一公共 `mq topics --limit 2` 路径在 Valkey
8.1.8、KeyDB 6.3.4、Dragonfly 1.39.0 Docker 服务上均返回成功、空目录且
`meta.truncated=false`，证明只读页脚本没有破坏现有三种兼容 scheme。

该测试已接入 `scripts/integration-mq-test.sh`；`scripts/integration-mq-native-test.sh` 也会用 `full-native` 重新执行同一目录合同，覆盖 librdkafka 的真实 Docker 路径。

每个测试执行相同闭环：读取未截断基线，创建第一个持久资源，使用 `limit=基线+1` 验证恰好 N 项且 `truncated=false`；创建第二个资源，使用同一 limit 验证只返回 N 项且 `truncated=true`；随后通过各后端的真实删除路径清理，并等待目录恢复到基线。每个用例还在创建前安装 failure-safe cleanup guard；中途断言失败时，守卫先查目录确认资源是否存在，再在十秒截止时间内重试挑战/确认删除。目录截断不能证明资源不存在；首次未发现后还必须保持两秒稳定缺席，覆盖刚创建但尚未传播到 RabbitMQ/Kafka/NATS 目录的窗口。这也覆盖 RabbitMQ 管理计数尚未就绪的删除窗口，并避免把失败制品永久并入后续基线。

清理复核：Redis DB14 `DBSIZE=0`；NATS `streams=consumers=messages=0`；RabbitMQ 管理队列为空；Kafka 本轮创建的 catalog probe topic 全部删除。Kafka 容器中保留一个其它任务既有 `dbtool_it_kafka_topic_*`，未越权删除，也未计为本轮残留。
