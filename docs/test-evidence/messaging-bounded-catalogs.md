# Messaging 顶层目录有界化测试证据

状态：PASS（2026-07-16）

## 合同

- CLI `mq topics` 使用全局 `--limit`，在解析 DSN 或建立连接前拒绝 `0` 和无法保留 N+1 探针位的上限。
- 连接建立后必须协商 `message.admin.list_topics_bounded`；缺少该 operation 时返回 `UNSUPPORTED_CAPABILITY`，不得回退到旧的全量 `list_topics`。
- 成功响应最多返回 N 项。只有后端观察到第 N+1 项时，`meta.truncated` 才为 `true`。
- `caps.data.operations` 必须明确列出 `message.admin.list_topics_bounded`；旧的 `admin=true` 或 `message.admin.list_topics` 不会隐式授予该能力。

## 后端边界

| 后端 | 有界读取方法 | 完整性说明 |
| --- | --- | --- |
| Redis / Valkey / KeyDB / Dragonfly | 每页通过只读 Lua 包装执行 `SCAN TYPE stream COUNT min(剩余探针, 100)`，跨页按名称去重，收集到 N+1 个唯一 stream 即停止 | `COUNT` 是提示值；Lua 在响应传输前强制最多 4096 个 key、key 总字节最多 896 KiB，超限或 cursor 重复均失败关闭 |
| NATS JetStream | 消费 `streams()` offset 分页迭代器，可移植结果最多保留 N+1 项 | `async-nats` 会整页解码；重复 stream 失败关闭，不会为了排序继续耗尽后续页 |
| RabbitMQ management | `/api/queues/{vhost}` 显式传入 `page`、`page_size`、`pagination=true` 和稳定名称排序，逐页读取至 N+1 | 每页受 1 MiB HTTP 响应上限保护；页号、页大小、页数、重复队列及非末页短页均严格校验 |
| Kafka pure (`rskafka`) | Kafka Metadata API 无分页；独立 catalog client 用 `max_message_size` 将 broker 响应帧硬限制为 16 MiB，再只保留 N+1 个 topic | 普通生产/消费 client 保持原有 100 MiB 默认接收能力；超过目录帧预算时仅目录请求失败关闭 |
| Kafka native (`librdkafka`) | 独立 catalog consumer 强制 `receive.message.max.bytes=16777216` 及 `fetch.max.bytes=16776704`，再只保留 N+1 个 topic | 普通 producer/consumer 保留原 DSN/默认预算；DSN 不能放大目录 consumer 的预算 |

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
- RabbitMQ adapter：20/20。
- Redis adapter：34/34。
- NATS adapter：11/11。
- Kafka pure adapter：14/14。
- Kafka native adapter：22/22。

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

结果：pure Kafka + Redis + NATS + RabbitMQ 为 4/4；同一 Kafka 用例切换 `dbtool-registry/kafka-native` 后为 1/1。

Redis 协议兼容复核：同一公共 `mq topics --limit 2` 路径在 Valkey
8.1.8、KeyDB 6.3.4、Dragonfly 1.39.0 Docker 服务上均返回成功、空目录且
`meta.truncated=false`，证明只读页脚本没有破坏现有三种兼容 scheme。

该测试已接入 `scripts/integration-mq-test.sh`；`scripts/integration-mq-native-test.sh` 也会用 `full-native` 重新执行同一目录合同，覆盖 librdkafka 的真实 Docker 路径。

每个测试执行相同闭环：读取未截断基线，创建第一个持久资源，使用 `limit=基线+1` 验证恰好 N 项且 `truncated=false`；创建第二个资源，使用同一 limit 验证只返回 N 项且 `truncated=true`；随后通过各后端的真实删除路径清理，并等待目录恢复到基线。每个用例还在创建前安装 failure-safe cleanup guard；中途断言失败时，守卫先查目录确认资源是否存在，再在十秒截止时间内重试挑战/确认删除。目录截断不能证明资源不存在；首次未发现后还必须保持两秒稳定缺席，覆盖刚创建但尚未传播到 RabbitMQ/Kafka/NATS 目录的窗口。这也覆盖 RabbitMQ 管理计数尚未就绪的删除窗口，并避免把失败制品永久并入后续基线。

清理复核：Redis、NATS、RabbitMQ 测试目录为空；Kafka 保留一个此前存在的 `dbtool_it_kafka_topic_*`，本测试专用 `dbtool_it_bounded_*` topic 为零残留。
