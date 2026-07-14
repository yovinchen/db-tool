# Kafka API On Redpanda Completeness Evidence

Task ID: DB-KAFKA-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:45:08Z

Environment: Docker on macOS arm64; Rust 1.96.0; Redpanda single broker

Product version: Redpanda v24.3.6; pure `rskafka` and native `librdkafka` backends

Command: `./scripts/integration-mq-test.sh`; `./scripts/integration-mq-native-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool_it_kafka_topic_*` through `kafka://` | broker topic creation and one `kafka-payload` record PASS | bounded consume returned the exact payload PASS | topic list and detail name PASS; low watermark 0 and high watermark at least 1 PASS | produce without permission blocked; max 1/timeout 5 PASS | public topic delete UNSUPPORTED; disposable broker volume removed PASS |
| `dbtool_it_redpanda_topic_*` through `redpanda://` | independent topic and one `redpanda-payload` record PASS | exact payload read through the product-named scheme PASS | connector kind `redpanda`, list, detail, and watermarks PASS | same write and read bounds PASS | public topic delete UNSUPPORTED; disposable broker volume removed PASS |

Both resources passed once with the default pure Rust backend and once with
`full-native`. Native configuration warnings no longer pollute JSON errors.
Consumer-group lag is explicitly `UNSUPPORTED_CAPABILITY`; dbtool does not
return a misleading successful empty list and does not claim offset commits.

Cleanup: PASS by disposable Docker volume; protocol-level topic delete is UNSUPPORTED

Commits: `e24fb79`, `85c7954`, `acff12b`, `1279cbd`, `300e94b`, this commit
