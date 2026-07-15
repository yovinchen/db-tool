# Kafka API On Redpanda Completeness Evidence

Task ID: DB-KAFKA-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:45:08Z

Latest field-fidelity rerun (UTC): 2026-07-15T17:34:19Z

Environment: Docker on macOS arm64; Rust 1.96.0; Redpanda single broker

Product version: Redpanda v24.3.6; pure `rskafka` and native `librdkafka` backends

Command: `./scripts/integration-mq-test.sh`; `./scripts/integration-mq-native-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool_it_kafka_topic_*` through `kafka://` | broker topic creation and one record with key, two headers, partition 0 and exact epoch-ms timestamp PASS | bounded consume from the returned placement offset reproduced payload, key, headers, partition, offset and timestamp PASS | topic list and detail name PASS; low watermark 0 and high watermark at least 1 PASS | produce without permission blocked; positive max 1/timeout 5; negative position and overflowing timeout rejected before connection PASS | public topic delete UNSUPPORTED; disposable broker volume is the current cleanup boundary |
| `dbtool_it_redpanda_topic_*` through `redpanda://` | independent topic with the same complete message metadata PASS | exact field-by-field read through the product-named scheme PASS | connector kind `redpanda`, list, detail, and watermarks PASS | same write, validation and read bounds PASS | public topic delete UNSUPPORTED; disposable broker volume is the current cleanup boundary |

Both resources passed once with the default pure Rust backend and once with
`full-native`. Native configuration warnings no longer pollute JSON errors.
Consumer-group lag is explicitly `UNSUPPORTED_CAPABILITY`; dbtool does not
return a misleading successful empty list and does not claim offset commits.

The latest rerun executed both exact tests separately with `portable` and
`full-native`. Producer placements were used as the consumer start offsets, so
the assertions did not depend on a new topic always starting at offset zero.
CLI validation tests also proved malformed/duplicate/whitespace header names,
negative partition/offset, zero max/timeout, and platform-overflowing timeout
values fail before any broker connection.

Cleanup: DEFERRED to disposable Docker volume; protocol-level topic delete remains IF-T47

Commits: `e24fb79`, `85c7954`, `acff12b`, `1279cbd`, `300e94b`, `d2c88a2`
