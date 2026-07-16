# Kafka API On Redpanda Completeness Evidence

Task ID: DB-KAFKA-001

Result: LIVE_PASS

Run at (UTC): 2026-07-14T20:45:08Z

Latest field-fidelity rerun (UTC): 2026-07-15T17:34:19Z

Latest native group lifecycle rerun (UTC): 2026-07-15T20:48:15Z

Environment: Docker on macOS arm64; Rust 1.96.0; Redpanda single broker

Product version: Redpanda v24.3.6; pure `rskafka` and native `librdkafka` backends

Command: `./scripts/integration-mq-test.sh`; `./scripts/integration-mq-native-test.sh`

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/admin | Guard/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool_it_kafka_topic_*` through `kafka://` | broker topic creation and one record with key, two headers, partition 0 and exact epoch-ms timestamp PASS | bounded consume from the returned placement offset reproduced payload, key, headers, partition, offset and timestamp PASS | topic list/detail/watermarks and native committed-offset lag PASS | produce without permission blocked; positive max/timeout and exact position validation PASS | target-bound public topic delete acknowledged and absence verified PASS |
| `dbtool_it_redpanda_topic_*` through `redpanda://` | independent topic with the same complete message metadata PASS | exact field-by-field read through the product-named scheme PASS | connector kind `redpanda`, list, detail, and watermarks PASS | same write, validation and read bounds PASS | target-bound public topic delete and zero residual topic PASS |
| `dbtool_it_native_group_*` through native `kafka://` | explicit two-partition topic and two messages per partition PASS | same group with `ack none` returned all four messages twice; `ack on-success` then returned and committed all four PASS | before ACK no committed entries existed; after ACK both partitions reported committed sum 4, latest sum 4, lag sum 0 PASS | static member rejected; durable/pure group operations not advertised; per-partition commit offset is highest observed + 1 PASS | public delete acknowledged, absence verified, final topic list contained no test topic PASS |

The field-fidelity resources passed with both the default pure Rust backend and
`full-native`. Native configuration warnings no longer pollute JSON errors.
Consumer-group and ACK operations are advertised only by `full-native`; pure
Kafka still returns `UNSUPPORTED_CAPABILITY` rather than a misleading empty
success.

The latest rerun executed both exact tests separately with `portable` and
`full-native`. Producer placements were used as the consumer start offsets, so
the assertions did not depend on a new topic always starting at offset zero.
CLI validation tests also proved malformed/duplicate/whitespace header names,
negative partition/offset, zero max/timeout, and platform-overflowing timeout
values fail before any broker connection.

The native group lifecycle also proves commit happens only after the complete
portable batch has been formed. Null/tombstone payloads plus null, duplicate, or
non-UTF-8 headers are rejected before a commit because the public Message model
cannot represent them losslessly. The contract is at-least-once and does not
claim exactly-once behavior.

## IF-T74 topic scalar-byte envelope refresh

Run at (UTC): 2026-07-16T03:24:39Z

Both pure `rskafka` and native `librdkafka` backends now advertise and implement
`message.admin.list_topics_budgeted`. `ReadBudget` is validated before a
catalog client or metadata request is created; no more than N+1 complete
`TopicInfo` values are constructed and retained, and the final `BoundedList`
plus probe is byte-accounted. Item N/N+1 and exact byte N/N-1 boundaries passed
for both implementations.

Kafka Metadata has no pagination. The dedicated pure/native catalog clients
therefore retain their independent 16 MiB receive-frame ceiling; this is a
protocol memory bound, not a claim that the broker scans only N+1 topics. The
Redpanda Docker live test created an auxiliary catalog-probe topic, passed the
exact boundaries, and deleted all topics created by this run. One unrelated
pre-existing `dbtool_it_kafka_topic_*` was deliberately not deleted.

Verification: Kafka pure 18 unit + 1 integration PASS; native 26 unit PASS;
strict Clippy for both feature sets, rustfmt and diff check PASS; Redpanda live
exact catalog and cleanup PASS.

## IF-T77 producer input envelope refresh

Run date: 2026-07-16

Both pure `rskafka` and native `librdkafka` advertise exact
`message.produce_budgeted`. They validate non-empty/count, every complete
Message, complete batch bytes, topic, headers, partition/timestamp and the
broker-owned offset rule before topic creation or the first record send.
Offline preflight passed exact N and rejected per-message/batch N-1, N+1
messages, NUL headers, and caller-supplied offsets.

The pure Redpanda `live_bounded` test passed 1/1: a one-byte-short request did
not create its target topic, an exact request produced and consumed the exact
partition-0 payload, and the topic plus auxiliary catalog topic were deleted.
The native exact live test passed 1/1: a two-message one-byte-short request did
not create its target topic, the exact batch produced successfully, committed
offset/high-watermark lag proved the expected partition contents, and every
topic created by that run was deleted.

One old topic, `dbtool_it_kafka_topic_16721_1784144037118`, existed before this
IF-T77 run and was not one of its resource names. The test correctly avoided
deleting it; cleanup PASS means all current-run topics were removed, not that
the shared broker was globally empty.

Final campaign cleanup (UTC 2026-07-16T13:51:26Z): the historical name was
subsequently treated as an explicitly disposable `dbtool_it_*` resource and
deleted through `dbtool mq delete --kind kafka-topic` with a target-bound
confirmation token. A final `rpk topic list` contained no test topic. This does
not retroactively weaken the earlier test's ownership-scoped cleanup rule.

Kafka topic creation and per-partition append are separate remote mutations.
After either may have started, create/send/delivery/placement failures are
non-retryable `OUTCOME_INDETERMINATE`. Broker or topic
`message.max.bytes` can be lower than dbtool's portable 16 MiB ceiling and
cannot be atomically frozen during preflight, so a post-submit broker rejection
is not described as zero-write.

Cleanup: PASS; every topic created by the exact lifecycle and the one historical
test topic were deleted through the public API; final broker test-topic count 0.

Commits: `e24fb79`, `85c7954`, `acff12b`, `1279cbd`, `300e94b`, `d2c88a2`,
`de6b79e`
