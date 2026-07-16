# Message Produce Input Budget And Outcome Evidence

Task ID: IF-T77

Result: PASS for core/CLI, AMQP/RabbitMQ, NATS, pure Kafka, native Kafka, and Redis 7

Run date: 2026-07-16

Environment: macOS arm64; Rust 1.96.0; Docker RabbitMQ 3.13, NATS 2.10,
Redpanda 24.3.6, and Redis 7.4.9

## Frozen contract

`ProduceBudget(max_messages,max_message_bytes,max_batch_bytes)` is the
caller-owned input envelope for `MessageProducer::produce_budgeted`. The exact
operation name is `message.produce_budgeted`; a legacy `producer=true` or
`message.produce` capability never authorizes this method. Defaults are 100
messages, 8 MiB per complete portable `Message`, and 8 MiB for the complete
`Vec<Message>` envelope. The hard limits are 100,000 messages and 16 MiB for
each byte dimension.

`MessageWriteLimiter` rejects an empty batch, N+1 messages, one complete
message over its limit, or the complete batch over its limit. It uses compact
JSON counting so every public field participates: payload, key, headers,
partition, offset, timestamp, cursor, and metadata. Every adapter must validate
the budget, non-empty/count, every complete message, the complete batch, target,
and protocol-specific fields before it creates a resource/client/channel or
attempts the first send. The order among those pure checks is intentionally not
part of the public contract.

The exact method rejects an empty batch with `CONFIG_ERROR`. For 0.x embedded
compatibility, all four protocol adapters keep legacy `produce` as a no-op for
an empty batch after validating the target (`produced=0`); non-empty legacy
calls use the same finite default budget. New callers must negotiate and call
the exact method, and must not treat the compatibility no-op as authorization
for an empty write.

An exact N-byte envelope succeeds and N-1 returns
`INPUT_BUDGET_EXCEEDED` before remote mutation. Once an operation may have
created a resource or submitted a write, any later error is non-retryable
`OUTCOME_INDETERMINATE`; callers must inspect remote state before deciding
whether to compensate. CLI request/deadline timeout after message production
begins follows the same rule. Rate/concurrency admission failures happen
before execution and retain `RATE_LIMITED`/`OVERLOADED`.

## Verification matrix

| Layer / protocol | Exact N and N-1 | Oversize zero-write proof | Successful readback | Cleanup | Result |
| --- | --- | --- | --- | --- | --- |
| Core + CLI | unit tests measured exact message/batch bytes and one-byte-short limits; CLI also rejected per-message and batch input before DSN resolution | mock connector was not reached on invalid budget; empty and N+1 batches fail closed | exact mock production used only `message.produce_budgeted` | service-free | core 145, CLI 110, `mq_cli` 11 PASS; check, strict Clippy, fmt and diff checks PASS |
| RabbitMQ / AMQP | unit tests covered exact per-message/batch N, N-1, count and a late invalid item; live exact publish succeeded | one-byte/oversize request did not declare the passive-probed queue | exact payload was retrieved with `basic.get` and ACKed | isolated queue deleted; final management queue inventory empty | adapter 27 tests; Docker 4/4 PASS |
| NATS | unit tests covered portable message/batch N/N-1 plus exact HPUB wire bytes and server `max_payload`; live exact publish succeeded | precreated JetStream remained at zero messages after rejected Core NATS publish | retained payload matched exactly through JetStream raw-message read | isolated stream deleted; final `streams=consumers=messages=bytes=0` | adapter 20 tests; Docker 3/3 PASS |
| Kafka pure (`rskafka`) | exact portable message/batch budget succeeded; one-byte-short message budget failed | rejected target topic was absent after preflight failure | produced record was consumed from partition 0 with exact payload | topic and auxiliary catalog topic created by the run were deleted | Redpanda `live_bounded` 1/1 PASS |
| Kafka native (`librdkafka`) | two-message exact batch succeeded; one-byte-short maximum-message budget failed | rejected target topic was absent after preflight failure | broker latest offset/committed-offset lag proved both records reached the expected partition | topics created by this native run were deleted | Redpanda exact live 1/1 PASS |
| Redis Streams | exact message/batch budget succeeded; one-byte-short message budget failed | rejected Stream remained `TYPE none`, proving no XADD and no implicit Stream creation | payload, key, header, and native Stream ID were read back exactly | accepted/rejected names deleted; final `dbtool_it_produce_*` scan empty | Redis 7 exact live 1/1 PASS |

The first Redis live attempt failed because the assertion did not initially
allow the adapter-added lossless `redis_stream_id` header. The expectation was
corrected, the failed-run Stream was deleted, the exact test passed, and the
final prefix scan was empty. This is recorded rather than hiding the failed
attempt.

One pre-existing Redpanda topic named
`dbtool_it_kafka_topic_16721_1784144037118` was outside the current run's
resource names. The IF-T77 tests deleted every topic they created but did not
delete that historical resource; cleanup evidence is therefore scoped to the
current test run rather than misstated as a globally empty broker.

The final campaign-wide residue audit later deleted that historical test topic
through dbtool's public target-bound confirmation path. Redpanda then reported
zero `dbtool_it_*` topics; see
[`final-residue-audit.md`](final-residue-audit.md).

One root NATS rerun exposed the documented distinction between Core NATS
flush and JetStream PubAck: stream depth was transiently zero and became one
shortly afterward. The live assertion now polls inside a fixed two-second
deadline. The failed-run stream was deleted through the public CLI with a
target-bound confirmation token before the 3/3 rerun and final zero-state
check.

## Protocol boundaries

- Kafka topic creation and partition batches can succeed before a later
  partition/delivery failure. Broker/topic `message.max.bytes` may be lower
  than dbtool's portable 16 MiB ceiling and cannot be atomically frozen during
  preflight; rejection after submit is `OUTCOME_INDETERMINATE`.
- AMQP queue declaration is itself a mutation. All portable and AMQP field
  validation that dbtool can perform occurs before declaration; lapin remains
  responsible for body framing and a broker may enforce a lower policy.
  Publisher confirms are enabled, and declaration/publish/confirm failures
  after that boundary are indeterminate. AMQP 0.9.1 has no ACK-of-ACK and no
  exactly-once claim.
- Core NATS validates subject, headers, HPUB body, and advertised
  `INFO.max_payload` before publish. Its flush is not a per-message JetStream
  publish acknowledgement, and a custom server policy such as a separately
  lowered maximum control line may not be exposed through INFO.
- Redis Streams validates the full batch before the first XADD. Pub/Sub accepts
  payload only and is ephemeral, so persistent catalog inspection cannot prove
  whether a subscriber received a publish. Any failure after XADD/PUBLISH may
  have written an earlier item and is indeterminate.

## Focused reproduction

```text
cargo test -p dbtool-core
cargo test -p dbtool-cli
cargo test -p dbtool-cli --test mq_cli
cargo test -p adapter-amqp --all-targets
cargo test -p adapter-nats --all-targets
cargo test -p adapter-kafka --all-targets
cargo test -p adapter-kafka --no-default-features --features backend-native --all-targets
cargo test -p adapter-redis --all-targets

DBTOOL_RUN_MQ_INTEGRATION=1 DBTOOL_IT_AMQP_DSN=amqp://... \
  DBTOOL_IT_RABBITMQ_MANAGEMENT_DSN=rabbitmq+http://... \
  cargo test -p adapter-amqp --test live_stateful -- --nocapture
DBTOOL_IT_NATS_DSN=nats://... \
  cargo test -p adapter-nats --test live_stateful -- --nocapture
DBTOOL_RUN_MQ_INTEGRATION=1 DBTOOL_IT_KAFKA_DSN=kafka://... \
  cargo test -p adapter-kafka --test live_bounded -- --nocapture
DBTOOL_RUN_KAFKA_NATIVE_LIVE=1 DBTOOL_IT_KAFKA_DSN=kafka://... \
  cargo test -p adapter-kafka --no-default-features --features backend-native \
  backend::rdkafka_backend::tests::live_consumer_lag_reads_a_real_committed_group_offset \
  -- --exact --nocapture
DBTOOL_IT_REDIS_DSN=redis://... \
  cargo test -p adapter-redis redis_live_budgeted_produce_rejects_before_xadd_and_reads_back \
  -- --exact --nocapture
```

Implementation commits recorded for this slice: `d2dee2b` (core/CLI),
`9a813d8`, `d580664`, and `d69f866` (AMQP), `317829e` and `9c0a273` (NATS),
`de6b79e` (Kafka pure/native), and `7540ff3` (Redis). This cross-protocol
evidence is committed separately in the parent task's Lore sequence.
