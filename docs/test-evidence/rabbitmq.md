# RabbitMQ And AMQP Completeness Evidence

Task ID: DB-RABBITMQ-001

Result: LIVE_PASS

Run at (UTC): 2026-07-15T21:46:07Z (AMQP atomic-batch and public CLI refresh)

Environment: Docker on macOS arm64; Rust 1.96.0; single RabbitMQ node and isolated `dbtool_it` vhost

Product version: `rabbitmq:3.13-management-alpine`

Command: `./scripts/integration-mq-test.sh`; focused adapter and CLI commands shown below

Resource operations:

| Resource | Create/write | Read all fixture data | Metadata/lag | Guard/ack/bounds | Cleanup |
| --- | --- | --- | --- | --- | --- |
| `dbtool_it_amqp_queue_*` | queue declaration and two ordered payload publishes PASS | bounded `basic.get` returned both exact payloads and string headers PASS | passive detail reported name/count 2; portable list/consumer-group lag explicitly UNSUPPORTED PASS | explicit `--ack on-success` and write guard PASS; complete batch converted before one multiple ACK; max 2/timeout 5 PASS | queue drained and public confirmed AMQP queue delete verified absence PASS |
| `dbtool_it_amqp_atomic_ack_*` | valid payload followed by a direct fixture with a non-string header PASS | portable conversion rejected the malformed second delivery without exposing either payload/header value PASS | passive ready count returned to 2 after the consume channel closed PASS | first delivery was not ACKed early; both deliveries reappeared with `redelivered=true`; one fixture multiple ACK cleaned both PASS | isolated queue deleted and both connections closed PASS |
| `dbtool_it_rabbitmq_mgmt_queue_*` | AMQP declaration and one `rabbitmq-management-payload` publish PASS | exact payload consumed and acknowledged PASS | `rabbitmq+http` list and exact detail reported count 1 then 0; group lag correctly UNSUPPORTED PASS | management counts waited for a complete snapshot; missing/invalid/overflow fail closed; delete token bound kind/name/if-empty/if-unused PASS | `if-empty` delete acknowledged, subsequent detail was absent, final management queue list `[]` PASS |

Raw AMQP does not pretend to list queues or expose consumer-group lag. Queue
discovery and exact queue-depth detail are available only through the explicit
RabbitMQ management HTTP connector. Queue depth is not exposed as
consumer-group lag.

AMQP consume is deliberately state-mutating: callers must select
`ack=on-success`, negotiate `message.consume_ack`, and pass the shared write
guard. The adapter uses a call-owned channel and sends one multiple ACK only
after every delivery converts. A conversion error closes that channel and
requeues all unacknowledged deliveries only after close confirmation. Failure
to confirm close, or a local send error after ACK submission, returns the
non-retryable `OUTCOME_INDETERMINATE`; tests do not automatically retry
mutations. RabbitMQ 0.9.1 provides no ACK-of-ACK, and the CLI can still fail
after ACK but before observable formatted output, so no exactly-once or strict
end-to-end at-least-once output guarantee is claimed.

## IF-T74 queue scalar-byte envelope refresh

Run at (UTC): 2026-07-16T03:24:39Z

The `rabbitmq+http://` management adapter now advertises and implements exact
`message.admin.list_topics_budgeted`. It validates `ReadBudget` before HTTP,
retains stable server pagination, converts only the remaining N+1 queue
objects, accounts every complete `TopicInfo`, then validates the complete
`BoundedList` plus probe. The independent 1 MiB per-page transport ceiling
remains in force.

RabbitMQ 3.13 Docker validation declared two isolated queues, proved N+1
truncation and a one-byte budget failure, deleted both through AMQP, and
confirmed the management queue list was empty. Direct AMQP 0.9.1 continues to
advertise neither item-bounded nor byte-budgeted global queue listing because
that protocol has no portable discovery operation.

Verification: adapter-amqp 24 unit + 3 integration PASS; strict all-target
Clippy, rustfmt and diff check PASS; RabbitMQ management live catalog and empty
cleanup PASS.

## IF-T77 producer input envelope refresh

Run date: 2026-07-16

AMQP now advertises exact `message.produce_budgeted`. Before creating a channel
or declaring a queue, it validates the complete `ProduceBudget`, every portable
Message, the complete Vec envelope, the queue name, prebuilt BasicProperties/
field tables, and fixed-width header constraints for the entire batch. Body
segmentation remains lapin's responsibility. Offline tests passed
the exact per-message/batch N boundary and rejected N-1, N+1 messages, and a
valid first item followed by an invalid item before declaration.

RabbitMQ Docker 4/4 passed: an oversized request left the passive-probed queue
absent; the exact budget declared the queue and published one payload; direct
`basic.get` read back the exact bytes and ACKed them; deletion succeeded and
the final management queue inventory was empty. Adapter verification was 27
tests plus strict Clippy.

Queue declaration is itself a remote mutation, so declaration, publish, or
publisher-confirm failures after that boundary return non-retryable
`OUTCOME_INDETERMINATE`. AMQP 0.9.1 has no confirm-of-confirm. A broker policy
may also impose a payload/frame ceiling below dbtool's portable 16 MiB limit;
that remote rejection cannot be claimed as a preflight zero-write result.

Focused reproduction:

```text
DBTOOL_RUN_MQ_INTEGRATION=1 \
DBTOOL_IT_AMQP_DSN=amqp://... \
  cargo test -p adapter-amqp --test live_stateful -- --nocapture

DBTOOL_RUN_MQ_INTEGRATION=1 \
DBTOOL_IT_AMQP_DSN=amqp://... \
  cargo test -p dbtool-cli --features full --test live_messaging \
  amqp_live_queue_produce_detail_and_consume -- --exact --nocapture
```

Cleanup: PASS; focused management API final queue list was `[]`

Commits: `e24fb79`, `acff12b`, `1279cbd`, `d2c88a2`, `9a813d8`, `d580664`,
`d69f866`, IF-T47, IF-T59
