# Message Consume Byte Budget And ACK Evidence

Task ID: IF-T72

Result: LIVE_PASS

Run at (UTC): 2026-07-16

Environment: Docker RabbitMQ 3.13, Redis 7, Redpanda Kafka API, NATS 2.10 JetStream

## Contract

`ConsumeOptions.max_message_bytes` limits one complete portable Message. `max_batch_bytes` limits
the complete returned `Vec<Message>`. Both default to 8 MiB and are restricted to 1..=16 MiB. The
message charge includes payload, key, headers, cursor, metadata, partition, offset and timestamp.

CLI mapping:

- `mq consume --max-message-bytes N` -> single-message budget;
- global `--max-bytes N` -> complete batch budget.

Adapters must finish both checks before their first acknowledgement action.

## Protocol results

| Protocol | Budget failure evidence |
| --- | --- |
| AMQP | complete batch NACK/requeue before multiple ACK; replay marked redelivered |
| Redis Streams | entry remains in PEL before XACK; larger-budget replay succeeds and then XACKs |
| native Kafka | no group offset committed; later consume can replay |
| pure Kafka | no broker progress is committed; larger-budget stateless read succeeds |
| NATS JetStream | durable ACK floor remains zero before double-ack |

Pure/native Kafka consumer receive, aggregate fetch and per-partition fetch ceilings are frozen to
the adapter's 16 MiB response ceiling after DSN parameters are applied.

Core NATS and Redis Pub/Sub do not expose ACK or replay state. They still fail before returning an
oversized caller response, but a transient delivery cannot be restored by the protocol; this is an
explicit boundary rather than a claimed requeue.

Verification: core 111/111; AMQP 22+3; Redis 37; NATS 14+2; pure Kafka 15+1; native Kafka 23;
default/native Clippy with warnings denied; default-feature CLI regression passes. The TUI has no MQ
consume surface; its passing tests are non-MQ regression evidence only. No claim is made here for
the `--no-default-features` test profile.

Cleanup: PASS. RabbitMQ queues, Redis Streams/groups, Kafka test topics/groups, and JetStream
streams/consumers created for this check were removed.
