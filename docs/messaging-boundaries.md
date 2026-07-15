# Messaging Protocol Boundaries

dbtool exposes one shared `mq` command family, but each broker family has different metadata guarantees. The core adapter layer keeps protocol-native behavior separate from management-plugin behavior so a successful command does not imply a backend can discover metadata it does not actually expose.

## Kafka / Redpanda

The default pure-Rust backend provides bounded stateless reads and exact
partition/offset cursors. It does not advertise consumer-group or acknowledgement
operations. The `full-native` librdkafka backend additionally provides dynamic
group subscription and broker offset commits:

- `--group <name> --ack none` joins the group and reads from its committed
  positions without committing the returned batch.
- `--group <name> --ack on-success` converts the complete batch first, then
  synchronously commits each partition's highest observed offset plus one.
- A failed poll, unrepresentable tombstone/header, or commit failure is an
  error, never a successful partial response. Kafka may still accept a commit
  before a process dies while formatting output, so this is at-least-once
  progress handling, not exactly-once processing.
- `--consumer` would mean Kafka static membership. Static members do not leave
  immediately when this one-shot consumer closes, so the native adapter rejects
  the option rather than silently treating it as a client label.
- `mq lag <group>` uses committed offsets and partition high watermarks only in
  the native backend. Pure Kafka returns `UNSUPPORTED_CAPABILITY`.

## AMQP / RabbitMQ

AMQP 0.9.1 can publish, consume, acknowledge, and passively inspect a known queue. It does not provide a protocol operation for listing all queues in a virtual host.

Current core behavior:

- `mq produce <queue>` declares the named queue when needed and publishes to it.
- `mq consume <queue>` declares the named queue when needed and performs bounded `basic.get` with ack.
- `mq detail <queue>` uses passive queue declare and returns `message_count` and `consumer_count`.
- `mq delete --kind amqp-queue <queue>` can remove a known queue; `--if-empty` and
  `--if-unused` are part of the target-bound confirmation scope and cannot be
  changed after a token is issued.
- `mq topics` and `mq lag` return `UNSUPPORTED_CAPABILITY` for pure AMQP because queue discovery and queue-depth lag require RabbitMQ's HTTP management plugin. An empty success would incorrectly mean that the broker was inspected and contained no queues.

RabbitMQ queue listing is exposed through an explicit management boundary, not hidden inside the pure AMQP protocol path:

- `rabbitmq+http://user:pass@host:15672/vhost` registers an admin-only connector.
- `mq topics` lists queues through `/api/queues/{vhost}`.
- `mq detail <queue>` uses `/api/queues/{vhost}/{queue}` and reports an exact
  snapshot only after RabbitMQ publishes count fields. It prefers a valid
  `messages`; when that aggregate is absent it uses checked
  `messages_ready + messages_unacknowledged`. Missing, invalid, or overflowing
  counts are errors, never an invented zero.
- `mq delete --kind amqp-queue <queue>` uses the management DELETE endpoint and
  verifies absence after RabbitMQ acknowledges removal.
- `mq lag` remains `UNSUPPORTED_CAPABILITY`: queue depth is not a portable
  consumer-group committed-offset lag, so detail metadata is not relabeled as
  lag.

Do not infer the management API port from the AMQP port in core code; local deployments commonly remap one without the other.

## NATS

Core NATS subjects are ephemeral routing names and are not listable. JetStream streams are durable and expose metadata through the JetStream API.

Current core behavior:

- `mq produce <subject>` and `mq consume <subject>` use core NATS pub/sub.
- `mq topics`, `mq detail <stream>`, and `mq lag <durable>` are JetStream-scoped.
- A NATS server without JetStream enabled may accept pub/sub commands but reject admin commands.

## Redis

Redis Streams are durable and discoverable by scanning stream keys. Redis Pub/Sub channels are live-only and not durable.

Current core behavior:

- `stream:<name>` targets Redis Streams.
- `pubsub:<channel>` targets Redis Pub/Sub.
- Unprefixed Redis `mq` targets default to Streams.
- `mq topics` lists Streams only; Pub/Sub channels can be inspected with `mq detail pubsub:<channel>` while subscribers are active.
- Stateless Stream consumption uses bounded `XREAD` and supports exact native
  Redis Stream cursors. Stateless consumption accepts only `--ack none`.
- Stateful Stream consumption requires both `--group <group>` and
  `--consumer <member>`. dbtool never creates the group: operators must create
  it with the intended start position before consuming. Omitting the member or
  naming a missing group is an error.
- A group invocation first replays that member's own pending entries, then uses
  any remaining batch budget for new `>` entries. Therefore `--ack none`
  leaves deliveries in the PEL and the same member deterministically sees them
  again on its next invocation.
- `--ack on-success` converts the complete bounded batch before issuing one
  `XACK` for its unique Stream IDs. Missing payloads, unsupported fields,
  non-UTF-8 headers, duplicate payload/key/header/native IDs, oversized server
  batches, malformed RESP shapes, malformed IDs, and partial acknowledgements
  are errors; conversion failures leave all deliveries pending. XREAD replies
  are parsed from ordered RESP2/RESP3 pairs rather than a map that could fold
  duplicate fields. Parser errors report only type/count/length shapes and do
  not echo message bytes. This is explicit
  at-least-once progress handling, not exactly-once processing.
- A legal Stream ID whose millisecond component exceeds `i64::MAX` remains
  available through the exact Redis cursor; the compatibility `offset` and
  `timestamp` fields are `null` rather than forged as zero or used to reject
  the message.
- `mq lag <group>` uses Redis `entries-read`, `pending`, and server-reported
  `lag`. Its portable dimensions are `committed = entries-read - pending`,
  `latest = entries-read + server-lag`, and
  `lag = pending + server-lag`, so both delivered-unacknowledged and
  not-yet-delivered work remain visible. Missing or inconsistent server fields
  fail closed instead of falling back to `XLEN`. The adapter probes
  `INFO server` while connecting and advertises this method only when the
  Redis protocol version is at least 7; missing/malformed versions and KeyDB
  6.3 do not advertise lag, while group delivery and ACK remain available.
- The adapter timeout covers waiting for its shared connection and Redis
  `BLOCK`; the CLI `FlowControl` deadline bounds the complete request,
  including non-blocking pending reads and `XACK`. Embedded callers invoking
  the adapter directly must provide their own outer request deadline when
  they need the same end-to-end guarantee.
- Redis Pub/Sub rejects group/durable identities and acknowledgements because
  the protocol has neither replayable state nor an ACK operation.
