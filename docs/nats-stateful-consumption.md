# NATS Stateful Consumption Contract

The `nats://` and `nats+tls://` connectors expose two deliberately different
stateful identities. The identity selects the protocol path; dbtool does not
infer durable semantics from a subject name.

| Identity | Backend path | Replay/progress | Allowed ACK mode |
| --- | --- | --- | --- |
| stateless | Core NATS subscription | live delivery only | `none` |
| group | Core NATS queue subscription | load sharing only; no retained member progress | `none` |
| durable | JetStream pull consumer | server-owned durable progress and redelivery | `none` or `on-success` |

## Core NATS queue groups

`--group <name> --ack none` creates a queue subscription for the lifetime of
the command. The group name is passed to NATS exactly. Core NATS does not expose
a stable queue member identity, acknowledgement operation, replay cursor, or
committed position, so dbtool rejects `--consumer`, `--ack on-success`, and all
partition/offset/cursor combinations on this path.

The group capability means load sharing between simultaneous subscribers. It
does not mean Kafka-style durable progress.

## JetStream durable consumers

`--durable <name>` resolves the stream by the requested subject and binds to a
pull consumer. If the durable does not exist, dbtool creates it strictly with:

- durable name exactly equal to the requested name;
- `deliver=all`;
- `ack=explicit`;
- `replay=instant`;
- unlimited redelivery (`max_deliver` unset/unlimited);
- a single filter subject exactly equal to the requested subject;
- full payload delivery (`headers_only=false`).

If the durable already exists, all of those compatibility fields are checked.
An incompatible push consumer, filter, delivery policy, ACK policy, replay
policy, multi-filter configuration, or headers-only consumer is rejected.
Finite `max_deliver` is also rejected because it could make `--ack none`
silently exhaust redelivery attempts.
dbtool never updates an existing durable to make it fit. A concurrent strict
creator is re-read and subjected to the same validation.

`--ack none` returns the bounded batch without acknowledging it. NATS may
redeliver the batch after the durable's configured ACK wait. `--ack on-success`
first converts every native delivery, including its cursor, timestamp, payload,
headers, and delivery metadata. Only after the complete batch converts does
dbtool call JetStream `double_ack` for each delivery and wait for the server's
confirmation within the consume deadline. ACKing calls reserve the final third
of that deadline and issue the verified ACK waits concurrently, so a partially
filled pull batch cannot spend the whole budget waiting for more messages and
then return without attempting to ACK deliveries it already converted.

This is at-least-once progress handling, not an atomic multi-message broker
transaction. If a later verified ACK fails, earlier ACKs in the same converted
batch may already have advanced; the command returns an error and never claims
the whole batch succeeded. A conversion failure occurs before the first ACK and
leaves every fetched delivery eligible for redelivery.

Stateless `nats-jetstream:<stream-sequence>` reads remain separate. They use a
temporary `ack=none` consumer starting at that inclusive sequence and delete it
after the bounded read. A durable identity cannot be combined with that cursor.

## Lag dimensions

`mq lag <durable>` reports only server-owned JetStream facts:

- `committed`: the consumer ACK floor's stream sequence;
- `latest`: the stream's last sequence;
- `lag`: `num_ack_pending + num_pending`, with checked arithmetic.

The lag count therefore includes deliveries that reached the durable but have
not been acknowledged as well as messages not yet delivered to it. Because the
first two fields are sequence watermarks and `lag` is a count (and a stream can
contain filtered subjects or sequence gaps), dbtool does not invent lag as
`latest - committed`.

## Method-level operations

The NATS connector advertises `message.consume_group`,
`message.consume_durable`, and `message.consume_ack` in addition to its base
produce/consume and JetStream admin operations. Individual calls still apply
the protocol rules above; advertising ACK support does not make Core NATS
acknowledgeable.
