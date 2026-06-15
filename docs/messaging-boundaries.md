# Messaging Protocol Boundaries

dbtool exposes one shared `mq` command family, but each broker family has different metadata guarantees. The core adapter layer keeps protocol-native behavior separate from management-plugin behavior so a successful command does not imply a backend can discover metadata it does not actually expose.

## AMQP / RabbitMQ

AMQP 0.9.1 can publish, consume, acknowledge, and passively inspect a known queue. It does not provide a protocol operation for listing all queues in a virtual host.

Current core behavior:

- `mq produce <queue>` declares the named queue when needed and publishes to it.
- `mq consume <queue>` declares the named queue when needed and performs bounded `basic.get` with ack.
- `mq detail <queue>` uses passive queue declare and returns `message_count` and `consumer_count`.
- `mq topics` returns an empty list for pure AMQP because queue discovery requires RabbitMQ's HTTP management plugin.

RabbitMQ queue listing should be added behind a separate management boundary, not hidden inside the pure AMQP protocol path. Acceptable future shapes:

- An explicit management DSN, for example `rabbitmq+http://host:15672/vhost`, registered as an optional admin adapter.
- A feature-gated RabbitMQ management client that is only enabled when HTTP management support is requested.
- A CLI option that clearly identifies the management endpoint and credentials instead of deriving them silently from the AMQP DSN.

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
