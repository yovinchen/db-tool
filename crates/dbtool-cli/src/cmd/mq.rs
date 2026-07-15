use super::Context;
use clap::{Args, Subcommand, ValueEnum};
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{
        AckMode, ConsumeCursor, ConsumeOptions, ConsumerIdentity, DeleteResourceOptions, Message,
        MessageResource, MessageResourceKind,
    },
    port::CapabilityOperation,
    service::{limiter::ListLimiter, safety::SafetyGuard},
    Result,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Args)]
#[command(
    about = "Produce, consume, and inspect bounded message queue workflows.",
    long_about = "Message commands cover Kafka-compatible topics, AMQP/RabbitMQ queues, Redis Streams/PubSub, and NATS/JetStream where the selected connector exposes those capabilities. Produce requires --allow-write. Stateful group/durable consumption and successful-delivery acknowledgement also require --allow-write. AMQP/AMQPS consume requires explicit --ack on-success because successful delivery is ACKed and removed from the queue. Persistent resource deletion requires both --allow-write and a target-bound --confirm token. Topic/stream/queue catalogs honor the positive global --limit and require a backend-bounded operation; no unbounded fallback is attempted. Consume is always bounded by positive --max and --timeout values. When a consume result contains exactly --max messages, JSON meta.truncated marks that the limit was reached; it does not prove that more messages exist."
)]
pub struct MqCmd {
    #[command(subcommand)]
    pub action: MqAction,
}

#[derive(Subcommand)]
pub enum MqAction {
    /// Produce messages to a topic/queue
    Produce {
        /// Topic, stream, subject, or queue name.
        topic: String,
        /// Raw UTF-8 payload; JSON is not parsed or transformed.
        payload: String,
        /// Optional message key encoded from this UTF-8 text.
        #[arg(long, value_name = "TEXT")]
        key: Option<String>,
        /// Message header in KEY=VALUE form; repeat for multiple unique keys.
        #[arg(long, value_name = "KEY=VALUE")]
        header: Vec<String>,
        /// Non-negative target partition for backends that support partitions.
        #[arg(long)]
        partition: Option<i32>,
        /// Optional message timestamp as Unix epoch milliseconds.
        #[arg(long = "timestamp-ms", value_name = "EPOCH_MILLIS")]
        timestamp_ms: Option<i64>,
    },
    /// Consume messages (always bounded)
    #[command(
        long_about = "Consume a bounded batch of messages. Both --max and --timeout must be greater than zero. Stateless --ack none is the compatibility default. --group and --durable select broker-owned state, require an explicit --ack choice, and require the global --allow-write flag even with --ack none. --consumer names a member only when --group is present. AMQP/AMQPS rejects group/durable identities and requires explicit --ack on-success plus --allow-write because each successful delivery is ACKed and removed from the queue. Stateful identities cannot yet be combined with --partition, --offset, or --cursor. A JSON meta.truncated value of true means the returned count reached --max; it does not prove that another message exists in the backend. Optional --partition and --offset values must be non-negative. --cursor is an inclusive backend-native starting position: replaying a returned Kafka, Redis Stream, or NATS JetStream cursor returns that retained message again without compressing its native identity into the legacy offset field."
    )]
    Consume {
        /// Topic, stream, subject, or queue name.
        topic: String,
        /// Maximum messages to return.
        #[arg(long, default_value = "10")]
        max: usize,
        /// Maximum time to wait, in seconds.
        #[arg(long, default_value = "5")]
        timeout: u64,
        /// Non-negative source partition for backends that support partitions.
        #[arg(long)]
        partition: Option<i32>,
        /// Non-negative starting offset for backends that support offsets.
        #[arg(long)]
        offset: Option<i64>,
        /// Exact native cursor: kafka:P:O, redis-stream:M-S, or nats-jetstream:S.
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
        /// Stateful consumer group. Mutually exclusive with --durable.
        #[arg(long, value_name = "GROUP")]
        group: Option<String>,
        /// Optional consumer member name; valid only together with --group.
        #[arg(long, value_name = "MEMBER")]
        consumer: Option<String>,
        /// Stateful durable consumer. Mutually exclusive with --group.
        #[arg(long, value_name = "NAME")]
        durable: Option<String>,
        /// Acknowledgement mode. Group/durable consumers require this option explicitly.
        #[arg(long, value_enum, value_name = "MODE")]
        ack: Option<MqAckMode>,
    },
    /// List topics/streams/queues within the global --limit
    Topics,
    /// Show topic/queue detail when the backend exposes admin metadata
    Detail {
        /// Topic, stream, subject, or queue name to inspect.
        topic: String,
    },
    /// Show consumer group lag
    Lag {
        /// Consumer group, durable consumer, or queue name depending on backend.
        group: String,
    },
    /// Delete one persistent topic, queue, or stream after confirmation.
    #[command(
        long_about = "Delete one persistent messaging resource. The explicit --kind prevents a topic, queue, Redis Stream, and NATS JetStream from being treated as interchangeable. This destructive operation requires --allow-write and a target-bound --confirm token. --if-empty and --if-unused apply only to AMQP queues."
    )]
    Delete {
        /// Persistent resource type.
        #[arg(long, value_enum)]
        kind: MqResourceKind,
        /// Exact topic, queue, or stream name.
        name: String,
        /// For AMQP queues, refuse deletion while messages remain.
        #[arg(long)]
        if_empty: bool,
        /// For AMQP queues, refuse deletion while consumers are attached.
        #[arg(long)]
        if_unused: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MqResourceKind {
    KafkaTopic,
    AmqpQueue,
    RedisStream,
    NatsJetstream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MqAckMode {
    None,
    OnSuccess,
}

impl From<MqAckMode> for AckMode {
    fn from(value: MqAckMode) -> Self {
        match value {
            MqAckMode::None => Self::None,
            MqAckMode::OnSuccess => Self::OnSuccess,
        }
    }
}

impl MqResourceKind {
    fn into_core(self) -> MessageResourceKind {
        match self {
            Self::KafkaTopic => MessageResourceKind::KafkaTopic,
            Self::AmqpQueue => MessageResourceKind::AmqpQueue,
            Self::RedisStream => MessageResourceKind::RedisStream,
            Self::NatsJetstream => MessageResourceKind::NatsJetstream,
        }
    }
}

struct DeleteRequest {
    resource: MessageResource,
    options: DeleteResourceOptions,
}

struct ConsumeRequest {
    options: ConsumeOptions,
    ack_explicit: bool,
}

pub async fn run(ctx: &Context, cmd: MqCmd) -> Result<String> {
    if matches!(cmd.action, MqAction::Topics) {
        ListLimiter::new(ctx.limit).probe_items()?;
    }
    if matches!(cmd.action, MqAction::Produce { .. }) {
        ensure_write_allowed(ctx)?;
    }

    let produce_message = match &cmd.action {
        MqAction::Produce {
            payload,
            key,
            header,
            partition,
            timestamp_ms,
            ..
        } => Some(build_message(
            payload,
            key.as_deref(),
            header,
            *partition,
            *timestamp_ms,
        )?),
        _ => None,
    };
    let consume_request = match &cmd.action {
        MqAction::Consume {
            max,
            timeout,
            partition,
            offset,
            cursor,
            group,
            consumer,
            durable,
            ack,
            ..
        } => Some(build_consume_request(
            *max,
            *timeout,
            *partition,
            *offset,
            cursor.as_deref(),
            group.as_deref(),
            consumer.as_deref(),
            durable.as_deref(),
            *ack,
        )?),
        _ => None,
    };
    let delete_request = match &cmd.action {
        MqAction::Delete {
            kind,
            name,
            if_empty,
            if_unused,
        } => Some(build_delete_request(*kind, name, *if_empty, *if_unused)?),
        _ => None,
    };

    let dsn = ctx.resolve_dsn()?;
    if let Some(request) = &consume_request {
        validate_consume_backend_policy(&dsn, request)?;
        if consume_requires_write(&request.options) {
            ensure_write_allowed(ctx)?;
        }
    }
    if let Some(request) = &delete_request {
        ensure_write_allowed(ctx)?;
        let safety_resource = format!(
            "{}:{:?}",
            request.resource.kind.as_str(),
            request.resource.name
        );
        let confirmation_scope = delete_confirmation_scope(request)?;
        SafetyGuard::check_destructive_operation_with_scope(
            "delete_message_resource",
            &safety_resource,
            &ctx.safety_target(&dsn),
            &confirmation_scope,
            ctx.allow_write,
            ctx.confirm.as_deref(),
        )?;
    }
    let conn = ctx.registry.connect(&dsn).await?;
    let start = std::time::Instant::now();
    let elapsed = || start.elapsed().as_millis() as u64;
    let kind = conn.kind().0.clone();

    Ok(match cmd.action {
        MqAction::Produce {
            topic,
            payload: _,
            key: _,
            header: _,
            partition: _,
            timestamp_ms: _,
        } => {
            let producer = conn
                .as_producer()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "MessageProducer",
                })?;
            let msg = produce_message.ok_or_else(|| {
                Error::Internal("validated message producer input is missing".into())
            })?;
            let outcome = producer.produce(&topic, vec![msg]).await?;
            ctx.render_success(&kind, outcome, elapsed(), false)
        }
        MqAction::Consume {
            topic,
            max: _,
            timeout: _,
            partition: _,
            offset: _,
            cursor: _,
            group: _,
            consumer: _,
            durable: _,
            ack: _,
        } => {
            let consumer = conn
                .as_consumer()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "MessageConsumer",
                })?;
            let request = consume_request.ok_or_else(|| {
                Error::Internal("validated message consumer options are missing".into())
            })?;
            let opts = request.options;
            require_consume_operations(&conn.operations(), &opts, &kind)?;
            let max = opts.max;
            let msgs = consumer.consume(&topic, opts).await?;
            // Reaching the requested count proves only that the CLI budget was
            // exhausted. It does not prove that the backend has another message.
            let limit_reached = msgs.len() == max;
            ctx.render_success(&kind, msgs, elapsed(), limit_reached)
        }
        MqAction::Topics => {
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            require_message_operation(
                &conn.operations(),
                CapabilityOperation::MessageAdminListTopicsBounded,
                &kind,
            )?;
            let topics = admin.list_topics_bounded(ctx.limit).await?;
            let truncated = topics.truncated;
            ctx.render_success(&kind, topics.items, elapsed(), truncated)
        }
        MqAction::Detail { topic } => {
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            let detail = admin.topic_detail(&topic).await?;
            ctx.render_success(&kind, detail, elapsed(), false)
        }
        MqAction::Lag { group } => {
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            let lag = admin.consumer_lag(&group).await?;
            ctx.render_success(&kind, lag, elapsed(), false)
        }
        MqAction::Delete { .. } => {
            let admin = conn
                .as_admin_mutate()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminMutate.delete_resource",
                })?;
            let request = delete_request.ok_or_else(|| {
                Error::Internal("validated message resource deletion input is missing".into())
            })?;
            let outcome = admin
                .delete_resource(request.resource, request.options)
                .await?;
            ctx.render_success(&kind, outcome, elapsed(), false)
        }
    })
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

fn validate_consume_backend_policy(raw_dsn: &str, request: &ConsumeRequest) -> Result<()> {
    let dsn = Dsn::parse(raw_dsn)?;
    if matches!(dsn.scheme.as_str(), "amqp" | "amqps") {
        if request.options.identity.is_stateful() {
            return Err(Error::Config(
                "AMQP consume does not support --group or --durable identities".into(),
            ));
        }
        if !request.ack_explicit || request.options.ack != AckMode::OnSuccess {
            return Err(Error::Config(
                "AMQP consume requires explicit --ack on-success".into(),
            ));
        }
    }
    Ok(())
}

fn consume_requires_write(options: &ConsumeOptions) -> bool {
    options.identity.is_stateful() || options.ack == AckMode::OnSuccess
}

fn build_delete_request(
    kind: MqResourceKind,
    name: &str,
    if_empty: bool,
    if_unused: bool,
) -> Result<DeleteRequest> {
    if name.is_empty() || name.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(Error::Config(
            "mq delete resource name must be non-empty and contain no control characters".into(),
        ));
    }
    let kind = kind.into_core();
    if (if_empty || if_unused) && kind != MessageResourceKind::AmqpQueue {
        return Err(Error::Config(
            "mq delete --if-empty and --if-unused apply only to --kind amqp-queue".into(),
        ));
    }

    Ok(DeleteRequest {
        resource: MessageResource {
            kind,
            name: name.to_owned(),
        },
        options: DeleteResourceOptions {
            if_empty,
            if_unused,
        },
    })
}

fn delete_confirmation_scope(request: &DeleteRequest) -> Result<String> {
    SafetyGuard::confirmation_scope_digest(&(&request.resource, request.options))
}

fn build_message(
    payload: &str,
    key: Option<&str>,
    headers: &[String],
    partition: Option<i32>,
    timestamp_ms: Option<i64>,
) -> Result<Message> {
    validate_partition(partition)?;
    Ok(Message {
        key: key.map(|value| value.as_bytes().to_vec().into()),
        payload: payload.as_bytes().to_vec().into(),
        headers: parse_headers(headers)?,
        partition,
        offset: None,
        timestamp: timestamp_ms,
        cursor: None,
        metadata: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_consume_request(
    max: usize,
    timeout_secs: u64,
    partition: Option<i32>,
    offset: Option<i64>,
    cursor: Option<&str>,
    group: Option<&str>,
    consumer: Option<&str>,
    durable: Option<&str>,
    ack: Option<MqAckMode>,
) -> Result<ConsumeRequest> {
    if max == 0 {
        return Err(Error::Config(
            "mq consume --max must be greater than zero".into(),
        ));
    }
    if timeout_secs == 0 {
        return Err(Error::Config(
            "mq consume --timeout must be greater than zero".into(),
        ));
    }
    validate_partition(partition)?;
    if offset.is_some_and(|value| value < 0) {
        return Err(Error::Config(
            "mq consume --offset must be greater than or equal to zero".into(),
        ));
    }
    let cursor = cursor
        .map(str::parse::<ConsumeCursor>)
        .transpose()
        .map_err(|message| Error::Config(format!("mq consume --cursor: {message}")))?;
    if cursor.is_some() && (partition.is_some() || offset.is_some()) {
        return Err(Error::Config(
            "mq consume --cursor cannot be combined with --partition or --offset".into(),
        ));
    }
    let timeout = Duration::from_secs(timeout_secs);
    Instant::now().checked_add(timeout).ok_or_else(|| {
        Error::Config("mq consume --timeout is too large for this platform".into())
    })?;
    if group.is_some() && durable.is_some() {
        return Err(Error::Config(
            "mq consume --group and --durable are mutually exclusive".into(),
        ));
    }
    if consumer.is_some() && group.is_none() {
        return Err(Error::Config(
            "mq consume --consumer requires --group".into(),
        ));
    }
    let identity = match (group, consumer, durable) {
        (Some(group), member, None) => ConsumerIdentity::Group {
            group: group.to_owned(),
            member: member.map(str::to_owned),
        },
        (None, None, Some(name)) => ConsumerIdentity::Durable {
            name: name.to_owned(),
        },
        (None, None, None) => ConsumerIdentity::Stateless,
        _ => unreachable!("conflicting consumer identity options were rejected"),
    };
    if identity.is_stateful() && ack.is_none() {
        return Err(Error::Config(
            "mq consume --group and --durable require an explicit --ack choice".into(),
        ));
    }
    let ack_explicit = ack.is_some();
    let options = ConsumeOptions {
        max,
        timeout,
        partition,
        offset,
        cursor,
        identity,
        ack: ack.map(Into::into).unwrap_or_default(),
    };
    options
        .validate()
        .map_err(|message| Error::Config(format!("mq consume: {message}")))?;
    Ok(ConsumeRequest {
        options,
        ack_explicit,
    })
}

fn require_consume_operations(
    operations: &[CapabilityOperation],
    options: &ConsumeOptions,
    kind: &str,
) -> Result<()> {
    let identity_operation = match options.identity {
        ConsumerIdentity::Stateless => None,
        ConsumerIdentity::Group { .. } => Some(CapabilityOperation::MessageConsumeGroup),
        ConsumerIdentity::Durable { .. } => Some(CapabilityOperation::MessageConsumeDurable),
    };
    if let Some(operation) = identity_operation {
        require_consume_operation(operations, operation, kind)?;
    }
    if options.ack == AckMode::OnSuccess {
        require_consume_operation(operations, CapabilityOperation::MessageConsumeAck, kind)?;
    }
    Ok(())
}

fn require_consume_operation(
    operations: &[CapabilityOperation],
    operation: CapabilityOperation,
    kind: &str,
) -> Result<()> {
    require_message_operation(operations, operation, kind)
}

fn require_message_operation(
    operations: &[CapabilityOperation],
    operation: CapabilityOperation,
    kind: &str,
) -> Result<()> {
    if operations.contains(&operation) {
        Ok(())
    } else {
        Err(Error::UnsupportedCapability {
            kind: kind.to_owned(),
            needed: operation.as_str(),
        })
    }
}

fn validate_partition(partition: Option<i32>) -> Result<()> {
    if partition.is_some_and(|value| value < 0) {
        return Err(Error::Config(
            "mq --partition must be greater than or equal to zero".into(),
        ));
    }
    Ok(())
}

fn parse_headers(headers: &[String]) -> Result<HashMap<String, String>> {
    let mut parsed = HashMap::with_capacity(headers.len());
    for header in headers {
        let (key, value) = header.split_once('=').ok_or_else(|| {
            Error::Config(format!(
                "invalid message header {header:?}; expected KEY=VALUE"
            ))
        })?;
        if key.trim().is_empty() {
            return Err(Error::Config("message header key must not be empty".into()));
        }
        if key != key.trim() {
            return Err(Error::Config(format!(
                "message header key must not have leading or trailing whitespace: {key:?}"
            )));
        }
        if parsed.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err(Error::Config(format!(
                "duplicate message header key: {key:?}"
            )));
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_stateless_consume(
        max: usize,
        timeout_secs: u64,
        partition: Option<i32>,
        offset: Option<i64>,
        cursor: Option<&str>,
    ) -> Result<ConsumeRequest> {
        build_consume_request(
            max,
            timeout_secs,
            partition,
            offset,
            cursor,
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn legacy_topic_listing_never_satisfies_bounded_catalog_negotiation() {
        let operations = [CapabilityOperation::MessageAdminListTopics];
        assert!(matches!(
            require_message_operation(
                &operations,
                CapabilityOperation::MessageAdminListTopicsBounded,
                "legacy-mq"
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-mq"
                    && needed == CapabilityOperation::MessageAdminListTopicsBounded.as_str()
        ));
    }

    #[test]
    fn message_builder_preserves_all_exposed_fields_as_utf8() {
        let message = build_message(
            "你好, mq",
            Some("订单-42"),
            &["trace=abc".to_owned(), "content-type=text/plain".to_owned()],
            Some(2),
            Some(1_710_000_000_123),
        )
        .unwrap();

        assert_eq!(message.payload.as_ref(), "你好, mq".as_bytes());
        assert_eq!(message.key.as_deref(), Some("订单-42".as_bytes()));
        assert_eq!(message.headers["trace"], "abc");
        assert_eq!(message.headers["content-type"], "text/plain");
        assert_eq!(message.partition, Some(2));
        assert_eq!(message.offset, None);
        assert_eq!(message.timestamp, Some(1_710_000_000_123));
    }

    #[test]
    fn header_parser_rejects_lossy_or_ambiguous_inputs() {
        assert!(matches!(
            parse_headers(&["missing-separator".to_owned()]),
            Err(Error::Config(message)) if message.contains("expected KEY=VALUE")
        ));
        assert!(matches!(
            parse_headers(&["=value".to_owned()]),
            Err(Error::Config(message)) if message.contains("must not be empty")
        ));
        assert!(matches!(
            parse_headers(&["trace=one".to_owned(), "trace=two".to_owned()]),
            Err(Error::Config(message)) if message.contains("duplicate")
        ));
        assert!(matches!(
            parse_headers(&[" trace=value".to_owned()]),
            Err(Error::Config(message)) if message.contains("whitespace")
        ));

        let parsed = parse_headers(&["empty-value=".to_owned()]).unwrap();
        assert_eq!(parsed["empty-value"], "");
    }

    #[test]
    fn consume_options_require_positive_bounds_and_non_negative_position() {
        assert!(matches!(
            build_stateless_consume(0, 1, None, None, None),
            Err(Error::Config(message)) if message.contains("--max")
        ));
        assert!(matches!(
            build_stateless_consume(1, 0, None, None, None),
            Err(Error::Config(message)) if message.contains("--timeout")
        ));
        assert!(matches!(
            build_stateless_consume(1, 1, Some(-1), None, None),
            Err(Error::Config(message)) if message.contains("--partition")
        ));
        assert!(matches!(
            build_stateless_consume(1, 1, None, Some(-1), None),
            Err(Error::Config(message)) if message.contains("--offset")
        ));
        assert!(matches!(
            build_stateless_consume(1, u64::MAX, None, None, None),
            Err(Error::Config(message)) if message.contains("too large")
        ));

        let options = build_stateless_consume(25, 3, Some(4), Some(99), None)
            .unwrap()
            .options;
        assert_eq!(options.max, 25);
        assert_eq!(options.timeout, Duration::from_secs(3));
        assert_eq!(options.partition, Some(4));
        assert_eq!(options.offset, Some(99));
        assert_eq!(options.identity, ConsumerIdentity::Stateless);
        assert_eq!(options.ack, AckMode::None);

        let exact =
            build_stateless_consume(1, 1, None, None, Some("redis-stream:1710000000000-42"))
                .unwrap()
                .options;
        assert_eq!(
            exact.cursor,
            Some(ConsumeCursor::RedisStream {
                id: "1710000000000-42".to_owned(),
            })
        );
        assert!(matches!(
            build_stateless_consume(1, 1, Some(0), None, Some("kafka:0:1")),
            Err(Error::Config(message)) if message.contains("cannot be combined")
        ));
    }

    #[test]
    fn stateful_identity_requires_explicit_ack_and_rejects_ambiguous_names_or_positions() {
        assert!(matches!(
            build_consume_request(
                1,
                1,
                None,
                None,
                None,
                Some("orders"),
                None,
                None,
                None,
            ),
            Err(Error::Config(message)) if message.contains("explicit --ack")
        ));
        assert!(matches!(
            build_consume_request(
                1,
                1,
                None,
                None,
                None,
                Some("orders"),
                None,
                Some("billing"),
                Some(MqAckMode::None),
            ),
            Err(Error::Config(message)) if message.contains("mutually exclusive")
        ));
        assert!(matches!(
            build_consume_request(
                1,
                1,
                None,
                None,
                None,
                None,
                Some("worker-1"),
                None,
                Some(MqAckMode::None),
            ),
            Err(Error::Config(message)) if message.contains("requires --group")
        ));
        for invalid in ["", "   ", " orders", "orders ", "orders\nnext"] {
            assert!(matches!(
                build_consume_request(
                    1,
                    1,
                    None,
                    None,
                    None,
                    Some(invalid),
                    None,
                    None,
                    Some(MqAckMode::None),
                ),
                Err(Error::Config(message)) if message.contains("consumer group")
            ));
        }
        assert!(matches!(
            build_consume_request(
                1,
                1,
                Some(0),
                None,
                None,
                Some("orders"),
                Some("worker-1"),
                None,
                Some(MqAckMode::None),
            ),
            Err(Error::Config(message)) if message.contains("stateful consume identity")
        ));

        let request = build_consume_request(
            1,
            1,
            None,
            None,
            None,
            Some("orders"),
            Some("worker-1"),
            None,
            Some(MqAckMode::None),
        )
        .unwrap();
        assert_eq!(
            request.options.identity,
            ConsumerIdentity::Group {
                group: "orders".to_owned(),
                member: Some("worker-1".to_owned()),
            }
        );
        assert_eq!(request.options.ack, AckMode::None);
        assert!(request.ack_explicit);
        assert!(consume_requires_write(&request.options));
    }

    #[test]
    fn producer_rejects_negative_partition_before_adapter_dispatch() {
        assert!(matches!(
            build_message("payload", None, &[], Some(-1), None),
            Err(Error::Config(message)) if message.contains("--partition")
        ));
    }

    #[test]
    fn acknowledgement_and_stateful_identity_require_write_permission() {
        let mut options = ConsumeOptions::default();
        assert!(!consume_requires_write(&options));
        options.ack = AckMode::OnSuccess;
        assert!(consume_requires_write(&options));
        options.ack = AckMode::None;
        options.identity = ConsumerIdentity::Durable {
            name: "billing".to_owned(),
        };
        assert!(consume_requires_write(&options));
    }

    #[test]
    fn amqp_requires_explicit_success_ack_and_rejects_stateful_identities() {
        let implicit = build_stateless_consume(1, 1, None, None, None).unwrap();
        assert!(matches!(
            validate_consume_backend_policy("amqp://127.0.0.1:5672/%2f", &implicit),
            Err(Error::Config(message)) if message.contains("explicit --ack on-success")
        ));

        let explicit_none = build_consume_request(
            1,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(MqAckMode::None),
        )
        .unwrap();
        assert!(
            validate_consume_backend_policy("amqps://127.0.0.1:5671/%2f", &explicit_none).is_err()
        );

        let on_success = build_consume_request(
            1,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(MqAckMode::OnSuccess),
        )
        .unwrap();
        assert!(validate_consume_backend_policy("amqp://127.0.0.1:5672/%2f", &on_success).is_ok());

        let group = build_consume_request(
            1,
            1,
            None,
            None,
            None,
            Some("orders"),
            None,
            None,
            Some(MqAckMode::OnSuccess),
        )
        .unwrap();
        assert!(matches!(
            validate_consume_backend_policy("amqp://127.0.0.1:5672/%2f", &group),
            Err(Error::Config(message)) if message.contains("does not support")
        ));
    }

    #[test]
    fn coarse_consumer_capability_does_not_authorize_stateful_or_acknowledging_modes() {
        let coarse = CapabilityOperation::MESSAGE_CONSUMER;
        for (options, needed) in [
            (
                ConsumeOptions {
                    identity: ConsumerIdentity::Group {
                        group: "orders".to_owned(),
                        member: None,
                    },
                    ..Default::default()
                },
                "message.consume_group",
            ),
            (
                ConsumeOptions {
                    identity: ConsumerIdentity::Durable {
                        name: "billing".to_owned(),
                    },
                    ..Default::default()
                },
                "message.consume_durable",
            ),
            (
                ConsumeOptions {
                    ack: AckMode::OnSuccess,
                    ..Default::default()
                },
                "message.consume_ack",
            ),
        ] {
            assert!(matches!(
                require_consume_operations(coarse, &options, "legacy-message"),
                Err(Error::UnsupportedCapability { kind, needed: actual })
                    if kind == "legacy-message" && actual == needed
            ));
        }

        let mut explicit = coarse.to_vec();
        explicit.extend_from_slice(CapabilityOperation::MESSAGE_CONSUMER_EXTENSIONS);
        let options = ConsumeOptions {
            identity: ConsumerIdentity::Group {
                group: "orders".to_owned(),
                member: Some("worker-1".to_owned()),
            },
            ack: AckMode::OnSuccess,
            ..Default::default()
        };
        assert!(require_consume_operations(&explicit, &options, "native-kafka").is_ok());
    }

    #[test]
    fn delete_request_keeps_resource_kind_and_amqp_conditions_explicit() {
        let request = build_delete_request(MqResourceKind::AmqpQueue, "jobs", true, true).unwrap();
        assert_eq!(request.resource.kind, MessageResourceKind::AmqpQueue);
        assert_eq!(request.resource.name, "jobs");
        assert!(request.options.if_empty);
        assert!(request.options.if_unused);

        assert!(matches!(
            build_delete_request(MqResourceKind::KafkaTopic, "events", true, false),
            Err(Error::Config(message)) if message.contains("only to --kind amqp-queue")
        ));
        assert!(build_delete_request(MqResourceKind::RedisStream, "", false, false).is_err());
    }
}
