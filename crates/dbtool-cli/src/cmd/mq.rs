use super::Context;
use clap::{Args, Subcommand, ValueEnum};
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{
        AckMode, ConsumeCursor, ConsumeOptions, ConsumerIdentity, DeleteResourceOptions, Message,
        MessageResource, MessageResourceKind, ProduceBudget, DEFAULT_CONSUME_MESSAGE_BYTES,
        DEFAULT_PRODUCE_MESSAGE_BYTES, MAX_READ_BYTES,
    },
    port::CapabilityOperation,
    service::{safety::SafetyGuard, MessageWriteLimiter},
    Result,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Args)]
#[command(
    about = "Produce, consume, and inspect bounded message queue workflows.",
    long_about = "Message commands cover Kafka-compatible topics, AMQP/RabbitMQ queues, Redis Streams/PubSub, and NATS/JetStream where the selected connector exposes those capabilities. Produce requires --allow-write and the exact message.produce_budgeted operation; --max-message-bytes bounds its complete message and global --max-bytes bounds its complete batch before connection or send. Stateful group/durable consumption and successful-delivery acknowledgement also require --allow-write. AMQP/AMQPS consume requires explicit --ack on-success because successful delivery is ACKed and removed from the queue. Persistent resource deletion requires both --allow-write and a target-bound --confirm token. Topic/stream/queue catalogs honor both the positive global --limit item budget and --max-bytes response budget and require an exact backend-budgeted operation; no unbounded fallback is attempted. Consume is always bounded by positive --max and --timeout values. When a consume result contains exactly --max messages, JSON meta.truncated marks that the limit was reached; it does not prove that more messages exist."
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
        /// Maximum bytes in the complete message input, including key, payload, headers, and portable metadata fields.
        #[arg(long, default_value_t = DEFAULT_PRODUCE_MESSAGE_BYTES)]
        max_message_bytes: usize,
    },
    /// Consume messages (always bounded)
    #[command(
        long_about = "Consume a bounded batch of messages. --max-message-bytes limits each complete portable message and the global --max-bytes limits the complete batch before ACK/XACK/commit/JetStream double-ACK. Core NATS and Redis Pub/Sub have no acknowledgement or replay guarantee; they still reject an oversized response before returning it. Both --max and --timeout must be greater than zero. Stateless --ack none is the compatibility default. --group and --durable select broker-owned state, require an explicit --ack choice, and require the global --allow-write flag even with --ack none. --consumer names a member only when --group is present. AMQP/AMQPS rejects group/durable identities and requires explicit --ack on-success plus --allow-write because each successful delivery is ACKed and removed from the queue. Stateful identities cannot yet be combined with --partition, --offset, or --cursor. A JSON meta.truncated value of true means the returned count reached --max; it does not prove that another message exists in the backend. Optional --partition and --offset values must be non-negative. --cursor is an inclusive backend-native starting position: replaying a returned Kafka, Redis Stream, or NATS JetStream cursor returns that retained message again without compressing its native identity into the legacy offset field."
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
        /// Maximum bytes in one complete portable message: payload, key, headers, cursor, placement, timestamps, and metadata.
        #[arg(long, default_value_t = DEFAULT_CONSUME_MESSAGE_BYTES)]
        max_message_bytes: usize,
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

pub(crate) fn action_may_mutate(action: &MqAction) -> bool {
    match action {
        MqAction::Produce { .. } | MqAction::Delete { .. } => true,
        MqAction::Consume {
            group,
            durable,
            ack,
            ..
        } => group.is_some() || durable.is_some() || matches!(ack, Some(MqAckMode::OnSuccess)),
        MqAction::Topics | MqAction::Detail { .. } | MqAction::Lag { .. } => false,
    }
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

struct ProduceRequest {
    message: Message,
    budget: ProduceBudget,
}

pub async fn run(ctx: &Context, cmd: MqCmd) -> Result<String> {
    let topics_budget = match &cmd.action {
        MqAction::Topics => Some(ctx.read_budget()?),
        _ => None,
    };
    if matches!(cmd.action, MqAction::Produce { .. }) {
        ensure_write_allowed(ctx)?;
    }
    let metadata_budget = match &cmd.action {
        MqAction::Detail { .. } | MqAction::Lag { .. } => Some(ctx.metadata_budget()?),
        _ => None,
    };

    let produce_request = match &cmd.action {
        MqAction::Produce {
            payload,
            key,
            header,
            partition,
            timestamp_ms,
            max_message_bytes,
            ..
        } => Some(build_produce_request(
            payload,
            key.as_deref(),
            header,
            *partition,
            *timestamp_ms,
            *max_message_bytes,
            ctx.max_bytes,
        )?),
        _ => None,
    };
    let consume_request = match &cmd.action {
        MqAction::Consume {
            max,
            timeout,
            max_message_bytes,
            partition,
            offset,
            cursor,
            group,
            consumer,
            durable,
            ack,
            ..
        } => Some(build_consume_request_with_budgets(
            *max,
            *timeout,
            *max_message_bytes,
            ctx.max_bytes,
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
            &ctx.confirmation_target(&dsn)?,
            &confirmation_scope,
            ctx.allow_write,
            ctx.confirm.as_deref(),
        )?;
    }
    let conn = ctx.registry.connect(&dsn).await?;
    let operations = conn.operations();
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
            max_message_bytes: _,
        } => {
            require_message_operation(
                &operations,
                CapabilityOperation::MessageProduceBudgeted,
                &kind,
            )?;
            let producer = conn
                .as_producer()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "MessageProducer",
                })?;
            let request = produce_request.ok_or_else(|| {
                Error::Internal("validated message producer request is missing".into())
            })?;
            let outcome = producer
                .produce_budgeted(&topic, vec![request.message], request.budget)
                .await?;
            ctx.render_success(&kind, outcome, elapsed(), false)
        }
        MqAction::Consume {
            topic,
            max: _,
            timeout: _,
            max_message_bytes: _,
            partition: _,
            offset: _,
            cursor: _,
            group: _,
            consumer: _,
            durable: _,
            ack: _,
        } => {
            require_message_operation(&operations, CapabilityOperation::MessageConsume, &kind)?;
            let request = consume_request.ok_or_else(|| {
                Error::Internal("validated message consumer options are missing".into())
            })?;
            let opts = request.options;
            require_consume_operations(&operations, &opts, &kind)?;
            let consumer = conn
                .as_consumer()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "MessageConsumer",
                })?;
            let max = opts.max;
            let msgs = consumer.consume(&topic, opts).await?;
            // Reaching the requested count proves only that the CLI budget was
            // exhausted. It does not prove that the backend has another message.
            let limit_reached = msgs.len() == max;
            ctx.render_success(&kind, msgs, elapsed(), limit_reached)
        }
        MqAction::Topics => {
            require_message_operation(
                &operations,
                CapabilityOperation::MessageAdminListTopicsBudgeted,
                &kind,
            )?;
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            let topics = admin
                .list_topics_budgeted(topics_budget.ok_or_else(|| {
                    Error::Internal("topic-list budget was not initialized".into())
                })?)
                .await?;
            let truncated = topics.truncated;
            ctx.render_success(&kind, topics.items, elapsed(), truncated)
        }
        MqAction::Detail { topic } => {
            require_message_operation(
                &operations,
                CapabilityOperation::MessageAdminTopicDetailBounded,
                &kind,
            )?;
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            let detail = admin
                .topic_detail_bounded(
                    &topic,
                    metadata_budget.expect("detail actions construct a metadata budget"),
                )
                .await?;
            ctx.render_success(&kind, detail, elapsed(), false)
        }
        MqAction::Lag { group } => {
            require_message_operation(
                &operations,
                CapabilityOperation::MessageAdminConsumerLagBounded,
                &kind,
            )?;
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            let lag = admin
                .consumer_lag_bounded(
                    &group,
                    metadata_budget.expect("lag actions construct a metadata budget"),
                )
                .await?;
            ctx.render_success(&kind, lag, elapsed(), false)
        }
        MqAction::Delete { .. } => {
            require_message_operation(&operations, CapabilityOperation::MessageAdminDelete, &kind)?;
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
fn build_produce_request(
    payload: &str,
    key: Option<&str>,
    headers: &[String],
    partition: Option<i32>,
    timestamp_ms: Option<i64>,
    max_message_bytes: usize,
    max_batch_bytes: usize,
) -> Result<ProduceRequest> {
    let budget = ProduceBudget::new(1, max_message_bytes, max_batch_bytes)?;
    let message = build_message(payload, key, headers, partition, timestamp_ms)?;
    MessageWriteLimiter::new(budget, "CLI message produce")?
        .validate(std::slice::from_ref(&message))?;
    Ok(ProduceRequest { message, budget })
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
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
    build_consume_request_with_budgets(
        max,
        timeout_secs,
        DEFAULT_CONSUME_MESSAGE_BYTES,
        dbtool_core::model::DEFAULT_CONSUME_BATCH_BYTES,
        partition,
        offset,
        cursor,
        group,
        consumer,
        durable,
        ack,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_consume_request_with_budgets(
    max: usize,
    timeout_secs: u64,
    max_message_bytes: usize,
    max_batch_bytes: usize,
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
    validate_consume_byte_budget("--max-message-bytes", max_message_bytes)?;
    validate_consume_byte_budget("--max-bytes", max_batch_bytes)?;
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
        max_message_bytes,
        max_batch_bytes,
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

fn validate_consume_byte_budget(option: &str, value: usize) -> Result<()> {
    if value == 0 {
        return Err(Error::Config(format!(
            "mq consume {option} must be greater than zero"
        )));
    }
    if value > MAX_READ_BYTES {
        return Err(Error::Config(format!(
            "mq consume {option} exceeds the hard {MAX_READ_BYTES}-byte ceiling"
        )));
    }
    Ok(())
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
    fn legacy_topic_listing_never_satisfies_budgeted_catalog_negotiation() {
        let operations = [CapabilityOperation::MessageAdminListTopics];
        assert!(matches!(
            require_message_operation(
                &operations,
                CapabilityOperation::MessageAdminListTopicsBudgeted,
                "legacy-mq"
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-mq"
                    && needed == CapabilityOperation::MessageAdminListTopicsBudgeted.as_str()
        ));
    }

    #[test]
    fn legacy_producer_never_satisfies_budgeted_produce_negotiation() {
        let operations = [CapabilityOperation::MessageProduce];
        assert!(matches!(
            require_message_operation(
                &operations,
                CapabilityOperation::MessageProduceBudgeted,
                "legacy-mq"
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-mq"
                    && needed == CapabilityOperation::MessageProduceBudgeted.as_str()
        ));
    }

    #[tokio::test]
    async fn produce_budget_is_rejected_before_dsn_resolution() {
        let base = Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: dbtool_core::service::formatter::Format::Json,
            limit: 1,
            max_bytes: dbtool_core::model::DEFAULT_PRODUCE_BATCH_BYTES,
            max_item_bytes: dbtool_core::model::DEFAULT_INPUT_ITEM_BYTES,
            throttle_overrides: Default::default(),
            allow_write: true,
            confirm: None,
        };

        let invalid_budget = run(
            &base,
            MqCmd {
                action: MqAction::Produce {
                    topic: "events".to_owned(),
                    payload: "payload".to_owned(),
                    key: None,
                    header: Vec::new(),
                    partition: None,
                    timestamp_ms: None,
                    max_message_bytes: 0,
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            invalid_budget,
            Error::Config(message) if message.contains("per-message byte budget")
        ));

        let oversized_message = run(
            &base,
            MqCmd {
                action: MqAction::Produce {
                    topic: "events".to_owned(),
                    payload: "payload".to_owned(),
                    key: None,
                    header: Vec::new(),
                    partition: None,
                    timestamp_ms: None,
                    max_message_bytes: 1,
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            oversized_message,
            Error::InputBudgetExceeded {
                unit: "bytes",
                limit: 1,
                ..
            }
        ));

        let batch_limited = Context {
            max_bytes: 1,
            ..base
        };
        let oversized_batch = run(
            &batch_limited,
            MqCmd {
                action: MqAction::Produce {
                    topic: "events".to_owned(),
                    payload: "payload".to_owned(),
                    key: None,
                    header: Vec::new(),
                    partition: None,
                    timestamp_ms: None,
                    max_message_bytes: DEFAULT_PRODUCE_MESSAGE_BYTES,
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            oversized_batch,
            Error::InputBudgetExceeded {
                unit: "bytes",
                limit: 1,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn topic_catalog_byte_budget_is_rejected_before_dsn_resolution() {
        let ctx = Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: dbtool_core::service::formatter::Format::Json,
            limit: 1,
            max_bytes: 0,
            max_item_bytes: dbtool_core::model::DEFAULT_INPUT_ITEM_BYTES,
            throttle_overrides: Default::default(),
            allow_write: false,
            confirm: None,
        };
        let error = run(
            &ctx,
            MqCmd {
                action: MqAction::Topics,
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::Config(message) if message.contains("byte budget")));
    }

    #[test]
    fn partial_admin_profiles_cannot_dispatch_unadvertised_methods() {
        let legacy_admin_without_operations = [];
        for operation in [
            CapabilityOperation::MessageAdminListTopicsBudgeted,
            CapabilityOperation::MessageAdminTopicDetailBounded,
            CapabilityOperation::MessageAdminConsumerLagBounded,
            CapabilityOperation::MessageAdminDelete,
        ] {
            assert!(matches!(
                require_message_operation(
                    &legacy_admin_without_operations,
                    operation,
                    "legacy-admin"
                ),
                Err(Error::UnsupportedCapability { kind, needed })
                    if kind == "legacy-admin" && needed == operation.as_str()
            ));
        }

        let legacy_topic_only = [CapabilityOperation::MessageAdminTopicDetail];
        assert!(matches!(
            require_message_operation(
                &legacy_topic_only,
                CapabilityOperation::MessageAdminTopicDetailBounded,
                "legacy-amqp"
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == CapabilityOperation::MessageAdminTopicDetailBounded.as_str()
        ));

        let topic_only = [CapabilityOperation::MessageAdminTopicDetailBounded];
        assert!(require_message_operation(
            &topic_only,
            CapabilityOperation::MessageAdminTopicDetailBounded,
            "amqp"
        )
        .is_ok());

        for operation in [
            CapabilityOperation::MessageAdminListTopicsBudgeted,
            CapabilityOperation::MessageAdminConsumerLagBounded,
            CapabilityOperation::MessageAdminDelete,
        ] {
            assert!(matches!(
                require_message_operation(&topic_only, operation, "amqp"),
                Err(Error::UnsupportedCapability { kind, needed })
                    if kind == "amqp" && needed == operation.as_str()
            ));
        }

        let list_only = [CapabilityOperation::MessageAdminListTopicsBudgeted];
        assert!(require_message_operation(
            &list_only,
            CapabilityOperation::MessageAdminListTopicsBudgeted,
            "list-only"
        )
        .is_ok());
        assert!(matches!(
            require_message_operation(
                &list_only,
                CapabilityOperation::MessageAdminTopicDetailBounded,
                "list-only"
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "list-only"
                    && needed == CapabilityOperation::MessageAdminTopicDetailBounded.as_str()
        ));
    }

    #[tokio::test]
    async fn detail_and_lag_budgets_are_rejected_before_dsn_resolution() {
        let ctx = Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: dbtool_core::service::formatter::Format::Json,
            limit: usize::MAX,
            max_bytes: dbtool_core::model::DEFAULT_READ_BYTES,
            max_item_bytes: dbtool_core::model::DEFAULT_INPUT_ITEM_BYTES,
            throttle_overrides: Default::default(),
            allow_write: false,
            confirm: None,
        };

        for action in [
            MqAction::Detail {
                topic: "events".to_owned(),
            },
            MqAction::Lag {
                group: "workers".to_owned(),
            },
        ] {
            let error = run(&ctx, MqCmd { action }).await.unwrap_err();
            assert!(matches!(error, Error::Config(message) if message.contains("too large")));
        }
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
    fn produce_request_uses_one_message_and_exact_message_and_batch_bytes() {
        let message = build_message("payload", Some("key"), &[], None, None).unwrap();
        let message_bytes = serde_json::to_vec(&message).unwrap().len();
        let batch_bytes = serde_json::to_vec(&vec![message]).unwrap().len();

        let request = build_produce_request(
            "payload",
            Some("key"),
            &[],
            None,
            None,
            message_bytes,
            batch_bytes,
        )
        .unwrap();
        assert_eq!(request.budget.max_messages, 1);
        assert_eq!(request.budget.max_message_bytes, message_bytes);
        assert_eq!(request.budget.max_batch_bytes, batch_bytes);

        assert!(matches!(
            build_produce_request(
                "payload",
                Some("key"),
                &[],
                None,
                None,
                message_bytes - 1,
                batch_bytes,
            ),
            Err(Error::InputBudgetExceeded { unit: "bytes", limit, .. })
                if limit == message_bytes - 1
        ));
        assert!(matches!(
            build_produce_request(
                "payload",
                Some("key"),
                &[],
                None,
                None,
                message_bytes,
                batch_bytes - 1,
            ),
            Err(Error::InputBudgetExceeded { unit: "bytes", limit, .. })
                if limit == batch_bytes - 1
        ));
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

        let byte_bounded = build_consume_request_with_budgets(
            2, 1, 1024, 4096, None, None, None, None, None, None, None,
        )
        .unwrap()
        .options;
        assert_eq!(byte_bounded.max_message_bytes, 1024);
        assert_eq!(byte_bounded.max_batch_bytes, 4096);
        assert!(matches!(
            build_consume_request_with_budgets(
                2, 1, 0, 4096, None, None, None, None, None, None, None,
            ),
            Err(Error::Config(message)) if message.contains("--max-message-bytes")
        ));
        assert!(matches!(
            build_consume_request_with_budgets(
                2, 1, 1024, 0, None, None, None, None, None, None, None,
            ),
            Err(Error::Config(message)) if message.contains("--max-bytes")
        ));

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
