use super::Context;
use clap::{Args, Subcommand, ValueEnum};
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{
        ConsumeCursor, ConsumeOptions, DeleteResourceOptions, Message, MessageResource,
        MessageResourceKind,
    },
    service::safety::SafetyGuard,
    Result,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Args)]
#[command(
    about = "Produce, consume, and inspect bounded message queue workflows.",
    long_about = "Message commands cover Kafka-compatible topics, AMQP/RabbitMQ queues, Redis Streams/PubSub, and NATS/JetStream where the selected connector exposes those capabilities. Produce requires --allow-write. AMQP/AMQPS consume also requires --allow-write because successful delivery is ACKed and removed from the queue. Persistent resource deletion requires both --allow-write and a target-bound --confirm token. Consume is always bounded by positive --max and --timeout values. When a consume result contains exactly --max messages, JSON meta.truncated marks that the limit was reached; it does not prove that more messages exist."
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
        long_about = "Consume a bounded batch of messages. Both --max and --timeout must be greater than zero. AMQP/AMQPS consume requires the global --allow-write flag because each successful delivery is ACKed and removed from the queue. A JSON meta.truncated value of true means the returned count reached --max; it does not prove that another message exists in the backend. Optional --partition and --offset values must be non-negative. --cursor is an inclusive backend-native starting position: replaying a returned Kafka, Redis Stream, or NATS JetStream cursor returns that retained message again without compressing its native identity into the legacy offset field."
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
    },
    /// List topics
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

pub async fn run(ctx: &Context, cmd: MqCmd) -> Result<String> {
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
    let consume_options = match &cmd.action {
        MqAction::Consume {
            max,
            timeout,
            partition,
            offset,
            cursor,
            ..
        } => Some(build_consume_options(
            *max,
            *timeout,
            *partition,
            *offset,
            cursor.as_deref(),
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
    if matches!(cmd.action, MqAction::Consume { .. }) && consume_requires_write(&dsn)? {
        ensure_write_allowed(ctx)?;
    }
    if let Some(request) = &delete_request {
        ensure_write_allowed(ctx)?;
        let safety_resource = format!(
            "{}:{:?}",
            request.resource.kind.as_str(),
            request.resource.name
        );
        SafetyGuard::check_destructive_operation(
            "delete_message_resource",
            &safety_resource,
            &ctx.safety_target(&dsn),
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
        } => {
            let consumer = conn
                .as_consumer()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "MessageConsumer",
                })?;
            let opts = consume_options.ok_or_else(|| {
                Error::Internal("validated message consumer options are missing".into())
            })?;
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
            let topics = admin.list_topics().await?;
            ctx.render_success(&kind, topics, elapsed(), false)
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

fn consume_requires_write(raw_dsn: &str) -> Result<bool> {
    let dsn = Dsn::parse(raw_dsn)?;
    Ok(matches!(dsn.scheme.as_str(), "amqp" | "amqps"))
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

fn build_consume_options(
    max: usize,
    timeout_secs: u64,
    partition: Option<i32>,
    offset: Option<i64>,
    cursor: Option<&str>,
) -> Result<ConsumeOptions> {
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
    Ok(ConsumeOptions {
        max,
        timeout,
        partition,
        offset,
        cursor,
    })
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
            build_consume_options(0, 1, None, None, None),
            Err(Error::Config(message)) if message.contains("--max")
        ));
        assert!(matches!(
            build_consume_options(1, 0, None, None, None),
            Err(Error::Config(message)) if message.contains("--timeout")
        ));
        assert!(matches!(
            build_consume_options(1, 1, Some(-1), None, None),
            Err(Error::Config(message)) if message.contains("--partition")
        ));
        assert!(matches!(
            build_consume_options(1, 1, None, Some(-1), None),
            Err(Error::Config(message)) if message.contains("--offset")
        ));
        assert!(matches!(
            build_consume_options(1, u64::MAX, None, None, None),
            Err(Error::Config(message)) if message.contains("too large")
        ));

        let options = build_consume_options(25, 3, Some(4), Some(99), None).unwrap();
        assert_eq!(options.max, 25);
        assert_eq!(options.timeout, Duration::from_secs(3));
        assert_eq!(options.partition, Some(4));
        assert_eq!(options.offset, Some(99));

        let exact =
            build_consume_options(1, 1, None, None, Some("redis-stream:1710000000000-42")).unwrap();
        assert_eq!(
            exact.cursor,
            Some(ConsumeCursor::RedisStream {
                id: "1710000000000-42".to_owned(),
            })
        );
        assert!(matches!(
            build_consume_options(1, 1, Some(0), None, Some("kafka:0:1")),
            Err(Error::Config(message)) if message.contains("cannot be combined")
        ));
    }

    #[test]
    fn producer_rejects_negative_partition_before_adapter_dispatch() {
        assert!(matches!(
            build_message("payload", None, &[], Some(-1), None),
            Err(Error::Config(message)) if message.contains("--partition")
        ));
    }

    #[test]
    fn only_ack_destructive_amqp_consumers_require_write_permission() {
        assert!(consume_requires_write("amqp://127.0.0.1:5672/%2f").unwrap());
        assert!(consume_requires_write("amqps://127.0.0.1:5671/%2f").unwrap());
        assert!(!consume_requires_write("redis://127.0.0.1:6379").unwrap());
        assert!(!consume_requires_write("nats://127.0.0.1:4222").unwrap());
        assert!(!consume_requires_write("kafka://127.0.0.1:9092").unwrap());
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
