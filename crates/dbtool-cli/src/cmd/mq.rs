use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{ConsumeOptions, Message},
    Result,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Args)]
#[command(
    about = "Produce, consume, and inspect bounded message queue workflows.",
    long_about = "Message commands cover Kafka-compatible topics, AMQP/RabbitMQ queues, Redis Streams/PubSub, and NATS/JetStream where the selected connector exposes those capabilities. Produce requires --allow-write. AMQP/AMQPS consume also requires --allow-write because successful delivery is ACKed and removed from the queue. Consume is always bounded by positive --max and --timeout values. When a consume result contains exactly --max messages, JSON meta.truncated marks that the limit was reached; it does not prove that more messages exist."
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
        long_about = "Consume a bounded batch of messages. Both --max and --timeout must be greater than zero. AMQP/AMQPS consume requires the global --allow-write flag because each successful delivery is ACKed and removed from the queue. A JSON meta.truncated value of true means the returned count reached --max; it does not prove that another message exists in the backend. Optional --partition and --offset values must be non-negative."
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
            ..
        } => Some(build_consume_options(*max, *timeout, *partition, *offset)?),
        _ => None,
    };

    let dsn = ctx.resolve_dsn()?;
    if matches!(cmd.action, MqAction::Consume { .. }) && consume_requires_write(&dsn)? {
        ensure_write_allowed(ctx)?;
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
    })
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

fn consume_requires_write(raw_dsn: &str) -> Result<bool> {
    let dsn = Dsn::parse(raw_dsn)?;
    Ok(matches!(dsn.scheme.as_str(), "amqp" | "amqps"))
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
    })
}

fn build_consume_options(
    max: usize,
    timeout_secs: u64,
    partition: Option<i32>,
    offset: Option<i64>,
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
    let timeout = Duration::from_secs(timeout_secs);
    Instant::now().checked_add(timeout).ok_or_else(|| {
        Error::Config("mq consume --timeout is too large for this platform".into())
    })?;
    Ok(ConsumeOptions {
        max,
        timeout,
        partition,
        offset,
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
            build_consume_options(0, 1, None, None),
            Err(Error::Config(message)) if message.contains("--max")
        ));
        assert!(matches!(
            build_consume_options(1, 0, None, None),
            Err(Error::Config(message)) if message.contains("--timeout")
        ));
        assert!(matches!(
            build_consume_options(1, 1, Some(-1), None),
            Err(Error::Config(message)) if message.contains("--partition")
        ));
        assert!(matches!(
            build_consume_options(1, 1, None, Some(-1)),
            Err(Error::Config(message)) if message.contains("--offset")
        ));
        assert!(matches!(
            build_consume_options(1, u64::MAX, None, None),
            Err(Error::Config(message)) if message.contains("too large")
        ));

        let options = build_consume_options(25, 3, Some(4), Some(99)).unwrap();
        assert_eq!(options.max, 25);
        assert_eq!(options.timeout, Duration::from_secs(3));
        assert_eq!(options.partition, Some(4));
        assert_eq!(options.offset, Some(99));
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
}
