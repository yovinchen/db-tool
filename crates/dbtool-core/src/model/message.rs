use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};

use super::{DEFAULT_READ_BYTES, MAX_READ_BYTES};

/// Default byte ceiling for one complete caller-visible consumed message.
pub const DEFAULT_CONSUME_MESSAGE_BYTES: usize = DEFAULT_READ_BYTES;

/// Default cumulative byte ceiling for one complete consumed message batch.
pub const DEFAULT_CONSUME_BATCH_BYTES: usize = DEFAULT_READ_BYTES;

/// Absolute number of messages accepted by one portable produce call.
///
/// The byte ceiling normally becomes the tighter bound, but a separate item
/// ceiling prevents batches of tiny or empty messages from growing without a
/// finite process-level limit.
pub const MAX_PRODUCE_MESSAGES: usize = 100_000;

/// Absolute byte ceiling for one message or one complete produce batch.
pub const MAX_PRODUCE_BYTES: usize = MAX_READ_BYTES;

/// Finite compatibility default used by legacy producer entry points.
pub const DEFAULT_PRODUCE_MESSAGES: usize = 100;

/// Default byte ceiling for one complete message submitted for production.
pub const DEFAULT_PRODUCE_MESSAGE_BYTES: usize = DEFAULT_CONSUME_MESSAGE_BYTES;

/// Default cumulative byte ceiling for one complete produce batch.
pub const DEFAULT_PRODUCE_BATCH_BYTES: usize = DEFAULT_CONSUME_BATCH_BYTES;

const fn default_consume_message_bytes() -> usize {
    DEFAULT_CONSUME_MESSAGE_BYTES
}

const fn default_consume_batch_bytes() -> usize {
    DEFAULT_CONSUME_BATCH_BYTES
}

/// Caller-owned input envelope for a message produce operation.
///
/// `max_message_bytes` charges the compact JSON representation of every
/// complete [`Message`], including its key, payload, headers, placement fields,
/// cursor, and metadata. `max_batch_bytes` independently charges the complete
/// `Vec<Message>` envelope. Adapters must validate this budget and all messages
/// before creating a remote resource or attempting the first send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProduceBudget {
    pub max_messages: usize,
    pub max_message_bytes: usize,
    pub max_batch_bytes: usize,
}

impl ProduceBudget {
    pub fn new(
        max_messages: usize,
        max_message_bytes: usize,
        max_batch_bytes: usize,
    ) -> crate::Result<Self> {
        let budget = Self {
            max_messages,
            max_message_bytes,
            max_batch_bytes,
        };
        budget.validate()
    }

    /// Revalidate deserialized or directly constructed budgets at a trust
    /// boundary. No field may disable its process-level hard ceiling.
    pub fn validate(self) -> crate::Result<Self> {
        if self.max_messages == 0 {
            return Err(crate::Error::Config(
                "produce message budget must be greater than zero".to_owned(),
            ));
        }
        if self.max_messages > MAX_PRODUCE_MESSAGES {
            return Err(crate::Error::Config(format!(
                "produce message budget exceeds the hard {MAX_PRODUCE_MESSAGES}-message ceiling"
            )));
        }
        validate_produce_byte_budget("per-message", self.max_message_bytes)?;
        validate_produce_byte_budget("batch", self.max_batch_bytes)?;
        Ok(self)
    }
}

impl Default for ProduceBudget {
    fn default() -> Self {
        Self {
            max_messages: DEFAULT_PRODUCE_MESSAGES,
            max_message_bytes: DEFAULT_PRODUCE_MESSAGE_BYTES,
            max_batch_bytes: DEFAULT_PRODUCE_BATCH_BYTES,
        }
    }
}

fn validate_produce_byte_budget(label: &str, value: usize) -> crate::Result<()> {
    if value == 0 {
        return Err(crate::Error::Config(format!(
            "produce {label} byte budget must be greater than zero"
        )));
    }
    if value > MAX_PRODUCE_BYTES {
        return Err(crate::Error::Config(format!(
            "produce {label} byte budget exceeds the hard {MAX_PRODUCE_BYTES}-byte ceiling"
        )));
    }
    Ok(())
}

/// A backend-native, lossless position for a consumed message.
///
/// The legacy [`Message::partition`] and [`Message::offset`] fields remain for
/// compatibility, but cannot represent Redis Stream IDs or JetStream sequence
/// numbers without discarding information. Consumers should prefer this field
/// when it is present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum MessageCursor {
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    RedisStream {
        stream: String,
        id: String,
    },
    NatsJetstream {
        stream: String,
        stream_sequence: u64,
    },
}

/// Backend-native delivery facts that are useful for diagnostics but are not
/// necessarily stable or replayable cursors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum MessageMetadata {
    NatsJetstream {
        consumer: String,
        consumer_sequence: u64,
        delivery_attempt: i64,
        pending: u64,
    },
    Amqp {
        /// Channel-scoped delivery tag observed before dbtool ACKed the
        /// delivery. It is diagnostic only and cannot be used for a later ACK.
        /// This is intentionally metadata rather than a [`MessageCursor`].
        delivery_tag: u64,
        redelivered: bool,
        exchange: String,
        routing_key: String,
    },
}

/// Exact backend-native starting position for a bounded consume request.
/// The position is inclusive: replaying a returned [`MessageCursor`] through
/// its CLI spelling must return that message again when it is still retained.
///
/// CLI spelling is stable and round-trippable:
/// `kafka:<partition>:<offset>`, `redis-stream:<milliseconds>-<sequence>`, or
/// `nats-jetstream:<stream-sequence>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ConsumeCursor {
    Kafka { partition: i32, offset: i64 },
    RedisStream { id: String },
    NatsJetstream { stream_sequence: u64 },
}

impl ConsumeCursor {
    /// Revalidate cursors that may have been constructed through serde or by
    /// an embedded caller instead of the CLI parser.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Kafka { partition, offset } => {
                if *partition < 0 || *offset < 0 {
                    return Err("Kafka cursor partition and offset must be non-negative".to_owned());
                }
            }
            Self::RedisStream { id } => validate_redis_stream_id(id)?,
            Self::NatsJetstream { stream_sequence } => {
                if *stream_sequence == 0 {
                    return Err(
                        "NATS JetStream stream sequence must be greater than zero".to_owned()
                    );
                }
            }
        }
        Ok(())
    }
}

impl FromStr for ConsumeCursor {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(position) = value.strip_prefix("kafka:") {
            let (partition, offset) = position
                .split_once(':')
                .ok_or_else(|| "Kafka cursor must use kafka:<partition>:<offset>".to_owned())?;
            let partition = partition
                .parse::<i32>()
                .map_err(|_| "Kafka cursor partition must be an integer".to_owned())?;
            let offset = offset
                .parse::<i64>()
                .map_err(|_| "Kafka cursor offset must be an integer".to_owned())?;
            let cursor = Self::Kafka { partition, offset };
            cursor.validate()?;
            return Ok(cursor);
        }

        if let Some(id) = value.strip_prefix("redis-stream:") {
            let cursor = Self::RedisStream { id: id.to_owned() };
            cursor.validate()?;
            return Ok(cursor);
        }

        if let Some(sequence) = value.strip_prefix("nats-jetstream:") {
            let stream_sequence = sequence.parse::<u64>().map_err(|_| {
                "NATS JetStream cursor must use nats-jetstream:<positive-stream-sequence>"
                    .to_owned()
            })?;
            let cursor = Self::NatsJetstream { stream_sequence };
            cursor.validate()?;
            return Ok(cursor);
        }

        Err("cursor must use kafka:<partition>:<offset>, redis-stream:<milliseconds>-<sequence>, or nats-jetstream:<stream-sequence>".to_owned())
    }
}

fn validate_redis_stream_id(id: &str) -> Result<(), String> {
    let (millis, sequence) = id.split_once('-').ok_or_else(|| {
        "Redis Stream cursor must contain the full <milliseconds>-<sequence> ID".to_owned()
    })?;
    let (Ok(millis), Ok(sequence)) = (millis.parse::<u64>(), sequence.parse::<u64>()) else {
        return Err(
            "Redis Stream cursor must contain the full numeric <milliseconds>-<sequence> ID"
                .to_owned(),
        );
    };
    if millis == 0 && sequence == 0 {
        return Err("Redis Stream cursor 0-0 is not a record position".to_owned());
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub key: Option<Bytes>,
    pub payload: Bytes,
    pub headers: HashMap<String, String>,
    /// Partition / offset / subject — adapter fills what makes sense.
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    /// Epoch millis.
    pub timestamp: Option<i64>,
    /// Lossless backend-native position. Omitted for protocols such as Core
    /// NATS and AMQP that do not expose a stable replay cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<MessageCursor>,
    /// Native delivery facts that do not fit the portable fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MessageMetadata>,
}

/// Stable identity used by a message consumer.
///
/// Stateless consumption preserves the legacy dbtool behavior. Group and
/// durable identities may advance broker-owned state and therefore require
/// explicit capability negotiation and CLI write authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ConsumerIdentity {
    #[default]
    Stateless,
    Group {
        group: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<String>,
    },
    Durable {
        name: String,
    },
}

impl ConsumerIdentity {
    pub const fn is_stateful(&self) -> bool {
        !matches!(self, Self::Stateless)
    }

    /// Revalidate identities constructed by embedded callers or serde before
    /// an adapter uses them as broker resource names.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Stateless => Ok(()),
            Self::Group { group, member } => {
                validate_consumer_identity_part("consumer group", group)?;
                if let Some(member) = member {
                    validate_consumer_identity_part("consumer member", member)?;
                }
                Ok(())
            }
            Self::Durable { name } => validate_consumer_identity_part("durable consumer", name),
        }
    }
}

fn validate_consumer_identity_part(label: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value != value.trim() || value.chars().any(char::is_control) {
        return Err(format!(
            "{label} must be non-empty, have no leading or trailing whitespace, and contain no control characters"
        ));
    }
    Ok(())
}

/// Acknowledgement behavior requested for successful deliveries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum AckMode {
    #[default]
    None,
    OnSuccess,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeOptions {
    pub max: usize,
    pub timeout: std::time::Duration,
    /// Hard byte ceiling for one complete caller-visible [`Message`]. The
    /// payload, key, headers, cursor, metadata, and portable scalar fields all
    /// contribute through the stable serde representation.
    #[serde(default = "default_consume_message_bytes")]
    pub max_message_bytes: usize,
    /// Hard cumulative byte ceiling for the complete caller-visible
    /// `Vec<Message>` response. This is independent of the message count.
    #[serde(default = "default_consume_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Partition to read from (Kafka-style); None = all.
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    /// Exact, inclusive native starting position. This supersedes
    /// `partition`/`offset` when present; adapters reject conflicting legacy
    /// fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ConsumeCursor>,
    /// Stateless by default for backward compatibility. Stateful identities
    /// must be negotiated through method-level connector operations.
    #[serde(default)]
    pub identity: ConsumerIdentity,
    /// No acknowledgement by default for backward compatibility.
    #[serde(default)]
    pub ack: AckMode,
}

impl ConsumeOptions {
    /// Validate protocol-independent bounds, positions, and identity shape.
    /// Backend-specific restrictions remain the adapter's responsibility.
    pub fn validate(&self) -> Result<(), String> {
        if self.max == 0 {
            return Err("consume max must be greater than zero".to_owned());
        }
        if self.max.checked_add(1).is_none() {
            return Err("consume max is too large to reserve an internal item probe".to_owned());
        }
        if self.timeout.is_zero() {
            return Err("consume timeout must be greater than zero".to_owned());
        }
        if self.max_message_bytes == 0 || self.max_message_bytes > MAX_READ_BYTES {
            return Err(format!(
                "consume max_message_bytes must be between 1 and the hard {MAX_READ_BYTES}-byte ceiling"
            ));
        }
        if self.max_batch_bytes == 0 || self.max_batch_bytes > MAX_READ_BYTES {
            return Err(format!(
                "consume max_batch_bytes must be between 1 and the hard {MAX_READ_BYTES}-byte ceiling"
            ));
        }
        if self.partition.is_some_and(|partition| partition < 0) {
            return Err("consume partition must be non-negative".to_owned());
        }
        if self.offset.is_some_and(|offset| offset < 0) {
            return Err("consume offset must be non-negative".to_owned());
        }
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
            if self.partition.is_some() || self.offset.is_some() {
                return Err("consume cursor cannot be combined with partition or offset".to_owned());
            }
        }
        self.identity.validate()?;
        if self.identity.is_stateful()
            && (self.partition.is_some() || self.offset.is_some() || self.cursor.is_some())
        {
            return Err(
                "stateful consume identity cannot be combined with partition, offset, or cursor"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

impl Default for ConsumeOptions {
    fn default() -> Self {
        Self {
            max: 100,
            timeout: std::time::Duration::from_secs(5),
            max_message_bytes: DEFAULT_CONSUME_MESSAGE_BYTES,
            max_batch_bytes: DEFAULT_CONSUME_BATCH_BYTES,
            partition: None,
            offset: None,
            cursor: None,
            identity: ConsumerIdentity::Stateless,
            ack: AckMode::None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceOutcome {
    pub produced: u64,
    /// Per-message partition/offset pairs (Kafka-style).
    pub placements: Vec<MessagePlacement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePlacement {
    pub partition: i32,
    pub offset: i64,
    /// Lossless broker-native placement when the legacy partition/offset pair
    /// cannot represent it exactly (for example a Redis Stream ID).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<MessageCursor>,
}

/// A persistent messaging resource that can be removed through
/// [`crate::port::AdminMutate`].
///
/// Core NATS subjects and Redis Pub/Sub channels are intentionally absent:
/// neither protocol exposes them as persistent resources that can be deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MessageResourceKind {
    KafkaTopic,
    AmqpQueue,
    RedisStream,
    NatsJetstream,
}

impl MessageResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KafkaTopic => "kafka-topic",
            Self::AmqpQueue => "amqp-queue",
            Self::RedisStream => "redis-stream",
            Self::NatsJetstream => "nats-jetstream",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageResource {
    pub kind: MessageResourceKind,
    pub name: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteResourceOptions {
    /// Refuse to remove an AMQP queue that still contains messages.
    /// Other resource kinds reject this option instead of ignoring it.
    #[serde(default)]
    pub if_empty: bool,
    /// Refuse to remove an AMQP queue that still has consumers.
    /// Other resource kinds reject this option instead of ignoring it.
    #[serde(default)]
    pub if_unused: bool,
}

/// Structured result for a destructive messaging resource operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteResourceOutcome {
    pub resource: MessageResource,
    /// The backend accepted the delete request.
    pub acknowledged: bool,
    /// The backend synchronously confirmed removal or a bounded post-delete
    /// check observed that the resource no longer exists.
    pub verified_absent: bool,
    /// Message count observed before deletion when the backend exposes it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_before: Option<u64>,
    /// Consumer count observed before deletion when the backend exposes it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumers_before: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_kinds_have_stable_kebab_case_json_names() {
        for (kind, name) in [
            (MessageResourceKind::KafkaTopic, "kafka-topic"),
            (MessageResourceKind::AmqpQueue, "amqp-queue"),
            (MessageResourceKind::RedisStream, "redis-stream"),
            (MessageResourceKind::NatsJetstream, "nats-jetstream"),
        ] {
            assert_eq!(kind.as_str(), name);
            assert_eq!(serde_json::to_value(kind).unwrap(), name);
            assert_eq!(
                serde_json::from_value::<MessageResourceKind>(name.into()).unwrap(),
                kind
            );
        }
    }

    #[test]
    fn native_consume_cursors_parse_without_losing_protocol_identity() {
        assert_eq!(
            "kafka:2:9223372036854775807"
                .parse::<ConsumeCursor>()
                .unwrap(),
            ConsumeCursor::Kafka {
                partition: 2,
                offset: i64::MAX,
            }
        );
        assert_eq!(
            "redis-stream:1710000000000-42"
                .parse::<ConsumeCursor>()
                .unwrap(),
            ConsumeCursor::RedisStream {
                id: "1710000000000-42".to_owned(),
            }
        );
        assert_eq!(
            "nats-jetstream:18446744073709551615"
                .parse::<ConsumeCursor>()
                .unwrap(),
            ConsumeCursor::NatsJetstream {
                stream_sequence: u64::MAX,
            }
        );
    }

    #[test]
    fn malformed_or_lossy_native_cursors_are_rejected() {
        for cursor in [
            "kafka:0",
            "kafka:-1:0",
            "redis-stream:1710000000000",
            "redis-stream:1710000000000-bad",
            "redis-stream:0-0",
            "nats-jetstream:0",
            "unknown:1",
        ] {
            assert!(
                cursor.parse::<ConsumeCursor>().is_err(),
                "cursor should be rejected: {cursor}"
            );
        }

        assert!(ConsumeCursor::Kafka {
            partition: -1,
            offset: 0,
        }
        .validate()
        .is_err());
        assert!(ConsumeCursor::RedisStream {
            id: "bad".to_owned(),
        }
        .validate()
        .is_err());
        assert!(ConsumeCursor::NatsJetstream { stream_sequence: 0 }
            .validate()
            .is_err());
    }

    #[test]
    fn native_message_cursor_has_stable_tagged_json() {
        let cursor = MessageCursor::RedisStream {
            stream: "events".to_owned(),
            id: "1710000000000-42".to_owned(),
        };
        assert_eq!(
            serde_json::to_value(cursor).unwrap(),
            serde_json::json!({
                "kind": "redis-stream",
                "stream": "events",
                "id": "1710000000000-42",
            })
        );
    }

    #[test]
    fn consumer_identity_and_ack_mode_have_stable_wire_names() {
        assert_eq!(
            serde_json::to_value(ConsumerIdentity::Stateless).unwrap(),
            serde_json::json!({ "kind": "stateless" })
        );
        assert_eq!(
            serde_json::to_value(ConsumerIdentity::Group {
                group: "orders".to_owned(),
                member: Some("worker-1".to_owned()),
            })
            .unwrap(),
            serde_json::json!({
                "kind": "group",
                "group": "orders",
                "member": "worker-1",
            })
        );
        assert_eq!(
            serde_json::to_value(ConsumerIdentity::Durable {
                name: "billing".to_owned(),
            })
            .unwrap(),
            serde_json::json!({ "kind": "durable", "name": "billing" })
        );
        assert_eq!(
            serde_json::to_value(AckMode::OnSuccess).unwrap(),
            serde_json::json!("on-success")
        );
    }

    #[test]
    fn consume_defaults_preserve_stateless_non_acknowledging_behavior() {
        let options = ConsumeOptions::default();
        assert_eq!(options.identity, ConsumerIdentity::Stateless);
        assert_eq!(options.ack, AckMode::None);
        assert_eq!(options.max_message_bytes, DEFAULT_CONSUME_MESSAGE_BYTES);
        assert_eq!(options.max_batch_bytes, DEFAULT_CONSUME_BATCH_BYTES);
        assert!(options.validate().is_ok());

        let legacy: ConsumeOptions = serde_json::from_value(serde_json::json!({
            "max": 10,
            "timeout": { "secs": 1, "nanos": 0 },
            "partition": null,
            "offset": null,
        }))
        .unwrap();
        assert_eq!(legacy.identity, ConsumerIdentity::Stateless);
        assert_eq!(legacy.ack, AckMode::None);
        assert_eq!(legacy.max_message_bytes, DEFAULT_CONSUME_MESSAGE_BYTES);
        assert_eq!(legacy.max_batch_bytes, DEFAULT_CONSUME_BATCH_BYTES);
    }

    #[test]
    fn consume_byte_budgets_have_stable_wire_names_and_hard_bounds() {
        let options = ConsumeOptions {
            max_message_bytes: 1024,
            max_batch_bytes: 4096,
            ..Default::default()
        };
        let wire = serde_json::to_value(&options).unwrap();
        assert_eq!(wire["max_message_bytes"], 1024);
        assert_eq!(wire["max_batch_bytes"], 4096);
        assert!(options.validate().is_ok());

        for invalid in [
            ConsumeOptions {
                max_message_bytes: 0,
                ..Default::default()
            },
            ConsumeOptions {
                max_message_bytes: MAX_READ_BYTES + 1,
                ..Default::default()
            },
            ConsumeOptions {
                max_batch_bytes: 0,
                ..Default::default()
            },
            ConsumeOptions {
                max_batch_bytes: MAX_READ_BYTES + 1,
                ..Default::default()
            },
            ConsumeOptions {
                max: usize::MAX,
                ..Default::default()
            },
        ] {
            assert!(invalid.validate().is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn produce_budgets_have_stable_wire_names_defaults_and_hard_bounds() {
        let budget = ProduceBudget::new(7, 1024, 4096).unwrap();
        assert_eq!(
            serde_json::to_value(budget).unwrap(),
            serde_json::json!({
                "max_messages": 7,
                "max_message_bytes": 1024,
                "max_batch_bytes": 4096,
            })
        );
        assert_eq!(
            serde_json::from_value::<ProduceBudget>(serde_json::to_value(budget).unwrap()).unwrap(),
            budget
        );
        assert_eq!(
            ProduceBudget::default(),
            ProduceBudget {
                max_messages: DEFAULT_PRODUCE_MESSAGES,
                max_message_bytes: DEFAULT_PRODUCE_MESSAGE_BYTES,
                max_batch_bytes: DEFAULT_PRODUCE_BATCH_BYTES,
            }
        );

        for invalid in [
            ProduceBudget {
                max_messages: 0,
                ..ProduceBudget::default()
            },
            ProduceBudget {
                max_messages: MAX_PRODUCE_MESSAGES + 1,
                ..ProduceBudget::default()
            },
            ProduceBudget {
                max_messages: usize::MAX,
                ..ProduceBudget::default()
            },
            ProduceBudget {
                max_message_bytes: 0,
                ..ProduceBudget::default()
            },
            ProduceBudget {
                max_message_bytes: MAX_PRODUCE_BYTES + 1,
                ..ProduceBudget::default()
            },
            ProduceBudget {
                max_batch_bytes: 0,
                ..ProduceBudget::default()
            },
            ProduceBudget {
                max_batch_bytes: usize::MAX,
                ..ProduceBudget::default()
            },
        ] {
            assert!(invalid.validate().is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn stateful_consume_identity_is_validated_without_lossy_normalization() {
        for identity in [
            ConsumerIdentity::Group {
                group: String::new(),
                member: None,
            },
            ConsumerIdentity::Group {
                group: "orders".to_owned(),
                member: Some("worker\n1".to_owned()),
            },
            ConsumerIdentity::Group {
                group: "   ".to_owned(),
                member: None,
            },
            ConsumerIdentity::Group {
                group: " orders".to_owned(),
                member: None,
            },
            ConsumerIdentity::Group {
                group: "orders".to_owned(),
                member: Some("worker-1 ".to_owned()),
            },
            ConsumerIdentity::Durable {
                name: "\u{7}".to_owned(),
            },
        ] {
            assert!(identity.validate().is_err(), "accepted {identity:?}");
        }

        let stateful = ConsumeOptions {
            identity: ConsumerIdentity::Group {
                group: "orders with spaces".to_owned(),
                member: Some("worker-1".to_owned()),
            },
            ack: AckMode::None,
            ..Default::default()
        };
        assert!(stateful.validate().is_ok());

        for positioned in [
            ConsumeOptions {
                partition: Some(0),
                ..stateful.clone()
            },
            ConsumeOptions {
                offset: Some(0),
                ..stateful.clone()
            },
            ConsumeOptions {
                cursor: Some(ConsumeCursor::Kafka {
                    partition: 0,
                    offset: 0,
                }),
                ..stateful.clone()
            },
        ] {
            assert!(positioned
                .validate()
                .is_err_and(|message| message.contains("stateful consume identity")));
        }
    }

    #[test]
    fn delete_outcome_omits_unavailable_preflight_counts() {
        let value = serde_json::to_value(DeleteResourceOutcome {
            resource: MessageResource {
                kind: MessageResourceKind::KafkaTopic,
                name: "events".to_owned(),
            },
            acknowledged: true,
            verified_absent: true,
            messages_before: None,
            consumers_before: None,
        })
        .unwrap();

        assert_eq!(value["resource"]["kind"], "kafka-topic");
        assert!(value.get("messages_before").is_none());
        assert!(value.get("consumers_before").is_none());
    }
}
