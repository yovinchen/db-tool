use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeOptions {
    pub max: usize,
    pub timeout: std::time::Duration,
    /// Partition to read from (Kafka-style); None = all.
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    /// Exact, inclusive native starting position. This supersedes
    /// `partition`/`offset` when present; adapters reject conflicting legacy
    /// fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ConsumeCursor>,
}

impl Default for ConsumeOptions {
    fn default() -> Self {
        Self {
            max: 100,
            timeout: std::time::Duration::from_secs(5),
            partition: None,
            offset: None,
            cursor: None,
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
