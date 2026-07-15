use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeOptions {
    pub max: usize,
    pub timeout: std::time::Duration,
    /// Partition to read from (Kafka-style); None = all.
    pub partition: Option<i32>,
    pub offset: Option<i64>,
}

impl Default for ConsumeOptions {
    fn default() -> Self {
        Self {
            max: 100,
            timeout: std::time::Duration::from_secs(5),
            partition: None,
            offset: None,
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
