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
