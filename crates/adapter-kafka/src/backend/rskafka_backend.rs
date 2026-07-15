// Pure-Rust backend via rskafka (default — self-contained, feature-limited).
// Covers Kafka / AutoMQ / Redpanda / WarpStream via the Kafka wire protocol.
use chrono::{DateTime, Utc};
use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ConsumeOptions, LagInfo, Message, MessagePlacement, PartitionWatermark, ProduceOutcome,
        TopicDetail, TopicInfo,
    },
    port::{
        capability::{AdminInspect, MessageConsumer, MessageProducer},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use rskafka::{
    client::{
        partition::{Compression, OffsetAt, UnknownTopicHandling},
        Client, ClientBuilder,
    },
    record::Record,
};
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use super::{validate_consume_position, validate_produce_message};

pub struct RskafkaAdapter {
    client: Client,
    kind: ConnectorKind,
}

pub fn connect(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let host = dsn.host.unwrap_or_else(|| "localhost".into());
        let port = dsn.port.unwrap_or(9092);
        let brokers = vec![format!("{host}:{port}")];
        let client = ClientBuilder::new(brokers)
            .client_id("dbtool")
            .build()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(Box::new(RskafkaAdapter {
            client,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for RskafkaAdapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            producer: true,
            consumer: true,
            admin: true,
            ..Default::default()
        }
    }

    fn operations(&self) -> Vec<CapabilityOperation> {
        let mut operations = self.capabilities().operations();
        operations.extend([
            CapabilityOperation::MessageAdminListTopics,
            CapabilityOperation::MessageAdminTopicDetail,
        ]);
        operations
    }

    async fn ping(&self) -> Result<()> {
        self.client
            .list_topics()
            .await
            .map(|_| ())
            .map_err(|e| Error::Connection(e.to_string()))
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_producer(&self) -> Option<&dyn MessageProducer> {
        Some(self)
    }

    fn as_consumer(&self) -> Option<&dyn MessageConsumer> {
        Some(self)
    }

    fn as_admin(&self) -> Option<&dyn AdminInspect> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl MessageProducer for RskafkaAdapter {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        validate_topic(target)?;
        if messages.is_empty() {
            return Ok(ProduceOutcome {
                produced: 0,
                placements: vec![],
            });
        }

        let message_count = messages.len();
        let grouped_records = group_records_by_partition(messages)?;
        self.ensure_topic(target).await?;

        // rskafka produces through a partition-specific client. Keep batches
        // efficient while restoring placements to the caller's input order.
        let mut placements = std::iter::repeat_with(|| None)
            .take(message_count)
            .collect::<Vec<Option<MessagePlacement>>>();
        for (partition, indexed_records) in grouped_records {
            let client = self
                .client
                .partition_client(target, partition, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            let (indices, records): (Vec<_>, Vec<_>) = indexed_records.into_iter().unzip();
            let offsets = client
                .produce(records, Compression::NoCompression)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            if offsets.len() != indices.len() {
                return Err(Error::Internal(format!(
                    "Kafka produced {} offsets for {} records in partition {partition}",
                    offsets.len(),
                    indices.len()
                )));
            }

            for (index, offset) in indices.into_iter().zip(offsets) {
                placements[index] = Some(MessagePlacement { partition, offset });
            }
        }

        let placements = placements
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| Error::Internal("Kafka produce result is missing a placement".into()))?;

        Ok(ProduceOutcome {
            produced: placements.len() as u64,
            placements,
        })
    }
}

#[async_trait::async_trait]
impl MessageConsumer for RskafkaAdapter {
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        validate_topic(source)?;
        validate_consume_position(options.partition, options.offset)?;
        if options.max == 0 {
            return Ok(vec![]);
        }

        let deadline = consume_deadline(options.timeout)?;
        let Some(remaining) = remaining_until(deadline) else {
            return Ok(vec![]);
        };
        let topics = match tokio::time::timeout(remaining, self.topic_infos()).await {
            Ok(result) => result?,
            Err(_) => return Ok(vec![]),
        };
        let topic = require_topic(topics, source)?;

        let mut messages = Vec::new();
        let partitions = if let Some(partition) = options.partition {
            vec![partition]
        } else {
            (0..topic.partitions).collect()
        };

        for partition in partitions {
            if messages.len() >= options.max {
                break;
            }

            let Some(remaining) = remaining_until(deadline) else {
                break;
            };
            let max_wait_ms = duration_millis_i32(remaining);
            let fetch = async {
                let client = self
                    .client
                    .partition_client(source, partition, UnknownTopicHandling::Retry)
                    .await
                    .map_err(|e| Error::Connection(e.to_string()))?;
                let offset = match options.offset {
                    Some(offset) => offset,
                    None => client
                        .get_offset(OffsetAt::Earliest)
                        .await
                        .map_err(|e| Error::Query(e.to_string()))?,
                };
                let (records, _) = client
                    .fetch_records(offset, 1..1_048_576, max_wait_ms)
                    .await
                    .map_err(|e| Error::Query(e.to_string()))?;
                Ok::<_, Error>(records)
            };
            let records = match tokio::time::timeout(remaining, fetch).await {
                Ok(result) => result?,
                Err(_) => break,
            };

            for record in records {
                if messages.len() >= options.max {
                    break;
                }
                messages.push(Message {
                    key: record.record.key.map(Into::into),
                    payload: record.record.value.unwrap_or_default().into(),
                    headers: record
                        .record
                        .headers
                        .into_iter()
                        .map(|(key, value)| (key, String::from_utf8_lossy(&value).into_owned()))
                        .collect(),
                    partition: Some(partition),
                    offset: Some(record.offset),
                    timestamp: Some(record.record.timestamp.timestamp_millis()),
                });
            }
        }

        Ok(messages)
    }
}

#[async_trait::async_trait]
impl AdminInspect for RskafkaAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        self.topic_infos().await
    }

    async fn topic_detail(&self, name: &str) -> Result<TopicDetail> {
        validate_topic(name)?;
        let topics = self.topic_infos().await?;
        let info = topics
            .into_iter()
            .find(|topic| topic.name == name)
            .ok_or_else(|| Error::Query(format!("topic not found: {name}")))?;

        let mut watermarks = Vec::new();
        for partition in 0..info.partitions {
            let client = self
                .client
                .partition_client(name, partition, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
            let low = client
                .get_offset(OffsetAt::Earliest)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            let high = client
                .get_offset(OffsetAt::Latest)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            watermarks.push(PartitionWatermark {
                partition,
                low,
                high,
            });
        }

        Ok(TopicDetail {
            info,
            config: HashMap::new(),
            watermarks,
        })
    }

    async fn consumer_lag(&self, _group: &str) -> Result<Vec<LagInfo>> {
        Err(Error::UnsupportedCapability {
            kind: self.kind.0.clone(),
            needed: "ConsumerLag",
        })
    }
}

impl RskafkaAdapter {
    async fn ensure_topic(&self, name: &str) -> Result<()> {
        if self
            .client
            .list_topics()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?
            .iter()
            .any(|topic| topic.name == name)
        {
            return Ok(());
        }

        self.client
            .controller_client()
            .map_err(|e| Error::Connection(e.to_string()))?
            .create_topic(name, 1, 1, 5_000)
            .await
            .map_err(|e| Error::Query(e.to_string()))
    }

    async fn topic_infos(&self) -> Result<Vec<TopicInfo>> {
        let mut topics = self
            .client
            .list_topics()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?
            .into_iter()
            .map(|topic| TopicInfo {
                name: topic.name,
                partitions: topic.partitions.len() as i32,
                replicas: 1,
            })
            .collect::<Vec<_>>();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }
}

fn validate_topic(topic: &str) -> Result<()> {
    if topic.is_empty()
        || topic.len() > 249
        || topic
            .bytes()
            .any(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')))
    {
        return Err(Error::Query(format!("invalid Kafka topic name: {topic:?}")));
    }

    Ok(())
}

fn group_records_by_partition(
    messages: Vec<Message>,
) -> Result<BTreeMap<i32, Vec<(usize, Record)>>> {
    let mut grouped = BTreeMap::<i32, Vec<(usize, Record)>>::new();
    for (index, message) in messages.into_iter().enumerate() {
        validate_produce_message(&message)?;
        let partition = message.partition.unwrap_or(0);
        let timestamp = kafka_timestamp(message.timestamp)?;
        let record = Record {
            key: message.key.map(|key| key.to_vec()),
            value: Some(message.payload.to_vec()),
            headers: message
                .headers
                .into_iter()
                .map(|(key, value)| (key, value.into_bytes()))
                .collect(),
            timestamp,
        };
        grouped.entry(partition).or_default().push((index, record));
    }
    Ok(grouped)
}

fn kafka_timestamp(timestamp: Option<i64>) -> Result<DateTime<Utc>> {
    match timestamp {
        Some(timestamp) => DateTime::from_timestamp_millis(timestamp).ok_or_else(|| {
            Error::Config(format!(
                "Kafka timestamp is outside the supported epoch-millisecond range: {timestamp}"
            ))
        }),
        None => Ok(Utc::now()),
    }
}

fn consume_deadline(timeout: Duration) -> Result<Instant> {
    Instant::now().checked_add(timeout).ok_or_else(|| {
        Error::Config("Kafka consume timeout is too large for this platform".to_owned())
    })
}

fn require_topic(topics: Vec<TopicInfo>, name: &str) -> Result<TopicInfo> {
    topics
        .into_iter()
        .find(|topic| topic.name == name)
        .ok_or_else(|| Error::Query(format!("topic not found: {name}")))
}

fn remaining_until(deadline: Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
}

fn duration_millis_i32(duration: std::time::Duration) -> i32 {
    duration
        .as_millis()
        .clamp(1, i32::MAX as u128)
        .try_into()
        .unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn message(partition: Option<i32>, timestamp: Option<i64>) -> Message {
        Message {
            key: Some(Bytes::from_static(b"key")),
            payload: Bytes::from_static(b"payload"),
            headers: HashMap::from([("trace".to_owned(), "abc".to_owned())]),
            partition,
            offset: None,
            timestamp,
        }
    }

    #[test]
    fn records_preserve_partition_key_headers_and_timestamp() {
        let grouped = group_records_by_partition(vec![
            message(Some(2), Some(1_710_000_000_123)),
            message(None, Some(1_710_000_000_456)),
        ])
        .unwrap();

        assert_eq!(grouped.keys().copied().collect::<Vec<_>>(), vec![0, 2]);
        let (_, partition_two) = &grouped[&2][0];
        assert_eq!(partition_two.key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(partition_two.value.as_deref(), Some(b"payload".as_slice()));
        assert_eq!(partition_two.headers["trace"], b"abc");
        assert_eq!(
            partition_two.timestamp.timestamp_millis(),
            1_710_000_000_123
        );

        let (_, default_partition) = &grouped[&0][0];
        assert_eq!(
            default_partition.timestamp.timestamp_millis(),
            1_710_000_000_456
        );
    }

    #[test]
    fn default_timestamp_is_current_time() {
        let before = Utc::now().timestamp_millis();
        let timestamp = kafka_timestamp(None).unwrap().timestamp_millis();
        let after = Utc::now().timestamp_millis();

        assert!((before..=after).contains(&timestamp));
    }

    #[test]
    fn timestamp_outside_chrono_range_is_rejected() {
        assert!(matches!(
            kafka_timestamp(Some(i64::MAX)),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn remaining_time_uses_one_shared_deadline() {
        let expired = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        assert_eq!(remaining_until(expired), None);

        let deadline = Instant::now() + Duration::from_millis(100);
        let remaining = remaining_until(deadline).unwrap();
        assert!(remaining > Duration::ZERO);
        assert!(remaining <= Duration::from_millis(100));
    }

    #[test]
    fn missing_topic_is_a_query_error() {
        let error = require_topic(vec![], "missing-topic").unwrap_err();

        assert!(matches!(error, Error::Query(_)));
        assert!(error.to_string().contains("missing-topic"));
    }
}
