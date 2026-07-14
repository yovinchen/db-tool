// Pure-Rust backend via rskafka (default — self-contained, feature-limited).
// Covers Kafka / AutoMQ / Redpanda / WarpStream via the Kafka wire protocol.
use chrono::Utc;
use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ConsumeOptions, LagInfo, Message, MessagePlacement, PartitionWatermark, ProduceOutcome,
        TopicDetail, TopicInfo,
    },
    port::{
        capability::{AdminInspect, MessageConsumer, MessageProducer},
        connector::{Capabilities, Connector, ConnectorKind},
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
use std::collections::{BTreeMap, HashMap};

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

        self.ensure_topic(target).await?;
        let partition = 0;
        let client = self
            .client
            .partition_client(target, partition, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let records = messages
            .into_iter()
            .map(|message| Record {
                key: message.key.map(|key| key.to_vec()),
                value: Some(message.payload.to_vec()),
                headers: message
                    .headers
                    .into_iter()
                    .map(|(key, value)| (key, value.into_bytes()))
                    .collect::<BTreeMap<_, _>>(),
                timestamp: Utc::now(),
            })
            .collect::<Vec<_>>();

        let offsets = client
            .produce(records, Compression::NoCompression)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        Ok(ProduceOutcome {
            produced: offsets.len() as u64,
            placements: offsets
                .into_iter()
                .map(|offset| MessagePlacement { partition, offset })
                .collect(),
        })
    }
}

#[async_trait::async_trait]
impl MessageConsumer for RskafkaAdapter {
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        validate_topic(source)?;
        if options.max == 0 {
            return Ok(vec![]);
        }

        let topics = self.topic_infos().await?;
        let Some(topic) = topics.into_iter().find(|topic| topic.name == source) else {
            return Ok(vec![]);
        };

        let mut messages = Vec::new();
        let partitions = if let Some(partition) = options.partition {
            vec![partition]
        } else {
            (0..topic.partitions).collect()
        };
        let max_wait_ms = duration_millis_i32(options.timeout);

        for partition in partitions {
            if messages.len() >= options.max {
                break;
            }

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

fn duration_millis_i32(duration: std::time::Duration) -> i32 {
    duration
        .as_millis()
        .clamp(1, i32::MAX as u128)
        .try_into()
        .unwrap_or(i32::MAX)
}
