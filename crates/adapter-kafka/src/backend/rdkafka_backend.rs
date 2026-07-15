// Native backend via librdkafka/rdkafka.
use bytes::Bytes;
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
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header, Headers, Message as KafkaMessage, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    Offset, TopicPartitionList,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use super::{validate_consume_position, validate_produce_message};

pub struct RdkafkaAdapter {
    producer: FutureProducer,
    consumer: BaseConsumer,
    admin: AdminClient<DefaultClientContext>,
    kind: ConnectorKind,
}

pub fn connect(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let brokers = brokers_from_dsn(&dsn);
        let producer = kafka_config(&dsn, &brokers)
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>()
            .map_err(kafka_connection_error)?;
        let consumer = kafka_config(&dsn, &brokers)
            .set("group.id", "dbtool")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .set("auto.offset.reset", "earliest")
            .create::<BaseConsumer>()
            .map_err(kafka_connection_error)?;
        let admin = kafka_config(&dsn, &brokers)
            .create::<AdminClient<DefaultClientContext>>()
            .map_err(kafka_connection_error)?;

        Ok(Box::new(RdkafkaAdapter {
            producer,
            consumer,
            admin,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for RdkafkaAdapter {
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
        self.consumer
            .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))
            .map(|_| ())
            .map_err(kafka_connection_error)
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
impl MessageProducer for RdkafkaAdapter {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        validate_topic(target)?;
        if messages.is_empty() {
            return Ok(ProduceOutcome {
                produced: 0,
                placements: vec![],
            });
        }

        for message in &messages {
            validate_native_produce_message(message)?;
        }
        self.ensure_topic(target).await?;
        let mut placements = Vec::with_capacity(messages.len());
        for message in messages {
            let key = message.key.as_ref().map(|key| key.to_vec());
            let mut record = FutureRecord::to(target).payload(message.payload.as_ref());
            if let Some(key) = key.as_ref() {
                record = record.key(key);
            }
            if !message.headers.is_empty() {
                record = record.headers(core_headers_to_native(&message.headers));
            }
            if let Some(partition) = message.partition {
                record = record.partition(partition);
            }
            if let Some(timestamp) = message.timestamp {
                record = record.timestamp(timestamp);
            }
            let (partition, offset) = self
                .producer
                .send(record, Timeout::After(Duration::from_secs(5)))
                .await
                .map_err(|(error, _)| Error::Query(error.to_string()))?;
            placements.push(MessagePlacement { partition, offset });
        }

        Ok(ProduceOutcome {
            produced: placements.len() as u64,
            placements,
        })
    }
}

#[async_trait::async_trait]
impl MessageConsumer for RdkafkaAdapter {
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        validate_topic(source)?;
        validate_consume_position(options.partition, options.offset)?;
        if options.max == 0 {
            return Ok(vec![]);
        }

        let topics = self.topic_infos()?;
        let Some(topic) = topics.into_iter().find(|topic| topic.name == source) else {
            return Ok(vec![]);
        };
        let partitions = if let Some(partition) = options.partition {
            vec![partition]
        } else {
            (0..topic.partitions).collect()
        };
        let mut assignment = TopicPartitionList::new();
        for partition in partitions {
            let offset = match options.offset {
                Some(offset) => offset,
                None => self.low_watermark(source, partition)?,
            };
            assignment
                .add_partition_offset(source, partition, Offset::Offset(offset))
                .map_err(kafka_query_error)?;
        }
        self.consumer
            .assign(&assignment)
            .map_err(kafka_query_error)?;

        let deadline = Instant::now().checked_add(options.timeout).ok_or_else(|| {
            Error::Config("Kafka consume timeout is too large for this platform".to_owned())
        })?;
        let mut messages = Vec::new();
        while messages.len() < options.max && Instant::now() < deadline {
            let poll_timeout = remaining_poll_timeout(deadline);
            match self.consumer.poll(poll_timeout) {
                Some(Ok(message)) => {
                    messages.push(Message {
                        key: message.key().map(Bytes::copy_from_slice),
                        payload: Bytes::copy_from_slice(message.payload().unwrap_or_default()),
                        headers: native_headers_to_core(message.headers()),
                        partition: Some(message.partition()),
                        offset: Some(message.offset()),
                        timestamp: message.timestamp().to_millis(),
                    });
                }
                Some(Err(KafkaError::PartitionEOF(_))) | None => {}
                Some(Err(error)) => return Err(kafka_query_error(error)),
            }
        }

        Ok(messages)
    }
}

#[async_trait::async_trait]
impl AdminInspect for RdkafkaAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        self.topic_infos()
    }

    async fn topic_detail(&self, name: &str) -> Result<TopicDetail> {
        validate_topic(name)?;
        let metadata = self
            .consumer
            .fetch_metadata(Some(name), Timeout::After(Duration::from_secs(5)))
            .map_err(kafka_connection_error)?;
        let topic = metadata
            .topics()
            .iter()
            .find(|topic| topic.name() == name)
            .ok_or_else(|| Error::Query(format!("topic not found: {name}")))?;
        let info = topic_info(topic);
        let mut watermarks = Vec::new();
        for partition in topic.partitions() {
            let (low, high) = self
                .consumer
                .fetch_watermarks(name, partition.id(), Timeout::After(Duration::from_secs(5)))
                .map_err(kafka_query_error)?;
            watermarks.push(PartitionWatermark {
                partition: partition.id(),
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

impl RdkafkaAdapter {
    async fn ensure_topic(&self, name: &str) -> Result<()> {
        if self.topic_infos()?.iter().any(|topic| topic.name == name) {
            return Ok(());
        }

        let topic = NewTopic::new(name, 1, TopicReplication::Fixed(1));
        let results = self
            .admin
            .create_topics(&[topic], &AdminOptions::new())
            .await
            .map_err(kafka_query_error)?;
        for result in results {
            match result {
                Ok(_) => {}
                Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((topic, error)) => {
                    return Err(Error::Query(format!(
                        "failed to create Kafka topic {topic}: {error}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn topic_infos(&self) -> Result<Vec<TopicInfo>> {
        let metadata = self
            .consumer
            .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))
            .map_err(kafka_connection_error)?;
        let mut topics = metadata.topics().iter().map(topic_info).collect::<Vec<_>>();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    fn low_watermark(&self, topic: &str, partition: i32) -> Result<i64> {
        self.consumer
            .fetch_watermarks(topic, partition, Timeout::After(Duration::from_secs(5)))
            .map(|(low, _)| low)
            .map_err(kafka_query_error)
    }
}

fn kafka_config(dsn: &Dsn, brokers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("client.id", "dbtool")
        .set("socket.timeout.ms", "5000");
    apply_client_params(&mut config, dsn);
    config
}

fn brokers_from_dsn(dsn: &Dsn) -> String {
    let host = dsn.host.clone().unwrap_or_else(|| "localhost".into());
    let port = dsn.port.unwrap_or(9092);
    format!("{host}:{port}")
}

fn apply_client_params(config: &mut ClientConfig, dsn: &Dsn) {
    if let Some(username) = dsn.username.as_deref() {
        config.set("sasl.username", username);
    }
    if let Some(password) = dsn.password.as_deref() {
        config.set("sasl.password", password);
    }

    for (key, value) in &dsn.params {
        if let Some(config_key) = kafka_config_key(key) {
            config.set(config_key, value);
        }
    }
}

fn kafka_config_key(key: &str) -> Option<&str> {
    match key {
        "security.protocol" | "security-protocol" => Some("security.protocol"),
        "sasl.mechanism" | "sasl-mechanism" => Some("sasl.mechanism"),
        "sasl.username" | "sasl-username" => Some("sasl.username"),
        "sasl.password" | "sasl-password" => Some("sasl.password"),
        "ssl.ca.location" | "ssl-ca-location" | "tls-ca" | "ssl-ca" => Some("ssl.ca.location"),
        "ssl.certificate.location" | "ssl-certificate-location" => Some("ssl.certificate.location"),
        "ssl.key.location" | "ssl-key-location" => Some("ssl.key.location"),
        "ssl.key.password" | "ssl-key-password" => Some("ssl.key.password"),
        _ => key
            .strip_prefix("kafka.")
            .filter(|stripped| !stripped.is_empty()),
    }
}

fn topic_info(topic: &rdkafka::metadata::MetadataTopic) -> TopicInfo {
    let replicas = topic
        .partitions()
        .first()
        .map(|partition| partition.replicas().len() as i16)
        .unwrap_or(0);
    TopicInfo {
        name: topic.name().to_owned(),
        partitions: topic.partitions().len() as i32,
        replicas,
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

fn validate_native_produce_message(message: &Message) -> Result<()> {
    validate_produce_message(message)?;
    if let Some(key) = message.headers.keys().find(|key| key.contains('\0')) {
        return Err(Error::Config(format!(
            "Kafka header name must not contain a NUL byte: {key:?}"
        )));
    }
    Ok(())
}

fn core_headers_to_native(headers: &HashMap<String, String>) -> OwnedHeaders {
    headers.iter().fold(
        OwnedHeaders::new_with_capacity(headers.len()),
        |headers, (key, value)| {
            headers.insert(Header {
                key,
                value: Some(value.as_bytes()),
            })
        },
    )
}

fn native_headers_to_core<H: Headers>(headers: Option<&H>) -> HashMap<String, String> {
    headers
        .into_iter()
        .flat_map(Headers::iter)
        .map(|header| {
            (
                header.key.to_owned(),
                String::from_utf8_lossy(header.value.unwrap_or_default()).into_owned(),
            )
        })
        .collect()
}

fn remaining_poll_timeout(deadline: Instant) -> Duration {
    deadline
        .checked_duration_since(Instant::now())
        .unwrap_or_default()
        .min(Duration::from_millis(100))
}

fn kafka_connection_error(error: KafkaError) -> Error {
    Error::Connection(error.to_string())
}

fn kafka_query_error(error: KafkaError) -> Error {
    Error::Query(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(headers: HashMap<String, String>) -> Message {
        Message {
            key: Some(Bytes::from_static(b"key")),
            payload: Bytes::from_static(b"payload"),
            headers,
            partition: Some(2),
            offset: None,
            timestamp: Some(1_710_000_000_123),
        }
    }

    #[test]
    fn native_headers_round_trip_without_loss() {
        let expected = HashMap::from([
            ("traceparent".to_owned(), "00-abc".to_owned()),
            ("content-type".to_owned(), "application/json".to_owned()),
        ]);
        let native = core_headers_to_native(&expected);

        assert_eq!(native_headers_to_core(Some(&native)), expected);
    }

    #[test]
    fn native_produce_validation_rejects_nul_header_names() {
        let error = validate_native_produce_message(&message(HashMap::from([(
            "bad\0header".to_owned(),
            "value".to_owned(),
        )])))
        .unwrap_err();

        assert!(matches!(error, Error::Config(_)));
        assert!(error.to_string().contains("NUL"));
    }

    #[test]
    fn absent_native_headers_decode_to_empty_map() {
        assert!(native_headers_to_core::<OwnedHeaders>(None).is_empty());
    }
}
