// Native backend via librdkafka/rdkafka.
use bytes::Bytes;
use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ConsumeOptions, DeleteResourceOptions, DeleteResourceOutcome, LagInfo, Message,
        MessageCursor, MessagePlacement, MessageResource, PartitionWatermark, ProduceOutcome,
        TopicDetail, TopicInfo,
    },
    port::{
        capability::{AdminInspect, AdminMutate, MessageConsumer, MessageProducer},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
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

use super::{
    kafka_messages_before, resolve_consume_position, validate_kafka_delete_request,
    validate_produce_message,
};

pub struct RdkafkaAdapter {
    producer: FutureProducer,
    consumer_config: ClientConfig,
    admin: AdminClient<DefaultClientContext>,
    kind: ConnectorKind,
}

pub fn connect(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let brokers = brokers_from_dsn(&dsn);
        let consumer_config = kafka_config(&dsn, &brokers);
        let producer = consumer_config
            .clone()
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>()
            .map_err(kafka_connection_error)?;
        let admin = consumer_config
            .create::<AdminClient<DefaultClientContext>>()
            .map_err(kafka_connection_error)?;

        Ok(Box::new(RdkafkaAdapter {
            producer,
            consumer_config,
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

    fn operations(&self) -> Vec<CapabilityOperation> {
        let mut operations = self.capabilities().operations();
        operations.extend([
            CapabilityOperation::MessageAdminListTopics,
            CapabilityOperation::MessageAdminTopicDetail,
            CapabilityOperation::MessageAdminConsumerLag,
            CapabilityOperation::MessageAdminDelete,
        ]);
        operations
    }

    async fn ping(&self) -> Result<()> {
        self.consumer("dbtool")?
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

    fn as_admin_mutate(&self) -> Option<&dyn AdminMutate> {
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
            placements.push(MessagePlacement {
                partition,
                offset,
                cursor: Some(MessageCursor::Kafka {
                    topic: target.to_owned(),
                    partition,
                    offset,
                }),
            });
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
        let (requested_partition, requested_offset) = resolve_consume_position(&options)?;
        if options.max == 0 {
            return Ok(vec![]);
        }

        let deadline = consume_deadline(options.timeout)?;
        let consumer = self.consumer("dbtool")?;
        let Some(remaining) = remaining_until(deadline) else {
            return Ok(vec![]);
        };
        let topics = self.topic_infos_with(&consumer, remaining)?;
        let topic = require_topic(topics, source)?;
        let partitions = if let Some(partition) = requested_partition {
            vec![partition]
        } else {
            (0..topic.partitions).collect()
        };
        let mut assignment = TopicPartitionList::new();
        for partition in partitions {
            let offset = match requested_offset {
                Some(offset) => offset,
                None => {
                    let Some(remaining) = remaining_until(deadline) else {
                        return Ok(vec![]);
                    };
                    self.low_watermark_with(&consumer, source, partition, remaining)?
                }
            };
            assignment
                .add_partition_offset(source, partition, Offset::Offset(offset))
                .map_err(kafka_query_error)?;
        }
        consumer.assign(&assignment).map_err(kafka_query_error)?;

        let mut messages = Vec::new();
        while messages.len() < options.max && Instant::now() < deadline {
            let poll_timeout = remaining_poll_timeout(deadline);
            match consumer.poll(poll_timeout) {
                Some(Ok(message)) => {
                    messages.push(Message {
                        key: message.key().map(Bytes::copy_from_slice),
                        payload: Bytes::copy_from_slice(message.payload().unwrap_or_default()),
                        headers: native_headers_to_core(message.headers()),
                        partition: Some(message.partition()),
                        offset: Some(message.offset()),
                        timestamp: message.timestamp().to_millis(),
                        cursor: Some(MessageCursor::Kafka {
                            topic: source.to_owned(),
                            partition: message.partition(),
                            offset: message.offset(),
                        }),
                        metadata: None,
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
        let consumer = self.consumer("dbtool")?;
        let metadata = consumer
            .fetch_metadata(Some(name), Timeout::After(Duration::from_secs(5)))
            .map_err(kafka_connection_error)?;
        let topic = metadata
            .topics()
            .iter()
            .find(|topic| topic.name() == name)
            .ok_or_else(|| Error::Query(format!("topic not found: {name}")))?;
        if let Some(error) = topic.error() {
            return Err(Error::Query(format!(
                "Kafka topic {name:?} is unavailable: {error:?}"
            )));
        }
        let info = topic_info(topic);
        let mut watermarks = Vec::new();
        for partition in topic.partitions() {
            let (low, high) = consumer
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

    async fn consumer_lag(&self, group: &str) -> Result<Vec<LagInfo>> {
        let group = normalize_consumer_group(group)?;
        let consumer = self.consumer(group)?;
        let deadline = consume_deadline(Duration::from_secs(5))?;
        let metadata = consumer
            .fetch_metadata(
                None,
                native_timeout(remaining_until(deadline).ok_or(Error::Timeout)?),
            )
            .map_err(kafka_connection_error)?;

        let mut requested = TopicPartitionList::new();
        for topic in metadata
            .topics()
            .iter()
            .filter(|topic| !topic.name().starts_with("__"))
        {
            if let Some(error) = topic.error() {
                return Err(Error::Query(format!(
                    "failed to inspect Kafka topic {:?}: {error:?}",
                    topic.name()
                )));
            }
            for partition in topic.partitions() {
                if let Some(error) = partition.error() {
                    return Err(Error::Query(format!(
                        "failed to inspect Kafka topic {:?} partition {}: {error:?}",
                        topic.name(),
                        partition.id()
                    )));
                }
                requested.add_partition(topic.name(), partition.id());
            }
        }

        if requested.count() == 0 {
            return Ok(vec![]);
        }
        let committed = consumer
            .committed_offsets(
                requested,
                native_timeout(remaining_until(deadline).ok_or(Error::Timeout)?),
            )
            .map_err(kafka_query_error)?;
        let mut lag = Vec::with_capacity(committed.count());
        for entry in committed.elements() {
            entry.error().map_err(kafka_query_error)?;
            let Some(committed_offset) = committed_offset(entry.offset())? else {
                continue;
            };
            let (_, latest) = consumer
                .fetch_watermarks(
                    entry.topic(),
                    entry.partition(),
                    native_timeout(remaining_until(deadline).ok_or(Error::Timeout)?),
                )
                .map_err(kafka_query_error)?;
            lag.push(lag_info(
                group,
                entry.topic(),
                entry.partition(),
                committed_offset,
                latest,
            )?);
        }
        lag.sort_by(|a, b| {
            a.topic
                .cmp(&b.topic)
                .then_with(|| a.partition.cmp(&b.partition))
        });
        Ok(lag)
    }
}

#[async_trait::async_trait]
impl AdminMutate for RdkafkaAdapter {
    async fn delete_resource(
        &self,
        resource: MessageResource,
        options: DeleteResourceOptions,
    ) -> Result<DeleteResourceOutcome> {
        validate_kafka_delete_request(&resource, options)?;
        validate_topic(&resource.name)?;

        let detail = self.topic_detail(&resource.name).await?;
        let messages_before = kafka_messages_before(&detail.watermarks);
        let admin_options = AdminOptions::new()
            .request_timeout(Some(Duration::from_secs(5)))
            .operation_timeout(Some(Duration::from_secs(5)));
        let results = self
            .admin
            .delete_topics(&[resource.name.as_str()], &admin_options)
            .await
            .map_err(kafka_query_error)?;
        require_deleted_topic(results, &resource.name)?;
        self.wait_for_topic_absence(&resource.name).await?;

        Ok(DeleteResourceOutcome {
            resource,
            acknowledged: true,
            verified_absent: true,
            messages_before,
            consumers_before: None,
        })
    }
}

impl RdkafkaAdapter {
    fn consumer(&self, group: &str) -> Result<BaseConsumer> {
        consumer_client_config(&self.consumer_config, group)?
            .create::<BaseConsumer>()
            .map_err(kafka_connection_error)
    }

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
        let consumer = self.consumer("dbtool")?;
        self.topic_infos_with(&consumer, Duration::from_secs(5))
    }

    fn topic_infos_with(
        &self,
        consumer: &BaseConsumer,
        timeout: Duration,
    ) -> Result<Vec<TopicInfo>> {
        let metadata = consumer
            .fetch_metadata(None, native_timeout(timeout))
            .map_err(kafka_connection_error)?;
        let mut topics = metadata.topics().iter().map(topic_info).collect::<Vec<_>>();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    fn low_watermark_with(
        &self,
        consumer: &BaseConsumer,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> Result<i64> {
        consumer
            .fetch_watermarks(topic, partition, native_timeout(timeout))
            .map(|(low, _)| low)
            .map_err(kafka_query_error)
    }

    async fn wait_for_topic_absence(&self, name: &str) -> Result<()> {
        let consumer = self.consumer("dbtool")?;
        let deadline = consume_deadline(Duration::from_secs(5))?;
        loop {
            let Some(remaining) = remaining_until(deadline) else {
                return Err(topic_deletion_not_verified(name));
            };
            let topics = self.topic_infos_with(&consumer, remaining)?;
            if !topics.iter().any(|topic| topic.name == name) {
                return Ok(());
            }

            let Some(remaining) = remaining_until(deadline) else {
                return Err(topic_deletion_not_verified(name));
            };
            tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
        }
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

fn consumer_client_config(base: &ClientConfig, group: &str) -> Result<ClientConfig> {
    let group = normalize_consumer_group(group)?;
    let mut config = base.clone();
    config
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest");
    Ok(config)
}

fn normalize_consumer_group(group: &str) -> Result<&str> {
    let group = group.trim();
    if group.is_empty() {
        return Err(Error::Config(
            "Kafka consumer group must not be empty".to_owned(),
        ));
    }
    Ok(group)
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

fn require_topic(topics: Vec<TopicInfo>, name: &str) -> Result<TopicInfo> {
    topics
        .into_iter()
        .find(|topic| topic.name == name)
        .ok_or_else(|| Error::Query(format!("topic not found: {name}")))
}

fn committed_offset(offset: Offset) -> Result<Option<i64>> {
    match offset {
        Offset::Offset(offset) if offset >= 0 => Ok(Some(offset)),
        Offset::Invalid => Ok(None),
        offset => Err(Error::Query(format!(
            "unexpected Kafka committed offset: {offset:?}"
        ))),
    }
}

fn lag_info(
    group: &str,
    topic: &str,
    partition: i32,
    committed: i64,
    latest: i64,
) -> Result<LagInfo> {
    if latest < 0 {
        return Err(Error::Query(format!(
            "Kafka returned a negative high watermark for {topic} partition {partition}: {latest}"
        )));
    }
    Ok(LagInfo {
        topic: topic.to_owned(),
        partition,
        group: group.to_owned(),
        committed,
        latest,
        lag: latest.saturating_sub(committed).max(0),
    })
}

fn require_deleted_topic(
    mut results: Vec<rdkafka::admin::TopicResult>,
    expected: &str,
) -> Result<()> {
    if results.len() != 1 {
        return Err(Error::Query(format!(
            "Kafka returned {} delete results for one requested topic",
            results.len()
        )));
    }
    match results.pop().expect("length checked above") {
        Ok(topic) if topic == expected => Ok(()),
        Ok(topic) => Err(Error::Query(format!(
            "Kafka acknowledged deletion for topic {topic:?} instead of {expected:?}"
        ))),
        Err((topic, error)) => Err(Error::Query(format!(
            "failed to delete Kafka topic {topic:?}: {error}"
        ))),
    }
}

fn topic_deletion_not_verified(name: &str) -> Error {
    Error::Query(format!(
        "Kafka acknowledged deletion of topic {name:?}, but its absence was not verified within 5 seconds"
    ))
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

fn consume_deadline(timeout: Duration) -> Result<Instant> {
    Instant::now().checked_add(timeout).ok_or_else(|| {
        Error::Config("Kafka consume timeout is too large for this platform".to_owned())
    })
}

fn remaining_until(deadline: Instant) -> Option<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
}

fn native_timeout(timeout: Duration) -> Timeout {
    Timeout::After(timeout.min(Duration::from_millis(i32::MAX as u64)))
}

fn remaining_poll_timeout(deadline: Instant) -> Duration {
    remaining_until(deadline)
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
    use rdkafka::consumer::CommitMode;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn message(headers: HashMap<String, String>) -> Message {
        Message {
            key: Some(Bytes::from_static(b"key")),
            payload: Bytes::from_static(b"payload"),
            headers,
            partition: Some(2),
            offset: None,
            timestamp: Some(1_710_000_000_123),
            cursor: None,
            metadata: None,
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

    #[test]
    fn consumer_configs_are_isolated_per_group() {
        let mut base = ClientConfig::new();
        base.set("bootstrap.servers", "127.0.0.1:9092")
            .set("group.id", "dsn-group")
            .set("enable.auto.commit", "true");

        let first = consumer_client_config(&base, "group-a").unwrap();
        let second = consumer_client_config(&base, "group-b").unwrap();

        assert_eq!(base.get("group.id"), Some("dsn-group"));
        assert_eq!(base.get("enable.auto.commit"), Some("true"));
        assert_eq!(first.get("group.id"), Some("group-a"));
        assert_eq!(second.get("group.id"), Some("group-b"));
        assert_eq!(first.get("enable.auto.commit"), Some("false"));
        assert_eq!(first.get("enable.partition.eof"), Some("true"));
        assert_eq!(first.get("auto.offset.reset"), Some("earliest"));
    }

    #[test]
    fn empty_consumer_group_is_rejected() {
        let error = consumer_client_config(&ClientConfig::new(), "  ").unwrap_err();

        assert!(matches!(error, Error::Config(_)));
        assert!(error.to_string().contains("must not be empty"));
    }

    #[test]
    fn missing_topic_is_a_query_error() {
        let error = require_topic(vec![], "missing-topic").unwrap_err();

        assert!(matches!(error, Error::Query(_)));
        assert!(error.to_string().contains("missing-topic"));
    }

    #[test]
    fn lag_uses_committed_offset_and_high_watermark() {
        assert_eq!(committed_offset(Offset::Invalid).unwrap(), None);
        assert_eq!(committed_offset(Offset::Offset(7)).unwrap(), Some(7));

        let lag = lag_info("group-a", "orders", 2, 7, 12).unwrap();
        assert_eq!(lag.topic, "orders");
        assert_eq!(lag.partition, 2);
        assert_eq!(lag.group, "group-a");
        assert_eq!(lag.committed, 7);
        assert_eq!(lag.latest, 12);
        assert_eq!(lag.lag, 5);
    }

    #[test]
    fn native_timeouts_do_not_overflow_librdkafka_milliseconds() {
        assert_eq!(
            native_timeout(Duration::MAX),
            Timeout::After(Duration::from_millis(i32::MAX as u64))
        );
    }

    #[test]
    fn native_delete_requires_one_matching_success_result() {
        require_deleted_topic(vec![Ok("events".to_owned())], "events").unwrap();

        let wrong_topic =
            require_deleted_topic(vec![Ok("other".to_owned())], "events").unwrap_err();
        assert!(matches!(wrong_topic, Error::Query(_)));
        assert!(wrong_topic.to_string().contains("instead of"));

        let broker_error = require_deleted_topic(
            vec![Err((
                "events".to_owned(),
                RDKafkaErrorCode::TopicAlreadyExists,
            ))],
            "events",
        )
        .unwrap_err();
        assert!(matches!(broker_error, Error::Query(_)));
        assert!(broker_error.to_string().contains("failed to delete"));

        let missing_result = require_deleted_topic(vec![], "events").unwrap_err();
        assert!(matches!(missing_result, Error::Query(_)));
    }

    #[tokio::test]
    async fn live_consumer_lag_reads_a_real_committed_group_offset() {
        if std::env::var("DBTOOL_RUN_KAFKA_NATIVE_LIVE").as_deref() != Ok("1") {
            return;
        }
        let raw_dsn = std::env::var("DBTOOL_IT_KAFKA_DSN")
            .expect("DBTOOL_IT_KAFKA_DSN is required for the native live test");
        let dsn = Dsn::parse(&raw_dsn).expect("native live Kafka DSN should parse");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after Unix epoch")
            .as_millis();
        let topic = format!("dbtool_it_native_lag_{}_{}", std::process::id(), unique);
        let group = format!("dbtool-it-native-lag-{}-{unique}", std::process::id());

        let connector = connect(dsn.clone()).await.expect("Kafka should connect");
        let messages = ["first", "second"]
            .into_iter()
            .map(|payload| Message {
                key: None,
                payload: Bytes::copy_from_slice(payload.as_bytes()),
                headers: HashMap::new(),
                partition: Some(0),
                offset: None,
                timestamp: None,
                cursor: None,
                metadata: None,
            })
            .collect();
        connector
            .as_producer()
            .expect("native Kafka exposes MessageProducer")
            .produce(&topic, messages)
            .await
            .expect("messages should be produced");

        let brokers = brokers_from_dsn(&dsn);
        let base = kafka_config(&dsn, &brokers);
        let consumer = consumer_client_config(&base, &group)
            .expect("consumer config should be valid")
            .create::<BaseConsumer>()
            .expect("group consumer should be created");
        let mut committed = TopicPartitionList::new();
        committed
            .add_partition_offset(&topic, 0, Offset::Offset(1))
            .expect("group offset should be representable");
        consumer
            .commit(&committed, CommitMode::Sync)
            .expect("group offset should commit");

        let lag = connector
            .as_admin()
            .expect("native Kafka exposes AdminInspect")
            .consumer_lag(&group)
            .await
            .expect("group lag should be readable");
        let item = lag
            .iter()
            .find(|item| item.topic == topic && item.partition == 0)
            .expect("lag should include the test topic");
        assert_eq!(item.group, group);
        assert_eq!(item.committed, 1);
        assert!(item.latest >= 2);
        assert_eq!(item.lag, item.latest - 1);

        let admin = base
            .create::<AdminClient<DefaultClientContext>>()
            .expect("cleanup admin client should be created");
        let deleted = admin
            .delete_topics(&[&topic], &AdminOptions::new())
            .await
            .expect("cleanup topic deletion request should succeed");
        assert_eq!(deleted.len(), 1);
        assert!(
            deleted[0].is_ok(),
            "cleanup topic deletion failed: {deleted:?}"
        );
    }
}
