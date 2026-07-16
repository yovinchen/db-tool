// Native backend via librdkafka/rdkafka.
use bytes::Bytes;
use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        AckMode, BoundedList, ConsumeOptions, ConsumerIdentity, DeleteResourceOptions,
        DeleteResourceOutcome, LagInfo, Message, MessageCursor, MessagePlacement, MessageResource,
        MetadataBudget, PartitionWatermark, ProduceOutcome, TopicDetail, TopicInfo,
    },
    port::{
        capability::{AdminInspect, AdminMutate, MessageConsumer, MessageProducer},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{ListLimiter, MessageReadLimiter, MetadataLimiter},
};
use futures::future::BoxFuture;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, CommitMode, Consumer},
    error::{KafkaError, RDKafkaErrorCode},
    message::{BorrowedMessage, Header, Headers, Message as KafkaMessage, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    Offset, TopicPartitionList,
};
use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use super::{
    kafka_messages_before, resolve_consume_position, validate_kafka_consume_options,
    validate_kafka_delete_request, validate_produce_message,
};

// Kafka metadata has no pagination. librdkafka enforces this receive-frame
// ceiling before exposing a decoded metadata object to the adapter.
const KAFKA_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const KAFKA_RESPONSE_OVERHEAD_BYTES: usize = 512;
const KAFKA_MAX_FETCH_BYTES: usize = KAFKA_MAX_RESPONSE_BYTES - KAFKA_RESPONSE_OVERHEAD_BYTES;
const KAFKA_DELETE_MAX_PARTITIONS: usize = 100_000;

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
        native_operations(self.capabilities())
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

fn native_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::MessageConsumeGroup,
        CapabilityOperation::MessageConsumeAck,
        CapabilityOperation::MessageAdminListTopics,
        CapabilityOperation::MessageAdminListTopicsBounded,
        CapabilityOperation::MessageAdminTopicDetail,
        CapabilityOperation::MessageAdminTopicDetailBounded,
        CapabilityOperation::MessageAdminConsumerLag,
        CapabilityOperation::MessageAdminConsumerLagBounded,
        CapabilityOperation::MessageAdminDelete,
    ]);
    operations
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
        validate_kafka_consume_options(&options)?;
        let group = native_consumer_group(&self.kind.0, &options)?;
        let deadline = consume_deadline(options.timeout)?;
        match group {
            Some(group) => self.consume_group(source, group, &options, deadline),
            None => self.consume_stateless(source, &options, deadline),
        }
    }
}

#[async_trait::async_trait]
impl AdminInspect for RdkafkaAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        self.topic_infos()
    }

    async fn list_topics_bounded(&self, max_items: usize) -> Result<BoundedList<TopicInfo>> {
        let limiter = ListLimiter::new(max_items);
        let probe_items = limiter.probe_items()?;
        // Metadata uses a separately capped consumer so ordinary production
        // and consumption retain the caller's established frame limits.
        let consumer = bounded_catalog_consumer(&self.consumer_config)?;
        let metadata = consumer
            .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))
            .map_err(kafka_connection_error)?;
        let mut topics = metadata
            .topics()
            .iter()
            .take(probe_items)
            .map(topic_info)
            .collect::<Vec<_>>();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(limiter.finish(topics))
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

    async fn topic_detail_bounded(
        &self,
        name: &str,
        budget: MetadataBudget,
    ) -> Result<TopicDetail> {
        let consumer = bounded_admin_consumer(&self.consumer_config, "dbtool-topic-detail")?;
        self.topic_detail_with_bounded_consumer(&consumer, name, budget)
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

    async fn consumer_lag_bounded(
        &self,
        group: &str,
        budget: MetadataBudget,
    ) -> Result<Vec<LagInfo>> {
        let budget = budget.validate()?;
        let group = normalize_consumer_group(group)?;
        // Preserve the requested group identity, then force receive ceilings
        // after every DSN override. Reusing the catalog helper here would
        // silently query committed offsets for "dbtool-catalog" instead.
        let consumer = bounded_admin_consumer(&self.consumer_config, group)?;
        let deadline = consume_deadline(Duration::from_secs(5))?;
        let metadata = consumer
            .fetch_metadata(
                None,
                native_timeout(remaining_until(deadline).ok_or(Error::Timeout)?),
            )
            .map_err(kafka_connection_error)?;
        let mut response_limiter = MetadataLimiter::new(budget, "Kafka consumer lag")?;
        let mut inspected_partitions = 0_usize;
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
                observe_kafka_lag_work(&mut inspected_partitions, budget)?;
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
            response_limiter.ensure_complete(&Vec::<LagInfo>::new())?;
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
            let item = lag_info(
                group,
                entry.topic(),
                entry.partition(),
                committed_offset,
                latest,
            )?;
            response_limiter.observe(&item)?;
            lag.push(item);
        }
        lag.sort_by(|a, b| {
            a.topic
                .cmp(&b.topic)
                .then_with(|| a.partition.cmp(&b.partition))
        });
        response_limiter.ensure_complete(&lag)?;
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

        let bounded_consumer =
            bounded_admin_consumer(&self.consumer_config, "dbtool-delete-inspect")?;
        let detail = self.topic_detail_with_bounded_consumer(
            &bounded_consumer,
            &resource.name,
            MetadataBudget::new(KAFKA_DELETE_MAX_PARTITIONS, KAFKA_MAX_RESPONSE_BYTES)?,
        )?;
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
        self.wait_for_topic_absence_with(&bounded_consumer, &resource.name)
            .await?;

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

    fn topic_detail_with_bounded_consumer(
        &self,
        consumer: &BaseConsumer,
        name: &str,
        budget: MetadataBudget,
    ) -> Result<TopicDetail> {
        validate_topic(name)?;
        let mut limiter = MetadataLimiter::new(budget, format!("Kafka topic detail {name}"))?;
        let deadline = consume_deadline(Duration::from_secs(5))?;
        let metadata = consumer
            .fetch_metadata(
                Some(name),
                native_timeout(remaining_until(deadline).ok_or(Error::Timeout)?),
            )
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
        if topic.partitions().len() > budget.max_items {
            return Err(Error::MetadataBudgetExceeded {
                subject: format!("Kafka topic detail {name}"),
                unit: "items",
                limit: budget.max_items,
            });
        }
        let mut info = topic_info(topic);
        info.partitions = i32::try_from(topic.partitions().len())
            .map_err(|_| Error::Serialization("Kafka partition count exceeds i32".into()))?;
        let mut watermarks = Vec::with_capacity(topic.partitions().len());
        for partition in topic.partitions() {
            if let Some(error) = partition.error() {
                return Err(Error::Query(format!(
                    "Kafka topic {name:?} partition {} is unavailable: {error:?}",
                    partition.id()
                )));
            }
            let (low, high) = consumer
                .fetch_watermarks(
                    name,
                    partition.id(),
                    native_timeout(remaining_until(deadline).ok_or(Error::Timeout)?),
                )
                .map_err(kafka_query_error)?;
            let watermark = PartitionWatermark {
                partition: partition.id(),
                low,
                high,
            };
            limiter.observe(&watermark)?;
            watermarks.push(watermark);
        }
        let detail = TopicDetail {
            info,
            config: HashMap::new(),
            watermarks,
        };
        limiter.ensure_complete(&detail)?;
        Ok(detail)
    }

    fn consume_stateless(
        &self,
        source: &str,
        options: &ConsumeOptions,
        deadline: Instant,
    ) -> Result<Vec<Message>> {
        let (requested_partition, requested_offset) = resolve_consume_position(options)?;
        let consumer = self.consumer("dbtool")?;
        let Some(remaining) = remaining_until(deadline) else {
            return MessageReadLimiter::new(options, "Kafka consume")?.finish(vec![]);
        };
        let topic = require_topic(self.topic_infos_with(&consumer, remaining)?, source)?;
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
                        return MessageReadLimiter::new(options, "Kafka consume")?.finish(vec![]);
                    };
                    self.low_watermark_with(&consumer, source, partition, remaining)?
                }
            };
            assignment
                .add_partition_offset(source, partition, Offset::Offset(offset))
                .map_err(kafka_query_error)?;
        }
        consumer.assign(&assignment).map_err(kafka_query_error)?;
        collect_native_messages(&consumer, source, options, deadline)
    }

    fn consume_group(
        &self,
        source: &str,
        group: &str,
        options: &ConsumeOptions,
        deadline: Instant,
    ) -> Result<Vec<Message>> {
        let consumer = self.consumer(group)?;
        let Some(remaining) = remaining_until(deadline) else {
            return MessageReadLimiter::new(options, "Kafka consume")?.finish(vec![]);
        };
        require_topic(self.topic_infos_with(&consumer, remaining)?, source)?;
        consumer.subscribe(&[source]).map_err(kafka_query_error)?;

        let messages = collect_native_messages(&consumer, source, options, deadline)?;
        if options.ack == AckMode::OnSuccess && !messages.is_empty() {
            let offsets = next_offsets_for_batch(source, &messages)?;
            consumer
                .commit(&offsets, CommitMode::Sync)
                .map_err(kafka_query_error)?;
        }
        consumer.unsubscribe();
        Ok(messages)
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

    async fn wait_for_topic_absence_with(&self, consumer: &BaseConsumer, name: &str) -> Result<()> {
        let deadline = consume_deadline(Duration::from_secs(5))?;
        loop {
            let Some(remaining) = remaining_until(deadline) else {
                return Err(topic_deletion_not_verified(name));
            };
            let topics = self.topic_infos_with(consumer, remaining)?;
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

fn native_consumer_group<'a>(kind: &str, options: &'a ConsumeOptions) -> Result<Option<&'a str>> {
    options
        .validate()
        .map_err(|message| Error::Config(format!("Kafka consume: {message}")))?;
    match &options.identity {
        ConsumerIdentity::Stateless if options.ack == AckMode::None => Ok(None),
        ConsumerIdentity::Stateless => Err(Error::Config(
            "Kafka --ack on-success requires a consumer group".to_owned(),
        )),
        ConsumerIdentity::Group {
            group,
            member: None,
        } => normalize_consumer_group(group).map(Some),
        ConsumerIdentity::Group {
            member: Some(_), ..
        } => Err(Error::Config(
            "Kafka's short-lived consume operation cannot safely retain a static member; omit --consumer"
                .to_owned(),
        )),
        ConsumerIdentity::Durable { .. } => Err(Error::UnsupportedCapability {
            kind: kind.to_owned(),
            needed: CapabilityOperation::MessageConsumeDurable.as_str(),
        }),
    }
}

fn collect_native_messages(
    consumer: &BaseConsumer,
    source: &str,
    options: &ConsumeOptions,
    deadline: Instant,
) -> Result<Vec<Message>> {
    let mut messages = Vec::new();
    let mut read_limiter = MessageReadLimiter::new(options, "Kafka consume")?;
    while messages.len() < options.max && Instant::now() < deadline {
        match consumer.poll(remaining_poll_timeout(deadline)) {
            Some(Ok(message)) => {
                let message = native_message_to_core(source, &message)?;
                read_limiter.observe(&message)?;
                messages.push(message);
            }
            Some(Err(KafkaError::PartitionEOF(_))) | None => {}
            Some(Err(error)) => return Err(kafka_query_error(error)),
        }
    }
    read_limiter.finish(messages)
}

fn native_message_to_core(expected_topic: &str, message: &BorrowedMessage<'_>) -> Result<Message> {
    if message.topic() != expected_topic {
        return Err(Error::Serialization(format!(
            "Kafka group returned topic {:?} while consuming {expected_topic:?}",
            message.topic()
        )));
    }
    let partition = message.partition();
    let offset = message.offset();
    if partition < 0 || offset < 0 {
        return Err(Error::Serialization(format!(
            "Kafka returned invalid position {partition}:{offset}"
        )));
    }
    let payload = message.payload().ok_or_else(|| {
        Error::Serialization(
            "Kafka tombstone payload cannot be represented by the portable Message model".into(),
        )
    })?;
    Ok(Message {
        key: message.key().map(Bytes::copy_from_slice),
        payload: Bytes::copy_from_slice(payload),
        headers: native_headers_to_core(message.headers())?,
        partition: Some(partition),
        offset: Some(offset),
        timestamp: message.timestamp().to_millis(),
        cursor: Some(MessageCursor::Kafka {
            topic: message.topic().to_owned(),
            partition,
            offset,
        }),
        metadata: None,
    })
}

fn next_offsets_for_batch(topic: &str, messages: &[Message]) -> Result<TopicPartitionList> {
    let mut next_by_partition = BTreeMap::<i32, i64>::new();
    for message in messages {
        let Some(MessageCursor::Kafka {
            topic: message_topic,
            partition,
            offset,
        }) = &message.cursor
        else {
            return Err(Error::Serialization(
                "Kafka commit batch contains a message without an exact Kafka cursor".into(),
            ));
        };
        if message_topic != topic || *partition < 0 || *offset < 0 {
            return Err(Error::Serialization(format!(
                "Kafka commit cursor does not match topic {topic:?}: {message_topic:?}:{partition}:{offset}"
            )));
        }
        let next = offset.checked_add(1).ok_or_else(|| {
            Error::Serialization(format!(
                "Kafka offset overflow while committing {message_topic:?}:{partition}:{offset}"
            ))
        })?;
        next_by_partition
            .entry(*partition)
            .and_modify(|current| *current = (*current).max(next))
            .or_insert(next);
    }

    let mut offsets = TopicPartitionList::new();
    for (partition, offset) in next_by_partition {
        offsets
            .add_partition_offset(topic, partition, Offset::Offset(offset))
            .map_err(kafka_query_error)?;
    }
    Ok(offsets)
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

fn bounded_catalog_consumer(base: &ClientConfig) -> Result<BaseConsumer> {
    bounded_catalog_config(base)?
        .create::<BaseConsumer>()
        .map_err(kafka_connection_error)
}

fn bounded_catalog_config(base: &ClientConfig) -> Result<ClientConfig> {
    bounded_admin_config(base, "dbtool-catalog")
}

fn bounded_admin_consumer(base: &ClientConfig, group: &str) -> Result<BaseConsumer> {
    bounded_admin_config(base, group)?
        .create::<BaseConsumer>()
        .map_err(kafka_connection_error)
}

fn bounded_admin_config(base: &ClientConfig, group: &str) -> Result<ClientConfig> {
    let mut config = consumer_client_config(base, group)?;
    freeze_consumer_receive_budget(&mut config);
    Ok(config)
}

fn freeze_consumer_receive_budget(config: &mut ClientConfig) {
    // Apply only after every DSN parameter has entered the base config. This
    // protects ordinary consumption as well as metadata/lag clients from a
    // caller raising librdkafka's receive frame ceilings.
    config.set(
        "receive.message.max.bytes",
        KAFKA_MAX_RESPONSE_BYTES.to_string(),
    );
    // librdkafka requires the receive ceiling to exceed the aggregate fetch
    // ceiling by at least 512 bytes. Keep the per-partition ceiling inside the
    // same hard response envelope.
    config
        .set("fetch.max.bytes", KAFKA_MAX_FETCH_BYTES.to_string())
        .set(
            "max.partition.fetch.bytes",
            KAFKA_MAX_FETCH_BYTES.to_string(),
        );
}

fn observe_kafka_lag_work(observed: &mut usize, budget: MetadataBudget) -> Result<()> {
    if *observed >= budget.max_items {
        return Err(Error::MetadataBudgetExceeded {
            subject: "Kafka consumer lag scan".to_owned(),
            unit: "items",
            limit: budget.max_items,
        });
    }
    *observed = observed
        .checked_add(1)
        .ok_or_else(|| Error::Query("Kafka consumer lag scan item count overflow".into()))?;
    Ok(())
}

fn consumer_client_config(base: &ClientConfig, group: &str) -> Result<ClientConfig> {
    let group = normalize_consumer_group(group)?;
    let mut config = base.clone();
    config.remove("group.instance.id");
    config
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest");
    freeze_consumer_receive_budget(&mut config);
    Ok(config)
}

fn normalize_consumer_group(group: &str) -> Result<&str> {
    if group.trim().is_empty() || group != group.trim() || group.chars().any(char::is_control) {
        return Err(Error::Config(
            "Kafka consumer group must be non-empty, have no surrounding whitespace, and contain no control characters"
                .to_owned(),
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

fn native_headers_to_core<H: Headers>(headers: Option<&H>) -> Result<HashMap<String, String>> {
    let mut decoded = HashMap::new();
    for header in headers.into_iter().flat_map(Headers::iter) {
        let value = header.value.ok_or_else(|| {
            Error::Serialization(format!(
                "Kafka header {:?} has a null value that the portable Message model cannot represent",
                header.key
            ))
        })?;
        let value = std::str::from_utf8(value).map_err(|_| {
            Error::Serialization(format!(
                "Kafka header {:?} contains a non-UTF-8 value",
                header.key
            ))
        })?;
        if decoded
            .insert(header.key.to_owned(), value.to_owned())
            .is_some()
        {
            return Err(Error::Serialization(format!(
                "Kafka message contains duplicate header key {:?}",
                header.key
            )));
        }
    }
    Ok(decoded)
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
    use dbtool_core::model::MessageResourceKind;
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

        assert_eq!(native_headers_to_core(Some(&native)).unwrap(), expected);
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
        assert!(native_headers_to_core::<OwnedHeaders>(None)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn unrepresentable_native_headers_fail_instead_of_becoming_different_values() {
        let null_value = OwnedHeaders::new().insert(Header::<&[u8]> {
            key: "nullable",
            value: None,
        });
        assert!(native_headers_to_core(Some(&null_value))
            .unwrap_err()
            .to_string()
            .contains("null value"));

        let non_utf8 = OwnedHeaders::new().insert(Header {
            key: "binary",
            value: Some(&[0xff][..]),
        });
        assert!(native_headers_to_core(Some(&non_utf8))
            .unwrap_err()
            .to_string()
            .contains("non-UTF-8"));

        let duplicates = OwnedHeaders::new()
            .insert(Header {
                key: "trace",
                value: Some("one"),
            })
            .insert(Header {
                key: "trace",
                value: Some("two"),
            });
        assert!(native_headers_to_core(Some(&duplicates))
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
    }

    #[test]
    fn consumer_configs_are_isolated_per_group() {
        let mut base = ClientConfig::new();
        base.set("bootstrap.servers", "127.0.0.1:9092")
            .set("group.id", "dsn-group")
            .set("group.instance.id", "dsn-static-member")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "true");

        let first = consumer_client_config(&base, "group-a").unwrap();
        let second = consumer_client_config(&base, "group-b").unwrap();

        assert_eq!(base.get("group.id"), Some("dsn-group"));
        assert_eq!(base.get("enable.auto.commit"), Some("true"));
        assert_eq!(first.get("group.id"), Some("group-a"));
        assert_eq!(second.get("group.id"), Some("group-b"));
        assert_eq!(first.get("enable.auto.commit"), Some("false"));
        assert_eq!(first.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(first.get("group.instance.id"), None);
        assert_eq!(first.get("enable.partition.eof"), Some("true"));
        assert_eq!(first.get("auto.offset.reset"), Some("earliest"));
    }

    #[test]
    fn empty_consumer_group_is_rejected() {
        let error = consumer_client_config(&ClientConfig::new(), "  ").unwrap_err();

        assert!(matches!(error, Error::Config(_)));
        assert!(error.to_string().contains("non-empty"));

        for invalid in [" group", "group ", "group\nmember"] {
            assert!(consumer_client_config(&ClientConfig::new(), invalid).is_err());
        }
    }

    #[test]
    fn native_consume_contract_is_group_scoped_and_never_claims_durable_state() {
        let operations = native_operations(Capabilities {
            consumer: true,
            ..Default::default()
        });
        assert!(operations.contains(&CapabilityOperation::MessageConsumeGroup));
        assert!(operations.contains(&CapabilityOperation::MessageConsumeAck));
        assert!(operations.contains(&CapabilityOperation::MessageAdminListTopicsBounded));
        assert!(operations.contains(&CapabilityOperation::MessageAdminTopicDetailBounded));
        assert!(operations.contains(&CapabilityOperation::MessageAdminConsumerLagBounded));
        assert!(!operations.contains(&CapabilityOperation::MessageConsumeDurable));

        let stateless = ConsumeOptions::default();
        assert_eq!(native_consumer_group("kafka", &stateless).unwrap(), None);

        let stateless_ack = ConsumeOptions {
            ack: AckMode::OnSuccess,
            ..Default::default()
        };
        assert!(native_consumer_group("kafka", &stateless_ack)
            .unwrap_err()
            .to_string()
            .contains("requires a consumer group"));

        let mut grouped = ConsumeOptions {
            identity: ConsumerIdentity::Group {
                group: "orders".to_owned(),
                member: None,
            },
            ..Default::default()
        };
        assert_eq!(
            native_consumer_group("kafka", &grouped).unwrap(),
            Some("orders")
        );
        grouped.identity = ConsumerIdentity::Group {
            group: "orders".to_owned(),
            member: Some("worker-1".to_owned()),
        };
        assert!(native_consumer_group("kafka", &grouped)
            .unwrap_err()
            .to_string()
            .contains("static member"));

        grouped.identity = ConsumerIdentity::Durable {
            name: "orders".to_owned(),
        };
        assert!(matches!(
            native_consumer_group("kafka", &grouped),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "message.consume_durable"
        ));
    }

    #[test]
    fn kafka_receive_budgets_override_dsn_for_every_consumer_client() {
        let dsn = Dsn::parse(
            "kafka://127.0.0.1:9092?kafka.receive.message.max.bytes=999999999&kafka.fetch.max.bytes=999999999",
        )
        .unwrap();
        let config = kafka_config(&dsn, "127.0.0.1:9092");

        assert_eq!(config.get("receive.message.max.bytes"), Some("999999999"));
        assert_eq!(config.get("fetch.max.bytes"), Some("999999999"));

        let ordinary = consumer_client_config(&config, "ordinary-group").unwrap();
        assert_eq!(ordinary.get("group.id"), Some("ordinary-group"));
        assert_eq!(
            ordinary.get("receive.message.max.bytes"),
            Some(KAFKA_MAX_RESPONSE_BYTES.to_string().as_str())
        );
        assert_eq!(
            ordinary.get("fetch.max.bytes"),
            Some(KAFKA_MAX_FETCH_BYTES.to_string().as_str())
        );
        assert_eq!(
            ordinary.get("max.partition.fetch.bytes"),
            Some(KAFKA_MAX_FETCH_BYTES.to_string().as_str())
        );

        let catalog = bounded_catalog_config(&config).unwrap();
        assert_eq!(
            catalog.get("receive.message.max.bytes"),
            Some(KAFKA_MAX_RESPONSE_BYTES.to_string().as_str())
        );
        assert_eq!(
            catalog.get("fetch.max.bytes"),
            Some(KAFKA_MAX_FETCH_BYTES.to_string().as_str())
        );

        let lag = bounded_admin_config(&config, "requested-group").unwrap();
        assert_eq!(lag.get("group.id"), Some("requested-group"));
        assert_eq!(
            lag.get("receive.message.max.bytes"),
            Some(KAFKA_MAX_RESPONSE_BYTES.to_string().as_str())
        );
        assert_eq!(
            lag.get("fetch.max.bytes"),
            Some(KAFKA_MAX_FETCH_BYTES.to_string().as_str())
        );
    }

    #[test]
    fn group_commit_offsets_use_each_partitions_highest_observed_offset_plus_one() {
        let consumed = |partition, offset| Message {
            key: None,
            payload: Bytes::from_static(b"payload"),
            headers: HashMap::new(),
            partition: Some(partition),
            offset: Some(offset),
            timestamp: None,
            cursor: Some(MessageCursor::Kafka {
                topic: "orders".to_owned(),
                partition,
                offset,
            }),
            metadata: None,
        };
        let offsets =
            next_offsets_for_batch("orders", &[consumed(1, 7), consumed(0, 3), consumed(1, 9)])
                .unwrap();
        assert_eq!(
            offsets.find_partition("orders", 0).unwrap().offset(),
            Offset::Offset(4)
        );
        assert_eq!(
            offsets.find_partition("orders", 1).unwrap().offset(),
            Offset::Offset(10)
        );

        let wrong_topic = next_offsets_for_batch("payments", &[consumed(0, 0)]).unwrap_err();
        assert!(matches!(wrong_topic, Error::Serialization(_)));
        let overflow = next_offsets_for_batch("orders", &[consumed(0, i64::MAX)]).unwrap_err();
        assert!(matches!(overflow, Error::Serialization(_)));
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

        let brokers = brokers_from_dsn(&dsn);
        let base = kafka_config(&dsn, &brokers);
        let admin = base
            .create::<AdminClient<DefaultClientContext>>()
            .expect("fixture admin client should be created");
        let created = admin
            .create_topics(
                &[NewTopic::new(&topic, 2, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .expect("two-partition topic creation should complete");
        assert_eq!(created.len(), 1);
        assert!(created[0].is_ok(), "topic creation failed: {created:?}");

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

        assert!(matches!(
            connector
                .as_admin()
                .expect("native Kafka exposes AdminInspect")
                .topic_detail_bounded(&topic, MetadataBudget::with_default_bytes(1).unwrap())
                .await,
            Err(Error::MetadataBudgetExceeded {
                unit: "items",
                limit: 1,
                ..
            })
        ));
        let bounded_detail = connector
            .as_admin()
            .expect("native Kafka exposes AdminInspect")
            .topic_detail_bounded(&topic, MetadataBudget::with_default_bytes(2).unwrap())
            .await
            .expect("two-partition topic detail should fit exact item budget");
        assert_eq!(bounded_detail.watermarks.len(), 2);

        let candidate_partitions = consumer
            .fetch_metadata(None, Timeout::After(Duration::from_secs(5)))
            .expect("candidate metadata should load")
            .topics()
            .iter()
            .filter(|topic| !topic.name().starts_with("__"))
            .map(|topic| topic.partitions().len())
            .sum::<usize>();
        assert!(candidate_partitions >= 2);
        assert!(matches!(
            connector
                .as_admin()
                .expect("native Kafka exposes AdminInspect")
                .consumer_lag_bounded(
                    &group,
                    MetadataBudget::with_default_bytes(candidate_partitions - 1).unwrap(),
                )
                .await,
            Err(Error::MetadataBudgetExceeded {
                unit: "items",
                limit,
                ..
            }) if limit == candidate_partitions - 1
        ));
        let bounded_lag = connector
            .as_admin()
            .expect("native Kafka exposes AdminInspect")
            .consumer_lag_bounded(
                &group,
                MetadataBudget::with_default_bytes(candidate_partitions).unwrap(),
            )
            .await
            .expect("bounded lag must preserve the requested committed-offset group");
        let bounded_item = bounded_lag
            .iter()
            .find(|item| item.topic == topic && item.partition == 0)
            .expect("bounded lag should include the test topic");
        assert_eq!(bounded_item.group, group);
        assert_eq!(bounded_item.committed, 1);
        assert_eq!(bounded_item.lag, bounded_item.latest - 1);

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

    #[tokio::test]
    async fn live_group_consume_replays_without_ack_and_commits_complete_batches() {
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
        let topic = format!("dbtool_it_native_group_{}_{}", std::process::id(), unique);
        let group = format!("dbtool-it-native-group-{}-{unique}", std::process::id());
        let brokers = brokers_from_dsn(&dsn);
        let base = kafka_config(&dsn, &brokers);
        let admin = base
            .create::<AdminClient<DefaultClientContext>>()
            .expect("cleanup admin client should be created");
        let created = admin
            .create_topics(
                &[NewTopic::new(&topic, 2, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .expect("two-partition topic creation should complete");
        assert_eq!(created.len(), 1);
        assert!(created[0].is_ok(), "topic creation failed: {created:?}");

        let connector = connect(dsn).await.expect("Kafka should connect");
        let produced = connector
            .as_producer()
            .expect("native Kafka exposes MessageProducer")
            .produce(
                &topic,
                [(0, "p0-a"), (0, "p0-b"), (1, "p1-a"), (1, "p1-b")]
                    .into_iter()
                    .map(|(partition, payload)| Message {
                        key: None,
                        payload: Bytes::copy_from_slice(payload.as_bytes()),
                        headers: HashMap::new(),
                        partition: Some(partition),
                        offset: None,
                        timestamp: None,
                        cursor: None,
                        metadata: None,
                    })
                    .collect(),
            )
            .await
            .expect("messages should be produced to both partitions");
        assert_eq!(produced.produced, 4);

        let options = |ack, max, timeout| ConsumeOptions {
            max,
            timeout,
            identity: ConsumerIdentity::Group {
                group: group.clone(),
                member: None,
            },
            ack,
            ..Default::default()
        };
        let consumer = connector
            .as_consumer()
            .expect("native Kafka exposes MessageConsumer");
        let first = consumer
            .consume(&topic, options(AckMode::None, 4, Duration::from_secs(10)))
            .await
            .expect("ack-none group consume should succeed");
        assert_eq!(first.len(), 4);
        let replay = consumer
            .consume(&topic, options(AckMode::None, 4, Duration::from_secs(10)))
            .await
            .expect("ack-none group consume should replay");
        let mut first_payloads = first
            .iter()
            .map(|message| message.payload.to_vec())
            .collect::<Vec<_>>();
        let mut replay_payloads = replay
            .iter()
            .map(|message| message.payload.to_vec())
            .collect::<Vec<_>>();
        first_payloads.sort();
        replay_payloads.sort();
        assert_eq!(replay_payloads, first_payloads);

        let admin_inspect = connector
            .as_admin()
            .expect("native Kafka exposes AdminInspect");
        assert!(admin_inspect
            .consumer_lag(&group)
            .await
            .expect("uncommitted group lag inspection should succeed")
            .iter()
            .all(|entry| entry.topic != topic));

        let mut budget_failure = options(AckMode::OnSuccess, 4, Duration::from_secs(10));
        budget_failure.max_message_bytes = 1;
        assert!(matches!(
            consumer.consume(&topic, budget_failure).await,
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit: 1,
                ..
            })
        ));
        assert!(admin_inspect
            .consumer_lag(&group)
            .await
            .expect("budget failure must leave committed offsets absent")
            .iter()
            .all(|entry| entry.topic != topic));

        let committed = consumer
            .consume(
                &topic,
                options(AckMode::OnSuccess, 4, Duration::from_secs(10)),
            )
            .await
            .expect("successful batch should commit");
        assert_eq!(committed.len(), 4);
        let lag = admin_inspect
            .consumer_lag(&group)
            .await
            .expect("committed group lag should be readable")
            .into_iter()
            .filter(|entry| entry.topic == topic)
            .collect::<Vec<_>>();
        assert_eq!(lag.len(), 2);
        assert_eq!(lag.iter().map(|entry| entry.latest).sum::<i64>(), 4);
        assert_eq!(lag.iter().map(|entry| entry.committed).sum::<i64>(), 4);
        assert_eq!(lag.iter().map(|entry| entry.lag).sum::<i64>(), 0);

        let no_more = consumer
            .consume(
                &topic,
                options(AckMode::OnSuccess, 1, Duration::from_secs(3)),
            )
            .await
            .expect("fully committed group should remain readable");
        assert!(no_more.is_empty());

        let deleted = connector
            .as_admin_mutate()
            .expect("native Kafka exposes AdminMutate")
            .delete_resource(
                MessageResource {
                    kind: MessageResourceKind::KafkaTopic,
                    name: topic.clone(),
                },
                DeleteResourceOptions::default(),
            )
            .await
            .expect("test topic should be deleted");
        assert!(deleted.acknowledged && deleted.verified_absent);
        assert!(!admin_inspect
            .list_topics()
            .await
            .expect("topics should remain inspectable")
            .iter()
            .any(|entry| entry.name == topic));
    }
}
