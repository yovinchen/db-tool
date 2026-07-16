use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        AckMode, ConsumeOptions, ConsumerIdentity, DeleteResourceOptions, DeleteResourceOutcome,
        LagInfo, Message, MessageMetadata, MessageResource, MessageResourceKind, MetadataBudget,
        ProduceBudget, ProduceOutcome, TopicDetail, TopicInfo,
    },
    port::{
        capability::{AdminInspect, AdminMutate, MessageConsumer, MessageProducer},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{MessageReadLimiter, MessageWriteLimiter, MetadataLimiter},
};
use futures::future::BoxFuture;
use lapin::{
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
        QueueDeclareOptions, QueueDeleteOptions,
    },
    publisher_confirm::Confirmation,
    tcp::OwnedTLSConfig,
    types::{AMQPValue, FieldTable},
    BasicProperties, Connection, ConnectionProperties,
};
use std::{collections::HashMap, fs};
use tokio::time::{sleep_until, timeout_at, Instant};
use url::Url;

mod management;

pub use management::management_factory;

pub struct AmqpAdapter {
    conn: Connection,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let driver_url = amqp_driver_url(&dsn)?;
        let tls_config = amqp_tls_config(&dsn)?;
        let conn = Connection::connect_with_config(
            &driver_url,
            ConnectionProperties::default(),
            tls_config,
        )
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(Box::new(AmqpAdapter {
            conn,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for AmqpAdapter {
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
        direct_amqp_operations(self.capabilities())
    }
    async fn ping(&self) -> Result<()> {
        self.conn
            .create_channel()
            .await
            .map(|_| ())
            .map_err(|e| Error::Connection(e.to_string()))
    }
    async fn close(self: Box<Self>) -> Result<()> {
        self.conn
            .close(200, "dbtool close")
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
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
impl MessageProducer for AmqpAdapter {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        self.produce_budgeted(target, messages, ProduceBudget::default())
            .await
    }

    async fn produce_budgeted(
        &self,
        target: &str,
        messages: Vec<Message>,
        budget: ProduceBudget,
    ) -> Result<ProduceOutcome> {
        validate_publish_queue(target)?;
        let prepared = prepare_amqp_messages(messages, budget)?;

        let channel = self.channel().await?;
        declare_queue(&channel, target, false)
            .await
            .map_err(|error| amqp_produce_indeterminate("queue declaration", error))?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|error| amqp_produce_indeterminate("publisher-confirm setup", error))?;

        let mut produced = 0;
        for message in prepared {
            let confirm = channel
                .basic_publish(
                    "",
                    target,
                    BasicPublishOptions::default(),
                    &message.payload,
                    message.properties,
                )
                .await
                .map_err(|error| amqp_produce_indeterminate("publish dispatch", error))?
                .await
                .map_err(|error| amqp_produce_indeterminate("publisher confirmation", error))?;
            match confirm {
                Confirmation::Ack(_) => produced += 1,
                Confirmation::Nack(_) => {
                    return Err(amqp_produce_indeterminate(
                        "publisher confirmation",
                        "broker returned NACK",
                    ));
                }
                Confirmation::NotRequested => {
                    return Err(amqp_produce_indeterminate(
                        "publisher confirmation",
                        "confirmation was not requested",
                    ));
                }
            }
        }

        Ok(ProduceOutcome {
            produced,
            placements: vec![],
        })
    }
}

#[async_trait::async_trait]
impl MessageConsumer for AmqpAdapter {
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        validate_queue(source)?;
        validate_consume_options(&options)?;
        let mut read_limiter = MessageReadLimiter::new(&options, "AMQP consume")?;

        let deadline = checked_deadline(options.timeout)?;
        let acknowledgement_reserve =
            std::cmp::min(options.timeout / 5, std::time::Duration::from_millis(250));
        let receive_deadline = deadline
            .checked_sub(acknowledgement_reserve)
            .unwrap_or(deadline);
        let channel = timeout_at(deadline, self.channel())
            .await
            .map_err(|_| Error::Timeout)??;
        // Consuming is not a queue-creation operation. Passive declaration
        // makes a missing queue an explicit broker error instead of mutating
        // broker state as a side effect of a read-shaped command.
        timeout_at(deadline, declare_queue(&channel, source, true))
            .await
            .map_err(|_| Error::Timeout)??;

        let mut messages = Vec::new();
        let mut last_delivery_tag = None;
        while messages.len() < options.max && Instant::now() < receive_deadline {
            let delivery = match timeout_at(
                receive_deadline,
                channel.basic_get(source, BasicGetOptions::default()),
            )
            .await
            {
                Ok(Ok(delivery)) => delivery,
                Ok(Err(error)) => {
                    let error = Error::Query(error.to_string());
                    return Err(abort_unacknowledged_batch(&channel, deadline, error).await);
                }
                Err(_) => {
                    return Err(
                        abort_unacknowledged_batch(&channel, deadline, Error::Timeout).await,
                    );
                }
            };

            match delivery {
                Some(delivery) => {
                    let headers = match headers_from_properties(&delivery.delivery.properties) {
                        Ok(headers) => headers,
                        Err(error) => {
                            return Err(abort_unacknowledged_batch(&channel, deadline, error).await);
                        }
                    };
                    let payload = delivery.delivery.data.clone();
                    let delivery_tag = delivery.delivery.delivery_tag;
                    let redelivered = delivery.delivery.redelivered;
                    let exchange = delivery.delivery.exchange.as_str().to_owned();
                    let routing_key = delivery.delivery.routing_key.as_str().to_owned();
                    if last_delivery_tag.is_some_and(|previous| delivery_tag <= previous) {
                        return Err(abort_unacknowledged_batch(
                            &channel,
                            deadline,
                            Error::Query(
                                "AMQP delivery tags were not strictly increasing on the consume channel"
                                    .into(),
                            ),
                        )
                        .await);
                    }
                    last_delivery_tag = Some(delivery_tag);
                    let message = Message {
                        key: None,
                        payload: payload.into(),
                        headers,
                        partition: None,
                        // Record the channel-scoped delivery tag observed
                        // before the ACK above; it is diagnostic only and is
                        // not a stable consumer offset or reusable ACK handle.
                        offset: None,
                        timestamp: None,
                        cursor: None,
                        metadata: Some(MessageMetadata::Amqp {
                            delivery_tag,
                            redelivered,
                            exchange,
                            routing_key,
                        }),
                    };
                    if let Err(error) = read_limiter.observe(&message) {
                        return Err(abort_unacknowledged_batch(&channel, deadline, error).await);
                    }
                    messages.push(message);

                    // basic.get reports the number of ready messages that
                    // remained immediately after this delivery. Once it is
                    // zero, waiting for the full timeout would only hold this
                    // already-converted batch unacknowledged.
                    if delivery.message_count == 0 {
                        break;
                    }
                }
                None => {
                    let wake_at = std::cmp::min(
                        receive_deadline,
                        Instant::now() + std::time::Duration::from_millis(50),
                    );
                    sleep_until(wake_at).await;
                }
            }
        }

        let messages = match read_limiter.finish(messages) {
            Ok(messages) => messages,
            Err(error) => {
                return Err(abort_unacknowledged_batch(&channel, deadline, error).await);
            }
        };

        if let Some(delivery_tag) = last_delivery_tag {
            // This channel is created exclusively for this one consume call,
            // so one multiple ACK cannot include deliveries owned by another
            // caller. Sending a single protocol frame also avoids a loop that
            // could acknowledge only an arbitrary prefix after conversion.
            // Do not poll a one-way AMQP ACK unless it still has caller-owned
            // budget. Once first-polled, lapin may enqueue the irreversible
            // frame; cancelling it on a timeout and then closing the channel
            // could report failure even though RabbitMQ deleted the batch.
            if Instant::now() >= deadline {
                return Err(abort_unacknowledged_batch(&channel, deadline, Error::Timeout).await);
            }
            if channel
                .basic_ack(delivery_tag, BasicAckOptions { multiple: true })
                .await
                .is_err()
            {
                // AMQP 0.9.1 has no ACK-of-ACK. A local I/O error cannot prove
                // whether the already-submitted frame reached RabbitMQ, and a
                // follow-up channel close cannot undo it. This error is
                // intentionally non-retryable.
                return Err(Error::OutcomeIndeterminate(
                    "AMQP batch ACK may or may not have reached RabbitMQ; inspect queue state before retrying"
                        .into(),
                ));
            }
        }

        Ok(messages)
    }
}

#[async_trait::async_trait]
impl AdminInspect for AmqpAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        Err(Error::UnsupportedCapability {
            kind: self.kind.0.clone(),
            needed: "TopicListing (use rabbitmq+http)",
        })
    }

    async fn topic_detail(&self, name: &str) -> Result<TopicDetail> {
        validate_queue(name)?;
        let channel = self.channel().await?;
        let queue = declare_queue(&channel, name, true).await?;
        let mut config = HashMap::new();
        config.insert(
            "message_count".to_owned(),
            queue.message_count().to_string(),
        );
        config.insert(
            "consumer_count".to_owned(),
            queue.consumer_count().to_string(),
        );

        Ok(TopicDetail {
            info: TopicInfo {
                name: queue.name().as_str().to_owned(),
                partitions: 1,
                replicas: 1,
            },
            config,
            watermarks: vec![],
        })
    }

    async fn topic_detail_bounded(
        &self,
        name: &str,
        budget: MetadataBudget,
    ) -> Result<TopicDetail> {
        // A passive queue declaration has a protocol-fixed reply shape: the
        // queue name plus two counters. It is therefore safe to materialize
        // before applying the caller's complete-object budget.
        let detail = self.topic_detail(name).await?;
        enforce_topic_detail_budget(detail, budget, "AMQP queue detail")
    }

    async fn consumer_lag(&self, _group: &str) -> Result<Vec<LagInfo>> {
        Err(Error::UnsupportedCapability {
            kind: self.kind.0.clone(),
            needed: "ConsumerLag (use rabbitmq+http)",
        })
    }
}

#[async_trait::async_trait]
impl AdminMutate for AmqpAdapter {
    async fn delete_resource(
        &self,
        resource: MessageResource,
        options: DeleteResourceOptions,
    ) -> Result<DeleteResourceOutcome> {
        validate_amqp_delete_request(&resource)?;

        let channel = self.channel().await?;
        let queue = declare_queue(&channel, &resource.name, true).await?;
        let messages_before = u64::from(queue.message_count());
        let consumers_before = u64::from(queue.consumer_count());
        channel
            .queue_delete(
                &resource.name,
                QueueDeleteOptions {
                    if_empty: options.if_empty,
                    if_unused: options.if_unused,
                    // A synchronous queue.delete-ok is the broker's
                    // authoritative confirmation that the named queue is gone.
                    nowait: false,
                },
            )
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        Ok(DeleteResourceOutcome {
            resource,
            acknowledged: true,
            verified_absent: true,
            messages_before: Some(messages_before),
            consumers_before: Some(consumers_before),
        })
    }
}

impl AmqpAdapter {
    async fn channel(&self) -> Result<lapin::Channel> {
        self.conn
            .create_channel()
            .await
            .map_err(|e| Error::Connection(e.to_string()))
    }
}

fn direct_amqp_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::MessageProduceBudgeted,
        CapabilityOperation::MessageConsumeAck,
        CapabilityOperation::MessageAdminTopicDetail,
        CapabilityOperation::MessageAdminTopicDetailBounded,
        CapabilityOperation::MessageAdminDelete,
    ]);
    operations
}

struct PreparedAmqpMessage {
    payload: bytes::Bytes,
    properties: BasicProperties,
}

fn prepare_amqp_messages(
    messages: Vec<Message>,
    budget: ProduceBudget,
) -> Result<Vec<PreparedAmqpMessage>> {
    MessageWriteLimiter::new(budget, "AMQP produce input")?.validate(&messages)?;
    messages
        .into_iter()
        .map(|message| {
            validate_produce_message(&message)?;
            Ok(PreparedAmqpMessage {
                payload: message.payload,
                properties: properties_with_headers(&message.headers)?,
            })
        })
        .collect()
}

fn amqp_produce_indeterminate(stage: &str, error: impl std::fmt::Display) -> Error {
    Error::OutcomeIndeterminate(format!(
        "AMQP produce failed during {stage} after a queue declaration or publish may have reached RabbitMQ ({error}); inspect queue state before retrying"
    ))
}

pub(crate) fn enforce_topic_detail_budget(
    detail: TopicDetail,
    budget: MetadataBudget,
    subject: &str,
) -> Result<TopicDetail> {
    let mut limiter = MetadataLimiter::new(budget, subject)?;
    for item in &detail.config {
        limiter.observe(&item)?;
    }
    for watermark in &detail.watermarks {
        limiter.observe(watermark)?;
    }
    limiter.ensure_complete(&detail)?;
    Ok(detail)
}

fn validate_amqp_delete_request(resource: &MessageResource) -> Result<()> {
    if resource.kind != MessageResourceKind::AmqpQueue {
        return Err(Error::Config(format!(
            "direct AMQP can delete only amqp-queue resources, not {}",
            resource.kind.as_str()
        )));
    }
    validate_queue(&resource.name)
}

async fn declare_queue(
    channel: &lapin::Channel,
    queue: &str,
    passive: bool,
) -> Result<lapin::Queue> {
    channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                passive,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .map_err(|e| Error::Query(e.to_string()))
}

pub(crate) fn validate_queue(queue: &str) -> Result<()> {
    if queue.is_empty() || queue.len() > 255 || queue.bytes().any(|b| b.is_ascii_control()) {
        return Err(Error::Query(format!("invalid AMQP queue name: {queue:?}")));
    }

    Ok(())
}

fn validate_publish_queue(queue: &str) -> Result<()> {
    validate_queue(queue)?;
    if queue.starts_with("amq.") {
        return Err(Error::Config(format!(
            "AMQP queue names starting with amq. are broker-reserved: {queue:?}"
        )));
    }
    Ok(())
}

fn validate_produce_message(message: &Message) -> Result<()> {
    if message.key.is_some() {
        return Err(Error::Config(
            "AMQP producer does not support an exact message key mapping".into(),
        ));
    }
    if message.partition.is_some() {
        return Err(Error::Config(
            "AMQP producer does not support partitions".into(),
        ));
    }
    if message.offset.is_some() {
        return Err(Error::Config(
            "AMQP producer does not support producer offsets".into(),
        ));
    }
    if message.timestamp.is_some() {
        return Err(Error::Config(
            "AMQP producer does not support an exact millisecond timestamp mapping".into(),
        ));
    }
    if message.cursor.is_some() || message.metadata.is_some() {
        return Err(Error::Config(
            "AMQP producer messages cannot set consumer cursor or delivery metadata".into(),
        ));
    }

    for (key, value) in &message.headers {
        if key.len() > u8::MAX as usize {
            return Err(Error::Config(format!(
                "AMQP header name exceeds the 255-byte protocol limit: {key:?}"
            )));
        }
        if value.len() > u32::MAX as usize {
            return Err(Error::Config(format!(
                "AMQP header value exceeds the protocol limit for {key:?}"
            )));
        }
    }

    Ok(())
}

fn validate_consume_options(options: &ConsumeOptions) -> Result<()> {
    options
        .validate()
        .map_err(|message| Error::Config(format!("AMQP consume: {message}")))?;
    if options.identity != ConsumerIdentity::Stateless {
        return Err(Error::Config(
            "AMQP consume does not support group or durable identities".into(),
        ));
    }
    if options.ack != AckMode::OnSuccess {
        return Err(Error::Config(
            "AMQP consume requires ack mode on-success because basic.get is destructive".into(),
        ));
    }
    if options.partition.is_some() {
        return Err(Error::Config(
            "AMQP consumer does not support partitions".into(),
        ));
    }
    if options.offset.is_some() {
        return Err(Error::Config(
            "AMQP consumer does not support offsets".into(),
        ));
    }
    if options.cursor.is_some() {
        return Err(Error::Config(
            "AMQP consumer does not support exact cursors".into(),
        ));
    }
    Ok(())
}

async fn abort_unacknowledged_batch(
    channel: &lapin::Channel,
    deadline: Instant,
    original: Error,
) -> Error {
    // channel.close-ok is the only confirmation available here that the
    // call-owned channel closed and RabbitMQ can requeue its unacknowledged
    // basic.get deliveries. Never wait beyond the caller's consume deadline.
    match timeout_at(deadline, channel.close(200, "dbtool consume aborted")).await {
        Ok(Ok(())) => original,
        Ok(Err(_)) | Err(_) => Error::OutcomeIndeterminate(format!(
            "AMQP consume failed with {}, and channel closure could not confirm requeue before the deadline; inspect queue state before retrying",
            original.code()
        )),
    }
}

fn properties_with_headers(headers: &HashMap<String, String>) -> Result<BasicProperties> {
    let mut table = FieldTable::default();
    for (key, value) in headers {
        if key.len() > u8::MAX as usize {
            return Err(Error::Config(format!(
                "AMQP header name exceeds the 255-byte protocol limit: {key:?}"
            )));
        }
        if value.len() > u32::MAX as usize {
            return Err(Error::Config(format!(
                "AMQP header value exceeds the protocol limit for {key:?}"
            )));
        }
        table.insert(
            key.as_str().into(),
            AMQPValue::LongString(value.as_bytes().to_vec().into()),
        );
    }

    Ok(if table.inner().is_empty() {
        BasicProperties::default()
    } else {
        BasicProperties::default().with_headers(table)
    })
}

fn headers_from_properties(properties: &BasicProperties) -> Result<HashMap<String, String>> {
    let Some(table) = properties.headers() else {
        return Ok(HashMap::new());
    };

    table
        .into_iter()
        .map(|(key, value)| {
            let value = match value {
                AMQPValue::LongString(value) => std::str::from_utf8(value.as_bytes())
                    .map_err(|error| {
                        Error::Serialization(format!(
                            "AMQP header {key:?} is not valid UTF-8: {error}"
                        ))
                    })?
                    .to_owned(),
                AMQPValue::ShortString(value) => value.as_str().to_owned(),
                other => {
                    return Err(Error::Serialization(format!(
                        "AMQP header {key:?} has unsupported non-string type {:?}",
                        other.get_type()
                    )))
                }
            };
            Ok((key.as_str().to_owned(), value))
        })
        .collect()
}

fn checked_deadline(timeout: std::time::Duration) -> Result<Instant> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::Config("AMQP consume timeout is too large for this platform".into()))
}

fn amqp_driver_url(dsn: &Dsn) -> Result<String> {
    let mut url = Url::parse(&dsn.raw).map_err(|e| Error::Dsn(format!("invalid URL: {e}")))?;
    match url.scheme() {
        "amqp" | "amqps" => {}
        scheme => {
            return Err(Error::Dsn(format!(
                "AMQP DSN must use amqp:// or amqps://, got {scheme}"
            )))
        }
    }

    let pairs = url
        .query_pairs()
        .filter(|(key, _)| !is_tls_ca_param(key))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    if !pairs.is_empty() {
        url.query_pairs_mut()
            .extend_pairs(pairs.iter().map(|(key, value)| (&**key, &**value)));
    }

    Ok(url.to_string())
}

fn amqp_tls_config(dsn: &Dsn) -> Result<OwnedTLSConfig> {
    let cert_chain = match amqp_tls_ca(dsn) {
        Some(path) => Some(
            fs::read_to_string(path)
                .map_err(|e| Error::Config(format!("failed to read AMQP TLS CA {path}: {e}")))?,
        ),
        None => None,
    };

    Ok(OwnedTLSConfig {
        identity: None,
        cert_chain,
    })
}

fn amqp_tls_ca(dsn: &Dsn) -> Option<&str> {
    dsn.params
        .get("tls-ca")
        .or_else(|| dsn.params.get("ssl-ca"))
        .map(String::as_str)
}

fn is_tls_ca_param(key: &str) -> bool {
    matches!(key, "tls-ca" | "ssl-ca")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Message {
        Message {
            key: None,
            payload: bytes::Bytes::from_static(b"payload"),
            headers: HashMap::from([
                ("trace".to_owned(), "abc".to_owned()),
                ("content-type".to_owned(), "text/plain".to_owned()),
            ]),
            partition: None,
            offset: None,
            timestamp: None,
            cursor: None,
            metadata: None,
        }
    }

    #[test]
    fn amqps_driver_url_strips_dbtool_tls_ca_param() {
        let dsn = Dsn::parse(
            "amqps://user:pass@127.0.0.1:5671/vhost?tls-ca=/tmp/ca.pem&connection_timeout=5000",
        )
        .unwrap();

        assert_eq!(
            amqp_driver_url(&dsn).unwrap(),
            "amqps://user:pass@127.0.0.1:5671/vhost?connection_timeout=5000"
        );
        assert_eq!(amqp_tls_ca(&dsn), Some("/tmp/ca.pem"));
    }

    #[test]
    fn amqp_string_headers_round_trip_through_basic_properties() {
        let message = message();
        let properties = properties_with_headers(&message.headers).unwrap();

        assert_eq!(
            headers_from_properties(&properties).unwrap(),
            message.headers
        );
    }

    #[test]
    fn amqp_rejects_metadata_without_an_exact_protocol_mapping() {
        let mut candidate = message();
        candidate.key = Some(bytes::Bytes::from_static(b"key"));
        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("message key")
        ));

        let mut candidate = message();
        candidate.partition = Some(0);
        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("partitions")
        ));

        let mut candidate = message();
        candidate.offset = Some(1);
        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("producer offsets")
        ));

        let mut candidate = message();
        candidate.timestamp = Some(1_710_000_000_123);
        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("millisecond timestamp")
        ));
    }

    #[test]
    fn amqp_rejects_consumer_positions_and_non_string_headers() {
        assert!(matches!(
            validate_consume_options(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: Some(0),
                offset: None,
                cursor: None,
                ack: AckMode::OnSuccess,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("partitions")
        ));
        assert!(matches!(
            validate_consume_options(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: None,
                offset: Some(0),
                cursor: None,
                ack: AckMode::OnSuccess,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("offsets")
        ));
        assert!(matches!(
            validate_consume_options(&ConsumeOptions {
                cursor: Some(dbtool_core::model::ConsumeCursor::RedisStream {
                    id: "1-0".to_owned(),
                }),
                ack: AckMode::OnSuccess,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("exact cursors")
        ));

        let mut table = FieldTable::default();
        table.insert("attempt".into(), AMQPValue::LongInt(3));
        let properties = BasicProperties::default().with_headers(table);
        assert!(matches!(
            headers_from_properties(&properties),
            Err(Error::Serialization(message)) if message.contains("non-string")
        ));

        assert!(matches!(
            validate_consume_options(&ConsumeOptions::default()),
            Err(Error::Config(message)) if message.contains("on-success")
        ));
        assert!(matches!(
            validate_consume_options(&ConsumeOptions {
                identity: ConsumerIdentity::Group {
                    group: "workers".to_owned(),
                    member: None,
                },
                ack: AckMode::OnSuccess,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("group or durable")
        ));
    }

    #[test]
    fn amqp_header_names_respect_short_string_wire_limit() {
        let mut candidate = message();
        candidate
            .headers
            .insert("x".repeat(256), "value".to_owned());

        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("255-byte")
        ));
    }

    #[test]
    fn amqp_produce_preflight_enforces_count_and_exact_byte_boundaries() {
        let candidate = message();
        let message_bytes = serde_json::to_vec(&candidate).unwrap().len();
        let batch_bytes = serde_json::to_vec(&vec![candidate.clone()]).unwrap().len();
        let exact = ProduceBudget::new(1, message_bytes, batch_bytes).unwrap();
        assert_eq!(
            prepare_amqp_messages(vec![candidate.clone()], exact)
                .unwrap()
                .len(),
            1
        );

        let per_message_short = ProduceBudget::new(1, message_bytes - 1, batch_bytes).unwrap();
        assert!(matches!(
            prepare_amqp_messages(vec![candidate.clone()], per_message_short),
            Err(Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == message_bytes - 1
        ));

        let batch_short = ProduceBudget::new(1, message_bytes, batch_bytes - 1).unwrap();
        assert!(matches!(
            prepare_amqp_messages(vec![candidate.clone()], batch_short),
            Err(Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == batch_bytes - 1
        ));

        let two = vec![candidate.clone(), candidate];
        assert!(matches!(
            prepare_amqp_messages(two, ProduceBudget::new(1, 4096, 4096).unwrap()),
            Err(Error::InputBudgetExceeded {
                unit: "messages",
                limit: 1,
                ..
            })
        ));
    }

    #[test]
    fn amqp_prepares_the_complete_batch_before_queue_declaration() {
        let valid = message();
        let mut invalid = message();
        invalid.key = Some(bytes::Bytes::from_static(b"unrepresentable"));

        assert!(matches!(
            prepare_amqp_messages(vec![valid, invalid], ProduceBudget::default()),
            Err(Error::Config(message)) if message.contains("message key")
        ));
        assert!(validate_publish_queue("amq.dbtool-reserved").is_err());
        assert!(validate_queue("amq.dbtool-existing").is_ok());
    }

    #[test]
    fn amqp_failures_after_produce_starts_are_nonretryable() {
        let error = amqp_produce_indeterminate("publisher confirmation", "socket closed");
        assert_eq!(error.code(), "OUTCOME_INDETERMINATE");
        assert!(!error.is_retryable());
        assert!(
            matches!(error, Error::OutcomeIndeterminate(message) if message.contains("inspect queue state"))
        );
    }

    #[test]
    fn direct_amqp_declares_only_real_admin_operations() {
        let operations = direct_amqp_operations(Capabilities {
            producer: true,
            consumer: true,
            admin: true,
            ..Default::default()
        });

        assert!(operations.contains(&CapabilityOperation::MessageProduce));
        assert!(operations.contains(&CapabilityOperation::MessageProduceBudgeted));
        assert!(operations.contains(&CapabilityOperation::MessageConsume));
        assert!(operations.contains(&CapabilityOperation::MessageConsumeAck));
        assert!(operations.contains(&CapabilityOperation::MessageAdminTopicDetail));
        assert!(operations.contains(&CapabilityOperation::MessageAdminTopicDetailBounded));
        assert!(operations.contains(&CapabilityOperation::MessageAdminDelete));
        assert!(!operations.contains(&CapabilityOperation::MessageAdminListTopics));
        assert!(!operations.contains(&CapabilityOperation::MessageAdminListTopicsBounded));
        assert!(!operations.contains(&CapabilityOperation::MessageAdminListTopicsBudgeted));
        assert!(!operations.contains(&CapabilityOperation::MessageAdminConsumerLag));
    }

    #[test]
    fn direct_amqp_delete_accepts_only_named_amqp_queues() {
        let queue = MessageResource {
            kind: MessageResourceKind::AmqpQueue,
            name: "jobs".to_owned(),
        };
        assert!(validate_amqp_delete_request(&queue).is_ok());

        let stream = MessageResource {
            kind: MessageResourceKind::RedisStream,
            name: "jobs".to_owned(),
        };
        assert!(matches!(
            validate_amqp_delete_request(&stream),
            Err(Error::Config(message)) if message.contains("amqp-queue")
        ));

        let unnamed = MessageResource {
            kind: MessageResourceKind::AmqpQueue,
            name: String::new(),
        };
        assert!(validate_amqp_delete_request(&unnamed).is_err());
    }
}
