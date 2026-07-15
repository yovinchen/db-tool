use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ConsumeOptions, DeleteResourceOptions, DeleteResourceOutcome, LagInfo, Message,
        MessageMetadata, MessageResource, MessageResourceKind, ProduceOutcome, TopicDetail,
        TopicInfo,
    },
    port::{
        capability::{AdminInspect, AdminMutate, MessageConsumer, MessageProducer},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
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
use tokio::time::{sleep, Instant};
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
        validate_queue(target)?;
        for message in &messages {
            validate_produce_message(message)?;
        }
        if messages.is_empty() {
            return Ok(ProduceOutcome {
                produced: 0,
                placements: vec![],
            });
        }

        let channel = self.channel().await?;
        declare_queue(&channel, target, false).await?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        let mut produced = 0;
        for message in messages {
            let properties = properties_with_headers(&message.headers)?;
            let confirm = channel
                .basic_publish(
                    "",
                    target,
                    BasicPublishOptions::default(),
                    &message.payload,
                    properties,
                )
                .await
                .map_err(|e| Error::Query(e.to_string()))?
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            match confirm {
                Confirmation::Ack(_) => produced += 1,
                Confirmation::Nack(_) => {
                    return Err(Error::Query(
                        "AMQP broker rejected published message".into(),
                    ));
                }
                Confirmation::NotRequested => {
                    return Err(Error::Query(
                        "AMQP publisher confirmation was not requested".into(),
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
        validate_consume_position(&options)?;
        if options.max == 0 {
            return Ok(vec![]);
        }

        let deadline = checked_deadline(options.timeout)?;
        let channel = self.channel().await?;
        // Consuming is not a queue-creation operation. Passive declaration
        // makes a missing queue an explicit broker error instead of mutating
        // broker state as a side effect of a read-shaped command.
        declare_queue(&channel, source, true).await?;

        let mut messages = Vec::new();
        while messages.len() < options.max && Instant::now() < deadline {
            match channel
                .basic_get(source, BasicGetOptions::default())
                .await
                .map_err(|e| Error::Query(e.to_string()))?
            {
                Some(delivery) => {
                    let headers = headers_from_properties(&delivery.delivery.properties)?;
                    let payload = delivery.delivery.data.clone();
                    let delivery_tag = delivery.delivery.delivery_tag;
                    let redelivered = delivery.delivery.redelivered;
                    let exchange = delivery.delivery.exchange.as_str().to_owned();
                    let routing_key = delivery.delivery.routing_key.as_str().to_owned();
                    delivery
                        .delivery
                        .ack(BasicAckOptions::default())
                        .await
                        .map_err(|e| Error::Query(e.to_string()))?;
                    messages.push(Message {
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
                    });
                }
                None => sleep(std::time::Duration::from_millis(50)).await,
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
        CapabilityOperation::MessageAdminTopicDetail,
        CapabilityOperation::MessageAdminDelete,
    ]);
    operations
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

fn validate_consume_position(options: &ConsumeOptions) -> Result<()> {
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
            validate_consume_position(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: Some(0),
                offset: None,
                cursor: None,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("partitions")
        ));
        assert!(matches!(
            validate_consume_position(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: None,
                offset: Some(0),
                cursor: None,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("offsets")
        ));
        assert!(matches!(
            validate_consume_position(&ConsumeOptions {
                cursor: Some(dbtool_core::model::ConsumeCursor::RedisStream {
                    id: "1-0".to_owned(),
                }),
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
    fn direct_amqp_declares_only_real_admin_operations() {
        let operations = direct_amqp_operations(Capabilities {
            producer: true,
            consumer: true,
            admin: true,
            ..Default::default()
        });

        assert!(operations.contains(&CapabilityOperation::MessageProduce));
        assert!(operations.contains(&CapabilityOperation::MessageConsume));
        assert!(operations.contains(&CapabilityOperation::MessageAdminTopicDetail));
        assert!(operations.contains(&CapabilityOperation::MessageAdminDelete));
        assert!(!operations.contains(&CapabilityOperation::MessageAdminListTopics));
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
