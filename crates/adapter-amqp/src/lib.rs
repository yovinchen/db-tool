use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{ConsumeOptions, LagInfo, Message, ProduceOutcome, TopicDetail, TopicInfo},
    port::{
        capability::{AdminInspect, MessageConsumer, MessageProducer},
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use lapin::{
    options::{BasicAckOptions, BasicGetOptions, BasicPublishOptions, QueueDeclareOptions},
    publisher_confirm::Confirmation,
    tcp::OwnedTLSConfig,
    types::FieldTable,
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
}

#[async_trait::async_trait]
impl MessageProducer for AmqpAdapter {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        validate_queue(target)?;
        if messages.is_empty() {
            return Ok(ProduceOutcome {
                produced: 0,
                placements: vec![],
            });
        }

        let channel = self.channel().await?;
        declare_queue(&channel, target, false).await?;

        let mut produced = 0;
        for message in messages {
            let confirm = channel
                .basic_publish(
                    "",
                    target,
                    BasicPublishOptions::default(),
                    &message.payload,
                    BasicProperties::default(),
                )
                .await
                .map_err(|e| Error::Query(e.to_string()))?
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            if matches!(confirm, Confirmation::Nack(_)) {
                return Err(Error::Query(
                    "AMQP broker rejected published message".into(),
                ));
            }
            produced += 1;
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
        if options.max == 0 {
            return Ok(vec![]);
        }

        let channel = self.channel().await?;
        declare_queue(&channel, source, false).await?;

        let deadline = Instant::now() + options.timeout;
        let mut messages = Vec::new();
        while messages.len() < options.max && Instant::now() < deadline {
            match channel
                .basic_get(source, BasicGetOptions::default())
                .await
                .map_err(|e| Error::Query(e.to_string()))?
            {
                Some(delivery) => {
                    let delivery_tag = delivery.delivery.delivery_tag as i64;
                    let payload = delivery.delivery.data.clone();
                    delivery
                        .delivery
                        .ack(BasicAckOptions::default())
                        .await
                        .map_err(|e| Error::Query(e.to_string()))?;
                    messages.push(Message {
                        key: None,
                        payload: payload.into(),
                        headers: HashMap::new(),
                        partition: None,
                        offset: Some(delivery_tag),
                        timestamp: None,
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
        Ok(vec![])
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
        // AMQP 0-9-1 has no consumer-group lag concept; use the RabbitMQ management adapter.
        Ok(vec![])
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
}
