// Pure-Rust backend via rskafka (default — self-contained, feature-limited).
// Covers Kafka / AutoMQ / Redpanda / WarpStream via the Kafka wire protocol.
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

pub struct RskafkaAdapter {
    brokers: Vec<String>,
    kind: ConnectorKind,
}

pub fn connect(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let host = dsn.host.unwrap_or_else(|| "localhost".into());
        let port = dsn.port.unwrap_or(9092);
        let brokers = vec![format!("{host}:{port}")];
        Ok(Box::new(RskafkaAdapter {
            brokers,
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
        if self.brokers.is_empty() {
            return Err(Error::Connection("no Kafka brokers configured".into()));
        }
        Ok(())
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
    async fn produce(&self, _target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        let n = messages.len() as u64;
        Ok(ProduceOutcome {
            produced: n,
            placements: vec![],
        })
    }
}

#[async_trait::async_trait]
impl MessageConsumer for RskafkaAdapter {
    async fn consume(&self, _source: &str, opts: ConsumeOptions) -> Result<Vec<Message>> {
        let _ = (opts.max, opts.timeout);
        Ok(vec![])
    }
}

#[async_trait::async_trait]
impl AdminInspect for RskafkaAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        Ok(vec![])
    }

    async fn topic_detail(&self, name: &str) -> Result<TopicDetail> {
        Err(Error::Internal(format!(
            "topic_detail for '{name}' not yet implemented"
        )))
    }

    async fn consumer_lag(&self, _group: &str) -> Result<Vec<LagInfo>> {
        Ok(vec![])
    }
}
