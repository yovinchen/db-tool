// NATS/JetStream adapter — stub; full implementation in P5.
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

pub struct NatsAdapter {
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        Ok(Box::new(NatsAdapter {
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for NatsAdapter {
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
        Err(Error::Internal("NATS adapter not yet implemented".into()))
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
impl MessageProducer for NatsAdapter {
    async fn produce(&self, _target: &str, _msgs: Vec<Message>) -> Result<ProduceOutcome> {
        Err(Error::Internal("NATS adapter not yet implemented".into()))
    }
}
#[async_trait::async_trait]
impl MessageConsumer for NatsAdapter {
    async fn consume(&self, _source: &str, _opts: ConsumeOptions) -> Result<Vec<Message>> {
        Err(Error::Internal("NATS adapter not yet implemented".into()))
    }
}
#[async_trait::async_trait]
impl AdminInspect for NatsAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        Ok(vec![])
    }
    async fn topic_detail(&self, _name: &str) -> Result<TopicDetail> {
        Err(Error::Internal("NATS adapter not yet implemented".into()))
    }
    async fn consumer_lag(&self, _group: &str) -> Result<Vec<LagInfo>> {
        Ok(vec![])
    }
}
