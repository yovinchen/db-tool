use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{ConsumeOptions, Message, ProduceOutcome},
    port::{
        capability::{MessageConsumer, MessageProducer},
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use futures::StreamExt;
use std::collections::HashMap;
use tokio::time::{timeout, Instant};

pub struct NatsAdapter {
    client: async_nats::Client,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let client = async_nats::connect(dsn.raw)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(Box::new(NatsAdapter {
            client,
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
            ..Default::default()
        }
    }
    async fn ping(&self) -> Result<()> {
        self.client
            .flush()
            .await
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
}

#[async_trait::async_trait]
impl MessageProducer for NatsAdapter {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        validate_subject(target)?;
        let mut produced = 0;
        for message in messages {
            self.client
                .publish(target.to_owned(), message.payload)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            produced += 1;
        }
        self.client
            .flush()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        Ok(ProduceOutcome {
            produced,
            placements: vec![],
        })
    }
}

#[async_trait::async_trait]
impl MessageConsumer for NatsAdapter {
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        validate_subject(source)?;
        if options.max == 0 {
            return Ok(vec![]);
        }

        let mut subscriber = self
            .client
            .subscribe(source.to_owned())
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        self.client
            .flush()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let deadline = Instant::now() + options.timeout;
        let mut messages = Vec::new();
        while messages.len() < options.max {
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            match timeout(deadline - now, subscriber.next()).await {
                Ok(Some(message)) => messages.push(Message {
                    key: None,
                    payload: message.payload,
                    headers: HashMap::new(),
                    partition: None,
                    offset: None,
                    timestamp: None,
                }),
                Ok(None) => break,
                Err(_) => break,
            }
        }

        Ok(messages)
    }
}

fn validate_subject(subject: &str) -> Result<()> {
    if subject.is_empty()
        || subject
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(Error::Query(format!("invalid NATS subject: {subject:?}")));
    }

    Ok(())
}
