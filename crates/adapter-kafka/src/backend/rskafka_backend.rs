// Pure-Rust backend via rskafka (default — self-contained, feature-limited).
// Covers Kafka / AutoMQ / Redpanda / WarpStream via the Kafka wire protocol.
use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    port::connector::{Capabilities, Connector, ConnectorKind},
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
        Capabilities::default()
    }

    async fn ping(&self) -> Result<()> {
        if self.brokers.is_empty() {
            return Err(Error::Connection("no Kafka brokers configured".into()));
        }
        Err(Error::UnsupportedCapability {
            kind: self.kind.0.clone(),
            needed: "Kafka pure backend implementation",
        })
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn kafka_shell_does_not_advertise_unimplemented_capabilities() {
        let connector = connect(Dsn::parse("kafka://localhost:9092").unwrap())
            .await
            .unwrap();
        let caps = connector.capabilities();

        assert!(!caps.producer);
        assert!(!caps.consumer);
        assert!(!caps.admin);
        assert!(connector.as_producer().is_none());
        assert!(connector.as_consumer().is_none());
        assert!(connector.as_admin().is_none());
    }
}
