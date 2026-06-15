use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    port::connector::{Capabilities, Connector, ConnectorKind},
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
        Capabilities::default()
    }
    async fn ping(&self) -> Result<()> {
        Err(Error::UnsupportedCapability {
            kind: self.kind.0.clone(),
            needed: "NATS adapter implementation",
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
    async fn nats_shell_does_not_advertise_unimplemented_capabilities() {
        let connector = factory(Dsn::parse("nats://localhost:4222").unwrap())
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
