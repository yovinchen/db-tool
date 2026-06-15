use crate::{
    dsn::Dsn,
    error::{Error, Result},
    port::Connector,
    registry::alias::{canonical_scheme, protocol_family},
};
use futures::future::BoxFuture;
use std::collections::HashMap;

/// Factory takes **ownership** of `Dsn` so the returned future can be `'static`.
pub type Factory = fn(Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>>;

pub struct Registry {
    map: HashMap<&'static str, Factory>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn register(&mut self, scheme: &'static str, factory: Factory) {
        self.map.insert(scheme, factory);
    }

    pub fn register_family(&mut self, canonical: &'static str, factory: Factory) {
        self.register(canonical, factory);
        if let Some((_, aliases)) = protocol_family(canonical) {
            for alias in aliases {
                self.register(alias, factory);
            }
        }
    }

    pub async fn connect(&self, dsn_str: &str) -> Result<Box<dyn Connector>> {
        let dsn = Dsn::parse(dsn_str)?;
        let scheme = canonical_scheme(dsn.scheme());
        let factory = self
            .map
            .get(scheme)
            .copied()
            .ok_or_else(|| Error::UnsupportedScheme(dsn.scheme().to_owned()))?;
        factory(dsn).await
    }

    pub fn supported_schemes(&self) -> Vec<&'static str> {
        let mut schemes: Vec<_> = self.map.keys().copied().collect();
        schemes.sort_unstable();
        schemes
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::port::{Capabilities, Connector, ConnectorKind};
    use futures::FutureExt;

    struct DummyConnector;

    #[async_trait::async_trait]
    impl Connector for DummyConnector {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind("dummy".to_owned())
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::default()
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }

    fn dummy_factory(_dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
        async { Ok(Box::new(DummyConnector) as Box<dyn Connector>) }.boxed()
    }

    #[tokio::test]
    async fn connect_resolves_registered_protocol_alias() {
        let mut registry = Registry::new();
        registry.register_family("postgres", dummy_factory);

        let conn = registry
            .connect("postgresql://user:pass@localhost/db")
            .await
            .unwrap();

        assert_eq!(conn.kind().0, "dummy");
    }
}
