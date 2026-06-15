use super::capability::{
    AdminInspect, DocumentStore, KeyValueStore, MessageConsumer, MessageProducer, SearchEngine,
    SqlEngine, TimeSeriesStore,
};
use crate::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Bitflag set of capabilities a connector exposes.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Capabilities {
    pub sql: bool,
    pub key_value: bool,
    pub document: bool,
    pub time_series: bool,
    pub search: bool,
    pub producer: bool,
    pub consumer: bool,
    pub admin: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorKind(pub String);

/// Base trait — all adapters implement this.
/// Capability accessors default to `None`; adapters override only what they support.
#[async_trait]
pub trait Connector: Send + Sync {
    fn kind(&self) -> ConnectorKind;
    fn capabilities(&self) -> Capabilities;
    async fn ping(&self) -> Result<()>;
    async fn close(self: Box<Self>) -> Result<()>;

    // ── Capability accessors ─────────────────────────────────────────────────
    fn as_sql(&self) -> Option<&dyn SqlEngine> {
        None
    }
    fn as_kv(&self) -> Option<&dyn KeyValueStore> {
        None
    }
    fn as_document(&self) -> Option<&dyn DocumentStore> {
        None
    }
    fn as_timeseries(&self) -> Option<&dyn TimeSeriesStore> {
        None
    }
    fn as_search(&self) -> Option<&dyn SearchEngine> {
        None
    }
    fn as_producer(&self) -> Option<&dyn MessageProducer> {
        None
    }
    fn as_consumer(&self) -> Option<&dyn MessageConsumer> {
        None
    }
    fn as_admin(&self) -> Option<&dyn AdminInspect> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::Value,
        port::capability::{KeyValueStore, SetOptions},
    };
    use bytes::Bytes;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct MockKvConnector {
        store: Mutex<HashMap<String, Bytes>>,
    }

    #[async_trait]
    impl Connector for MockKvConnector {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind("mock-kv".to_owned())
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities {
                key_value: true,
                ..Default::default()
            }
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        fn as_kv(&self) -> Option<&dyn KeyValueStore> {
            Some(self)
        }
    }

    #[async_trait]
    impl KeyValueStore for MockKvConnector {
        async fn get(&self, key: &str) -> Result<Option<Bytes>> {
            Ok(self.store.lock().await.get(key).cloned())
        }

        async fn set(&self, key: &str, value: &[u8], _options: SetOptions) -> Result<()> {
            self.store
                .lock()
                .await
                .insert(key.to_owned(), Bytes::copy_from_slice(value));
            Ok(())
        }

        async fn delete(&self, keys: &[String]) -> Result<u64> {
            let mut store = self.store.lock().await;
            let mut deleted = 0;
            for key in keys {
                if store.remove(key).is_some() {
                    deleted += 1;
                }
            }
            Ok(deleted)
        }

        async fn scan(&self, pattern: &str, limit: usize) -> Result<Vec<String>> {
            let prefix = pattern.strip_suffix('*').unwrap_or(pattern);
            let mut keys: Vec<_> = self
                .store
                .lock()
                .await
                .keys()
                .filter(|key| key.starts_with(prefix))
                .take(limit)
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }

        async fn raw_command(&self, args: &[String]) -> Result<Value> {
            Ok(Value::Json(serde_json::json!({ "args": args })))
        }
    }

    #[tokio::test]
    async fn mock_connector_exposes_only_declared_capability() {
        let connector = MockKvConnector::default();

        assert_eq!(connector.kind().0, "mock-kv");
        assert!(connector.capabilities().key_value);
        assert!(connector.as_kv().is_some());
        assert!(connector.as_sql().is_none());
        assert!(connector.as_document().is_none());
        assert!(connector.as_producer().is_none());
        connector.ping().await.unwrap();
    }

    #[tokio::test]
    async fn key_value_contract_round_trips_scans_and_deletes() {
        let connector = MockKvConnector::default();
        let kv = connector.as_kv().unwrap();

        kv.set("user:1", b"alice", SetOptions::default())
            .await
            .unwrap();
        kv.set("user:2", b"bob", SetOptions::default())
            .await
            .unwrap();

        assert_eq!(kv.get("user:1").await.unwrap(), Some(Bytes::from("alice")));
        assert_eq!(
            kv.scan("user:*", 10).await.unwrap(),
            vec!["user:1", "user:2"]
        );
        assert_eq!(kv.delete(&["user:1".to_owned()]).await.unwrap(), 1);
        assert_eq!(kv.get("user:1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn connector_can_be_closed_through_boxed_trait_object() {
        let connector: Box<dyn Connector> = Box::new(MockKvConnector::default());

        connector.close().await.unwrap();
    }
}
