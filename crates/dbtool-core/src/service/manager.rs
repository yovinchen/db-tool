use crate::{port::Connector, registry::Registry, Result};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

/// Connection manager for TUI / long-lived sessions.
/// Caches live connectors by name; CLI should use Registry directly.
pub struct ConnectionManager {
    registry: Arc<Registry>,
    connections: Mutex<HashMap<String, Arc<Box<dyn Connector>>>>,
}

impl ConnectionManager {
    pub fn new(registry: Arc<Registry>) -> Self {
        Self {
            registry,
            connections: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_or_connect(&self, dsn_str: &str) -> Result<Arc<Box<dyn Connector>>> {
        let mut map = self.connections.lock().await;
        if let Some(conn) = map.get(dsn_str) {
            return Ok(Arc::clone(conn));
        }
        let conn = self.registry.connect(dsn_str).await?;
        let arc = Arc::new(conn);
        map.insert(dsn_str.to_owned(), Arc::clone(&arc));
        Ok(arc)
    }

    pub async fn remove(&self, dsn_str: &str) {
        self.connections.lock().await.remove(dsn_str);
    }
}
