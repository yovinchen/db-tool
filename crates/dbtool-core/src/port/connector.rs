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
