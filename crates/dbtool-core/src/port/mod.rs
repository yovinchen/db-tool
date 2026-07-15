pub mod capability;
pub mod connector;

pub use capability::{
    AdminInspect, AdminMutate, DocumentStore, KeyValueStore, MessageConsumer, MessageProducer,
    SearchEngine, SqlEngine, TimeSeriesStore,
};
pub use connector::{
    Capabilities, CapabilityOperation, CapabilityReport, Connector, ConnectorKind,
};
