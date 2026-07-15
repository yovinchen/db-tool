use super::capability::{
    AdminInspect, AdminMutate, CqlEngine, Db2Engine, DocumentStore, KeyValueStore, MessageConsumer,
    MessageProducer, SearchEngine, SqlEngine, TimeSeriesStore,
};
use crate::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

macro_rules! capability_operations {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        /// A stable, method-level capability identifier exposed by a connector.
        ///
        /// The serialized names are part of the public negotiation contract and
        /// intentionally do not depend on Rust variant naming conventions.
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub enum CapabilityOperation {
            $(#[serde(rename = $name)] $variant),+
        }

        impl CapabilityOperation {
            /// Every operation understood by this version of dbtool-core.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// Return the stable serialized operation name.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name),+
                }
            }
        }
    };
}

capability_operations! {
    SqlQuery => "sql.query",
    SqlQueryBounded => "sql.query_bounded",
    SqlExecute => "sql.execute",
    SqlListSchemas => "sql.list_schemas",
    SqlListTables => "sql.list_tables",
    SqlDescribeTable => "sql.describe_table",
    CqlQuery => "cql.query",
    CqlQueryBounded => "cql.query_bounded",
    CqlExecute => "cql.execute",
    CqlListKeyspaces => "cql.list_keyspaces",
    CqlListTables => "cql.list_tables",
    CqlDescribeTable => "cql.describe_table",
    Db2ListSequences => "db2.list_sequences",
    Db2ListRoutines => "db2.list_routines",
    Db2ListTablespaces => "db2.list_tablespaces",
    Db2ListForeignKeys => "db2.list_foreign_keys",
    Db2GenerateDdl => "db2.generate_ddl",
    KeyValueGet => "kv.get",
    KeyValueSet => "kv.set",
    KeyValueDelete => "kv.delete",
    KeyValueScan => "kv.scan",
    KeyValueRawCommand => "kv.raw_command",
    DocumentListCollections => "document.list_collections",
    DocumentFind => "document.find",
    DocumentInsert => "document.insert",
    DocumentUpdate => "document.update",
    DocumentDelete => "document.delete",
    DocumentAggregate => "document.aggregate",
    DocumentAggregateBounded => "document.aggregate_bounded",
    DocumentDropCollection => "document.drop_collection",
    TimeSeriesListMeasurements => "time_series.list_measurements",
    TimeSeriesWritePoints => "time_series.write_points",
    TimeSeriesQueryRange => "time_series.query_range",
    SearchListIndices => "search.list_indices",
    SearchSearch => "search.search",
    SearchIndexDocument => "search.index_doc",
    SearchPutDocument => "search.put_doc",
    SearchGetDocument => "search.get_doc",
    SearchUpdateDocument => "search.update_doc",
    SearchDeleteDocument => "search.delete_doc",
    SearchDeleteIndex => "search.delete_index",
    MessageProduce => "message.produce",
    MessageConsume => "message.consume",
    MessageAdminListTopics => "message.admin.list_topics",
    MessageAdminTopicDetail => "message.admin.topic_detail",
    MessageAdminConsumerLag => "message.admin.consumer_lag",
    MessageAdminDelete => "message.admin.delete",
}

impl CapabilityOperation {
    pub const SQL: &'static [Self] = &[
        Self::SqlQuery,
        Self::SqlQueryBounded,
        Self::SqlExecute,
        Self::SqlListSchemas,
        Self::SqlListTables,
        Self::SqlDescribeTable,
    ];
    pub const CQL: &'static [Self] = &[
        Self::CqlQuery,
        Self::CqlQueryBounded,
        Self::CqlExecute,
        Self::CqlListKeyspaces,
        Self::CqlListTables,
        Self::CqlDescribeTable,
    ];
    pub const DB2: &'static [Self] = &[
        Self::Db2ListSequences,
        Self::Db2ListRoutines,
        Self::Db2ListTablespaces,
        Self::Db2ListForeignKeys,
        Self::Db2GenerateDdl,
    ];
    pub const KEY_VALUE: &'static [Self] = &[
        Self::KeyValueGet,
        Self::KeyValueSet,
        Self::KeyValueDelete,
        Self::KeyValueScan,
        Self::KeyValueRawCommand,
    ];
    pub const DOCUMENT: &'static [Self] = &[
        Self::DocumentListCollections,
        Self::DocumentFind,
        Self::DocumentInsert,
        Self::DocumentUpdate,
        Self::DocumentDelete,
        Self::DocumentAggregate,
        Self::DocumentAggregateBounded,
        Self::DocumentDropCollection,
    ];
    pub const TIME_SERIES: &'static [Self] = &[
        Self::TimeSeriesListMeasurements,
        Self::TimeSeriesWritePoints,
        Self::TimeSeriesQueryRange,
    ];
    pub const SEARCH: &'static [Self] = &[
        Self::SearchListIndices,
        Self::SearchSearch,
        Self::SearchIndexDocument,
        Self::SearchPutDocument,
        Self::SearchGetDocument,
        Self::SearchUpdateDocument,
        Self::SearchDeleteDocument,
        Self::SearchDeleteIndex,
    ];
    pub const MESSAGE_PRODUCER: &'static [Self] = &[Self::MessageProduce];
    pub const MESSAGE_CONSUMER: &'static [Self] = &[Self::MessageConsume];
    /// Admin operations must be declared by connector-specific overrides.
    pub const MESSAGE_ADMIN: &'static [Self] = &[
        Self::MessageAdminListTopics,
        Self::MessageAdminTopicDetail,
        Self::MessageAdminConsumerLag,
        Self::MessageAdminDelete,
    ];
}

/// Bitflag set of capabilities a connector exposes.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Capabilities {
    pub sql: bool,
    pub cql: bool,
    pub db2: bool,
    pub key_value: bool,
    pub document: bool,
    pub time_series: bool,
    pub search: bool,
    pub producer: bool,
    pub consumer: bool,
    pub admin: bool,
}

impl Capabilities {
    /// Derive method-level operations from the legacy coarse capability flags.
    ///
    /// `admin=true` is deliberately not expanded: legacy admin connectors may
    /// implement only a subset of list, detail, lag, and deletion operations.
    /// Such connectors must override [`Connector::operations`] explicitly.
    pub fn operations(self) -> Vec<CapabilityOperation> {
        let mut operations = Vec::new();

        if self.sql {
            operations.extend_from_slice(CapabilityOperation::SQL);
        }
        if self.cql {
            operations.extend_from_slice(CapabilityOperation::CQL);
        }
        if self.db2 {
            operations.extend_from_slice(CapabilityOperation::DB2);
        }
        if self.key_value {
            operations.extend_from_slice(CapabilityOperation::KEY_VALUE);
        }
        if self.document {
            operations.extend_from_slice(CapabilityOperation::DOCUMENT);
        }
        if self.time_series {
            operations.extend_from_slice(CapabilityOperation::TIME_SERIES);
        }
        if self.search {
            operations.extend_from_slice(CapabilityOperation::SEARCH);
        }
        if self.producer {
            operations.extend_from_slice(CapabilityOperation::MESSAGE_PRODUCER);
        }
        if self.consumer {
            operations.extend_from_slice(CapabilityOperation::MESSAGE_CONSUMER);
        }

        operations
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorKind(pub String);

/// Base trait — all adapters implement this.
/// Capability accessors default to `None`; adapters override only what they support.
#[async_trait]
pub trait Connector: Send + Sync {
    fn kind(&self) -> ConnectorKind;
    fn capabilities(&self) -> Capabilities;
    /// Return the precise methods callers may invoke on this connector.
    ///
    /// The default preserves compatibility for non-admin connectors by
    /// expanding their legacy capability booleans. Admin methods are never
    /// inferred from `Capabilities::admin`; partial admin implementations must
    /// override this method and append only operations they actually support.
    ///
    /// Embedded callers should negotiate before downcasting and invoking a
    /// capability method:
    ///
    /// ```no_run
    /// # use dbtool_core::port::{CapabilityOperation, Connector};
    /// # async fn read_key(connector: &dyn Connector) -> dbtool_core::Result<()> {
    /// if connector
    ///     .operations()
    ///     .contains(&CapabilityOperation::KeyValueGet)
    /// {
    ///     let key_value = connector
    ///         .as_kv()
    ///         .expect("kv.get declaration requires the KeyValueStore accessor");
    ///     let _value = key_value.get("app:health").await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    fn operations(&self) -> Vec<CapabilityOperation> {
        self.capabilities().operations()
    }
    async fn ping(&self) -> Result<()>;
    async fn close(self: Box<Self>) -> Result<()>;

    // ── Capability accessors ─────────────────────────────────────────────────
    fn as_sql(&self) -> Option<&dyn SqlEngine> {
        None
    }
    fn as_cql(&self) -> Option<&dyn CqlEngine> {
        None
    }
    fn as_db2(&self) -> Option<&dyn Db2Engine> {
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
    fn as_admin_mutate(&self) -> Option<&dyn AdminMutate> {
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
    use std::collections::{HashMap, HashSet};
    use tokio::sync::Mutex;

    struct LegacyConnector(Capabilities);

    #[async_trait]
    impl Connector for LegacyConnector {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind("legacy".to_owned())
        }

        fn capabilities(&self) -> Capabilities {
            self.0
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }

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
        assert_eq!(connector.operations(), CapabilityOperation::KEY_VALUE);
        assert!(connector.as_kv().is_some());
        assert!(connector.as_sql().is_none());
        assert!(connector.as_cql().is_none());
        assert!(connector.as_document().is_none());
        assert!(connector.as_producer().is_none());
        assert!(connector.as_admin_mutate().is_none());
        connector.ping().await.unwrap();
    }

    #[test]
    fn operation_names_are_unique_stable_and_round_trip_through_serde() {
        let mut names = HashSet::new();

        for operation in CapabilityOperation::ALL {
            assert!(names.insert(operation.as_str()));
            assert_eq!(
                serde_json::to_value(operation).unwrap(),
                serde_json::Value::String(operation.as_str().to_owned())
            );
            assert_eq!(
                serde_json::from_value::<CapabilityOperation>(serde_json::Value::String(
                    operation.as_str().to_owned()
                ))
                .unwrap(),
                *operation
            );
        }
    }

    #[test]
    fn legacy_flags_derive_every_non_admin_method_but_never_guess_admin_methods() {
        let connector = LegacyConnector(Capabilities {
            sql: true,
            cql: true,
            db2: true,
            key_value: true,
            document: true,
            time_series: true,
            search: true,
            producer: true,
            consumer: true,
            admin: true,
        });
        let expected = CapabilityOperation::ALL
            .iter()
            .copied()
            .filter(|operation| !CapabilityOperation::MESSAGE_ADMIN.contains(operation))
            .collect::<Vec<_>>();

        assert_eq!(connector.operations(), expected);
        assert!(CapabilityOperation::MESSAGE_ADMIN
            .iter()
            .all(|operation| !connector.operations().contains(operation)));

        let admin_only = LegacyConnector(Capabilities {
            admin: true,
            ..Default::default()
        });
        assert!(admin_only.operations().is_empty());
    }

    #[tokio::test]
    async fn embedded_callers_can_negotiate_before_downcasting_and_invoking() {
        let connector = MockKvConnector::default();
        let operations = connector.operations();

        assert!(operations.contains(&CapabilityOperation::KeyValueSet));
        assert!(!operations.contains(&CapabilityOperation::SqlQuery));

        if operations.contains(&CapabilityOperation::KeyValueSet) {
            connector
                .as_kv()
                .expect("kv.set requires a matching KeyValueStore accessor")
                .set("negotiated", b"yes", SetOptions::default())
                .await
                .unwrap();
        }

        assert_eq!(
            connector.as_kv().unwrap().get("negotiated").await.unwrap(),
            Some(Bytes::from_static(b"yes"))
        );
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
