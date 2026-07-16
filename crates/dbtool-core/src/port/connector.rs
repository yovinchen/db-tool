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
    SqlQueryBudgeted => "sql.query_budgeted",
    SqlExecute => "sql.execute",
    SqlExecuteBudgeted => "sql.execute_budgeted",
    SqlInsertRowsAtomic => "sql.insert_rows_atomic",
    SqlInsertRowsAtomicBudgeted => "sql.insert_rows_atomic_budgeted",
    SqlListSchemas => "sql.list_schemas",
    SqlListSchemasBounded => "sql.list_schemas_bounded",
    SqlListSchemasBudgeted => "sql.list_schemas_budgeted",
    SqlListTables => "sql.list_tables",
    SqlListTablesBounded => "sql.list_tables_bounded",
    SqlListTablesBudgeted => "sql.list_tables_budgeted",
    SqlDescribeTable => "sql.describe_table",
    SqlDescribeTableBounded => "sql.describe_table_bounded",
    CqlQuery => "cql.query",
    CqlQueryBounded => "cql.query_bounded",
    CqlQueryBudgeted => "cql.query_budgeted",
    CqlExecute => "cql.execute",
    CqlExecuteBudgeted => "cql.execute_budgeted",
    CqlListKeyspaces => "cql.list_keyspaces",
    CqlListKeyspacesBounded => "cql.list_keyspaces_bounded",
    CqlListKeyspacesBudgeted => "cql.list_keyspaces_budgeted",
    CqlListTables => "cql.list_tables",
    CqlListTablesBounded => "cql.list_tables_bounded",
    CqlListTablesBudgeted => "cql.list_tables_budgeted",
    CqlDescribeTable => "cql.describe_table",
    CqlDescribeTableBounded => "cql.describe_table_bounded",
    Db2ListSequences => "db2.list_sequences",
    Db2ListSequencesBounded => "db2.list_sequences_bounded",
    Db2ListSequencesBudgeted => "db2.list_sequences_budgeted",
    Db2ListRoutines => "db2.list_routines",
    Db2ListRoutinesBounded => "db2.list_routines_bounded",
    Db2ListRoutinesBudgeted => "db2.list_routines_budgeted",
    Db2ListTablespaces => "db2.list_tablespaces",
    Db2ListTablespacesBounded => "db2.list_tablespaces_bounded",
    Db2ListTablespacesBudgeted => "db2.list_tablespaces_budgeted",
    Db2ListForeignKeys => "db2.list_foreign_keys",
    Db2ListForeignKeysBounded => "db2.list_foreign_keys_bounded",
    Db2ListForeignKeysBudgeted => "db2.list_foreign_keys_budgeted",
    Db2GenerateDdl => "db2.generate_ddl",
    Db2GenerateDdlBounded => "db2.generate_ddl_bounded",
    KeyValueGet => "kv.get",
    KeyValueExists => "kv.exists",
    KeyValueGetBounded => "kv.get_bounded",
    KeyValueGetWithExpiry => "kv.get_with_expiry",
    KeyValueGetWithExpiryBounded => "kv.get_with_expiry_bounded",
    KeyValueSet => "kv.set",
    KeyValueSetBudgeted => "kv.set_budgeted",
    KeyValueRestoreWithExpiry => "kv.restore_with_expiry",
    KeyValueRestoreWithExpiryBudgeted => "kv.restore_with_expiry_budgeted",
    KeyValueDelete => "kv.delete",
    KeyValueDeleteBudgeted => "kv.delete_budgeted",
    KeyValueScan => "kv.scan",
    KeyValueScanBounded => "kv.scan_bounded",
    KeyValueRawCommand => "kv.raw_command",
    KeyValueRawCommandBounded => "kv.raw_command_bounded",
    KeyValueRawCommandIoBudgeted => "kv.raw_command_io_budgeted",
    DocumentListCollections => "document.list_collections",
    DocumentListCollectionsBounded => "document.list_collections_bounded",
    DocumentListCollectionsBudgeted => "document.list_collections_budgeted",
    DocumentFind => "document.find",
    DocumentFindBudgeted => "document.find_budgeted",
    DocumentInsert => "document.insert",
    DocumentInsertBudgeted => "document.insert_budgeted",
    DocumentUpdate => "document.update",
    DocumentDelete => "document.delete",
    DocumentUpdateOne => "document.update_one",
    DocumentUpdateOneBudgeted => "document.update_one_budgeted",
    DocumentUpdateMany => "document.update_many",
    DocumentUpdateManyBudgeted => "document.update_many_budgeted",
    DocumentDeleteOne => "document.delete_one",
    DocumentDeleteOneBudgeted => "document.delete_one_budgeted",
    DocumentDeleteMany => "document.delete_many",
    DocumentDeleteManyBudgeted => "document.delete_many_budgeted",
    DocumentAggregate => "document.aggregate",
    DocumentAggregateBounded => "document.aggregate_bounded",
    DocumentAggregateBudgeted => "document.aggregate_budgeted",
    DocumentDropCollection => "document.drop_collection",
    DocumentDropCollectionBudgeted => "document.drop_collection_budgeted",
    TimeSeriesListMeasurements => "time_series.list_measurements",
    TimeSeriesListMeasurementsBounded => "time_series.list_measurements_bounded",
    TimeSeriesListMeasurementsBudgeted => "time_series.list_measurements_budgeted",
    TimeSeriesWritePoints => "time_series.write_points",
    TimeSeriesWritePointsBudgeted => "time_series.write_points_budgeted",
    TimeSeriesQueryRange => "time_series.query_range",
    TimeSeriesQueryRangeBounded => "time_series.query_range_bounded",
    SearchListIndices => "search.list_indices",
    SearchListIndicesBounded => "search.list_indices_bounded",
    SearchListIndicesBudgeted => "search.list_indices_budgeted",
    SearchSearch => "search.search",
    SearchSearchBudgeted => "search.search_budgeted",
    SearchIndexDocument => "search.index_doc",
    SearchIndexDocumentBudgeted => "search.index_doc_budgeted",
    SearchPutDocument => "search.put_doc",
    SearchPutDocumentBudgeted => "search.put_doc_budgeted",
    SearchGetDocument => "search.get_doc",
    SearchGetDocumentBudgeted => "search.get_doc_budgeted",
    SearchUpdateDocument => "search.update_doc",
    SearchUpdateDocumentBudgeted => "search.update_doc_budgeted",
    SearchDeleteDocument => "search.delete_doc",
    SearchDeleteDocumentBudgeted => "search.delete_doc_budgeted",
    SearchDeleteIndex => "search.delete_index",
    SearchDeleteIndexBudgeted => "search.delete_index_budgeted",
    MessageProduce => "message.produce",
    MessageProduceBudgeted => "message.produce_budgeted",
    MessageConsume => "message.consume",
    MessageConsumeGroup => "message.consume_group",
    MessageConsumeDurable => "message.consume_durable",
    MessageConsumeAck => "message.consume_ack",
    MessageAdminListTopics => "message.admin.list_topics",
    MessageAdminListTopicsBounded => "message.admin.list_topics_bounded",
    MessageAdminListTopicsBudgeted => "message.admin.list_topics_budgeted",
    MessageAdminTopicDetail => "message.admin.topic_detail",
    MessageAdminTopicDetailBounded => "message.admin.topic_detail_bounded",
    MessageAdminConsumerLag => "message.admin.consumer_lag",
    MessageAdminConsumerLagBounded => "message.admin.consumer_lag_bounded",
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
    /// Exact SQL mutation contracts. Legacy `sql=true` and the corresponding
    /// unbudgeted operation names never authorize these methods.
    pub const SQL_BUDGETED_MUTATIONS: &'static [Self] =
        &[Self::SqlExecuteBudgeted, Self::SqlInsertRowsAtomicBudgeted];
    /// Exact CQL mutation contracts. Legacy `cql=true` never authorizes them.
    pub const CQL_BUDGETED_MUTATIONS: &'static [Self] = &[Self::CqlExecuteBudgeted];
    /// Exact key-value mutation contracts, including the raw command contract
    /// whose input and response budgets are intentionally separate.
    pub const KEY_VALUE_BUDGETED_MUTATIONS: &'static [Self] = &[
        Self::KeyValueSetBudgeted,
        Self::KeyValueRestoreWithExpiryBudgeted,
        Self::KeyValueDeleteBudgeted,
        Self::KeyValueRawCommandIoBudgeted,
    ];
    /// Exact document mutation contracts. Target-only drop operations have no
    /// document body, but still budget their variable-sized target input and
    /// retain the same exact preflight and outcome semantics.
    pub const DOCUMENT_BUDGETED_MUTATIONS: &'static [Self] = &[
        Self::DocumentInsertBudgeted,
        Self::DocumentUpdateOneBudgeted,
        Self::DocumentUpdateManyBudgeted,
        Self::DocumentDeleteOneBudgeted,
        Self::DocumentDeleteManyBudgeted,
        Self::DocumentDropCollectionBudgeted,
    ];
    /// Exact time-series mutation contracts.
    pub const TIME_SERIES_BUDGETED_MUTATIONS: &'static [Self] =
        &[Self::TimeSeriesWritePointsBudgeted];
    /// Exact search mutation contracts. Target-only deletion methods still
    /// budget their variable-sized index and identifier inputs.
    pub const SEARCH_BUDGETED_MUTATIONS: &'static [Self] = &[
        Self::SearchIndexDocumentBudgeted,
        Self::SearchPutDocumentBudgeted,
        Self::SearchUpdateDocumentBudgeted,
        Self::SearchDeleteDocumentBudgeted,
        Self::SearchDeleteIndexBudgeted,
    ];
    /// Every first-party exact mutation operation governed by the shared
    /// preflight and post-write outcome contract.
    pub const BUDGETED_MUTATIONS: &'static [Self] = &[
        Self::SqlExecuteBudgeted,
        Self::SqlInsertRowsAtomicBudgeted,
        Self::CqlExecuteBudgeted,
        Self::KeyValueSetBudgeted,
        Self::KeyValueRestoreWithExpiryBudgeted,
        Self::KeyValueDeleteBudgeted,
        Self::KeyValueRawCommandIoBudgeted,
        Self::DocumentInsertBudgeted,
        Self::DocumentUpdateOneBudgeted,
        Self::DocumentUpdateManyBudgeted,
        Self::DocumentDeleteOneBudgeted,
        Self::DocumentDeleteManyBudgeted,
        Self::DocumentDropCollectionBudgeted,
        Self::TimeSeriesWritePointsBudgeted,
        Self::SearchIndexDocumentBudgeted,
        Self::SearchPutDocumentBudgeted,
        Self::SearchUpdateDocumentBudgeted,
        Self::SearchDeleteDocumentBudgeted,
        Self::SearchDeleteIndexBudgeted,
    ];
    /// Item-and-byte read envelopes are optional backend-aware contracts.
    /// Legacy family booleans and row/item-only bounded methods never imply
    /// them.
    pub const BUDGETED_READS: &'static [Self] = &[
        Self::SqlQueryBudgeted,
        Self::SqlListSchemasBudgeted,
        Self::SqlListTablesBudgeted,
        Self::CqlQueryBudgeted,
        Self::CqlListKeyspacesBudgeted,
        Self::CqlListTablesBudgeted,
        Self::Db2ListSequencesBudgeted,
        Self::Db2ListRoutinesBudgeted,
        Self::Db2ListTablespacesBudgeted,
        Self::Db2ListForeignKeysBudgeted,
        Self::KeyValueGetBounded,
        Self::KeyValueGetWithExpiryBounded,
        Self::KeyValueScanBounded,
        Self::KeyValueRawCommandBounded,
        Self::DocumentFindBudgeted,
        Self::DocumentAggregateBudgeted,
        Self::DocumentListCollectionsBudgeted,
        Self::TimeSeriesQueryRangeBounded,
        Self::TimeSeriesListMeasurementsBudgeted,
        Self::SearchSearchBudgeted,
        Self::SearchGetDocumentBudgeted,
        Self::SearchListIndicesBudgeted,
        Self::MessageAdminListTopicsBudgeted,
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
    /// Native key-existence checks are optional. The legacy `kv.get`
    /// operation is not a safe substitute because it materializes the value.
    pub const KEY_VALUE_EXISTENCE: &'static [Self] = &[Self::KeyValueExists];
    /// Complete key-value read envelopes are optional backend-aware contracts.
    /// Neither the legacy family boolean nor the similarly named unbounded or
    /// row-only operation authorizes these methods.
    pub const KEY_VALUE_BUDGETED_READS: &'static [Self] = &[
        Self::KeyValueGetBounded,
        Self::KeyValueGetWithExpiryBounded,
        Self::KeyValueScanBounded,
        Self::KeyValueRawCommandBounded,
    ];
    /// Atomic lifetime operations are optional and must be declared by each
    /// connector. The legacy `key_value=true` flag never implies that a
    /// backend can observe or restore a value and its expiry atomically.
    pub const KEY_VALUE_LIFETIME: &'static [Self] =
        &[Self::KeyValueGetWithExpiry, Self::KeyValueRestoreWithExpiry];
    pub const DOCUMENT: &'static [Self] = &[
        Self::DocumentListCollections,
        Self::DocumentFind,
        Self::DocumentInsert,
        Self::DocumentUpdate,
        Self::DocumentDelete,
        Self::DocumentAggregate,
        Self::DocumentAggregateBounded,
    ];
    /// Collection lifecycle is optional. A legacy `document=true` connector
    /// may rely on the trait's fail-closed default and must not advertise this
    /// method until its adapter implements and verifies the backend operation.
    pub const DOCUMENT_LIFECYCLE: &'static [Self] = &[Self::DocumentDropCollection];
    pub const TIME_SERIES: &'static [Self] = &[
        Self::TimeSeriesListMeasurements,
        Self::TimeSeriesWritePoints,
        Self::TimeSeriesQueryRange,
    ];
    /// Complete time-series range envelopes are optional because their series,
    /// cumulative-sample, and byte limits must be enforced by the backend
    /// adapter rather than inferred from the legacy family flag.
    pub const TIME_SERIES_BUDGETED_READS: &'static [Self] = &[Self::TimeSeriesQueryRangeBounded];
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
    /// Complete search-result envelopes are optional backend-aware contracts.
    /// The legacy search family and its unbounded methods never authorize
    /// these exact operations.
    pub const SEARCH_BUDGETED_READS: &'static [Self] =
        &[Self::SearchSearchBudgeted, Self::SearchGetDocumentBudgeted];
    pub const MESSAGE_PRODUCER: &'static [Self] = &[Self::MessageProduce];
    /// Exact producer input envelopes are optional. The legacy producer flag
    /// and `message.produce` operation never imply this contract.
    pub const MESSAGE_PRODUCER_BUDGETED: &'static [Self] = &[Self::MessageProduceBudgeted];
    pub const MESSAGE_CONSUMER: &'static [Self] = &[Self::MessageConsume];
    /// Stateful identity and acknowledgement operations are optional and must
    /// be declared by connector-specific overrides. A legacy `consumer=true`
    /// flag never implies broker state mutation support.
    pub const MESSAGE_CONSUMER_EXTENSIONS: &'static [Self] = &[
        Self::MessageConsumeGroup,
        Self::MessageConsumeDurable,
        Self::MessageConsumeAck,
    ];
    /// Admin operations must be declared by connector-specific overrides.
    pub const MESSAGE_ADMIN: &'static [Self] = &[
        Self::MessageAdminListTopics,
        Self::MessageAdminTopicDetail,
        Self::MessageAdminTopicDetailBounded,
        Self::MessageAdminConsumerLag,
        Self::MessageAdminConsumerLagBounded,
        Self::MessageAdminDelete,
    ];
    /// Bounded catalog operations require backend-aware N+1 reads and are
    /// therefore never inferred from any legacy coarse capability flag.
    pub const BOUNDED_CATALOGS: &'static [Self] = &[
        Self::SqlListSchemasBounded,
        Self::SqlListTablesBounded,
        Self::CqlListKeyspacesBounded,
        Self::CqlListTablesBounded,
        Self::Db2ListSequencesBounded,
        Self::Db2ListRoutinesBounded,
        Self::Db2ListTablespacesBounded,
        Self::Db2ListForeignKeysBounded,
        Self::DocumentListCollectionsBounded,
        Self::TimeSeriesListMeasurementsBounded,
        Self::SearchListIndicesBounded,
        Self::MessageAdminListTopicsBounded,
    ];
    /// Catalog reads that enforce both N+1 completeness and the caller's
    /// serialized byte envelope. These exact methods are optional and never
    /// inferred from legacy family flags or item-only bounded operations.
    pub const BUDGETED_CATALOGS: &'static [Self] = &[
        Self::SqlListSchemasBudgeted,
        Self::SqlListTablesBudgeted,
        Self::CqlListKeyspacesBudgeted,
        Self::CqlListTablesBudgeted,
        Self::Db2ListSequencesBudgeted,
        Self::Db2ListRoutinesBudgeted,
        Self::Db2ListTablespacesBudgeted,
        Self::Db2ListForeignKeysBudgeted,
        Self::DocumentListCollectionsBudgeted,
        Self::TimeSeriesListMeasurementsBudgeted,
        Self::SearchListIndicesBudgeted,
        Self::MessageAdminListTopicsBudgeted,
    ];
    /// Complete-object metadata methods that enforce both nested-item and byte
    /// budgets. These are optional and never inferred from legacy booleans or
    /// their unbounded compatibility methods.
    pub const BOUNDED_NESTED_METADATA: &'static [Self] = &[
        Self::SqlDescribeTableBounded,
        Self::CqlDescribeTableBounded,
        Self::Db2GenerateDdlBounded,
        Self::MessageAdminTopicDetailBounded,
        Self::MessageAdminConsumerLagBounded,
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

/// Backward-compatible capability report used by CLI, TUI, and embedded
/// callers. Legacy family booleans remain flattened while `operations` is the
/// authoritative, stable, method-level negotiation surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityReport {
    #[serde(flatten)]
    pub legacy: Capabilities,
    #[serde(default)]
    pub operations: Vec<CapabilityOperation>,
}

impl CapabilityReport {
    pub fn new(legacy: Capabilities, mut operations: Vec<CapabilityOperation>) -> Self {
        operations.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        operations.dedup();
        Self { legacy, operations }
    }
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
        model::{Message, ProduceBudget, ProduceOutcome, Value},
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

    #[async_trait]
    impl MessageProducer for LegacyConnector {
        async fn produce(&self, _target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
            Ok(ProduceOutcome {
                produced: messages.len() as u64,
                placements: Vec::new(),
            })
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
        assert!(CapabilityOperation::KEY_VALUE_BUDGETED_READS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::KEY_VALUE_BUDGETED_MUTATIONS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(connector.as_kv().is_some());
        assert!(connector.as_sql().is_none());
        assert!(connector.as_cql().is_none());
        assert!(connector.as_document().is_none());
        assert!(connector.as_producer().is_none());
        assert!(connector.as_admin_mutate().is_none());
        connector.ping().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_kv_connectors_neither_claim_nor_inherit_optional_read_operations() {
        let connector = MockKvConnector::default();
        assert!(CapabilityOperation::KEY_VALUE_EXISTENCE
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::KEY_VALUE_LIFETIME
            .iter()
            .all(|operation| !connector.operations().contains(operation)));

        assert!(matches!(
            connector.get_with_expiry("key").await,
            Err(crate::Error::UnsupportedCapability { kind, needed })
                if kind == "mock-kv" && needed == "KeyValueStore.get_with_expiry"
        ));
        assert!(matches!(
            connector
                .restore_with_expiry(
                    "key",
                    b"value",
                    crate::model::KeyExpiry::Persistent,
                    false,
                )
                .await,
            Err(crate::Error::UnsupportedCapability { kind, needed })
                if kind == "mock-kv" && needed == "KeyValueStore.restore_with_expiry"
        ));
        assert!(matches!(
            connector.exists("key").await,
            Err(crate::Error::UnsupportedCapability { kind, needed })
                if kind == "mock-kv" && needed == "KeyValueStore.exists"
        ));
        let budget = crate::model::ReadBudget::with_default_bytes(2).unwrap();
        assert!(matches!(
            connector.get_bounded("key", budget).await,
            Err(crate::Error::UnsupportedCapability { kind, needed })
                if kind == "mock-kv" && needed == "KeyValueStore.get_bounded"
        ));
        assert!(matches!(
            connector.get_with_expiry_bounded("key", budget).await,
            Err(crate::Error::UnsupportedCapability { kind, needed })
                if kind == "mock-kv" && needed == "KeyValueStore.get_with_expiry_bounded"
        ));
        assert!(matches!(
            connector.scan_bounded("*", budget).await,
            Err(crate::Error::UnsupportedCapability { kind, needed })
                if kind == "mock-kv" && needed == "KeyValueStore.scan_bounded"
        ));
        assert!(matches!(
            connector
                .raw_command_bounded(&["GET".to_owned(), "key".to_owned()], budget)
                .await,
            Err(crate::Error::UnsupportedCapability { kind, needed })
                if kind == "mock-kv" && needed == "KeyValueStore.raw_command_bounded"
        ));
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

        assert_eq!(
            serde_json::to_value(CapabilityOperation::SqlQueryBudgeted).unwrap(),
            serde_json::json!("sql.query_budgeted")
        );
        assert_eq!(
            serde_json::to_value(CapabilityOperation::CqlQueryBudgeted).unwrap(),
            serde_json::json!("cql.query_budgeted")
        );
        assert_eq!(
            serde_json::to_value(CapabilityOperation::DocumentFindBudgeted).unwrap(),
            serde_json::json!("document.find_budgeted")
        );
        assert_eq!(
            serde_json::to_value(CapabilityOperation::DocumentAggregateBudgeted).unwrap(),
            serde_json::json!("document.aggregate_budgeted")
        );
        assert_eq!(
            serde_json::to_value(CapabilityOperation::TimeSeriesQueryRangeBounded).unwrap(),
            serde_json::json!("time_series.query_range_bounded")
        );
        for (operation, stable_name) in [
            (CapabilityOperation::KeyValueExists, "kv.exists"),
            (CapabilityOperation::KeyValueGetBounded, "kv.get_bounded"),
            (
                CapabilityOperation::KeyValueGetWithExpiryBounded,
                "kv.get_with_expiry_bounded",
            ),
            (CapabilityOperation::KeyValueScanBounded, "kv.scan_bounded"),
            (
                CapabilityOperation::KeyValueRawCommandBounded,
                "kv.raw_command_bounded",
            ),
            (
                CapabilityOperation::SearchSearchBudgeted,
                "search.search_budgeted",
            ),
            (
                CapabilityOperation::SearchGetDocumentBudgeted,
                "search.get_doc_budgeted",
            ),
            (
                CapabilityOperation::SqlListSchemasBudgeted,
                "sql.list_schemas_budgeted",
            ),
            (
                CapabilityOperation::SqlListTablesBudgeted,
                "sql.list_tables_budgeted",
            ),
            (
                CapabilityOperation::CqlListKeyspacesBudgeted,
                "cql.list_keyspaces_budgeted",
            ),
            (
                CapabilityOperation::CqlListTablesBudgeted,
                "cql.list_tables_budgeted",
            ),
            (
                CapabilityOperation::Db2ListSequencesBudgeted,
                "db2.list_sequences_budgeted",
            ),
            (
                CapabilityOperation::Db2ListRoutinesBudgeted,
                "db2.list_routines_budgeted",
            ),
            (
                CapabilityOperation::Db2ListTablespacesBudgeted,
                "db2.list_tablespaces_budgeted",
            ),
            (
                CapabilityOperation::Db2ListForeignKeysBudgeted,
                "db2.list_foreign_keys_budgeted",
            ),
            (
                CapabilityOperation::DocumentListCollectionsBudgeted,
                "document.list_collections_budgeted",
            ),
            (
                CapabilityOperation::TimeSeriesListMeasurementsBudgeted,
                "time_series.list_measurements_budgeted",
            ),
            (
                CapabilityOperation::SearchListIndicesBudgeted,
                "search.list_indices_budgeted",
            ),
            (
                CapabilityOperation::MessageAdminListTopicsBudgeted,
                "message.admin.list_topics_budgeted",
            ),
            (
                CapabilityOperation::MessageProduceBudgeted,
                "message.produce_budgeted",
            ),
            (
                CapabilityOperation::SqlExecuteBudgeted,
                "sql.execute_budgeted",
            ),
            (
                CapabilityOperation::SqlInsertRowsAtomicBudgeted,
                "sql.insert_rows_atomic_budgeted",
            ),
            (
                CapabilityOperation::CqlExecuteBudgeted,
                "cql.execute_budgeted",
            ),
            (CapabilityOperation::KeyValueSetBudgeted, "kv.set_budgeted"),
            (
                CapabilityOperation::KeyValueRestoreWithExpiryBudgeted,
                "kv.restore_with_expiry_budgeted",
            ),
            (
                CapabilityOperation::KeyValueDeleteBudgeted,
                "kv.delete_budgeted",
            ),
            (
                CapabilityOperation::KeyValueRawCommandIoBudgeted,
                "kv.raw_command_io_budgeted",
            ),
            (
                CapabilityOperation::DocumentInsertBudgeted,
                "document.insert_budgeted",
            ),
            (
                CapabilityOperation::DocumentUpdateOneBudgeted,
                "document.update_one_budgeted",
            ),
            (
                CapabilityOperation::DocumentUpdateManyBudgeted,
                "document.update_many_budgeted",
            ),
            (
                CapabilityOperation::DocumentDeleteOneBudgeted,
                "document.delete_one_budgeted",
            ),
            (
                CapabilityOperation::DocumentDeleteManyBudgeted,
                "document.delete_many_budgeted",
            ),
            (
                CapabilityOperation::DocumentDropCollectionBudgeted,
                "document.drop_collection_budgeted",
            ),
            (
                CapabilityOperation::TimeSeriesWritePointsBudgeted,
                "time_series.write_points_budgeted",
            ),
            (
                CapabilityOperation::SearchIndexDocumentBudgeted,
                "search.index_doc_budgeted",
            ),
            (
                CapabilityOperation::SearchPutDocumentBudgeted,
                "search.put_doc_budgeted",
            ),
            (
                CapabilityOperation::SearchUpdateDocumentBudgeted,
                "search.update_doc_budgeted",
            ),
            (
                CapabilityOperation::SearchDeleteDocumentBudgeted,
                "search.delete_doc_budgeted",
            ),
            (
                CapabilityOperation::SearchDeleteIndexBudgeted,
                "search.delete_index_budgeted",
            ),
        ] {
            assert_eq!(operation.as_str(), stable_name);
            assert_eq!(
                serde_json::to_value(operation).unwrap(),
                serde_json::Value::String(stable_name.to_owned())
            );
            assert_eq!(
                serde_json::from_value::<CapabilityOperation>(serde_json::Value::String(
                    stable_name.to_owned()
                ))
                .unwrap(),
                operation
            );
        }
        assert_eq!(
            CapabilityOperation::KEY_VALUE_BUDGETED_READS,
            [
                CapabilityOperation::KeyValueGetBounded,
                CapabilityOperation::KeyValueGetWithExpiryBounded,
                CapabilityOperation::KeyValueScanBounded,
                CapabilityOperation::KeyValueRawCommandBounded,
            ]
        );
        assert_eq!(
            CapabilityOperation::TIME_SERIES_BUDGETED_READS,
            [CapabilityOperation::TimeSeriesQueryRangeBounded]
        );
        assert_eq!(
            CapabilityOperation::SEARCH_BUDGETED_READS,
            [
                CapabilityOperation::SearchSearchBudgeted,
                CapabilityOperation::SearchGetDocumentBudgeted,
            ]
        );
        assert!(!CapabilityOperation::TIME_SERIES
            .contains(&CapabilityOperation::TimeSeriesQueryRangeBounded));
        assert!(CapabilityOperation::SEARCH_BUDGETED_READS
            .iter()
            .all(|operation| !CapabilityOperation::SEARCH.contains(operation)));
        assert!(CapabilityOperation::TIME_SERIES_BUDGETED_READS
            .iter()
            .all(|operation| CapabilityOperation::BUDGETED_READS.contains(operation)));
        assert!(CapabilityOperation::SEARCH_BUDGETED_READS
            .iter()
            .all(|operation| CapabilityOperation::BUDGETED_READS.contains(operation)));
        assert!(CapabilityOperation::KEY_VALUE_BUDGETED_READS
            .iter()
            .all(|operation| CapabilityOperation::BUDGETED_READS.contains(operation)));
        assert_eq!(CapabilityOperation::BUDGETED_CATALOGS.len(), 12);
        assert!(CapabilityOperation::BUDGETED_CATALOGS
            .iter()
            .all(|operation| CapabilityOperation::BUDGETED_READS.contains(operation)));
        assert!(CapabilityOperation::BUDGETED_CATALOGS
            .iter()
            .all(|operation| !CapabilityOperation::BOUNDED_CATALOGS.contains(operation)));
        assert_eq!(
            CapabilityOperation::KEY_VALUE_EXISTENCE,
            [CapabilityOperation::KeyValueExists]
        );
        assert_eq!(CapabilityOperation::BUDGETED_MUTATIONS.len(), 19);
        for family in [
            CapabilityOperation::SQL_BUDGETED_MUTATIONS,
            CapabilityOperation::CQL_BUDGETED_MUTATIONS,
            CapabilityOperation::KEY_VALUE_BUDGETED_MUTATIONS,
            CapabilityOperation::DOCUMENT_BUDGETED_MUTATIONS,
            CapabilityOperation::TIME_SERIES_BUDGETED_MUTATIONS,
            CapabilityOperation::SEARCH_BUDGETED_MUTATIONS,
        ] {
            assert!(family
                .iter()
                .all(|operation| CapabilityOperation::BUDGETED_MUTATIONS.contains(operation)));
        }
        assert!(CapabilityOperation::BUDGETED_MUTATIONS
            .iter()
            .all(|operation| {
                !CapabilityOperation::SQL.contains(operation)
                    && !CapabilityOperation::CQL.contains(operation)
                    && !CapabilityOperation::KEY_VALUE.contains(operation)
                    && !CapabilityOperation::DOCUMENT.contains(operation)
                    && !CapabilityOperation::TIME_SERIES.contains(operation)
                    && !CapabilityOperation::SEARCH.contains(operation)
            }));
    }

    #[test]
    fn legacy_kv_operation_reports_do_not_authorize_exact_bounded_reads() {
        let report = CapabilityReport::new(
            Capabilities {
                key_value: true,
                ..Default::default()
            },
            vec![
                CapabilityOperation::KeyValueGet,
                CapabilityOperation::KeyValueGetWithExpiry,
                CapabilityOperation::KeyValueScan,
                CapabilityOperation::KeyValueRawCommand,
            ],
        );

        assert!(CapabilityOperation::KEY_VALUE_BUDGETED_READS
            .iter()
            .all(|operation| !report.operations.contains(operation)));
        assert!(!report
            .operations
            .contains(&CapabilityOperation::KeyValueExists));
        assert!(report
            .operations
            .contains(&CapabilityOperation::KeyValueGet));
        assert!(report
            .operations
            .contains(&CapabilityOperation::KeyValueGetWithExpiry));
        assert!(report
            .operations
            .contains(&CapabilityOperation::KeyValueScan));
        assert!(report
            .operations
            .contains(&CapabilityOperation::KeyValueRawCommand));
    }

    #[test]
    fn capability_report_preserves_legacy_fields_and_normalizes_operations() {
        let report = CapabilityReport::new(
            Capabilities {
                sql: true,
                ..Default::default()
            },
            vec![
                CapabilityOperation::SqlQuery,
                CapabilityOperation::SqlExecute,
                CapabilityOperation::SqlQuery,
            ],
        );
        assert_eq!(
            report.operations,
            [
                CapabilityOperation::SqlExecute,
                CapabilityOperation::SqlQuery
            ]
        );
        let value = serde_json::to_value(report).unwrap();
        assert_eq!(value["sql"], true);
        assert_eq!(value["operations"][0], "sql.execute");
        assert!(value.get("legacy").is_none());

        let legacy_only: CapabilityReport = serde_json::from_value(serde_json::json!({
            "sql": true,
            "cql": false,
            "db2": false,
            "key_value": false,
            "document": false,
            "time_series": false,
            "search": false,
            "producer": false,
            "consumer": false,
            "admin": false
        }))
        .unwrap();
        assert!(legacy_only.legacy.sql);
        assert!(legacy_only.operations.is_empty());
    }

    #[test]
    fn legacy_flags_derive_required_methods_but_never_guess_optional_extensions() {
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
            .filter(|operation| {
                !CapabilityOperation::MESSAGE_ADMIN.contains(operation)
                    && *operation != CapabilityOperation::SqlInsertRowsAtomic
                    && !CapabilityOperation::KEY_VALUE_EXISTENCE.contains(operation)
                    && !CapabilityOperation::KEY_VALUE_LIFETIME.contains(operation)
                    && !CapabilityOperation::DOCUMENT_LIFECYCLE.contains(operation)
                    && !CapabilityOperation::BOUNDED_CATALOGS.contains(operation)
                    && !CapabilityOperation::BOUNDED_NESTED_METADATA.contains(operation)
                    && !CapabilityOperation::BUDGETED_READS.contains(operation)
                    && !CapabilityOperation::BUDGETED_MUTATIONS.contains(operation)
                    && ![
                        CapabilityOperation::DocumentUpdateOne,
                        CapabilityOperation::DocumentUpdateMany,
                        CapabilityOperation::DocumentDeleteOne,
                        CapabilityOperation::DocumentDeleteMany,
                    ]
                    .contains(operation)
                    && !CapabilityOperation::MESSAGE_CONSUMER_EXTENSIONS.contains(operation)
                    && !CapabilityOperation::MESSAGE_PRODUCER_BUDGETED.contains(operation)
            })
            .collect::<Vec<_>>();

        assert_eq!(connector.operations(), expected);
        assert!(CapabilityOperation::MESSAGE_ADMIN
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(!connector
            .operations()
            .contains(&CapabilityOperation::SqlInsertRowsAtomic));
        assert!(CapabilityOperation::KEY_VALUE_LIFETIME
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::KEY_VALUE_EXISTENCE
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::DOCUMENT_LIFECYCLE
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::MESSAGE_CONSUMER_EXTENSIONS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::MESSAGE_PRODUCER_BUDGETED
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::BOUNDED_CATALOGS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::BUDGETED_CATALOGS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::BOUNDED_NESTED_METADATA
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::BUDGETED_READS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::KEY_VALUE_BUDGETED_READS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::TIME_SERIES_BUDGETED_READS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::SEARCH_BUDGETED_READS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::BUDGETED_MUTATIONS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        for operation in [
            CapabilityOperation::DocumentUpdateOne,
            CapabilityOperation::DocumentUpdateMany,
            CapabilityOperation::DocumentDeleteOne,
            CapabilityOperation::DocumentDeleteMany,
        ] {
            assert!(!connector.operations().contains(&operation));
        }

        let admin_only = LegacyConnector(Capabilities {
            admin: true,
            ..Default::default()
        });
        assert!(admin_only.operations().is_empty());
    }

    #[tokio::test]
    async fn coarse_producer_neither_claims_nor_inherits_budgeted_produce() {
        let connector = LegacyConnector(Capabilities {
            producer: true,
            ..Default::default()
        });
        assert_eq!(
            connector.operations(),
            [CapabilityOperation::MessageProduce]
        );
        assert!(!connector
            .operations()
            .contains(&CapabilityOperation::MessageProduceBudgeted));

        let error = MessageProducer::produce_budgeted(
            &connector,
            "events",
            Vec::new(),
            ProduceBudget::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            crate::Error::UnsupportedCapability { kind, needed }
                if kind == "legacy" && needed == "MessageProducer.produce_budgeted"
        ));
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
