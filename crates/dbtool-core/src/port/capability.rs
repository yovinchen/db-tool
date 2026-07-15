use crate::{
    model::{
        ConsumeOptions, DeleteResourceOptions, DeleteResourceOutcome, Document, ExecOutcome,
        FindOptions, ForeignKeyInfo, InsertOutcome, LagInfo, Message, MessageResource, Point,
        ProduceOutcome, ResultSet, RoutineInfo, SequenceInfo, SeriesSet, TableInfo, TableSchema,
        TablespaceInfo, TimeRange, TopicDetail, TopicInfo, UpdateOutcome, Value,
    },
    Result,
};
use async_trait::async_trait;

pub use crate::model::{SearchDeleteIndexOutcome, SearchDocument, SearchHits, SearchWriteOutcome};
pub use crate::port::connector::Connector;

#[async_trait]
pub trait SqlEngine: Connector {
    /// Execute a query without a client-side row budget.
    ///
    /// Interactive, export, and other user-controlled paths should use
    /// [`Self::query_bounded`] instead.
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet>;
    /// Execute a query while returning at most `max_rows` rows.
    ///
    /// Implementations must reject zero and overflowed limits, observe at most
    /// one additional row, and set `ResultSet::truncated` only when that probe
    /// row exists. This method is intentionally required: a default that calls
    /// `query` and truncates afterwards would silently reintroduce unbounded
    /// materialization in new adapters.
    async fn query_bounded(
        &self,
        sql: &str,
        params: &[Value],
        max_rows: usize,
    ) -> Result<ResultSet>;
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome>;
    /// Return the complete schema catalog visible to this connection.
    ///
    /// The current portable metadata API has no server-side page argument.
    /// Interactive callers must therefore apply their own result budget and
    /// report whether the returned list was truncated.
    async fn list_schemas(&self) -> Result<Vec<String>>;
    /// Return the complete table/view catalog for `schema`.
    ///
    /// Every returned [`TableInfo`] should carry its effective schema when the
    /// backend supports namespaces so portable unquoted names can be sent back
    /// to [`Self::describe_table`] without ambiguity.
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>>;
    async fn describe_table(&self, table: &str) -> Result<TableSchema>;
}

#[async_trait]
pub trait CqlEngine: Connector {
    /// Execute CQL without a client-side row budget.
    async fn query_cql(&self, cql: &str) -> Result<ResultSet>;
    /// Execute CQL while returning at most `max_rows` rows and probing one
    /// additional row to report truncation accurately.
    async fn query_cql_bounded(&self, cql: &str, max_rows: usize) -> Result<ResultSet>;
    async fn execute_cql(&self, cql: &str) -> Result<ExecOutcome>;
    async fn list_keyspaces(&self) -> Result<Vec<String>>;
    async fn list_cql_tables(&self, keyspace: Option<&str>) -> Result<Vec<TableInfo>>;
    async fn describe_cql_table(&self, table: &str) -> Result<TableSchema>;
}

#[async_trait]
pub trait KeyValueStore: Connector {
    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>>;
    async fn set(&self, key: &str, value: &[u8], options: SetOptions) -> Result<()>;
    async fn delete(&self, keys: &[String]) -> Result<u64>;
    async fn scan(&self, pattern: &str, limit: usize) -> Result<Vec<String>>;
    /// Escape hatch for raw protocol commands (e.g. `XLEN mystream`).
    async fn raw_command(&self, args: &[String]) -> Result<Value>;
}

#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// TTL in seconds; None = no expiry.
    pub ttl_secs: Option<u64>,
    /// Set only if key does not exist.
    pub nx: bool,
}

#[async_trait]
pub trait DocumentStore: Connector {
    async fn list_collections(&self) -> Result<Vec<String>>;
    async fn find(
        &self,
        collection: &str,
        filter: Value,
        options: FindOptions,
    ) -> Result<Vec<Document>>;
    async fn insert(&self, collection: &str, docs: Vec<Document>) -> Result<InsertOutcome>;
    async fn update(&self, collection: &str, filter: Value, update: Value)
        -> Result<UpdateOutcome>;
    async fn delete(&self, collection: &str, filter: Value) -> Result<u64>;
    async fn aggregate(&self, collection: &str, pipeline: Vec<Value>) -> Result<Vec<Document>>;

    /// Run an aggregation while bounding the number of documents read and
    /// retained by the adapter.
    ///
    /// This method is deliberately required: a default implementation that
    /// calls [`Self::aggregate`] and truncates afterwards would claim bounded
    /// behavior while still materializing an unbounded remote cursor.
    async fn aggregate_bounded(
        &self,
        collection: &str,
        pipeline: Vec<Value>,
        max_items: usize,
    ) -> Result<Vec<Document>>;

    /// Drop a document collection. Connectors that cannot manage collection
    /// lifecycle must reject the operation explicitly.
    async fn drop_collection(&self, _collection: &str) -> Result<()> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.drop_collection",
        })
    }
}

#[async_trait]
pub trait TimeSeriesStore: Connector {
    async fn list_measurements(&self) -> Result<Vec<String>>;
    async fn write_points(&self, points: Vec<Point>) -> Result<()>;
    async fn query_range(&self, query: &str, range: TimeRange) -> Result<SeriesSet>;
}

#[async_trait]
pub trait SearchEngine: Connector {
    async fn list_indices(&self) -> Result<Vec<crate::model::IndexInfo>>;
    async fn search(&self, index: &str, query: Value, options: SearchOptions)
        -> Result<SearchHits>;
    /// Create a document using a backend-generated identifier.
    async fn index_doc(&self, index: &str, doc: Value) -> Result<SearchWriteOutcome>;
    /// Create or replace a document using a caller-controlled stable identifier.
    async fn put_doc(&self, index: &str, id: &str, doc: Value) -> Result<SearchWriteOutcome>;
    /// Get one document by stable identifier. A backend HTTP 404 maps to `None`.
    async fn get_doc(&self, index: &str, id: &str) -> Result<Option<SearchDocument>>;
    /// Partially update one document by stable identifier.
    async fn update_doc(&self, index: &str, id: &str, patch: Value) -> Result<SearchWriteOutcome>;
    /// Delete one document by stable identifier.
    async fn delete_doc(&self, index: &str, id: &str) -> Result<SearchWriteOutcome>;
    /// Delete one complete index.
    async fn delete_index(&self, index: &str) -> Result<SearchDeleteIndexOutcome>;
}

#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    pub size: Option<usize>,
    pub from: Option<usize>,
    pub source: bool,
}

#[async_trait]
pub trait MessageProducer: Connector {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome>;
}

#[async_trait]
pub trait MessageConsumer: Connector {
    /// Always bounded — `options.max` and `options.timeout` are enforced by the adapter.
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>>;
}

#[async_trait]
pub trait AdminInspect: Connector {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>>;
    async fn topic_detail(&self, name: &str) -> Result<TopicDetail>;
    async fn consumer_lag(&self, group: &str) -> Result<Vec<LagInfo>>;
}

/// Destructive lifecycle operations for persistent messaging resources.
///
/// This is deliberately separate from [`AdminInspect`]. Callers must still
/// apply their write policy and target-bound confirmation before invoking it.
#[async_trait]
pub trait AdminMutate: Connector {
    async fn delete_resource(
        &self,
        resource: MessageResource,
        options: DeleteResourceOptions,
    ) -> Result<DeleteResourceOutcome>;
}

/// IBM Db2-specific schema inspection capability.
///
/// Exposes catalog-level metadata that is Db2-specific and not part of the
/// portable `SqlEngine` surface: sequences, routines, tablespaces, foreign
/// keys, and DDL generation from SYSCAT catalog tables.
#[async_trait]
pub trait Db2Engine: Connector {
    /// List user-defined sequences in a schema (defaults to current schema).
    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>>;
    /// List stored procedures and user-defined functions in a schema.
    async fn list_routines(&self, schema: Option<&str>) -> Result<Vec<RoutineInfo>>;
    /// List all tablespaces visible in the current database.
    async fn list_tablespaces(&self) -> Result<Vec<TablespaceInfo>>;
    /// List foreign-key constraints for a table (schema.table or bare table name).
    async fn list_foreign_keys(&self, table: &str) -> Result<Vec<ForeignKeyInfo>>;
    /// Generate a CREATE TABLE DDL statement from the Db2 catalog.
    async fn generate_ddl(&self, table: &str) -> Result<String>;
}
