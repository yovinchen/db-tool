use crate::{
    model::{
        BoundedList, ConsumeOptions, DeleteResourceOptions, DeleteResourceOutcome, Document,
        ExecOutcome, FindOptions, ForeignKeyInfo, InputBudget, InsertOutcome, KeyExpiry,
        KeyValueRestoreOutcome, KeyValueSnapshot, LagInfo, Message, MessageResource,
        MetadataBudget, Point, ProduceBudget, ProduceOutcome, ReadBudget, ResultSet, RoutineInfo,
        SequenceInfo, SeriesSet, TableInfo, TableSchema, TablespaceInfo, TimeRange,
        TimeSeriesReadBudget, TopicDetail, TopicInfo, UpdateOutcome, Value,
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
    /// Interactive, export, and other user-controlled paths should prefer
    /// [`Self::query_budgeted`]. [`Self::query_bounded`] remains the compatible
    /// row-only contract for callers that have not adopted byte envelopes.
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
    /// Execute a query within a cumulative item-and-byte read envelope.
    ///
    /// For SQL, `budget.max_items` is the returned-row limit. Implementations
    /// must account complete column metadata and every recursively serialized
    /// row before retention, observe at most N+1 rows, and fail without a
    /// partial result when the byte budget is exceeded. This exact operation
    /// is optional and never inferred from the legacy `sql=true` capability.
    async fn query_budgeted(
        &self,
        _sql: &str,
        _params: &[Value],
        _budget: ReadBudget,
    ) -> Result<ResultSet> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.query_budgeted",
        })
    }
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome>;
    /// Execute one fully input-budgeted SQL mutation.
    ///
    /// Implementations must completely prevalidate every caller input,
    /// including SQL text and parameters, against [`InputBudget`], then apply
    /// protocol syntax/fixed ceilings before constructing or sending the first
    /// backend mutation. Callers that create a connector must repeat portable
    /// preflight before connecting. After the first write may have reached the
    /// backend, every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn execute_budgeted(
        &self,
        _sql: &str,
        _params: &[Value],
        _budget: InputBudget,
    ) -> Result<ExecOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.execute_budgeted",
        })
    }
    /// Insert a complete row batch using bound parameters in one transaction.
    ///
    /// This is an optional capability. Implementations must validate the target
    /// identifiers and every row width independently of the caller, require
    /// exactly one affected row per input row, and roll back the whole batch on
    /// any error. Connectors that implement it must explicitly advertise
    /// `sql.insert_rows_atomic`; the coarse `sql=true` capability never implies
    /// this stronger transactional contract.
    async fn insert_rows_atomic(
        &self,
        _table: &str,
        _columns: &[String],
        _rows: &[Vec<Value>],
    ) -> Result<u64> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.insert_rows_atomic",
        })
    }
    /// Atomically insert one fully input-budgeted row batch.
    ///
    /// Implementations must completely prevalidate every row and the complete
    /// request, including table and column names, against [`InputBudget`], then
    /// apply protocol syntax/fixed ceilings before constructing or sending the
    /// first backend mutation. Callers that create a connector must repeat
    /// portable preflight before connecting. After the first write may have
    /// reached the backend, every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn insert_rows_atomic_budgeted(
        &self,
        _table: &str,
        _columns: &[String],
        _rows: &[Vec<Value>],
        _budget: InputBudget,
    ) -> Result<u64> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.insert_rows_atomic_budgeted",
        })
    }
    /// Return the complete schema catalog visible to this connection.
    ///
    /// The current portable metadata API has no server-side page argument.
    /// Interactive callers must therefore apply their own result budget and
    /// report whether the returned list was truncated.
    async fn list_schemas(&self) -> Result<Vec<String>>;
    /// Return at most `max_items` schemas while probing one additional item.
    ///
    /// This optional method must apply its limit before or during backend
    /// iteration. The default intentionally does not call [`Self::list_schemas`]
    /// because truncating a fully materialized catalog is not bounded.
    async fn list_schemas_bounded(&self, _max_items: usize) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.list_schemas_bounded",
        })
    }
    /// Return schemas within one caller-owned item-and-byte envelope.
    ///
    /// Implementations must charge every complete name before retention and
    /// the final [`BoundedList`] envelope, including its sole N+1 probe. This
    /// optional method is authorized only by `sql.list_schemas_budgeted`.
    async fn list_schemas_budgeted(&self, _budget: ReadBudget) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.list_schemas_budgeted",
        })
    }
    /// Return the complete table/view catalog for `schema`.
    ///
    /// Every returned [`TableInfo`] should carry its effective schema when the
    /// backend supports namespaces so portable unquoted names can be sent back
    /// to [`Self::describe_table`] without ambiguity.
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>>;
    /// Return at most `max_items` tables while probing one additional item.
    ///
    /// Implementations must bound backend work and explicitly advertise
    /// `sql.list_tables_bounded`; the legacy list method is never a fallback.
    async fn list_tables_bounded(
        &self,
        _schema: Option<&str>,
        _max_items: usize,
    ) -> Result<BoundedList<TableInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.list_tables_bounded",
        })
    }
    /// Return complete table identities inside a caller item-and-byte budget.
    async fn list_tables_budgeted(
        &self,
        _schema: Option<&str>,
        _budget: ReadBudget,
    ) -> Result<BoundedList<TableInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.list_tables_budgeted",
        })
    }
    async fn describe_table(&self, table: &str) -> Result<TableSchema>;
    /// Return one complete table schema within caller item/byte budgets.
    ///
    /// Implementations must apply an N+1 bound while reading columns and index
    /// memberships. Exceeding the budget is an error; returning a partial
    /// [`TableSchema`] is forbidden. This optional method requires the explicit
    /// `sql.describe_table_bounded` operation.
    async fn describe_table_bounded(
        &self,
        _table: &str,
        _budget: MetadataBudget,
    ) -> Result<TableSchema> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SqlEngine.describe_table_bounded",
        })
    }
}

#[async_trait]
pub trait CqlEngine: Connector {
    /// Execute CQL without a client-side row or byte budget.
    async fn query_cql(&self, cql: &str) -> Result<ResultSet>;
    /// Execute CQL while returning at most `max_rows` rows and probing one
    /// additional row to report truncation accurately. New interactive paths
    /// should prefer [`Self::query_cql_budgeted`].
    async fn query_cql_bounded(&self, cql: &str, max_rows: usize) -> Result<ResultSet>;
    /// Execute CQL within a cumulative row-and-byte read envelope.
    ///
    /// `budget.max_items` is the returned-row limit. This exact operation is
    /// optional and never inferred from the legacy `cql=true` capability.
    async fn query_cql_budgeted(&self, _cql: &str, _budget: ReadBudget) -> Result<ResultSet> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "CqlEngine.query_cql_budgeted",
        })
    }
    async fn execute_cql(&self, cql: &str) -> Result<ExecOutcome>;
    /// Execute one fully input-budgeted CQL mutation.
    ///
    /// Implementations must completely prevalidate the CQL text against
    /// [`InputBudget`], then apply protocol syntax/fixed ceilings before
    /// constructing or sending the first backend mutation. Callers that create
    /// a connector must repeat portable preflight before connecting. After the
    /// first write may have reached the backend, every later failure must be
    /// returned as [`crate::Error::OutcomeIndeterminate`].
    async fn execute_cql_budgeted(&self, _cql: &str, _budget: InputBudget) -> Result<ExecOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "CqlEngine.execute_cql_budgeted",
        })
    }
    async fn list_keyspaces(&self) -> Result<Vec<String>>;
    async fn list_keyspaces_bounded(&self, _max_items: usize) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "CqlEngine.list_keyspaces_bounded",
        })
    }
    async fn list_keyspaces_budgeted(&self, _budget: ReadBudget) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "CqlEngine.list_keyspaces_budgeted",
        })
    }
    async fn list_cql_tables(&self, keyspace: Option<&str>) -> Result<Vec<TableInfo>>;
    async fn list_cql_tables_bounded(
        &self,
        _keyspace: Option<&str>,
        _max_items: usize,
    ) -> Result<BoundedList<TableInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "CqlEngine.list_cql_tables_bounded",
        })
    }
    async fn list_cql_tables_budgeted(
        &self,
        _keyspace: Option<&str>,
        _budget: ReadBudget,
    ) -> Result<BoundedList<TableInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "CqlEngine.list_cql_tables_budgeted",
        })
    }
    async fn describe_cql_table(&self, table: &str) -> Result<TableSchema>;
    /// Return a complete CQL table schema within caller item/byte budgets.
    async fn describe_cql_table_bounded(
        &self,
        _table: &str,
        _budget: MetadataBudget,
    ) -> Result<TableSchema> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "CqlEngine.describe_cql_table_bounded",
        })
    }
}

#[async_trait]
pub trait KeyValueStore: Connector {
    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>>;
    /// Check key existence without materializing its value.
    ///
    /// This optional method must be implemented by a backend-native existence
    /// primitive and explicitly advertised as `kv.exists`. The fail-closed
    /// default deliberately does not call [`Self::get`], because replacement
    /// preflight must not read an arbitrarily large old value merely to learn
    /// whether its key exists.
    async fn exists(&self, _key: &str) -> Result<bool> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.exists",
        })
    }
    /// Read one complete value inside a caller-owned item/byte envelope.
    ///
    /// Implementations must validate `budget` before backend access and must
    /// enforce a protocol/transport ceiling before an oversized bulk value can
    /// be retained. A missing key is a complete `None` response; an oversized
    /// value fails with `READ_BUDGET_EXCEEDED` rather than returning a prefix.
    /// This optional contract is authorized only by `kv.get_bounded`.
    async fn get_bounded(&self, _key: &str, _budget: ReadBudget) -> Result<Option<bytes::Bytes>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.get_bounded",
        })
    }
    /// Read a value and its absolute expiry in one backend operation.
    ///
    /// This optional method must not be emulated with separate `GET` and TTL
    /// calls: a concurrent write or the passage of time would make that pair
    /// an unsafe transfer snapshot. Connectors implementing it must explicitly
    /// advertise `kv.get_with_expiry`.
    async fn get_with_expiry(&self, _key: &str) -> Result<Option<KeyValueSnapshot>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.get_with_expiry",
        })
    }
    /// Atomically read one complete value/lifetime snapshot inside a budget.
    ///
    /// The value and expiry must come from the same backend operation. Neither
    /// `kv.get_with_expiry` nor the coarse key-value capability authorizes this
    /// method; adapters must advertise `kv.get_with_expiry_bounded` explicitly.
    async fn get_with_expiry_bounded(
        &self,
        _key: &str,
        _budget: ReadBudget,
    ) -> Result<Option<KeyValueSnapshot>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.get_with_expiry_bounded",
        })
    }
    async fn set(&self, key: &str, value: &[u8], options: SetOptions) -> Result<()>;
    /// Set one key using a completely prevalidated input envelope.
    ///
    /// Implementations must completely prevalidate the key, value, and options
    /// as one request against [`InputBudget`], then apply protocol key syntax
    /// and fixed ceilings before constructing or sending the first backend
    /// mutation. Callers that create a connector must repeat portable preflight
    /// before connecting. After the first write may have reached the backend,
    /// every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn set_budgeted(
        &self,
        _key: &str,
        _value: &[u8],
        _options: SetOptions,
        _budget: InputBudget,
    ) -> Result<()> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.set_budgeted",
        })
    }
    /// Restore an exact value/lifetime pair in one backend operation.
    ///
    /// `nx=true` means the key must not exist. An already-expired absolute
    /// deadline returns [`KeyValueRestoreOutcome::Expired`] without writing;
    /// an NX conflict returns [`KeyValueRestoreOutcome::ConditionNotMet`].
    /// Connectors implementing this method must explicitly advertise
    /// `kv.restore_with_expiry`.
    async fn restore_with_expiry(
        &self,
        _key: &str,
        _value: &[u8],
        _expiry: KeyExpiry,
        _nx: bool,
    ) -> Result<KeyValueRestoreOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.restore_with_expiry",
        })
    }
    /// Restore one key/lifetime pair using a completely prevalidated input.
    ///
    /// Implementations must completely prevalidate the key, value, lifetime,
    /// and condition as one request against [`InputBudget`], then apply
    /// protocol key syntax/fixed ceilings before constructing or sending the
    /// first backend mutation. Callers that create a connector must repeat
    /// portable preflight before connecting. After the first write may have
    /// reached the backend, every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn restore_with_expiry_budgeted(
        &self,
        _key: &str,
        _value: &[u8],
        _expiry: KeyExpiry,
        _nx: bool,
        _budget: InputBudget,
    ) -> Result<KeyValueRestoreOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.restore_with_expiry_budgeted",
        })
    }
    async fn delete(&self, keys: &[String]) -> Result<u64>;
    /// Delete one completely prevalidated, non-empty key batch.
    ///
    /// Implementations must completely prevalidate every key and the complete
    /// batch against [`InputBudget`], then apply protocol key syntax/fixed
    /// ceilings before constructing or sending the first backend mutation.
    /// Callers that create a connector must repeat portable preflight before
    /// connecting. After the first write may have reached the backend, every
    /// later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn delete_budgeted(&self, _keys: &[String], _budget: InputBudget) -> Result<u64> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.delete_budgeted",
        })
    }
    async fn scan(&self, pattern: &str, limit: usize) -> Result<Vec<String>>;
    /// Return an exact N/N+1 key page inside one caller-owned byte envelope.
    ///
    /// Implementations must observe at most `budget.max_items + 1` unique keys,
    /// charge the probe key as well as the complete returned `BoundedList`, and
    /// stop backend pagination immediately after that probe. This optional
    /// contract is authorized only by `kv.scan_bounded`.
    async fn scan_bounded(
        &self,
        _pattern: &str,
        _budget: ReadBudget,
    ) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.scan_bounded",
        })
    }
    /// Escape hatch for raw protocol commands (e.g. `XLEN mystream`).
    async fn raw_command(&self, args: &[String]) -> Result<Value>;
    /// Execute an allowlisted raw command inside one complete response budget.
    ///
    /// Adapters must reject commands whose response shape cannot be bounded
    /// before protocol decoding. Collection responses must account their
    /// caller-visible items and nested bytes without returning a partial typed
    /// `Value`. This optional contract is authorized only by
    /// `kv.raw_command_bounded`.
    async fn raw_command_bounded(&self, _args: &[String], _budget: ReadBudget) -> Result<Value> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.raw_command_bounded",
        })
    }
    /// Execute one allowlisted raw mutation with separate input/output budgets.
    ///
    /// This is deliberately named `io_budgeted`: [`Self::raw_command_bounded`]
    /// limits only a read response, while this exact mutation contract requires
    /// both [`InputBudget`] for all arguments and [`ReadBudget`] for the complete
    /// response. Implementations must prevalidate the entire argument request,
    /// including commands and targets, then apply the command allowlist and
    /// protocol fixed ceilings before constructing or sending the first backend
    /// mutation. Callers that create a connector must repeat portable preflight
    /// before connecting. After the first write may have reached the backend,
    /// every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn raw_command_io_budgeted(
        &self,
        _args: &[String],
        _input_budget: InputBudget,
        _response_budget: ReadBudget,
    ) -> Result<Value> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "KeyValueStore.raw_command_io_budgeted",
        })
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SetOptions {
    /// TTL in seconds; None = no expiry.
    pub ttl_secs: Option<u64>,
    /// Set only if key does not exist.
    pub nx: bool,
}

#[async_trait]
pub trait DocumentStore: Connector {
    async fn list_collections(&self) -> Result<Vec<String>>;
    async fn list_collections_bounded(&self, _max_items: usize) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.list_collections_bounded",
        })
    }
    async fn list_collections_budgeted(&self, _budget: ReadBudget) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.list_collections_budgeted",
        })
    }
    async fn find(
        &self,
        collection: &str,
        filter: Value,
        options: FindOptions,
    ) -> Result<Vec<Document>>;
    /// Find documents within one cumulative item-and-byte read envelope.
    ///
    /// Implementations must account each complete native document before
    /// retention, observe at most N+1 documents, and fail without returning a
    /// partial collection when either the native or caller-visible byte budget
    /// is exceeded. This exact operation is optional and never inferred from
    /// the legacy `document=true` capability.
    async fn find_budgeted(
        &self,
        _collection: &str,
        _filter: Value,
        _options: FindOptions,
        _budget: ReadBudget,
    ) -> Result<BoundedList<Document>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.find_budgeted",
        })
    }
    async fn insert(&self, collection: &str, docs: Vec<Document>) -> Result<InsertOutcome>;
    /// Insert one completely prevalidated, non-empty document batch.
    ///
    /// Implementations must completely prevalidate every document and the full
    /// request, including the collection, against [`InputBudget`], then apply
    /// protocol collection syntax/fixed ceilings before constructing or
    /// sending the first backend mutation. Callers that create a connector must
    /// repeat portable preflight before connecting. After the first write may
    /// have reached the backend, every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn insert_budgeted(
        &self,
        _collection: &str,
        _docs: Vec<Document>,
        _budget: InputBudget,
    ) -> Result<InsertOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.insert_budgeted",
        })
    }
    /// Compatibility bulk-update entry point retained for embedded callers.
    ///
    /// This method keeps the historical dbtool contract: every matching
    /// document may be updated. New callers should negotiate and invoke
    /// [`Self::update_one`] or [`Self::update_many`] so cardinality is explicit.
    async fn update(&self, collection: &str, filter: Value, update: Value)
        -> Result<UpdateOutcome>;
    /// Compatibility bulk-delete entry point retained for embedded callers.
    ///
    /// This method keeps the historical dbtool contract: every matching
    /// document may be deleted. New callers should negotiate and invoke
    /// [`Self::delete_one`] or [`Self::delete_many`] so cardinality is explicit.
    async fn delete(&self, collection: &str, filter: Value) -> Result<u64>;

    /// Update at most one matching document.
    ///
    /// This is optional and therefore is never inferred from the coarse
    /// `document=true` capability. Implementations must advertise
    /// `document.update_one` before callers may rely on it.
    async fn update_one(
        &self,
        _collection: &str,
        _filter: Value,
        _update: Value,
    ) -> Result<UpdateOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.update_one",
        })
    }
    /// Update at most one document using a completely prevalidated request.
    ///
    /// Implementations must completely prevalidate collection, filter, and
    /// update as one request against [`InputBudget`], then apply protocol
    /// collection syntax/fixed ceilings before constructing or sending the
    /// first backend mutation. Callers that create a connector must repeat
    /// portable preflight before connecting. After the first write may have
    /// reached the backend, every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn update_one_budgeted(
        &self,
        _collection: &str,
        _filter: Value,
        _update: Value,
        _budget: InputBudget,
    ) -> Result<UpdateOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.update_one_budgeted",
        })
    }

    /// Update every matching document.
    ///
    /// The default deliberately delegates to the historical bulk-update
    /// method so existing embedded implementations keep their old behavior.
    /// It is not advertised automatically; connectors must explicitly declare
    /// `document.update_many` once that legacy behavior has been verified.
    async fn update_many(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
    ) -> Result<UpdateOutcome> {
        self.update(collection, filter, update).await
    }
    /// Update matching documents using a completely prevalidated request.
    ///
    /// Implementations must completely prevalidate collection, filter, and
    /// update as one request against [`InputBudget`], then apply protocol
    /// collection syntax/fixed ceilings before constructing or sending the
    /// first backend mutation. Callers that create a connector must repeat
    /// portable preflight before connecting. After the first write may have
    /// reached the backend, every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn update_many_budgeted(
        &self,
        _collection: &str,
        _filter: Value,
        _update: Value,
        _budget: InputBudget,
    ) -> Result<UpdateOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.update_many_budgeted",
        })
    }

    /// Delete at most one matching document.
    ///
    /// This is optional and therefore is never inferred from the coarse
    /// `document=true` capability. Implementations must advertise
    /// `document.delete_one` before callers may rely on it.
    async fn delete_one(&self, _collection: &str, _filter: Value) -> Result<u64> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.delete_one",
        })
    }
    /// Delete at most one document using a completely prevalidated filter.
    ///
    /// Implementations must completely prevalidate collection and filter as
    /// one request against [`InputBudget`], then apply protocol collection
    /// syntax/fixed ceilings before constructing or sending the first backend
    /// mutation. Callers that create a connector must repeat portable preflight
    /// before connecting. After the first write may have reached the backend,
    /// every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn delete_one_budgeted(
        &self,
        _collection: &str,
        _filter: Value,
        _budget: InputBudget,
    ) -> Result<u64> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.delete_one_budgeted",
        })
    }

    /// Delete every matching document.
    ///
    /// The default delegates to the historical bulk-delete method for embedded
    /// compatibility, but connectors must explicitly advertise
    /// `document.delete_many` after verifying that behavior.
    async fn delete_many(&self, collection: &str, filter: Value) -> Result<u64> {
        self.delete(collection, filter).await
    }
    /// Delete matching documents using a completely prevalidated filter.
    ///
    /// Implementations must completely prevalidate collection and filter as
    /// one request against [`InputBudget`], then apply protocol collection
    /// syntax/fixed ceilings before constructing or sending the first backend
    /// mutation. Callers that create a connector must repeat portable preflight
    /// before connecting. After the first write may have reached the backend,
    /// every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn delete_many_budgeted(
        &self,
        _collection: &str,
        _filter: Value,
        _budget: InputBudget,
    ) -> Result<u64> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.delete_many_budgeted",
        })
    }
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
    /// Run an aggregation within a cumulative item-and-byte read envelope.
    ///
    /// This exact operation is optional and never delegates to the legacy
    /// aggregate methods because post-hoc truncation cannot bound a remote
    /// cursor or a cumulative response.
    async fn aggregate_budgeted(
        &self,
        _collection: &str,
        _pipeline: Vec<Value>,
        _budget: ReadBudget,
    ) -> Result<BoundedList<Document>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.aggregate_budgeted",
        })
    }

    /// Run an explicitly mutating aggregation under separate input and output
    /// envelopes.
    ///
    /// This operation is reserved for pipelines such as MongoDB `$out` and
    /// `$merge`. Implementations must completely prevalidate the source
    /// collection, every stage, and every statically represented destination
    /// against [`InputBudget`] plus protocol syntax and fixed limits before
    /// constructing or sending the backend command. [`ReadBudget`] bounds any
    /// cursor response. After the command may have reached the backend, every
    /// transport, execution, response-budget, or decoding failure must be
    /// returned as [`crate::Error::OutcomeIndeterminate`]. Read-only aggregate
    /// methods must reject write stages instead of silently authorizing this
    /// stronger contract.
    async fn aggregate_write_budgeted(
        &self,
        _collection: &str,
        _pipeline: Vec<Value>,
        _input_budget: InputBudget,
        _response_budget: ReadBudget,
    ) -> Result<BoundedList<Document>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.aggregate_write_budgeted",
        })
    }

    /// Drop a document collection.
    ///
    /// This lifecycle method is optional and is never inferred from the coarse
    /// `document=true` capability. Implementations must advertise
    /// `document.drop_collection`; connectors that cannot manage collection
    /// lifecycle retain this fail-closed default.
    async fn drop_collection(&self, _collection: &str) -> Result<()> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.drop_collection",
        })
    }
    /// Drop one protocol-validated collection through the exact write contract.
    ///
    /// Implementations must charge the collection as a one-item complete
    /// request against [`InputBudget`], then apply protocol collection syntax
    /// and fixed ceilings before constructing or sending the first backend
    /// mutation. Callers that create a connector must repeat portable preflight
    /// before connecting. After the first write may have reached the backend,
    /// every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn drop_collection_budgeted(
        &self,
        _collection: &str,
        _budget: InputBudget,
    ) -> Result<()> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.drop_collection_budgeted",
        })
    }
}

#[async_trait]
pub trait TimeSeriesStore: Connector {
    async fn list_measurements(&self) -> Result<Vec<String>>;
    async fn list_measurements_bounded(&self, _max_items: usize) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "TimeSeriesStore.list_measurements_bounded",
        })
    }
    async fn list_measurements_budgeted(&self, _budget: ReadBudget) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "TimeSeriesStore.list_measurements_budgeted",
        })
    }
    async fn write_points(&self, points: Vec<Point>) -> Result<()>;
    /// Write one completely prevalidated, non-empty point batch.
    ///
    /// Implementations must completely prevalidate every point and its full
    /// request, including measurement names, against [`InputBudget`], then
    /// apply protocol measurement syntax/fixed ceilings before constructing or
    /// sending the first backend mutation. Callers that create a connector must
    /// repeat portable preflight before connecting. After the first write may
    /// have reached the backend, every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn write_points_budgeted(&self, _points: Vec<Point>, _budget: InputBudget) -> Result<()> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "TimeSeriesStore.write_points_budgeted",
        })
    }
    async fn query_range(&self, query: &str, range: TimeRange) -> Result<SeriesSet>;

    /// Run one range query inside caller-owned series, cumulative-sample, and
    /// serialized-byte bounds.
    ///
    /// This optional contract must be advertised as
    /// `time_series.query_range_bounded`. The legacy `time_series=true` family
    /// and unbounded [`Self::query_range`] method never authorize it.
    async fn query_range_bounded(
        &self,
        _query: &str,
        _range: TimeRange,
        _budget: TimeSeriesReadBudget,
    ) -> Result<SeriesSet> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "TimeSeriesStore.query_range_bounded",
        })
    }
}

#[async_trait]
pub trait SearchEngine: Connector {
    async fn list_indices(&self) -> Result<Vec<crate::model::IndexInfo>>;
    async fn list_indices_bounded(
        &self,
        _max_items: usize,
    ) -> Result<BoundedList<crate::model::IndexInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.list_indices_bounded",
        })
    }
    async fn list_indices_budgeted(
        &self,
        _budget: ReadBudget,
    ) -> Result<BoundedList<crate::model::IndexInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.list_indices_budgeted",
        })
    }
    async fn search(&self, index: &str, query: Value, options: SearchOptions)
        -> Result<SearchHits>;
    /// Search within a caller-owned hit-count and serialized-byte envelope.
    ///
    /// `budget.max_items` limits the number of returned hits. Implementations
    /// must account every complete hit before retaining it and then validate
    /// the complete [`SearchHits`] response, including aggregations, hit
    /// metadata, and backend-specific extra fields. Exceeding either limit
    /// fails without returning a partial response. This optional contract must
    /// be advertised as `search.search_budgeted`; neither the legacy
    /// `search=true` family nor [`Self::search`] authorizes it.
    async fn search_budgeted(
        &self,
        _index: &str,
        _query: Value,
        _options: SearchOptions,
        _budget: ReadBudget,
    ) -> Result<SearchHits> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.search_budgeted",
        })
    }
    /// Create a document using a backend-generated identifier.
    async fn index_doc(&self, index: &str, doc: Value) -> Result<SearchWriteOutcome>;
    /// Index one document using a completely prevalidated payload.
    ///
    /// Implementations must completely prevalidate index and document as one
    /// request against [`InputBudget`], then apply protocol index syntax/fixed
    /// ceilings before constructing or sending the first backend mutation.
    /// Callers that create a connector must repeat portable preflight before
    /// connecting. After the first write may have reached the backend, every
    /// later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn index_doc_budgeted(
        &self,
        _index: &str,
        _doc: Value,
        _budget: InputBudget,
    ) -> Result<SearchWriteOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.index_doc_budgeted",
        })
    }
    /// Create or replace a document using a caller-controlled stable identifier.
    async fn put_doc(&self, index: &str, id: &str, doc: Value) -> Result<SearchWriteOutcome>;
    /// Put one document using a completely prevalidated payload.
    ///
    /// Implementations must completely prevalidate index, id, and document as
    /// one request against [`InputBudget`], then apply protocol index/id
    /// syntax/fixed ceilings before constructing or sending the first backend
    /// mutation. Callers that create a connector must repeat portable preflight
    /// before connecting. After the first write may have reached the backend,
    /// every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn put_doc_budgeted(
        &self,
        _index: &str,
        _id: &str,
        _doc: Value,
        _budget: InputBudget,
    ) -> Result<SearchWriteOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.put_doc_budgeted",
        })
    }
    /// Get one document by stable identifier. A backend HTTP 404 maps to `None`.
    async fn get_doc(&self, index: &str, id: &str) -> Result<Option<SearchDocument>>;
    /// Get one document within a caller-owned serialized-byte envelope.
    ///
    /// A present document consumes one item and is charged in full, including
    /// `_source` and backend-specific extra fields. A missing document consumes
    /// no item but the complete serialized `None` response is still charged.
    /// This optional contract must be advertised as
    /// `search.get_doc_budgeted`; legacy search capabilities never imply it.
    async fn get_doc_budgeted(
        &self,
        _index: &str,
        _id: &str,
        _budget: ReadBudget,
    ) -> Result<Option<SearchDocument>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.get_doc_budgeted",
        })
    }
    /// Partially update one document by stable identifier.
    async fn update_doc(&self, index: &str, id: &str, patch: Value) -> Result<SearchWriteOutcome>;
    /// Update one document using a completely prevalidated patch.
    ///
    /// Implementations must completely prevalidate index, id, and patch as one
    /// request against [`InputBudget`], then apply protocol index/id
    /// syntax/fixed ceilings before constructing or sending the first backend
    /// mutation. Callers that create a connector must repeat portable preflight
    /// before connecting. After the first write may have reached the backend,
    /// every later failure must be returned as
    /// [`crate::Error::OutcomeIndeterminate`].
    async fn update_doc_budgeted(
        &self,
        _index: &str,
        _id: &str,
        _patch: Value,
        _budget: InputBudget,
    ) -> Result<SearchWriteOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.update_doc_budgeted",
        })
    }
    /// Delete one document by stable identifier.
    async fn delete_doc(&self, index: &str, id: &str) -> Result<SearchWriteOutcome>;
    /// Delete one protocol-validated document through the exact write contract.
    ///
    /// Implementations must charge index and id as one complete request against
    /// [`InputBudget`], then apply protocol index/id syntax/fixed ceilings
    /// before constructing or sending the first backend mutation. Callers that
    /// create a connector must repeat portable preflight before connecting.
    /// After the first write may have reached the backend, every later failure
    /// must be returned as [`crate::Error::OutcomeIndeterminate`].
    async fn delete_doc_budgeted(
        &self,
        _index: &str,
        _id: &str,
        _budget: InputBudget,
    ) -> Result<SearchWriteOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.delete_doc_budgeted",
        })
    }
    /// Delete one complete index.
    async fn delete_index(&self, index: &str) -> Result<SearchDeleteIndexOutcome>;
    /// Delete one protocol-validated index through the exact write contract.
    ///
    /// Implementations must charge the index as a one-item complete request
    /// against [`InputBudget`], then apply protocol index syntax/fixed ceilings
    /// before constructing or sending the first backend mutation. Callers that
    /// create a connector must repeat portable preflight before connecting.
    /// After the first write may have reached the backend, every later failure
    /// must be returned as [`crate::Error::OutcomeIndeterminate`].
    async fn delete_index_budgeted(
        &self,
        _index: &str,
        _budget: InputBudget,
    ) -> Result<SearchDeleteIndexOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "SearchEngine.delete_index_budgeted",
        })
    }
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

    /// Produce one completely prevalidated input batch.
    ///
    /// Implementations must validate the portable budget, target/resource
    /// name, every complete message, and any stricter protocol ceilings before
    /// creating resources or attempting the first send. Once an attempt may have reached the
    /// backend, later failures must be returned as
    /// [`crate::Error::OutcomeIndeterminate`]. This optional exact contract is
    /// never inferred from the legacy producer family flag.
    async fn produce_budgeted(
        &self,
        _target: &str,
        _messages: Vec<Message>,
        _budget: ProduceBudget,
    ) -> Result<ProduceOutcome> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "MessageProducer.produce_budgeted",
        })
    }
}

#[async_trait]
pub trait MessageConsumer: Connector {
    /// Always bounded — `options.max` and `options.timeout` are enforced by the adapter.
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>>;
}

#[async_trait]
pub trait AdminInspect: Connector {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>>;
    async fn list_topics_bounded(&self, _max_items: usize) -> Result<BoundedList<TopicInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "AdminInspect.list_topics_bounded",
        })
    }
    async fn list_topics_budgeted(&self, _budget: ReadBudget) -> Result<BoundedList<TopicInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "AdminInspect.list_topics_budgeted",
        })
    }
    async fn topic_detail(&self, name: &str) -> Result<TopicDetail>;
    /// Return complete topic config/watermarks within caller budgets.
    async fn topic_detail_bounded(
        &self,
        _name: &str,
        _budget: MetadataBudget,
    ) -> Result<TopicDetail> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "AdminInspect.topic_detail_bounded",
        })
    }
    async fn consumer_lag(&self, group: &str) -> Result<Vec<LagInfo>>;
    /// Return complete per-partition lag within caller budgets.
    async fn consumer_lag_bounded(
        &self,
        _group: &str,
        _budget: MetadataBudget,
    ) -> Result<Vec<LagInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "AdminInspect.consumer_lag_bounded",
        })
    }
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
    async fn list_sequences_bounded(
        &self,
        _schema: Option<&str>,
        _max_items: usize,
    ) -> Result<BoundedList<SequenceInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.list_sequences_bounded",
        })
    }
    async fn list_sequences_budgeted(
        &self,
        _schema: Option<&str>,
        _budget: ReadBudget,
    ) -> Result<BoundedList<SequenceInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.list_sequences_budgeted",
        })
    }
    /// List stored procedures and user-defined functions in a schema.
    async fn list_routines(&self, schema: Option<&str>) -> Result<Vec<RoutineInfo>>;
    async fn list_routines_bounded(
        &self,
        _schema: Option<&str>,
        _max_items: usize,
    ) -> Result<BoundedList<RoutineInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.list_routines_bounded",
        })
    }
    async fn list_routines_budgeted(
        &self,
        _schema: Option<&str>,
        _budget: ReadBudget,
    ) -> Result<BoundedList<RoutineInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.list_routines_budgeted",
        })
    }
    /// List all tablespaces visible in the current database.
    async fn list_tablespaces(&self) -> Result<Vec<TablespaceInfo>>;
    async fn list_tablespaces_bounded(
        &self,
        _max_items: usize,
    ) -> Result<BoundedList<TablespaceInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.list_tablespaces_bounded",
        })
    }
    async fn list_tablespaces_budgeted(
        &self,
        _budget: ReadBudget,
    ) -> Result<BoundedList<TablespaceInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.list_tablespaces_budgeted",
        })
    }
    /// List foreign-key constraints for a table (schema.table or bare table name).
    async fn list_foreign_keys(&self, table: &str) -> Result<Vec<ForeignKeyInfo>>;
    async fn list_foreign_keys_bounded(
        &self,
        _table: &str,
        _max_items: usize,
    ) -> Result<BoundedList<ForeignKeyInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.list_foreign_keys_bounded",
        })
    }
    async fn list_foreign_keys_budgeted(
        &self,
        _table: &str,
        _budget: ReadBudget,
    ) -> Result<BoundedList<ForeignKeyInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.list_foreign_keys_budgeted",
        })
    }
    /// Generate a CREATE TABLE DDL statement from the Db2 catalog.
    async fn generate_ddl(&self, table: &str) -> Result<String>;
    /// Generate DDL only when every required nested catalog item fits the
    /// caller budget. Partial DDL is never returned.
    async fn generate_ddl_bounded(&self, _table: &str, _budget: MetadataBudget) -> Result<String> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "Db2Engine.generate_ddl_bounded",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        port::{Capabilities, CapabilityOperation, ConnectorKind},
        Error,
    };
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct LegacyCatalogs {
        unbounded_calls: AtomicUsize,
    }

    impl LegacyCatalogs {
        fn new() -> Self {
            Self {
                unbounded_calls: AtomicUsize::new(0),
            }
        }

        fn record_unbounded<T>(&self, value: T) -> T {
            self.unbounded_calls.fetch_add(1, Ordering::SeqCst);
            value
        }
    }

    #[async_trait]
    impl Connector for LegacyCatalogs {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind("legacy-catalogs".into())
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities {
                sql: true,
                cql: true,
                db2: true,
                key_value: true,
                document: true,
                time_series: true,
                search: true,
                admin: true,
                ..Default::default()
            }
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl KeyValueStore for LegacyCatalogs {
        async fn get(&self, _key: &str) -> Result<Option<Bytes>> {
            Ok(self.record_unbounded(Some(Bytes::from_static(b"legacy"))))
        }

        async fn set(&self, _key: &str, _value: &[u8], _options: SetOptions) -> Result<()> {
            Ok(())
        }

        async fn delete(&self, _keys: &[String]) -> Result<u64> {
            Ok(0)
        }

        async fn scan(&self, _pattern: &str, _limit: usize) -> Result<Vec<String>> {
            Ok(self.record_unbounded(vec!["legacy".to_owned()]))
        }

        async fn raw_command(&self, _args: &[String]) -> Result<Value> {
            Ok(self.record_unbounded(Value::Text("legacy".to_owned())))
        }
    }

    #[async_trait]
    impl SqlEngine for LegacyCatalogs {
        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<ResultSet> {
            Ok(self.record_unbounded(ResultSet::empty()))
        }

        async fn query_bounded(
            &self,
            _sql: &str,
            _params: &[Value],
            _max_rows: usize,
        ) -> Result<ResultSet> {
            Ok(self.record_unbounded(ResultSet::empty()))
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<ExecOutcome> {
            Ok(ExecOutcome {
                rows_affected: 0,
                last_insert_id: None,
            })
        }

        async fn list_schemas(&self) -> Result<Vec<String>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn describe_table(&self, _table: &str) -> Result<TableSchema> {
            Err(Error::Query("unused".into()))
        }
    }

    #[async_trait]
    impl CqlEngine for LegacyCatalogs {
        async fn query_cql(&self, _cql: &str) -> Result<ResultSet> {
            Ok(self.record_unbounded(ResultSet::empty()))
        }

        async fn query_cql_bounded(&self, _cql: &str, _max_rows: usize) -> Result<ResultSet> {
            Ok(self.record_unbounded(ResultSet::empty()))
        }

        async fn execute_cql(&self, _cql: &str) -> Result<ExecOutcome> {
            Ok(ExecOutcome {
                rows_affected: 0,
                last_insert_id: None,
            })
        }

        async fn list_keyspaces(&self) -> Result<Vec<String>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn list_cql_tables(&self, _keyspace: Option<&str>) -> Result<Vec<TableInfo>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn describe_cql_table(&self, _table: &str) -> Result<TableSchema> {
            Err(Error::Query("unused".into()))
        }
    }

    #[async_trait]
    impl DocumentStore for LegacyCatalogs {
        async fn list_collections(&self) -> Result<Vec<String>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn find(
            &self,
            _collection: &str,
            _filter: Value,
            _options: FindOptions,
        ) -> Result<Vec<Document>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn insert(&self, _collection: &str, _docs: Vec<Document>) -> Result<InsertOutcome> {
            Ok(InsertOutcome {
                inserted: 0,
                ids: Vec::new(),
            })
        }

        async fn update(
            &self,
            _collection: &str,
            _filter: Value,
            _update: Value,
        ) -> Result<UpdateOutcome> {
            Ok(UpdateOutcome {
                matched: 0,
                modified: 0,
            })
        }

        async fn delete(&self, _collection: &str, _filter: Value) -> Result<u64> {
            Ok(0)
        }

        async fn aggregate(
            &self,
            _collection: &str,
            _pipeline: Vec<Value>,
        ) -> Result<Vec<Document>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn aggregate_bounded(
            &self,
            _collection: &str,
            _pipeline: Vec<Value>,
            _max_items: usize,
        ) -> Result<Vec<Document>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl TimeSeriesStore for LegacyCatalogs {
        async fn list_measurements(&self) -> Result<Vec<String>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn write_points(&self, _points: Vec<Point>) -> Result<()> {
            Ok(())
        }

        async fn query_range(&self, _query: &str, _range: TimeRange) -> Result<SeriesSet> {
            Err(Error::Query("unused".into()))
        }
    }

    #[async_trait]
    impl SearchEngine for LegacyCatalogs {
        async fn list_indices(&self) -> Result<Vec<crate::model::IndexInfo>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn search(
            &self,
            _index: &str,
            _query: Value,
            _options: SearchOptions,
        ) -> Result<SearchHits> {
            Err(Error::Query("unused".into()))
        }

        async fn index_doc(&self, _index: &str, _doc: Value) -> Result<SearchWriteOutcome> {
            Err(Error::Query("unused".into()))
        }

        async fn put_doc(
            &self,
            _index: &str,
            _id: &str,
            _doc: Value,
        ) -> Result<SearchWriteOutcome> {
            Err(Error::Query("unused".into()))
        }

        async fn get_doc(&self, _index: &str, _id: &str) -> Result<Option<SearchDocument>> {
            Err(Error::Query("unused".into()))
        }

        async fn update_doc(
            &self,
            _index: &str,
            _id: &str,
            _patch: Value,
        ) -> Result<SearchWriteOutcome> {
            Err(Error::Query("unused".into()))
        }

        async fn delete_doc(&self, _index: &str, _id: &str) -> Result<SearchWriteOutcome> {
            Err(Error::Query("unused".into()))
        }

        async fn delete_index(&self, _index: &str) -> Result<SearchDeleteIndexOutcome> {
            Err(Error::Query("unused".into()))
        }
    }

    #[async_trait]
    impl AdminInspect for LegacyCatalogs {
        async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn topic_detail(&self, _name: &str) -> Result<TopicDetail> {
            Err(Error::Query("unused".into()))
        }

        async fn consumer_lag(&self, _group: &str) -> Result<Vec<LagInfo>> {
            Err(Error::Query("unused".into()))
        }
    }

    #[async_trait]
    impl Db2Engine for LegacyCatalogs {
        async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn list_routines(&self, _schema: Option<&str>) -> Result<Vec<RoutineInfo>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn list_tablespaces(&self) -> Result<Vec<TablespaceInfo>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn list_foreign_keys(&self, _table: &str) -> Result<Vec<ForeignKeyInfo>> {
            Ok(self.record_unbounded(Vec::new()))
        }

        async fn generate_ddl(&self, _table: &str) -> Result<String> {
            Err(Error::Query("unused".into()))
        }
    }

    fn assert_unsupported<T>(result: Result<T>, expected: &'static str) {
        assert!(matches!(
            result,
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-catalogs" && needed == expected
        ));
    }

    #[tokio::test]
    async fn budgeted_read_and_metadata_defaults_never_materialize_legacy_methods() {
        let connector = LegacyCatalogs::new();
        let metadata_budget = MetadataBudget::with_default_bytes(10).unwrap();
        let read_budget = ReadBudget::with_default_bytes(10).unwrap();
        let time_series_budget = TimeSeriesReadBudget::with_default_bytes(10, 100).unwrap();

        assert_unsupported(
            SqlEngine::query_budgeted(&connector, "SELECT 1", &[], read_budget).await,
            "SqlEngine.query_budgeted",
        );
        assert_unsupported(
            CqlEngine::query_cql_budgeted(
                &connector,
                "SELECT now() FROM system.local",
                read_budget,
            )
            .await,
            "CqlEngine.query_cql_budgeted",
        );
        assert_unsupported(
            DocumentStore::find_budgeted(
                &connector,
                "users",
                Value::Null,
                FindOptions::default(),
                read_budget,
            )
            .await,
            "DocumentStore.find_budgeted",
        );
        assert_unsupported(
            DocumentStore::aggregate_budgeted(&connector, "users", Vec::new(), read_budget).await,
            "DocumentStore.aggregate_budgeted",
        );
        assert_unsupported(
            KeyValueStore::exists(&connector, "key").await,
            "KeyValueStore.exists",
        );
        assert_unsupported(
            KeyValueStore::get_bounded(&connector, "key", read_budget).await,
            "KeyValueStore.get_bounded",
        );
        assert_unsupported(
            KeyValueStore::get_with_expiry_bounded(&connector, "key", read_budget).await,
            "KeyValueStore.get_with_expiry_bounded",
        );
        assert_unsupported(
            KeyValueStore::scan_bounded(&connector, "*", read_budget).await,
            "KeyValueStore.scan_bounded",
        );
        assert_unsupported(
            KeyValueStore::raw_command_bounded(
                &connector,
                &["GET".to_owned(), "key".to_owned()],
                read_budget,
            )
            .await,
            "KeyValueStore.raw_command_bounded",
        );
        assert_unsupported(
            TimeSeriesStore::query_range_bounded(
                &connector,
                "up",
                TimeRange::closed(1, 2).unwrap(),
                time_series_budget,
            )
            .await,
            "TimeSeriesStore.query_range_bounded",
        );
        assert_unsupported(
            SearchEngine::search_budgeted(
                &connector,
                "users",
                Value::Null,
                SearchOptions::default(),
                read_budget,
            )
            .await,
            "SearchEngine.search_budgeted",
        );
        assert_unsupported(
            SearchEngine::get_doc_budgeted(&connector, "users", "user-1", read_budget).await,
            "SearchEngine.get_doc_budgeted",
        );

        assert_unsupported(
            SqlEngine::list_schemas_bounded(&connector, 10).await,
            "SqlEngine.list_schemas_bounded",
        );
        assert_unsupported(
            SqlEngine::list_tables_bounded(&connector, Some("public"), 10).await,
            "SqlEngine.list_tables_bounded",
        );
        assert_unsupported(
            CqlEngine::list_keyspaces_bounded(&connector, 10).await,
            "CqlEngine.list_keyspaces_bounded",
        );
        assert_unsupported(
            CqlEngine::list_cql_tables_bounded(&connector, Some("app"), 10).await,
            "CqlEngine.list_cql_tables_bounded",
        );
        assert_unsupported(
            DocumentStore::list_collections_bounded(&connector, 10).await,
            "DocumentStore.list_collections_bounded",
        );
        assert_unsupported(
            SearchEngine::list_indices_bounded(&connector, 10).await,
            "SearchEngine.list_indices_bounded",
        );
        assert_unsupported(
            TimeSeriesStore::list_measurements_bounded(&connector, 10).await,
            "TimeSeriesStore.list_measurements_bounded",
        );
        assert_unsupported(
            AdminInspect::list_topics_bounded(&connector, 10).await,
            "AdminInspect.list_topics_bounded",
        );
        assert_unsupported(
            Db2Engine::list_sequences_bounded(&connector, None, 10).await,
            "Db2Engine.list_sequences_bounded",
        );
        assert_unsupported(
            Db2Engine::list_routines_bounded(&connector, None, 10).await,
            "Db2Engine.list_routines_bounded",
        );
        assert_unsupported(
            Db2Engine::list_tablespaces_bounded(&connector, 10).await,
            "Db2Engine.list_tablespaces_bounded",
        );
        assert_unsupported(
            Db2Engine::list_foreign_keys_bounded(&connector, "app.orders", 10).await,
            "Db2Engine.list_foreign_keys_bounded",
        );
        assert_unsupported(
            SqlEngine::list_schemas_budgeted(&connector, read_budget).await,
            "SqlEngine.list_schemas_budgeted",
        );
        assert_unsupported(
            SqlEngine::list_tables_budgeted(&connector, Some("public"), read_budget).await,
            "SqlEngine.list_tables_budgeted",
        );
        assert_unsupported(
            CqlEngine::list_keyspaces_budgeted(&connector, read_budget).await,
            "CqlEngine.list_keyspaces_budgeted",
        );
        assert_unsupported(
            CqlEngine::list_cql_tables_budgeted(&connector, Some("app"), read_budget).await,
            "CqlEngine.list_cql_tables_budgeted",
        );
        assert_unsupported(
            DocumentStore::list_collections_budgeted(&connector, read_budget).await,
            "DocumentStore.list_collections_budgeted",
        );
        assert_unsupported(
            SearchEngine::list_indices_budgeted(&connector, read_budget).await,
            "SearchEngine.list_indices_budgeted",
        );
        assert_unsupported(
            TimeSeriesStore::list_measurements_budgeted(&connector, read_budget).await,
            "TimeSeriesStore.list_measurements_budgeted",
        );
        assert_unsupported(
            AdminInspect::list_topics_budgeted(&connector, read_budget).await,
            "AdminInspect.list_topics_budgeted",
        );
        assert_unsupported(
            Db2Engine::list_sequences_budgeted(&connector, None, read_budget).await,
            "Db2Engine.list_sequences_budgeted",
        );
        assert_unsupported(
            Db2Engine::list_routines_budgeted(&connector, None, read_budget).await,
            "Db2Engine.list_routines_budgeted",
        );
        assert_unsupported(
            Db2Engine::list_tablespaces_budgeted(&connector, read_budget).await,
            "Db2Engine.list_tablespaces_budgeted",
        );
        assert_unsupported(
            Db2Engine::list_foreign_keys_budgeted(&connector, "app.orders", read_budget).await,
            "Db2Engine.list_foreign_keys_budgeted",
        );
        assert_unsupported(
            SqlEngine::describe_table_bounded(&connector, "app.orders", metadata_budget).await,
            "SqlEngine.describe_table_bounded",
        );
        assert_unsupported(
            CqlEngine::describe_cql_table_bounded(&connector, "app.orders", metadata_budget).await,
            "CqlEngine.describe_cql_table_bounded",
        );
        assert_unsupported(
            AdminInspect::topic_detail_bounded(&connector, "events", metadata_budget).await,
            "AdminInspect.topic_detail_bounded",
        );
        assert_unsupported(
            AdminInspect::consumer_lag_bounded(&connector, "workers", metadata_budget).await,
            "AdminInspect.consumer_lag_bounded",
        );
        assert_unsupported(
            Db2Engine::generate_ddl_bounded(&connector, "app.orders", metadata_budget).await,
            "Db2Engine.generate_ddl_bounded",
        );

        assert_eq!(connector.unbounded_calls.load(Ordering::SeqCst), 0);
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
        assert!(CapabilityOperation::SEARCH_BUDGETED_READS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::KEY_VALUE_EXISTENCE
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
    }

    #[tokio::test]
    async fn exact_mutation_defaults_never_inherit_or_invoke_legacy_methods() {
        let connector = LegacyCatalogs::new();
        let input_budget = InputBudget::default();
        let read_budget = ReadBudget::with_default_bytes(10).unwrap();

        assert_unsupported(
            SqlEngine::execute_budgeted(&connector, "DELETE FROM app.events", &[], input_budget)
                .await,
            "SqlEngine.execute_budgeted",
        );
        assert_unsupported(
            SqlEngine::insert_rows_atomic_budgeted(
                &connector,
                "app.events",
                &["id".to_owned()],
                &[vec![Value::Int(1)]],
                input_budget,
            )
            .await,
            "SqlEngine.insert_rows_atomic_budgeted",
        );
        assert_unsupported(
            CqlEngine::execute_cql_budgeted(
                &connector,
                "DELETE FROM app.events WHERE id = 1",
                input_budget,
            )
            .await,
            "CqlEngine.execute_cql_budgeted",
        );
        assert_unsupported(
            KeyValueStore::set_budgeted(
                &connector,
                "key",
                b"value",
                SetOptions::default(),
                input_budget,
            )
            .await,
            "KeyValueStore.set_budgeted",
        );
        assert_unsupported(
            KeyValueStore::restore_with_expiry_budgeted(
                &connector,
                "key",
                b"value",
                KeyExpiry::Persistent,
                false,
                input_budget,
            )
            .await,
            "KeyValueStore.restore_with_expiry_budgeted",
        );
        assert_unsupported(
            KeyValueStore::delete_budgeted(&connector, &["key".to_owned()], input_budget).await,
            "KeyValueStore.delete_budgeted",
        );
        assert_unsupported(
            KeyValueStore::raw_command_io_budgeted(
                &connector,
                &["DEL".to_owned(), "key".to_owned()],
                input_budget,
                read_budget,
            )
            .await,
            "KeyValueStore.raw_command_io_budgeted",
        );

        assert_unsupported(
            DocumentStore::insert_budgeted(
                &connector,
                "users",
                vec![Document::new()],
                input_budget,
            )
            .await,
            "DocumentStore.insert_budgeted",
        );
        assert_unsupported(
            DocumentStore::update_one_budgeted(
                &connector,
                "users",
                Value::Null,
                Value::Null,
                input_budget,
            )
            .await,
            "DocumentStore.update_one_budgeted",
        );
        assert_unsupported(
            DocumentStore::update_many_budgeted(
                &connector,
                "users",
                Value::Null,
                Value::Null,
                input_budget,
            )
            .await,
            "DocumentStore.update_many_budgeted",
        );
        assert_unsupported(
            DocumentStore::delete_one_budgeted(&connector, "users", Value::Null, input_budget)
                .await,
            "DocumentStore.delete_one_budgeted",
        );
        assert_unsupported(
            DocumentStore::delete_many_budgeted(&connector, "users", Value::Null, input_budget)
                .await,
            "DocumentStore.delete_many_budgeted",
        );
        assert_unsupported(
            DocumentStore::aggregate_write_budgeted(
                &connector,
                "users",
                vec![Value::Json(serde_json::json!({ "$out": "archive" }))],
                input_budget,
                read_budget,
            )
            .await,
            "DocumentStore.aggregate_write_budgeted",
        );
        assert_unsupported(
            DocumentStore::drop_collection_budgeted(&connector, "users", input_budget).await,
            "DocumentStore.drop_collection_budgeted",
        );

        assert_unsupported(
            TimeSeriesStore::write_points_budgeted(
                &connector,
                vec![Point {
                    measurement: "cpu".to_owned(),
                    tags: std::collections::HashMap::new(),
                    fields: std::collections::HashMap::from([("value".to_owned(), 1.0)]),
                    timestamp: 1,
                }],
                input_budget,
            )
            .await,
            "TimeSeriesStore.write_points_budgeted",
        );
        assert_unsupported(
            SearchEngine::index_doc_budgeted(&connector, "users", Value::Null, input_budget).await,
            "SearchEngine.index_doc_budgeted",
        );
        assert_unsupported(
            SearchEngine::put_doc_budgeted(
                &connector,
                "users",
                "user-1",
                Value::Null,
                input_budget,
            )
            .await,
            "SearchEngine.put_doc_budgeted",
        );
        assert_unsupported(
            SearchEngine::update_doc_budgeted(
                &connector,
                "users",
                "user-1",
                Value::Null,
                input_budget,
            )
            .await,
            "SearchEngine.update_doc_budgeted",
        );
        assert_unsupported(
            SearchEngine::delete_doc_budgeted(&connector, "users", "user-1", input_budget).await,
            "SearchEngine.delete_doc_budgeted",
        );
        assert_unsupported(
            SearchEngine::delete_index_budgeted(&connector, "users", input_budget).await,
            "SearchEngine.delete_index_budgeted",
        );

        assert!(CapabilityOperation::BUDGETED_MUTATIONS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert_eq!(connector.unbounded_calls.load(Ordering::SeqCst), 0);
    }

    struct LegacySql;

    #[async_trait]
    impl Connector for LegacySql {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind("legacy-sql".into())
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities {
                sql: true,
                ..Default::default()
            }
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        fn as_sql(&self) -> Option<&dyn SqlEngine> {
            Some(self)
        }
    }

    #[async_trait]
    impl SqlEngine for LegacySql {
        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<ResultSet> {
            Ok(ResultSet::empty())
        }

        async fn query_bounded(
            &self,
            _sql: &str,
            _params: &[Value],
            _max_rows: usize,
        ) -> Result<ResultSet> {
            Ok(ResultSet::empty())
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<ExecOutcome> {
            Ok(ExecOutcome {
                rows_affected: 0,
                last_insert_id: None,
            })
        }

        async fn list_schemas(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
            Ok(vec![])
        }

        async fn describe_table(&self, _table: &str) -> Result<TableSchema> {
            Err(Error::Query("unused".into()))
        }
    }

    #[tokio::test]
    async fn legacy_sql_connectors_do_not_claim_or_inherit_atomic_import() {
        let connector = LegacySql;
        assert!(!connector
            .operations()
            .contains(&CapabilityOperation::SqlInsertRowsAtomic));
        assert!(matches!(
            connector
                .insert_rows_atomic("target", &["id".into()], &[vec![Value::Int(1)]])
                .await,
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-sql" && needed == "SqlEngine.insert_rows_atomic"
        ));
    }

    struct LegacyDocument;

    #[async_trait]
    impl Connector for LegacyDocument {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind("legacy-document".into())
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities {
                document: true,
                ..Default::default()
            }
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        fn as_document(&self) -> Option<&dyn DocumentStore> {
            Some(self)
        }
    }

    #[async_trait]
    impl DocumentStore for LegacyDocument {
        async fn list_collections(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn find(
            &self,
            _collection: &str,
            _filter: Value,
            _options: FindOptions,
        ) -> Result<Vec<Document>> {
            Ok(vec![])
        }

        async fn insert(&self, _collection: &str, _docs: Vec<Document>) -> Result<InsertOutcome> {
            Ok(InsertOutcome {
                inserted: 0,
                ids: vec![],
            })
        }

        async fn update(
            &self,
            _collection: &str,
            _filter: Value,
            _update: Value,
        ) -> Result<UpdateOutcome> {
            Ok(UpdateOutcome {
                matched: 3,
                modified: 2,
            })
        }

        async fn delete(&self, _collection: &str, _filter: Value) -> Result<u64> {
            Ok(4)
        }

        async fn aggregate(
            &self,
            _collection: &str,
            _pipeline: Vec<Value>,
        ) -> Result<Vec<Document>> {
            Ok(vec![])
        }

        async fn aggregate_bounded(
            &self,
            _collection: &str,
            _pipeline: Vec<Value>,
            _max_items: usize,
        ) -> Result<Vec<Document>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn legacy_document_bulk_methods_keep_their_mapping_without_claiming_optional_modes() {
        let connector = LegacyDocument;
        for operation in [
            CapabilityOperation::DocumentFindBudgeted,
            CapabilityOperation::DocumentAggregateBudgeted,
            CapabilityOperation::DocumentUpdateOne,
            CapabilityOperation::DocumentUpdateMany,
            CapabilityOperation::DocumentDeleteOne,
            CapabilityOperation::DocumentDeleteMany,
            CapabilityOperation::DocumentDropCollection,
        ] {
            assert!(!connector.operations().contains(&operation));
        }

        let filter = Value::Json(serde_json::json!({ "tenant": "one" }));
        let update = Value::Json(serde_json::json!({ "$set": { "active": true } }));
        let legacy_update = connector
            .update("users", filter.clone(), update.clone())
            .await
            .unwrap();
        let explicit_many = connector
            .update_many("users", filter.clone(), update.clone())
            .await
            .unwrap();
        assert_eq!(legacy_update.matched, explicit_many.matched);
        assert_eq!(legacy_update.modified, explicit_many.modified);
        assert_eq!(connector.delete("users", filter.clone()).await.unwrap(), 4);
        assert_eq!(
            connector
                .delete_many("users", filter.clone())
                .await
                .unwrap(),
            4
        );

        assert!(matches!(
            connector.update_one("users", filter.clone(), update).await,
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-document" && needed == "DocumentStore.update_one"
        ));
        assert!(matches!(
            connector.delete_one("users", filter).await,
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-document" && needed == "DocumentStore.delete_one"
        ));
        assert!(matches!(
            connector.drop_collection("users").await,
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-document" && needed == "DocumentStore.drop_collection"
        ));
    }
}
