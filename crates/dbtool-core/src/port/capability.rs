use crate::{
    model::{
        BoundedList, ConsumeOptions, DeleteResourceOptions, DeleteResourceOutcome, Document,
        ExecOutcome, FindOptions, ForeignKeyInfo, InsertOutcome, KeyExpiry, KeyValueRestoreOutcome,
        KeyValueSnapshot, LagInfo, Message, MessageResource, MetadataBudget, Point, ProduceOutcome,
        ReadBudget, ResultSet, RoutineInfo, SequenceInfo, SeriesSet, TableInfo, TableSchema,
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
    async fn list_keyspaces(&self) -> Result<Vec<String>>;
    async fn list_keyspaces_bounded(&self, _max_items: usize) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "CqlEngine.list_keyspaces_bounded",
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
    async fn delete(&self, keys: &[String]) -> Result<u64>;
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
    async fn list_collections_bounded(&self, _max_items: usize) -> Result<BoundedList<String>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "DocumentStore.list_collections_bounded",
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

    /// Delete every matching document.
    ///
    /// The default delegates to the historical bulk-delete method for embedded
    /// compatibility, but connectors must explicitly advertise
    /// `document.delete_many` after verifying that behavior.
    async fn delete_many(&self, collection: &str, filter: Value) -> Result<u64> {
        self.delete(collection, filter).await
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
    async fn write_points(&self, points: Vec<Point>) -> Result<()>;
    async fn query_range(&self, query: &str, range: TimeRange) -> Result<SeriesSet>;
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
    async fn list_topics_bounded(&self, _max_items: usize) -> Result<BoundedList<TopicInfo>> {
        Err(crate::Error::UnsupportedCapability {
            kind: self.kind().0,
            needed: "AdminInspect.list_topics_bounded",
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
        assert!(CapabilityOperation::BOUNDED_NESTED_METADATA
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::BUDGETED_READS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::KEY_VALUE_BUDGETED_READS
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
        assert!(CapabilityOperation::KEY_VALUE_EXISTENCE
            .iter()
            .all(|operation| !connector.operations().contains(operation)));
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
