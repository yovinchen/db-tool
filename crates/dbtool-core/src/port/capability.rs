use crate::{
    model::{
        ConsumeOptions, DeleteResourceOptions, DeleteResourceOutcome, Document, ExecOutcome,
        FindOptions, ForeignKeyInfo, InsertOutcome, KeyExpiry, KeyValueRestoreOutcome,
        KeyValueSnapshot, LagInfo, Message, MessageResource, Point, ProduceOutcome, ResultSet,
        RoutineInfo, SequenceInfo, SeriesSet, TableInfo, TableSchema, TablespaceInfo, TimeRange,
        TopicDetail, TopicInfo, UpdateOutcome, Value,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        port::{Capabilities, CapabilityOperation, ConnectorKind},
        Error,
    };

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
            CapabilityOperation::DocumentUpdateOne,
            CapabilityOperation::DocumentUpdateMany,
            CapabilityOperation::DocumentDeleteOne,
            CapabilityOperation::DocumentDeleteMany,
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
    }
}
