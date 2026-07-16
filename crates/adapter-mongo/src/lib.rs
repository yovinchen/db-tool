use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{BoundedList, Document, FindOptions, InsertOutcome, ReadBudget, UpdateOutcome, Value},
    port::{
        capability::DocumentStore,
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::{ListLimiter, ReadLimiter},
};
use futures::future::BoxFuture;
use mongodb::{
    bson::{self, Bson},
    Client, Database,
};
use std::collections::BTreeMap;

const MONGO_BUDGETED_BATCH_SIZE: u32 = 1;

pub struct MongoAdapter {
    db: Database,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let client = Client::with_uri_str(&dsn.raw)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        let db_name = dsn.database.unwrap_or_else(|| "admin".to_owned());
        let db = client.database(&db_name);
        Ok(Box::new(MongoAdapter { db }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for MongoAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind("mongodb".into())
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            document: true,
            ..Default::default()
        }
    }

    fn operations(&self) -> Vec<CapabilityOperation> {
        mongo_operations(self.capabilities())
    }

    async fn ping(&self) -> Result<()> {
        self.db
            .run_command(mongodb::bson::doc! { "ping": 1 })
            .await
            .map(|_| ())
            .map_err(|e| Error::Connection(e.to_string()))
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
    fn as_document(&self) -> Option<&dyn DocumentStore> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl DocumentStore for MongoAdapter {
    async fn list_collections(&self) -> Result<Vec<String>> {
        self.db
            .list_collection_names()
            .await
            .map_err(|e| Error::Query(e.to_string()))
    }

    async fn list_collections_bounded(&self, max_items: usize) -> Result<BoundedList<String>> {
        use futures::StreamExt;

        let limiter = ListLimiter::new(max_items);
        let probe_items = limiter.probe_items()?;
        let batch_size = u32::try_from(probe_items).map_err(|_| {
            Error::Config("MongoDB collection catalog limit exceeds the u32 range".into())
        })?;
        let mut cursor = self
            .db
            .list_collections()
            .batch_size(batch_size)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let mut names = Vec::with_capacity(probe_items.min(256));
        while names.len() < probe_items {
            let Some(specification) = cursor.next().await else {
                break;
            };
            names.push(specification.map_err(|e| Error::Query(e.to_string()))?.name);
        }
        Ok(limiter.finish(names))
    }

    async fn find(
        &self,
        collection: &str,
        filter: Value,
        opts: FindOptions,
    ) -> Result<Vec<Document>> {
        use futures::StreamExt;
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let filter = value_to_document(filter)?;
        let limit = mongo_limit(opts.limit)?;
        let skip = opts
            .skip
            .map(u64::try_from)
            .transpose()
            .map_err(|_| Error::Config("MongoDB skip exceeds the u64 range".into()))?;
        let find_opts = mongodb::options::FindOptions::builder()
            .limit(limit)
            .skip(skip)
            .sort(optional_document(opts.sort)?)
            .projection(optional_document(opts.projection)?)
            .build();
        let mut cursor = col
            .find(filter)
            .with_options(find_opts)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let mut docs = Vec::new();
        while let Some(doc) = cursor.next().await {
            let bson_doc = doc.map_err(|e| Error::Query(e.to_string()))?;
            docs.push(bson_document_to_core(bson_doc));
        }
        Ok(docs)
    }

    async fn find_budgeted(
        &self,
        collection: &str,
        filter: Value,
        opts: FindOptions,
        budget: ReadBudget,
    ) -> Result<BoundedList<Document>> {
        let limiter = MongoDocumentLimiter::new(budget, "MongoDB find result")?;
        let probe_items = limiter.probe_items()?;
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let filter = value_to_document(filter)?;
        let limit = mongo_budgeted_find_limit(opts.limit, budget.max_items, probe_items)?;
        let skip = opts
            .skip
            .map(u64::try_from)
            .transpose()
            .map_err(|_| Error::Config("MongoDB skip exceeds the u64 range".into()))?;
        let find_opts = mongodb::options::FindOptions::builder()
            .limit(Some(limit))
            .batch_size(MONGO_BUDGETED_BATCH_SIZE)
            .skip(skip)
            .sort(optional_document(opts.sort)?)
            .projection(optional_document(opts.projection)?)
            .build();

        // As with aggregation below, the driver must materialize one legal
        // server response before this adapter can inspect its raw BSON. A
        // single-document batch bounds that unavoidable residual to one
        // MongoDB-legal document inside one legal wire message.
        let cursor = col
            .find(filter)
            .with_options(find_opts)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        collect_budgeted_cursor(cursor, limiter, probe_items).await
    }

    async fn insert(&self, collection: &str, docs: Vec<Document>) -> Result<InsertOutcome> {
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let bson_docs: Vec<_> = docs
            .into_iter()
            .map(core_document_to_bson)
            .collect::<Result<Vec<_>>>()?;
        let count = u64::try_from(bson_docs.len())
            .map_err(|_| Error::Serialization("document count exceeds the u64 range".into()))?;
        let result = col
            .insert_many(bson_docs)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(InsertOutcome {
            inserted: count,
            ids: result
                .inserted_ids
                .into_values()
                .map(inserted_id_string)
                .collect(),
        })
    }

    async fn update(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
    ) -> Result<UpdateOutcome> {
        self.update_many(collection, filter, update).await
    }

    async fn delete(&self, collection: &str, filter: Value) -> Result<u64> {
        self.delete_many(collection, filter).await
    }

    async fn update_one(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
    ) -> Result<UpdateOutcome> {
        self.update_documents(collection, filter, update, false)
            .await
    }

    async fn update_many(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
    ) -> Result<UpdateOutcome> {
        self.update_documents(collection, filter, update, true)
            .await
    }

    async fn delete_one(&self, collection: &str, filter: Value) -> Result<u64> {
        self.delete_documents(collection, filter, false).await
    }

    async fn delete_many(&self, collection: &str, filter: Value) -> Result<u64> {
        self.delete_documents(collection, filter, true).await
    }

    async fn aggregate(&self, collection: &str, pipeline: Vec<Value>) -> Result<Vec<Document>> {
        self.aggregate_with_limit(collection, pipeline, None).await
    }

    async fn aggregate_bounded(
        &self,
        collection: &str,
        pipeline: Vec<Value>,
        max_items: usize,
    ) -> Result<Vec<Document>> {
        self.aggregate_with_limit(collection, pipeline, Some(max_items))
            .await
    }

    async fn aggregate_budgeted(
        &self,
        collection: &str,
        pipeline: Vec<Value>,
        budget: ReadBudget,
    ) -> Result<BoundedList<Document>> {
        let limiter = MongoDocumentLimiter::new(budget, "MongoDB aggregate result")?;
        let probe_items = limiter.probe_items()?;
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let pipeline = pipeline
            .into_iter()
            .map(value_to_document)
            .collect::<Result<Vec<_>>>()?;

        // The driver necessarily materializes one server response before the
        // adapter can inspect raw BSON lengths. Keeping the protocol batch at
        // one document confines that residual to one MongoDB-legal document
        // (normally at most 16 MiB) inside one legal wire message (normally at
        // most 48 MiB). Caller budgets are enforced immediately afterwards.
        let cursor = col
            .aggregate(pipeline)
            .with_type::<mongodb::bson::Document>()
            .batch_size(MONGO_BUDGETED_BATCH_SIZE)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        collect_budgeted_cursor(cursor, limiter, probe_items).await
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        self.db
            .collection::<mongodb::bson::Document>(collection)
            .drop()
            .await
            .map_err(|e| Error::Query(e.to_string()))
    }
}

impl MongoAdapter {
    async fn update_documents(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
        many: bool,
    ) -> Result<UpdateOutcome> {
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let filter = value_to_document(filter)?;
        ensure_nonempty_filter(&filter, if many { "update many" } else { "update one" })?;
        let update = update_document(update)?;
        let result = if many {
            col.update_many(filter, update).await
        } else {
            col.update_one(filter, update).await
        }
        .map_err(|e| Error::Query(e.to_string()))?;
        Ok(UpdateOutcome {
            matched: result.matched_count,
            modified: result.modified_count,
        })
    }

    async fn delete_documents(&self, collection: &str, filter: Value, many: bool) -> Result<u64> {
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let filter = value_to_document(filter)?;
        ensure_nonempty_filter(&filter, if many { "delete many" } else { "delete one" })?;
        let result = if many {
            col.delete_many(filter).await
        } else {
            col.delete_one(filter).await
        }
        .map_err(|e| Error::Query(e.to_string()))?;
        Ok(result.deleted_count)
    }

    async fn aggregate_with_limit(
        &self,
        collection: &str,
        pipeline: Vec<Value>,
        max_items: Option<usize>,
    ) -> Result<Vec<Document>> {
        use futures::StreamExt;
        if max_items == Some(0) {
            return Err(Error::Config(
                "MongoDB aggregate limit must be greater than zero".into(),
            ));
        }
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let pipeline = pipeline
            .into_iter()
            .map(value_to_document)
            .collect::<Result<Vec<_>>>()?;
        let mut cursor = col
            .aggregate(pipeline)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let mut docs = Vec::new();
        while let Some(doc) = cursor.next().await {
            docs.push(bson_document_to_core(
                doc.map_err(|e| Error::Query(e.to_string()))?,
            ));
            if max_items.is_some_and(|max_items| docs.len() >= max_items) {
                break;
            }
        }
        Ok(docs)
    }
}

fn mongo_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::DocumentListCollectionsBounded,
        CapabilityOperation::DocumentFindBudgeted,
        CapabilityOperation::DocumentAggregateBudgeted,
        CapabilityOperation::DocumentUpdateOne,
        CapabilityOperation::DocumentUpdateMany,
        CapabilityOperation::DocumentDeleteOne,
        CapabilityOperation::DocumentDeleteMany,
        CapabilityOperation::DocumentDropCollection,
    ]);
    operations
}

async fn collect_budgeted_cursor(
    mut cursor: mongodb::Cursor<mongodb::bson::Document>,
    mut limiter: MongoDocumentLimiter,
    probe_items: usize,
) -> Result<BoundedList<Document>> {
    let mut documents = Vec::with_capacity(limiter.retained_capacity());
    while limiter.observed_items() < probe_items {
        if !cursor
            .advance()
            .await
            .map_err(|error| Error::Query(error.to_string()))?
        {
            break;
        }

        // `current()` exposes the exact raw BSON document retained by the
        // driver, so duplicate fields and native BSON types are charged before
        // conversion can normalize them into the portable core representation.
        let raw_bson_bytes = cursor.current().as_bytes().len();
        limiter.observe_raw_bson(raw_bson_bytes)?;
        let document = cursor
            .deserialize_current()
            .map_err(|error| Error::Serialization(error.to_string()))?;
        limiter.retain_decoded_document(document, &mut documents)?;
    }
    drop(cursor);

    limiter.finish(documents)
}

struct MongoDocumentLimiter {
    visible: ReadLimiter,
    max_items: usize,
    max_bytes: usize,
    observed_bson_bytes: usize,
    subject: String,
}

impl MongoDocumentLimiter {
    fn new(budget: ReadBudget, subject: impl Into<String>) -> Result<Self> {
        let subject = subject.into();
        Ok(Self {
            visible: ReadLimiter::new(budget, subject.clone())?,
            max_items: budget.max_items,
            max_bytes: budget.max_bytes,
            observed_bson_bytes: 0,
            subject,
        })
    }

    fn probe_items(&self) -> Result<usize> {
        self.visible.probe_items()
    }

    fn retained_capacity(&self) -> usize {
        self.max_items.min(256)
    }

    fn observed_items(&self) -> usize {
        self.visible.observed_items()
    }

    fn retain_decoded_document(
        &mut self,
        document: bson::Document,
        retained: &mut Vec<Document>,
    ) -> Result<()> {
        self.visible
            .retain_item(bson_document_to_core(document), retained)
    }

    fn finish(self, documents: Vec<Document>) -> Result<BoundedList<Document>> {
        self.visible.finish(documents)
    }

    fn observe_raw_bson(&mut self, bytes: usize) -> Result<()> {
        let next = self
            .observed_bson_bytes
            .checked_add(bytes)
            .ok_or_else(|| self.byte_budget_error())?;
        if next > self.max_bytes {
            return Err(self.byte_budget_error());
        }
        self.observed_bson_bytes = next;
        Ok(())
    }

    fn byte_budget_error(&self) -> Error {
        Error::ReadBudgetExceeded {
            subject: self.subject.clone(),
            unit: "bytes",
            limit: self.max_bytes,
        }
    }
}

fn optional_document(value: Option<Value>) -> Result<Option<bson::Document>> {
    value.map(value_to_document).transpose()
}

fn mongo_limit(limit: Option<usize>) -> Result<Option<i64>> {
    limit
        .map(|limit| {
            if limit == 0 {
                return Err(Error::Config(
                    "MongoDB find limit must be greater than zero".into(),
                ));
            }
            i64::try_from(limit)
                .map_err(|_| Error::Config("MongoDB find limit exceeds the i64 range".into()))
        })
        .transpose()
}

fn mongo_budgeted_find_limit(
    requested_limit: Option<usize>,
    max_items: usize,
    probe_items: usize,
) -> Result<i64> {
    if requested_limit == Some(0) {
        return Err(Error::Config(
            "MongoDB find limit must be greater than zero".into(),
        ));
    }

    let fetch_items = match requested_limit {
        Some(requested) if requested <= max_items => requested,
        Some(_) | None => probe_items,
    };
    i64::try_from(fetch_items)
        .map_err(|_| Error::Config("MongoDB budgeted find limit exceeds the i64 range".into()))
}

fn inserted_id_string(id: Bson) -> String {
    match id {
        Bson::ObjectId(id) => id.to_hex(),
        Bson::String(id) => id,
        other => other.into_canonical_extjson().to_string(),
    }
}

fn ensure_nonempty_filter(filter: &bson::Document, operation: &str) -> Result<()> {
    if filter.is_empty() {
        return Err(Error::Query(format!(
            "refusing to {operation} documents without a filter"
        )));
    }
    Ok(())
}

fn value_to_document(value: Value) -> Result<bson::Document> {
    match value {
        Value::Null => Ok(bson::Document::new()),
        Value::Json(serde_json::Value::Object(map)) => {
            let mut doc = bson::Document::new();
            for (key, value) in map {
                doc.insert(key, json_to_bson(value)?);
            }
            Ok(doc)
        }
        Value::Map(map) => {
            let mut doc = bson::Document::new();
            for (key, value) in map {
                doc.insert(key, value_to_bson(value)?);
            }
            Ok(doc)
        }
        other => Err(Error::Serialization(format!(
            "expected JSON object/document, got {other:?}"
        ))),
    }
}

fn update_document(value: Value) -> Result<bson::Document> {
    let doc = value_to_document(value)?;
    if doc.keys().any(|key| key.starts_with('$')) {
        return Ok(doc);
    }

    Ok(bson::doc! { "$set": doc })
}

fn core_document_to_bson(document: Document) -> Result<bson::Document> {
    let mut bson_doc = bson::Document::new();
    for (key, value) in document {
        bson_doc.insert(key, value_to_bson(value)?);
    }
    Ok(bson_doc)
}

fn value_to_bson(value: Value) -> Result<Bson> {
    Ok(match value {
        Value::Null => Bson::Null,
        Value::Bool(value) => Bson::Boolean(value),
        Value::Int(value) => Bson::Int64(value),
        Value::Float(value) => Bson::Double(value),
        Value::Text(value) => Bson::String(value),
        Value::Bytes(value) => Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: value,
        }),
        Value::Timestamp(ms) => Bson::DateTime(bson::DateTime::from_millis(ms)),
        Value::Json(value) => json_to_bson(value)?,
        Value::Array(values) => Bson::Array(
            values
                .into_iter()
                .map(value_to_bson)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Map(map) => {
            let mut doc = bson::Document::new();
            for (key, value) in map {
                doc.insert(key, value_to_bson(value)?);
            }
            Bson::Document(doc)
        }
    })
}

fn json_to_bson(value: serde_json::Value) -> Result<Bson> {
    Bson::try_from(value).map_err(|err| Error::Serialization(err.to_string()))
}

fn bson_document_to_core(document: bson::Document) -> Document {
    document
        .into_iter()
        .map(|(key, value)| (key, bson_to_value(value)))
        .collect()
}

fn bson_to_value(value: Bson) -> Value {
    match value {
        Bson::Double(value) => Value::Float(value),
        Bson::String(value) => Value::Text(value),
        Bson::Array(values) => Value::Array(values.into_iter().map(bson_to_value).collect()),
        Bson::Document(document) => {
            let map: BTreeMap<_, _> = document
                .into_iter()
                .map(|(key, value)| (key, bson_to_value(value)))
                .collect();
            Value::Map(map)
        }
        Bson::Boolean(value) => Value::Bool(value),
        Bson::Null => Value::Null,
        Bson::Int32(value) => Value::Int(value.into()),
        Bson::Int64(value) => Value::Int(value),
        Bson::Binary(binary) if binary.subtype == bson::spec::BinarySubtype::Generic => {
            Value::Bytes(binary.bytes)
        }
        Bson::DateTime(value) => Value::Timestamp(value.timestamp_millis()),
        other => Value::Json(other.into_canonical_extjson()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_document_wraps_plain_document_in_set() {
        let update = update_document(Value::Json(serde_json::json!({ "name": "alice" }))).unwrap();

        assert!(update.contains_key("$set"));
    }

    #[test]
    fn update_document_preserves_operator_document() {
        let update =
            update_document(Value::Json(serde_json::json!({ "$inc": { "visits": 1 } }))).unwrap();

        assert!(update.contains_key("$inc"));
        assert!(!update.contains_key("$set"));
    }

    #[test]
    fn every_update_and_delete_mode_rejects_empty_filters() {
        let filter = bson::Document::new();

        let update_error = ensure_nonempty_filter(&filter, "update").unwrap_err();
        assert!(matches!(
            update_error,
            Error::Query(message) if message.contains("update documents without a filter")
        ));

        let delete_error = ensure_nonempty_filter(&filter, "delete").unwrap_err();
        assert!(matches!(
            delete_error,
            Error::Query(message) if message.contains("delete documents without a filter")
        ));
    }

    #[test]
    fn mongo_declares_explicit_document_extensions() {
        let operations = mongo_operations(Capabilities {
            document: true,
            ..Default::default()
        });

        for operation in [
            CapabilityOperation::DocumentListCollectionsBounded,
            CapabilityOperation::DocumentFindBudgeted,
            CapabilityOperation::DocumentAggregateBudgeted,
            CapabilityOperation::DocumentUpdateOne,
            CapabilityOperation::DocumentUpdateMany,
            CapabilityOperation::DocumentDeleteOne,
            CapabilityOperation::DocumentDeleteMany,
            CapabilityOperation::DocumentDropCollection,
        ] {
            assert!(operations.contains(&operation));
        }
        assert!(operations.contains(&CapabilityOperation::DocumentUpdate));
        assert!(operations.contains(&CapabilityOperation::DocumentDelete));
    }

    #[test]
    fn bson_round_trip_preserves_core_value_shapes() {
        let mut doc = Document::new();
        doc.insert("name".to_owned(), Value::Text("alice".to_owned()));
        doc.insert("active".to_owned(), Value::Bool(true));
        doc.insert("count".to_owned(), Value::Int(3));

        let bson = core_document_to_bson(doc).unwrap();
        let core = bson_document_to_core(bson);

        assert_eq!(core.get("name"), Some(&Value::Text("alice".to_owned())));
        assert_eq!(core.get("active"), Some(&Value::Bool(true)));
        assert_eq!(core.get("count"), Some(&Value::Int(3)));
    }

    #[test]
    fn find_and_aggregate_limits_reject_zero_and_overflow() {
        assert!(matches!(
            mongo_limit(Some(0)),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
        if usize::BITS > 63 {
            assert!(matches!(
                mongo_limit(Some(i64::MAX as usize + 1)),
                Err(Error::Config(message)) if message.contains("i64 range")
            ));
        }
        assert_eq!(mongo_limit(Some(7)).unwrap(), Some(7));
        assert_eq!(mongo_budgeted_find_limit(None, 2, 3).unwrap(), 3);
        assert_eq!(mongo_budgeted_find_limit(Some(5), 2, 3).unwrap(), 3);
        assert_eq!(mongo_budgeted_find_limit(Some(2), 2, 3).unwrap(), 2);
        assert_eq!(mongo_budgeted_find_limit(Some(1), 2, 3).unwrap(), 1);
        assert!(matches!(
            mongo_budgeted_find_limit(Some(0), 2, 3),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
        if usize::BITS > 63 {
            assert!(matches!(
                mongo_budgeted_find_limit(None, usize::MAX - 1, usize::MAX),
                Err(Error::Config(message)) if message.contains("i64 range")
            ));
        }
        assert_eq!(MONGO_BUDGETED_BATCH_SIZE, 1);
    }

    fn collect_test_documents(
        budget: ReadBudget,
        documents: Vec<bson::Document>,
    ) -> Result<BoundedList<Document>> {
        let mut limiter = MongoDocumentLimiter::new(budget, "MongoDB test result")?;
        let probe_items = limiter.probe_items()?;
        let mut retained = Vec::new();
        for document in documents.into_iter().take(probe_items) {
            let raw_bson_bytes = bson::to_vec(&document)
                .map_err(|error| Error::Serialization(error.to_string()))?
                .len();
            limiter.observe_raw_bson(raw_bson_bytes)?;
            limiter.retain_decoded_document(document, &mut retained)?;
        }
        limiter.finish(retained)
    }

    #[test]
    fn budgeted_documents_distinguish_exact_n_from_n_plus_one() {
        let first = bson::doc! { "id": 1, "name": "one" };
        let second = bson::doc! { "id": 2, "name": "two" };
        let probe = bson::doc! { "id": 3, "name": "probe" };
        let budget = ReadBudget::new(2, 4096).unwrap();

        let exact = collect_test_documents(budget, vec![first.clone(), second.clone()]).unwrap();
        assert_eq!(exact.items.len(), 2);
        assert!(!exact.truncated);

        let truncated = collect_test_documents(budget, vec![first, second, probe]).unwrap();
        assert_eq!(truncated.items.len(), 2);
        assert!(truncated.truncated);
    }

    #[test]
    fn budgeted_documents_charge_complete_visible_envelope_and_probe_at_n_and_n_minus_one() {
        let first = bson::doc! { "id": 1 };
        let probe = bson::doc! { "id": 2 };
        let first_core = bson_document_to_core(first.clone());
        let probe_core = bson_document_to_core(probe.clone());
        let visible = BoundedList {
            items: vec![first_core],
            truncated: true,
        };
        let visible_bytes = serde_json::to_vec(&visible).unwrap().len()
            + serde_json::to_vec(&probe_core).unwrap().len();
        let bson_bytes = bson::to_vec(&first).unwrap().len() + bson::to_vec(&probe).unwrap().len();
        assert!(visible_bytes > bson_bytes);

        let exact_budget = ReadBudget::new(1, visible_bytes).unwrap();
        let exact =
            collect_test_documents(exact_budget, vec![first.clone(), probe.clone()]).unwrap();
        assert_eq!(exact.items.len(), 1);
        assert!(exact.truncated);

        let error = collect_test_documents(
            ReadBudget::new(1, visible_bytes - 1).unwrap(),
            vec![first, probe],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == visible_bytes - 1
        ));
        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
    }

    #[test]
    fn budgeted_documents_fail_closed_on_first_and_cumulative_raw_bson_overflow() {
        let first = bson::doc! { "payload": "a".repeat(64) };
        let first_bytes = bson::to_vec(&first).unwrap().len();
        let first_error = collect_test_documents(
            ReadBudget::new(1, first_bytes - 1).unwrap(),
            vec![first.clone()],
        )
        .unwrap_err();
        assert_eq!(first_error.code(), "READ_BUDGET_EXCEEDED");

        let second = bson::doc! { "payload": "b".repeat(64) };
        let cumulative_bytes = first_bytes + bson::to_vec(&second).unwrap().len();
        let cumulative_error = collect_test_documents(
            ReadBudget::new(2, cumulative_bytes - 1).unwrap(),
            vec![first, second],
        )
        .unwrap_err();
        assert!(matches!(
            cumulative_error,
            Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == cumulative_bytes - 1
        ));
        assert_eq!(cumulative_error.code(), "READ_BUDGET_EXCEEDED");
    }

    #[test]
    fn collection_catalog_probe_rejects_invalid_limits_without_a_backend() {
        assert!(matches!(
            ListLimiter::new(0).probe_items(),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
        assert!(matches!(
            ListLimiter::new(usize::MAX).probe_items(),
            Err(Error::Config(message)) if message.contains("too large")
        ));
    }

    #[test]
    fn extended_json_preserves_native_bson_types_bidirectionally() {
        let cases = [
            Bson::ObjectId(bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap()),
            Bson::Decimal128("1234567890.0123456789".parse().unwrap()),
            Bson::RegularExpression(bson::Regex {
                pattern: "^dbtool".into(),
                options: "im".into(),
            }),
            Bson::Timestamp(bson::Timestamp {
                time: 1_700_000_000,
                increment: 42,
            }),
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: vec![0; 16],
            }),
        ];

        for expected in cases {
            let core = bson_to_value(expected.clone());
            let Value::Json(extended) = core else {
                panic!("special BSON value should use canonical Extended JSON")
            };
            assert_eq!(json_to_bson(extended).unwrap(), expected);
        }
    }

    #[test]
    fn extended_json_object_id_can_be_used_in_filters() {
        let document = value_to_document(Value::Json(serde_json::json!({
            "_id": {"$oid": "507f1f77bcf86cd799439011"}
        })))
        .unwrap();

        assert!(
            matches!(document.get("_id"), Some(Bson::ObjectId(id)) if id.to_hex() == "507f1f77bcf86cd799439011")
        );
    }

    #[test]
    fn inserted_object_ids_are_returned_as_reusable_hex_strings() {
        let id = bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap();
        assert_eq!(
            inserted_id_string(Bson::ObjectId(id)),
            "507f1f77bcf86cd799439011"
        );
    }
}
