use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        BoundedList, Document, FindOptions, InputBudget, InsertOutcome, ReadBudget, UpdateOutcome,
        Value,
    },
    port::{
        capability::DocumentStore,
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::{InputLimiter, ListLimiter, ReadLimiter},
};
use futures::future::BoxFuture;
use mongodb::{
    bson::{self, Bson},
    results::{DeleteResult, InsertManyResult, UpdateResult},
    Client, Database,
};
use serde::Serialize;
use std::collections::BTreeMap;

const MONGO_BUDGETED_BATCH_SIZE: u32 = 1;
const MONGO_MAX_BSON_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
const MONGO_MAX_MESSAGE_BYTES: usize = 48 * 1024 * 1024;
const MONGO_MAX_WRITE_BATCH_ITEMS: usize = 100_000;
const MONGO_MAX_NAMESPACE_BYTES: usize = 255;
const MONGO_OP_MSG_HEADER_BYTES: usize = 16 + 4;
const MONGO_OP_MSG_BODY_SECTION_BYTES: usize = 1;
const MONGO_OP_MSG_SEQUENCE_SECTION_BYTES: usize = 1 + 4;
const MONGO_DRIVER_COMMAND_HEADROOM_BYTES: usize = 16 * 1024;

#[derive(Serialize)]
struct MongoInsertInput<'a> {
    collection: &'a str,
    documents: &'a [Document],
}

#[derive(Serialize)]
struct MongoUpdateInput<'a> {
    collection: &'a str,
    filter: &'a Value,
    update: &'a Value,
    many: bool,
}

#[derive(Serialize)]
struct MongoDeleteInput<'a> {
    collection: &'a str,
    filter: &'a Value,
    many: bool,
}

#[derive(Serialize)]
struct MongoDropInput<'a> {
    collection: &'a str,
}

#[derive(Serialize)]
struct MongoAggregateInput<'a> {
    collection: &'a str,
    pipeline: &'a [Value],
}

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

    async fn list_collections_budgeted(&self, budget: ReadBudget) -> Result<BoundedList<String>> {
        use futures::StreamExt;

        let (mut limiter, probe_items, batch_size) = mongo_budgeted_catalog_plan(budget)?;
        let mut cursor = self
            .db
            .list_collections()
            .batch_size(batch_size)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let mut names = Vec::with_capacity(budget.max_items.min(256));
        while limiter.observed_items() < probe_items {
            let Some(specification) = cursor.next().await else {
                break;
            };
            let name = specification.map_err(|e| Error::Query(e.to_string()))?.name;
            limiter.retain_item(name, &mut names)?;
        }
        drop(cursor);
        limiter.finish(names)
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
        self.insert_budgeted(collection, docs, InputBudget::default())
            .await
    }

    async fn insert_budgeted(
        &self,
        collection: &str,
        docs: Vec<Document>,
        budget: InputBudget,
    ) -> Result<InsertOutcome> {
        let bson_docs = prepare_mongo_insert(self.db.name(), collection, &docs, budget)?;
        let expected = bson_docs.len();
        let result = self
            .db
            .collection::<mongodb::bson::Document>(collection)
            .insert_many(bson_docs)
            .ordered(true)
            .await
            .map_err(|error| mongo_outcome_indeterminate("insert many", error))?;
        decode_insert_result(result, expected)
    }

    async fn update(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
    ) -> Result<UpdateOutcome> {
        self.update_many_budgeted(collection, filter, update, InputBudget::default())
            .await
    }

    async fn delete(&self, collection: &str, filter: Value) -> Result<u64> {
        self.delete_many_budgeted(collection, filter, InputBudget::default())
            .await
    }

    async fn update_one(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
    ) -> Result<UpdateOutcome> {
        self.update_one_budgeted(collection, filter, update, InputBudget::default())
            .await
    }

    async fn update_one_budgeted(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
        budget: InputBudget,
    ) -> Result<UpdateOutcome> {
        self.update_documents_budgeted(collection, filter, update, false, budget)
            .await
    }

    async fn update_many(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
    ) -> Result<UpdateOutcome> {
        self.update_many_budgeted(collection, filter, update, InputBudget::default())
            .await
    }

    async fn update_many_budgeted(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
        budget: InputBudget,
    ) -> Result<UpdateOutcome> {
        self.update_documents_budgeted(collection, filter, update, true, budget)
            .await
    }

    async fn delete_one(&self, collection: &str, filter: Value) -> Result<u64> {
        self.delete_one_budgeted(collection, filter, InputBudget::default())
            .await
    }

    async fn delete_one_budgeted(
        &self,
        collection: &str,
        filter: Value,
        budget: InputBudget,
    ) -> Result<u64> {
        self.delete_documents_budgeted(collection, filter, false, budget)
            .await
    }

    async fn delete_many(&self, collection: &str, filter: Value) -> Result<u64> {
        self.delete_many_budgeted(collection, filter, InputBudget::default())
            .await
    }

    async fn delete_many_budgeted(
        &self,
        collection: &str,
        filter: Value,
        budget: InputBudget,
    ) -> Result<u64> {
        self.delete_documents_budgeted(collection, filter, true, budget)
            .await
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
        let pipeline = prepare_mongo_read_aggregate(collection, pipeline)?;

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

    async fn aggregate_write_budgeted(
        &self,
        collection: &str,
        pipeline: Vec<Value>,
        input_budget: InputBudget,
        response_budget: ReadBudget,
    ) -> Result<BoundedList<Document>> {
        let limiter =
            MongoDocumentLimiter::new(response_budget, "MongoDB mutating aggregate response")?;
        let probe_items = limiter.probe_items()?;
        let pipeline =
            prepare_mongo_aggregate_write(self.db.name(), collection, &pipeline, input_budget)?;
        let cursor = self
            .db
            .collection::<mongodb::bson::Document>(collection)
            .aggregate(pipeline)
            .with_type::<mongodb::bson::Document>()
            .batch_size(MONGO_BUDGETED_BATCH_SIZE)
            .await
            .map_err(|error| mongo_outcome_indeterminate("mutating aggregate", error))?;

        collect_budgeted_cursor(cursor, limiter, probe_items)
            .await
            .map_err(|error| mongo_outcome_indeterminate("mutating aggregate response", error))
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        self.drop_collection_budgeted(collection, InputBudget::default())
            .await
    }

    async fn drop_collection_budgeted(&self, collection: &str, budget: InputBudget) -> Result<()> {
        prepare_mongo_drop(self.db.name(), collection, budget)?;
        self.db
            .collection::<mongodb::bson::Document>(collection)
            .drop()
            .await
            .map_err(|error| mongo_outcome_indeterminate("drop collection", error))
    }
}

impl MongoAdapter {
    async fn update_documents_budgeted(
        &self,
        collection: &str,
        filter: Value,
        update: Value,
        many: bool,
        budget: InputBudget,
    ) -> Result<UpdateOutcome> {
        let (filter, update) =
            prepare_mongo_update(self.db.name(), collection, &filter, &update, many, budget)?;
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let result = if many {
            col.update_many(filter, update).await
        } else {
            col.update_one(filter, update).await
        }
        .map_err(|error| mongo_outcome_indeterminate("update documents", error))?;
        decode_update_result(result, many)
    }

    async fn delete_documents_budgeted(
        &self,
        collection: &str,
        filter: Value,
        many: bool,
        budget: InputBudget,
    ) -> Result<u64> {
        let filter = prepare_mongo_delete(self.db.name(), collection, &filter, many, budget)?;
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let result = if many {
            col.delete_many(filter).await
        } else {
            col.delete_one(filter).await
        }
        .map_err(|error| mongo_outcome_indeterminate("delete documents", error))?;
        decode_delete_result(result, many)
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
        let pipeline = prepare_mongo_read_aggregate(collection, pipeline)?;
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
        CapabilityOperation::DocumentListCollectionsBudgeted,
        CapabilityOperation::DocumentFindBudgeted,
        CapabilityOperation::DocumentAggregateBudgeted,
        CapabilityOperation::DocumentAggregateWriteBudgeted,
        CapabilityOperation::DocumentInsertBudgeted,
        CapabilityOperation::DocumentUpdateOne,
        CapabilityOperation::DocumentUpdateOneBudgeted,
        CapabilityOperation::DocumentUpdateMany,
        CapabilityOperation::DocumentUpdateManyBudgeted,
        CapabilityOperation::DocumentDeleteOne,
        CapabilityOperation::DocumentDeleteOneBudgeted,
        CapabilityOperation::DocumentDeleteMany,
        CapabilityOperation::DocumentDeleteManyBudgeted,
        CapabilityOperation::DocumentDropCollection,
        CapabilityOperation::DocumentDropCollectionBudgeted,
    ]);
    operations
}

fn prepare_mongo_insert(
    database: &str,
    collection: &str,
    documents: &[Document],
    budget: InputBudget,
) -> Result<Vec<bson::Document>> {
    validate_mongo_collection_name(database, collection)?;
    let request = MongoInsertInput {
        collection,
        documents,
    };
    InputLimiter::new(budget, "MongoDB insert input")?
        .validate_items_with_request(documents, &request)?;
    if documents.len() > MONGO_MAX_WRITE_BATCH_ITEMS {
        return Err(Error::Config(format!(
            "MongoDB insert batch exceeds the fixed {MONGO_MAX_WRITE_BATCH_ITEMS}-item ceiling"
        )));
    }

    let mut bson_documents = Vec::with_capacity(documents.len());
    for document in documents.iter().cloned() {
        let mut document = core_document_to_bson(document)?;
        if !document.contains_key("_id") {
            document.insert("_id", bson::oid::ObjectId::new());
        }
        validate_mongo_bson_document(&document, "insert document")?;
        bson_documents.push(document);
    }

    let command = bson::doc! {
        "insert": collection,
        "ordered": true,
        "$db": database,
    };
    validate_mongo_wire_request(&command, Some(("documents", &bson_documents)))?;
    Ok(bson_documents)
}

fn prepare_mongo_update(
    database: &str,
    collection: &str,
    filter: &Value,
    update: &Value,
    many: bool,
    budget: InputBudget,
) -> Result<(bson::Document, bson::Document)> {
    validate_mongo_collection_name(database, collection)?;
    let request = MongoUpdateInput {
        collection,
        filter,
        update,
        many,
    };
    InputLimiter::new(budget, "MongoDB update input")?.validate_request(&request)?;

    let filter = value_to_document(filter.clone())?;
    ensure_nonempty_filter(&filter, if many { "update many" } else { "update one" })?;
    let update = update_document(update.clone())?;
    validate_mongo_bson_document(&filter, "update filter")?;
    validate_mongo_bson_document(&update, "update expression")?;
    let operation = bson::doc! {
        "q": filter.clone(),
        "u": update.clone(),
        "multi": many,
        "upsert": false,
    };
    validate_mongo_bson_document(&operation, "update operation")?;
    let command = bson::doc! {
        "update": collection,
        "ordered": true,
        "$db": database,
    };
    validate_mongo_wire_request(
        &command,
        Some(("updates", std::slice::from_ref(&operation))),
    )?;
    Ok((filter, update))
}

fn prepare_mongo_delete(
    database: &str,
    collection: &str,
    filter: &Value,
    many: bool,
    budget: InputBudget,
) -> Result<bson::Document> {
    validate_mongo_collection_name(database, collection)?;
    let request = MongoDeleteInput {
        collection,
        filter,
        many,
    };
    InputLimiter::new(budget, "MongoDB delete input")?.validate_request(&request)?;

    let filter = value_to_document(filter.clone())?;
    ensure_nonempty_filter(&filter, if many { "delete many" } else { "delete one" })?;
    validate_mongo_bson_document(&filter, "delete filter")?;
    let operation = bson::doc! {
        "q": filter.clone(),
        "limit": if many { 0 } else { 1 },
    };
    validate_mongo_bson_document(&operation, "delete operation")?;
    let command = bson::doc! {
        "delete": collection,
        "ordered": true,
        "$db": database,
    };
    validate_mongo_wire_request(
        &command,
        Some(("deletes", std::slice::from_ref(&operation))),
    )?;
    Ok(filter)
}

fn prepare_mongo_drop(database: &str, collection: &str, budget: InputBudget) -> Result<()> {
    validate_mongo_collection_name(database, collection)?;
    InputLimiter::new(budget, "MongoDB drop collection input")?
        .validate_request(&MongoDropInput { collection })?;
    let command = bson::doc! {
        "drop": collection,
        "$db": database,
    };
    validate_mongo_wire_request(&command, None)
}

fn prepare_mongo_read_aggregate(
    collection: &str,
    pipeline: Vec<Value>,
) -> Result<Vec<bson::Document>> {
    let pipeline = pipeline
        .into_iter()
        .map(value_to_document)
        .collect::<Result<Vec<_>>>()?;
    if pipeline.iter().any(mongo_aggregate_stage_writes) {
        return Err(Error::Config(format!(
            "MongoDB read-only aggregate on collection '{collection}' contains $out/$merge; use document.aggregate_write_budgeted"
        )));
    }
    Ok(pipeline)
}

fn prepare_mongo_aggregate_write(
    database: &str,
    collection: &str,
    pipeline: &[Value],
    budget: InputBudget,
) -> Result<Vec<bson::Document>> {
    validate_mongo_collection_name(database, collection)?;
    let request = MongoAggregateInput {
        collection,
        pipeline,
    };
    InputLimiter::new(budget, "MongoDB mutating aggregate input")?
        .validate_items_with_request(pipeline, &request)?;

    let pipeline = pipeline
        .iter()
        .cloned()
        .map(value_to_document)
        .collect::<Result<Vec<_>>>()?;
    let mut destination = None;
    for (index, stage) in pipeline.iter().enumerate() {
        if stage.len() != 1 || !stage.keys().all(|key| key.starts_with('$')) {
            return Err(Error::Config(format!(
                "MongoDB aggregate pipeline stage {} must contain exactly one operator",
                index + 1
            )));
        }
        validate_mongo_bson_document(stage, "aggregate pipeline stage")?;
        let Some((target_database, target_collection)) = mongo_aggregate_destination(stage)? else {
            continue;
        };
        let target_database = if target_database.is_empty() {
            database.to_owned()
        } else {
            target_database
        };
        if destination.is_some() {
            return Err(Error::Config(
                "MongoDB mutating aggregate must contain exactly one $out/$merge stage".into(),
            ));
        }
        if index + 1 != pipeline.len() {
            return Err(Error::Config(
                "MongoDB $out/$merge stage must be the final pipeline stage".into(),
            ));
        }
        validate_mongo_database_name(&target_database)?;
        validate_mongo_collection_name(&target_database, &target_collection)?;
        destination = Some((target_database, target_collection));
    }
    if destination.is_none() {
        return Err(Error::Config(
            "MongoDB mutating aggregate requires exactly one final $out/$merge stage".into(),
        ));
    }

    let command = bson::doc! {
        "aggregate": collection,
        "pipeline": pipeline.clone(),
        "cursor": bson::Document::new(),
        "$db": database,
    };
    validate_mongo_wire_request(&command, None)?;
    Ok(pipeline)
}

fn mongo_aggregate_stage_writes(stage: &bson::Document) -> bool {
    stage.contains_key("$out") || stage.contains_key("$merge")
}

fn mongo_aggregate_destination(stage: &bson::Document) -> Result<Option<(String, String)>> {
    match (stage.get("$out"), stage.get("$merge")) {
        (Some(_), Some(_)) => Err(Error::Config(
            "MongoDB aggregate stage must not contain both $out and $merge".into(),
        )),
        (Some(specification), None) => {
            parse_mongo_aggregate_namespace(specification, "$out").map(Some)
        }
        (None, Some(Bson::String(collection))) => Ok(Some((String::new(), collection.clone()))),
        (None, Some(Bson::Document(options))) => {
            let target = options.get("into").ok_or_else(|| {
                Error::Config("MongoDB $merge requires an 'into' destination".into())
            })?;
            parse_mongo_aggregate_namespace(target, "$merge.into").map(Some)
        }
        (None, Some(_)) => Err(Error::Config(
            "MongoDB $merge destination must be a collection string or object".into(),
        )),
        (None, None) => Ok(None),
    }
}

fn parse_mongo_aggregate_namespace(value: &Bson, label: &str) -> Result<(String, String)> {
    match value {
        Bson::String(collection) => Ok((String::new(), collection.clone())),
        Bson::Document(options) => {
            let collection = options.get_str("coll").map_err(|_| {
                Error::Config(format!("MongoDB {label} object requires string 'coll'"))
            })?;
            let database = match options.get("db") {
                None => String::new(),
                Some(Bson::String(database)) => database.clone(),
                Some(_) => {
                    return Err(Error::Config(format!(
                        "MongoDB {label} object 'db' must be a string"
                    )))
                }
            };
            Ok((database, collection.to_owned()))
        }
        _ => Err(Error::Config(format!(
            "MongoDB {label} destination must be a collection string or object"
        ))),
    }
}

fn validate_mongo_database_name(database: &str) -> Result<()> {
    if database.is_empty() {
        return Ok(());
    }
    if database.as_bytes().contains(&0)
        || database
            .chars()
            .any(|character| matches!(character, '/' | '\\' | '.' | ' ' | '"' | '$'))
    {
        return Err(Error::Config(
            "MongoDB aggregate destination database contains a forbidden character".into(),
        ));
    }
    Ok(())
}

fn validate_mongo_collection_name(database: &str, collection: &str) -> Result<()> {
    if collection.is_empty() {
        return Err(Error::Config(
            "MongoDB collection name must not be empty".to_owned(),
        ));
    }
    if collection.as_bytes().contains(&0) {
        return Err(Error::Config(
            "MongoDB collection name must not contain NUL".to_owned(),
        ));
    }
    if collection.contains('$') {
        return Err(Error::Config(
            "MongoDB collection name must not contain '$'".to_owned(),
        ));
    }
    if collection.starts_with("system.") {
        return Err(Error::Config(
            "MongoDB system.* collections are reserved for server use".to_owned(),
        ));
    }
    let namespace_bytes = database
        .len()
        .checked_add(1)
        .and_then(|bytes| bytes.checked_add(collection.len()))
        .ok_or_else(|| Error::Config("MongoDB namespace length overflow".to_owned()))?;
    if namespace_bytes > MONGO_MAX_NAMESPACE_BYTES {
        return Err(Error::Config(format!(
            "MongoDB namespace exceeds the fixed {MONGO_MAX_NAMESPACE_BYTES}-byte ceiling"
        )));
    }
    Ok(())
}

fn validate_mongo_bson_document(document: &bson::Document, subject: &str) -> Result<usize> {
    let bytes = bson::to_vec(document)
        .map_err(|error| Error::Serialization(format!("failed to encode {subject}: {error}")))?
        .len();
    validate_mongo_fixed_bytes(subject, bytes, MONGO_MAX_BSON_DOCUMENT_BYTES, "BSON")?;
    Ok(bytes)
}

fn validate_mongo_fixed_bytes(subject: &str, bytes: usize, limit: usize, unit: &str) -> Result<()> {
    if bytes > limit {
        return Err(Error::Config(format!(
            "MongoDB {subject} exceeds the fixed {limit}-byte {unit} ceiling"
        )));
    }
    Ok(())
}

fn validate_mongo_wire_request(
    command: &bson::Document,
    sequence: Option<(&str, &[bson::Document])>,
) -> Result<()> {
    let command_bytes = validate_mongo_bson_document(command, "command body")?;
    let mut bytes = MONGO_OP_MSG_HEADER_BYTES
        .checked_add(MONGO_OP_MSG_BODY_SECTION_BYTES)
        .and_then(|bytes| bytes.checked_add(command_bytes))
        .and_then(|bytes| bytes.checked_add(MONGO_DRIVER_COMMAND_HEADROOM_BYTES))
        .ok_or_else(|| Error::Config("MongoDB command size overflow".to_owned()))?;

    if let Some((identifier, documents)) = sequence {
        bytes = bytes
            .checked_add(MONGO_OP_MSG_SEQUENCE_SECTION_BYTES)
            .and_then(|bytes| bytes.checked_add(identifier.len()))
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(|| Error::Config("MongoDB command sequence size overflow".to_owned()))?;
        for document in documents {
            let document_bytes = validate_mongo_bson_document(document, "write operation")?;
            bytes = bytes.checked_add(document_bytes).ok_or_else(|| {
                Error::Config("MongoDB command sequence size overflow".to_owned())
            })?;
        }
    }

    validate_mongo_fixed_bytes("write command", bytes, MONGO_MAX_MESSAGE_BYTES, "message")
}

fn mongo_outcome_indeterminate(operation: &str, error: impl std::fmt::Display) -> Error {
    Error::OutcomeIndeterminate(format!(
        "MongoDB {operation} may have reached the backend; inspect collection state before retrying ({error})"
    ))
}

fn decode_insert_result(result: InsertManyResult, expected: usize) -> Result<InsertOutcome> {
    if result.inserted_ids.len() != expected {
        return Err(mongo_outcome_indeterminate(
            "insert result",
            format!(
                "driver returned {} inserted ids for {expected} documents",
                result.inserted_ids.len()
            ),
        ));
    }
    let mut inserted_ids = result.inserted_ids;
    let mut ids = Vec::with_capacity(expected);
    for index in 0..expected {
        let id = inserted_ids.remove(&index).ok_or_else(|| {
            mongo_outcome_indeterminate(
                "insert result",
                format!("driver omitted inserted id at input index {index}"),
            )
        })?;
        ids.push(inserted_id_string(id));
    }
    if !inserted_ids.is_empty() {
        return Err(mongo_outcome_indeterminate(
            "insert result",
            "driver returned inserted ids outside the input index range",
        ));
    }
    Ok(InsertOutcome {
        inserted: expected as u64,
        ids,
    })
}

fn decode_update_result(result: UpdateResult, many: bool) -> Result<UpdateOutcome> {
    if result.modified_count > result.matched_count || (!many && result.matched_count > 1) {
        return Err(mongo_outcome_indeterminate(
            "update result",
            format!(
                "invalid matched/modified counts {}/{} for {}",
                result.matched_count,
                result.modified_count,
                if many { "update many" } else { "update one" }
            ),
        ));
    }
    if result.upserted_id.is_some() {
        return Err(mongo_outcome_indeterminate(
            "update result",
            "driver reported an upsert id even though upsert was disabled",
        ));
    }
    Ok(UpdateOutcome {
        matched: result.matched_count,
        modified: result.modified_count,
    })
}

fn decode_delete_result(result: DeleteResult, many: bool) -> Result<u64> {
    if !many && result.deleted_count > 1 {
        return Err(mongo_outcome_indeterminate(
            "delete result",
            format!(
                "driver reported {} deletions for delete one",
                result.deleted_count
            ),
        ));
    }
    Ok(result.deleted_count)
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

fn mongo_budgeted_catalog_plan(budget: ReadBudget) -> Result<(ReadLimiter, usize, u32)> {
    let limiter = ReadLimiter::new(budget, "MongoDB collection catalog response")?;
    let probe_items = limiter.probe_items()?;
    let batch_size = u32::try_from(probe_items).map_err(|_| {
        Error::Config("MongoDB collection catalog item budget exceeds the u32 range".into())
    })?;
    Ok((limiter, probe_items, batch_size))
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
            CapabilityOperation::DocumentListCollectionsBudgeted,
            CapabilityOperation::DocumentFindBudgeted,
            CapabilityOperation::DocumentAggregateBudgeted,
            CapabilityOperation::DocumentAggregateWriteBudgeted,
            CapabilityOperation::DocumentInsertBudgeted,
            CapabilityOperation::DocumentUpdateOne,
            CapabilityOperation::DocumentUpdateOneBudgeted,
            CapabilityOperation::DocumentUpdateMany,
            CapabilityOperation::DocumentUpdateManyBudgeted,
            CapabilityOperation::DocumentDeleteOne,
            CapabilityOperation::DocumentDeleteOneBudgeted,
            CapabilityOperation::DocumentDeleteMany,
            CapabilityOperation::DocumentDeleteManyBudgeted,
            CapabilityOperation::DocumentDropCollection,
            CapabilityOperation::DocumentDropCollectionBudgeted,
        ] {
            assert!(operations.contains(&operation));
        }
        assert!(operations.contains(&CapabilityOperation::DocumentUpdate));
        assert!(operations.contains(&CapabilityOperation::DocumentDelete));
    }

    fn input_document(id: i64, state: &str) -> Document {
        Document::from([
            ("_id".to_owned(), Value::Int(id)),
            ("kind".to_owned(), Value::Text("input-budget".to_owned())),
            ("state".to_owned(), Value::Text(state.to_owned())),
        ])
    }

    async fn complete_collection_catalog(store: &dyn DocumentStore) -> Result<Vec<String>> {
        let result = store
            .list_collections_budgeted(ReadBudget::with_default_bytes(10_000)?)
            .await?;
        if result.truncated {
            return Err(Error::Internal(
                "MongoDB test collection catalog exceeded its exact read envelope".into(),
            ));
        }
        Ok(result.items)
    }

    async fn complete_find(
        store: &dyn DocumentStore,
        collection: &str,
        filter: Value,
    ) -> Result<Vec<Document>> {
        let result = store
            .find_budgeted(
                collection,
                filter,
                FindOptions::default(),
                ReadBudget::with_default_bytes(10_000)?,
            )
            .await?;
        if result.truncated {
            return Err(Error::Internal(format!(
                "MongoDB test read for {collection} exceeded its exact read envelope"
            )));
        }
        Ok(result.items)
    }

    #[test]
    fn mongo_mutation_preflight_counts_complete_targets_items_and_requests() {
        let collection = "input_budget_docs";
        let documents = vec![input_document(1, "created"), input_document(2, "created")];
        let item_bytes = documents
            .iter()
            .map(|document| serde_json::to_vec(document).unwrap().len())
            .max()
            .unwrap();
        let request_bytes = serde_json::to_vec(&MongoInsertInput {
            collection,
            documents: &documents,
        })
        .unwrap()
        .len();
        assert!(prepare_mongo_insert(
            "dbtool",
            collection,
            &documents,
            InputBudget::new(documents.len(), item_bytes, request_bytes).unwrap(),
        )
        .is_ok());
        assert!(matches!(
            prepare_mongo_insert(
                "dbtool",
                collection,
                &documents,
                InputBudget::new(1, item_bytes, request_bytes).unwrap(),
            ),
            Err(Error::InputBudgetExceeded {
                unit: "items",
                limit: 1,
                ..
            })
        ));
        assert!(matches!(
            prepare_mongo_insert(
                "dbtool",
                collection,
                &documents,
                InputBudget::new(documents.len(), item_bytes, request_bytes - 1).unwrap(),
            ),
            Err(Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == request_bytes - 1
        ));
        assert!(matches!(
            prepare_mongo_insert("dbtool", collection, &[], InputBudget::default()),
            Err(Error::Config(message)) if message.contains("at least one")
        ));

        let filter = Value::Json(serde_json::json!({"_id": 1}));
        let update = Value::Json(serde_json::json!({"state": "updated"}));
        let update_request = MongoUpdateInput {
            collection,
            filter: &filter,
            update: &update,
            many: false,
        };
        let update_bytes = serde_json::to_vec(&update_request).unwrap().len();
        assert!(prepare_mongo_update(
            "dbtool",
            collection,
            &filter,
            &update,
            false,
            InputBudget::new(1, update_bytes, update_bytes).unwrap(),
        )
        .is_ok());
        assert!(matches!(
            prepare_mongo_update(
                "dbtool",
                collection,
                &filter,
                &update,
                false,
                InputBudget::new(1, update_bytes, update_bytes - 1).unwrap(),
            ),
            Err(Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == update_bytes - 1
        ));

        let delete_request = MongoDeleteInput {
            collection,
            filter: &filter,
            many: true,
        };
        let delete_bytes = serde_json::to_vec(&delete_request).unwrap().len();
        assert!(prepare_mongo_delete(
            "dbtool",
            collection,
            &filter,
            true,
            InputBudget::new(1, delete_bytes, delete_bytes).unwrap(),
        )
        .is_ok());
        assert!(matches!(
            prepare_mongo_delete(
                "dbtool",
                collection,
                &filter,
                true,
                InputBudget::new(1, delete_bytes - 1, delete_bytes).unwrap(),
            ),
            Err(Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == delete_bytes - 1
        ));

        let drop_bytes = serde_json::to_vec(&MongoDropInput { collection })
            .unwrap()
            .len();
        assert!(prepare_mongo_drop(
            "dbtool",
            collection,
            InputBudget::new(1, drop_bytes, drop_bytes).unwrap(),
        )
        .is_ok());
        assert!(matches!(
            prepare_mongo_drop(
                "dbtool",
                collection,
                InputBudget::new(1, drop_bytes, drop_bytes - 1).unwrap(),
            ),
            Err(Error::InputBudgetExceeded { .. })
        ));
    }

    #[test]
    fn mutating_aggregate_has_an_exact_input_contract_and_read_paths_reject_it() {
        let collection = "events";
        let pipeline = vec![
            Value::Json(serde_json::json!({"$match": {"active": true}})),
            Value::Json(serde_json::json!({"$out": "events_archive"})),
        ];
        let item_bytes = pipeline
            .iter()
            .map(|stage| serde_json::to_vec(stage).unwrap().len())
            .max()
            .unwrap();
        let request_bytes = serde_json::to_vec(&MongoAggregateInput {
            collection,
            pipeline: &pipeline,
        })
        .unwrap()
        .len();
        assert!(prepare_mongo_aggregate_write(
            "dbtool",
            collection,
            &pipeline,
            InputBudget::new(pipeline.len(), item_bytes, request_bytes).unwrap(),
        )
        .is_ok());
        let merge_pipeline = vec![Value::Json(serde_json::json!({
            "$merge": {
                "into": {"db": "archive_db", "coll": "events_archive"},
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }))];
        assert!(prepare_mongo_aggregate_write(
            "dbtool",
            collection,
            &merge_pipeline,
            InputBudget::default(),
        )
        .is_ok());
        assert!(matches!(
            prepare_mongo_aggregate_write(
                "dbtool",
                collection,
                &pipeline,
                InputBudget::new(pipeline.len(), item_bytes, request_bytes - 1).unwrap(),
            ),
            Err(Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == request_bytes - 1
        ));
        assert!(matches!(
            prepare_mongo_read_aggregate(collection, pipeline.clone()),
            Err(Error::Config(message)) if message.contains("aggregate_write_budgeted")
        ));
        assert!(matches!(
            prepare_mongo_aggregate_write(
                "dbtool",
                collection,
                &[Value::Json(serde_json::json!({"$match": {}}))],
                InputBudget::default(),
            ),
            Err(Error::Config(message)) if message.contains("requires exactly one")
        ));
        assert!(matches!(
            prepare_mongo_aggregate_write(
                "dbtool",
                collection,
                &[
                    Value::Json(serde_json::json!({"$out": "archive"})),
                    Value::Json(serde_json::json!({"$match": {}})),
                ],
                InputBudget::default(),
            ),
            Err(Error::Config(message)) if message.contains("final pipeline stage")
        ));
        assert!(matches!(
            prepare_mongo_aggregate_write(
                "dbtool",
                collection,
                &[Value::Json(serde_json::json!({
                    "$out": "archive",
                    "$merge": "other"
                }))],
                InputBudget::default(),
            ),
            Err(Error::Config(message)) if message.contains("exactly one operator")
        ));
    }

    #[test]
    fn mongo_native_write_limits_and_post_dispatch_errors_are_fail_closed() {
        for collection in ["", "bad\0name", "bad$name", "system.users"] {
            assert!(validate_mongo_collection_name("dbtool", collection).is_err());
        }
        assert!(validate_mongo_collection_name("dbtool", "valid_name").is_ok());
        assert!(validate_mongo_collection_name("d", &"x".repeat(253)).is_ok());
        assert!(validate_mongo_collection_name("d", &"x".repeat(254)).is_err());
        assert!(validate_mongo_fixed_bytes("fixture", 8, 8, "BSON").is_ok());
        assert!(matches!(
            validate_mongo_fixed_bytes("fixture", 9, 8, "BSON"),
            Err(Error::Config(message)) if message.contains("fixed 8-byte")
        ));

        let error = mongo_outcome_indeterminate("insert many", "socket closed");
        assert_eq!(error.code(), "OUTCOME_INDETERMINATE");
        assert!(!error.is_retryable());
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
        assert!(matches!(
            mongo_budgeted_catalog_plan(ReadBudget {
                max_items: 0,
                max_bytes: 1,
            }),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            mongo_budgeted_catalog_plan(ReadBudget {
                max_items: usize::MAX,
                max_bytes: 1,
            }),
            Err(Error::Config(_))
        ));
        let (_, probe_items, batch_size) =
            mongo_budgeted_catalog_plan(ReadBudget::new(2, 1024).unwrap()).unwrap();
        assert_eq!((probe_items, batch_size), (3, 3));
    }

    #[test]
    fn collection_catalog_budget_counts_items_probe_and_complete_envelope() {
        let visible = BoundedList {
            items: vec!["alpha".to_owned(), "beta".to_owned()],
            truncated: true,
        };
        let probe = "gamma".to_owned();
        let exact_bytes =
            serde_json::to_vec(&visible).unwrap().len() + serde_json::to_vec(&probe).unwrap().len();
        let finish = |max_bytes: usize| -> Result<BoundedList<String>> {
            let (mut limiter, probe_items, batch_size) =
                mongo_budgeted_catalog_plan(ReadBudget::new(2, max_bytes)?)?;
            assert_eq!((probe_items, batch_size), (3, 3));
            let mut retained = Vec::new();
            for item in ["alpha", "beta", "gamma"] {
                limiter.retain_item(item.to_owned(), &mut retained)?;
            }
            limiter.finish(retained)
        };

        assert_eq!(finish(exact_bytes).unwrap(), visible);
        assert!(matches!(
            finish(exact_bytes - 1),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == exact_bytes - 1
        ));

        let complete = BoundedList::complete(vec!["alpha".to_owned(), "beta".to_owned()]);
        let complete_bytes = serde_json::to_vec(&complete).unwrap().len();
        let (mut limiter, _, _) =
            mongo_budgeted_catalog_plan(ReadBudget::new(2, complete_bytes).unwrap()).unwrap();
        let mut retained = Vec::new();
        for item in ["alpha", "beta"] {
            limiter.retain_item(item.to_owned(), &mut retained).unwrap();
        }
        assert_eq!(limiter.finish(retained).unwrap(), complete);
    }

    #[tokio::test]
    async fn mongo_live_budgeted_collection_catalog_is_exact_and_cleans_up() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_MONGO_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let collections = [
            format!("dbt_budget_{suffix}_alpha"),
            format!("dbt_budget_{suffix}_beta"),
            format!("dbt_budget_{suffix}_gamma"),
        ];
        let connector = factory(Dsn::parse(&raw_dsn).unwrap()).await.unwrap();
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::DocumentListCollectionsBudgeted));
        let store = connector.as_document().unwrap();

        let exercise = async {
            for (index, collection) in collections.iter().enumerate() {
                let mut document = Document::new();
                document.insert("_id".to_owned(), Value::Int(index as i64));
                store
                    .insert_budgeted(collection, vec![document], InputBudget::default())
                    .await?;
            }

            let all = complete_collection_catalog(store).await?;
            let total = all.len();
            let expected = BoundedList::complete(all);
            let complete_bytes = serde_json::to_vec(&expected)
                .map_err(|error| Error::Serialization(error.to_string()))?
                .len();
            let exact = store
                .list_collections_budgeted(ReadBudget::new(total, complete_bytes)?)
                .await?;
            let short = store
                .list_collections_budgeted(ReadBudget::new(total, complete_bytes - 1)?)
                .await;
            let probed = store
                .list_collections_budgeted(ReadBudget::new(
                    total - 1,
                    dbtool_core::model::DEFAULT_READ_BYTES,
                )?)
                .await?;
            Ok::<_, Error>((exact, short, probed, total, complete_bytes))
        }
        .await;

        for collection in &collections {
            let _ = store
                .drop_collection_budgeted(collection, InputBudget::default())
                .await;
        }
        let remaining = complete_collection_catalog(store).await.unwrap();
        assert!(collections
            .iter()
            .all(|collection| !remaining.contains(collection)));

        let (exact, short, probed, total, complete_bytes) = exercise.unwrap();
        assert_eq!(exact.items.len(), total);
        assert!(!exact.truncated);
        assert!(matches!(
            short,
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == complete_bytes - 1
        ));
        assert_eq!(probed.items.len(), total - 1);
        assert!(probed.truncated);
    }

    #[tokio::test]
    async fn mongo_live_budgeted_mutations_reject_before_write_and_clean_collection() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_MONGO_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let collection = format!("dbtool_it_input_{suffix}");
        let aggregate_target = format!("{collection}_archive");
        let connector = factory(Dsn::parse(&raw_dsn).unwrap()).await.unwrap();
        for operation in [
            CapabilityOperation::DocumentInsertBudgeted,
            CapabilityOperation::DocumentUpdateOneBudgeted,
            CapabilityOperation::DocumentUpdateManyBudgeted,
            CapabilityOperation::DocumentDeleteOneBudgeted,
            CapabilityOperation::DocumentDeleteManyBudgeted,
            CapabilityOperation::DocumentAggregateWriteBudgeted,
            CapabilityOperation::DocumentDropCollectionBudgeted,
        ] {
            assert!(connector.operations().contains(&operation));
        }
        let store = connector.as_document().unwrap();

        let documents = vec![
            input_document(1, "created"),
            input_document(2, "created"),
            input_document(3, "created"),
        ];
        let item_bytes = documents
            .iter()
            .map(|document| serde_json::to_vec(document).unwrap().len())
            .max()
            .unwrap();
        let insert_bytes = serde_json::to_vec(&MongoInsertInput {
            collection: &collection,
            documents: &documents,
        })
        .unwrap()
        .len();

        let exercise = async {
            let error = store
                .insert_budgeted(
                    &collection,
                    documents.clone(),
                    InputBudget::new(documents.len(), item_bytes, insert_bytes - 1)?,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), "INPUT_BUDGET_EXCEEDED");
            assert!(!complete_collection_catalog(store)
                .await?
                .contains(&collection));

            let inserted = store
                .insert_budgeted(
                    &collection,
                    documents.clone(),
                    InputBudget::new(documents.len(), item_bytes, insert_bytes)?,
                )
                .await?;
            assert_eq!(inserted.inserted, 3);
            assert_eq!(inserted.ids.len(), 3);

            let aggregate_pipeline = vec![
                Value::Json(serde_json::json!({"$match": {"kind": "input-budget"}})),
                Value::Json(serde_json::json!({"$out": &aggregate_target})),
            ];
            let aggregate_item_bytes = aggregate_pipeline
                .iter()
                .map(|stage| serde_json::to_vec(stage).unwrap().len())
                .max()
                .unwrap();
            let aggregate_request_bytes = serde_json::to_vec(&MongoAggregateInput {
                collection: &collection,
                pipeline: &aggregate_pipeline,
            })
            .unwrap()
            .len();
            let error = store
                .aggregate_write_budgeted(
                    &collection,
                    aggregate_pipeline.clone(),
                    InputBudget::new(
                        aggregate_pipeline.len(),
                        aggregate_item_bytes,
                        aggregate_request_bytes - 1,
                    )?,
                    ReadBudget::with_default_bytes(1)?,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), "INPUT_BUDGET_EXCEEDED");
            assert!(!complete_collection_catalog(store)
                .await?
                .contains(&aggregate_target));

            let error = store
                .aggregate_budgeted(
                    &collection,
                    aggregate_pipeline.clone(),
                    ReadBudget::with_default_bytes(1)?,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), "CONFIG_ERROR");
            assert!(!complete_collection_catalog(store)
                .await?
                .contains(&aggregate_target));

            let aggregate_response = store
                .aggregate_write_budgeted(
                    &collection,
                    aggregate_pipeline,
                    InputBudget::new(2, aggregate_item_bytes, aggregate_request_bytes)?,
                    ReadBudget::with_default_bytes(1)?,
                )
                .await?;
            assert!(aggregate_response.items.is_empty());
            assert!(!aggregate_response.truncated);
            assert_eq!(
                complete_find(store, &aggregate_target, Value::Null)
                    .await?
                    .len(),
                3
            );

            let merge_pipeline = vec![
                Value::Json(serde_json::json!({"$match": {"kind": "input-budget"}})),
                Value::Json(serde_json::json!({
                    "$merge": {
                        "into": &aggregate_target,
                        "whenMatched": "replace",
                        "whenNotMatched": "insert"
                    }
                })),
            ];
            let merge_item_bytes = merge_pipeline
                .iter()
                .map(|stage| serde_json::to_vec(stage).unwrap().len())
                .max()
                .unwrap();
            let merge_request_bytes = serde_json::to_vec(&MongoAggregateInput {
                collection: &collection,
                pipeline: &merge_pipeline,
            })
            .unwrap()
            .len();
            let merge_response = store
                .aggregate_write_budgeted(
                    &collection,
                    merge_pipeline,
                    InputBudget::new(2, merge_item_bytes, merge_request_bytes)?,
                    ReadBudget::with_default_bytes(1)?,
                )
                .await?;
            assert!(merge_response.items.is_empty());
            assert!(!merge_response.truncated);
            assert_eq!(
                complete_find(store, &aggregate_target, Value::Null)
                    .await?
                    .len(),
                3
            );

            let aggregate_drop_bytes = serde_json::to_vec(&MongoDropInput {
                collection: &aggregate_target,
            })
            .unwrap()
            .len();
            store
                .drop_collection_budgeted(
                    &aggregate_target,
                    InputBudget::new(1, aggregate_drop_bytes, aggregate_drop_bytes)?,
                )
                .await?;
            assert!(!complete_collection_catalog(store)
                .await?
                .contains(&aggregate_target));

            let one_filter = Value::Json(serde_json::json!({"_id": 1}));
            let one_update = Value::Json(serde_json::json!({"state": "updated-one"}));
            let one_update_bytes = serde_json::to_vec(&MongoUpdateInput {
                collection: &collection,
                filter: &one_filter,
                update: &one_update,
                many: false,
            })
            .unwrap()
            .len();
            let error = store
                .update_one_budgeted(
                    &collection,
                    one_filter.clone(),
                    one_update.clone(),
                    InputBudget::new(1, one_update_bytes, one_update_bytes - 1)?,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), "INPUT_BUDGET_EXCEEDED");
            let unchanged = complete_find(store, &collection, one_filter.clone()).await?;
            assert_eq!(unchanged[0]["state"], Value::Text("created".into()));

            let updated = store
                .update_one_budgeted(
                    &collection,
                    one_filter.clone(),
                    one_update,
                    InputBudget::default(),
                )
                .await?;
            assert_eq!((updated.matched, updated.modified), (1, 1));

            let group_filter = Value::Json(serde_json::json!({"kind": "input-budget"}));
            let many_update = Value::Json(serde_json::json!({"state": "updated-many"}));
            let updated = store
                .update_many_budgeted(
                    &collection,
                    group_filter.clone(),
                    many_update,
                    InputBudget::default(),
                )
                .await?;
            assert_eq!(updated.matched, 3);
            assert_eq!(updated.modified, 3);

            let delete_filter = Value::Json(serde_json::json!({"_id": 1}));
            let delete_bytes = serde_json::to_vec(&MongoDeleteInput {
                collection: &collection,
                filter: &delete_filter,
                many: false,
            })
            .unwrap()
            .len();
            let error = store
                .delete_one_budgeted(
                    &collection,
                    delete_filter.clone(),
                    InputBudget::new(1, delete_bytes, delete_bytes - 1)?,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), "INPUT_BUDGET_EXCEEDED");
            assert_eq!(
                complete_find(store, &collection, delete_filter.clone())
                    .await?
                    .len(),
                1
            );
            assert_eq!(
                store
                    .delete_one_budgeted(&collection, delete_filter, InputBudget::default(),)
                    .await?,
                1
            );
            assert_eq!(
                store
                    .delete_many_budgeted(&collection, group_filter, InputBudget::default(),)
                    .await?,
                2
            );
            assert!(complete_find(store, &collection, Value::Null)
                .await?
                .is_empty());

            let drop_bytes = serde_json::to_vec(&MongoDropInput {
                collection: &collection,
            })
            .unwrap()
            .len();
            let error = store
                .drop_collection_budgeted(
                    &collection,
                    InputBudget::new(1, drop_bytes, drop_bytes - 1)?,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), "INPUT_BUDGET_EXCEEDED");
            assert!(complete_collection_catalog(store)
                .await?
                .contains(&collection));
            store
                .drop_collection_budgeted(&collection, InputBudget::new(1, drop_bytes, drop_bytes)?)
                .await?;
            assert!(!complete_collection_catalog(store)
                .await?
                .contains(&collection));
            Ok::<(), Error>(())
        }
        .await;

        let remaining = complete_collection_catalog(store).await.unwrap();
        for candidate in [&collection, &aggregate_target] {
            if remaining.contains(candidate) {
                store
                    .drop_collection_budgeted(candidate, InputBudget::default())
                    .await
                    .unwrap();
            }
        }
        let remaining = complete_collection_catalog(store).await.unwrap();
        assert!(!remaining.contains(&collection));
        assert!(!remaining.contains(&aggregate_target));
        exercise.unwrap();
        connector.close().await.unwrap();
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
