use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{Document, FindOptions, InsertOutcome, UpdateOutcome, Value},
    port::{
        capability::DocumentStore,
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use mongodb::{Client, Database};

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

    async fn find(
        &self,
        collection: &str,
        _filter: Value,
        opts: FindOptions,
    ) -> Result<Vec<Document>> {
        use futures::StreamExt;
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let find_opts = mongodb::options::FindOptions::builder()
            .limit(opts.limit.map(|n| n as i64))
            .skip(opts.skip.map(|n| n as u64))
            .build();
        let mut cursor = col
            .find(mongodb::bson::doc! {})
            .with_options(find_opts)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let mut docs = Vec::new();
        while let Some(doc) = cursor.next().await {
            let bson_doc = doc.map_err(|e| Error::Query(e.to_string()))?;
            let json_val: serde_json::Value = mongodb::bson::from_document(bson_doc)
                .map_err(|e| Error::Serialization(e.to_string()))?;
            if let serde_json::Value::Object(map) = json_val {
                docs.push(map.into_iter().map(|(k, v)| (k, Value::Json(v))).collect());
            }
        }
        Ok(docs)
    }

    async fn insert(&self, collection: &str, docs: Vec<Document>) -> Result<InsertOutcome> {
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let bson_docs: Vec<_> = docs
            .into_iter()
            .map(|d| {
                let json = serde_json::Value::Object(
                    d.into_iter()
                        .map(|(k, v)| (k, serde_json::to_value(v).unwrap()))
                        .collect(),
                );
                mongodb::bson::to_document(&json).unwrap()
            })
            .collect();
        let count = bson_docs.len() as u64;
        col.insert_many(bson_docs)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(InsertOutcome {
            inserted: count,
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
            matched: 0,
            modified: 0,
        })
    }

    async fn delete(&self, collection: &str, _filter: Value) -> Result<u64> {
        let col = self.db.collection::<mongodb::bson::Document>(collection);
        let r = col
            .delete_many(mongodb::bson::doc! {})
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(r.deleted_count)
    }

    async fn aggregate(&self, _collection: &str, _pipeline: Vec<Value>) -> Result<Vec<Document>> {
        Ok(vec![])
    }
}
