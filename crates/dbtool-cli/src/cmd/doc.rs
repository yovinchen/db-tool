use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{Document, FindOptions, Value},
    Result,
};

#[derive(Args)]
pub struct DocCmd {
    #[command(subcommand)]
    pub action: DocAction,
}

#[derive(Subcommand)]
pub enum DocAction {
    /// List document collections.
    Collections,
    /// Find documents with a JSON filter.
    Find {
        /// Collection name.
        collection: String,
        /// JSON filter object.
        #[arg(long, default_value = "{}")]
        filter: String,
    },
    /// Insert one JSON document.
    Insert {
        /// Collection name.
        collection: String,
        /// JSON document object.
        doc: String,
    },
    /// Update documents matching a JSON filter.
    Update {
        /// Collection name.
        collection: String,
        /// JSON filter object.
        #[arg(long)]
        filter: String,
        /// JSON update document; plain objects are wrapped in `$set` by MongoDB adapter.
        #[arg(long)]
        update: String,
    },
    /// Delete documents matching a non-empty JSON filter.
    Delete {
        /// Collection name.
        collection: String,
        /// JSON filter object.
        #[arg(long)]
        filter: String,
    },
    /// Run a JSON aggregation pipeline.
    Aggregate {
        /// Collection name.
        collection: String,
        /// JSON array pipeline.
        pipeline: String,
    },
}

pub async fn run(ctx: &Context, cmd: DocCmd) -> Result<String> {
    match &cmd.action {
        DocAction::Insert { .. } | DocAction::Update { .. } | DocAction::Delete { .. } => {
            ensure_write_allowed(ctx)?;
        }
        DocAction::Aggregate { pipeline, .. } => {
            let pipeline = parse_pipeline(pipeline)?;
            if pipeline_has_write_stage(&pipeline) {
                ensure_write_allowed(ctx)?;
            }
        }
        DocAction::Collections | DocAction::Find { .. } => {}
    }

    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let doc = conn
        .as_document()
        .ok_or_else(|| Error::UnsupportedCapability {
            kind: conn.kind().0.clone(),
            needed: "DocumentStore",
        })?;
    let start = std::time::Instant::now();
    let elapsed = || start.elapsed().as_millis() as u64;
    let kind = conn.kind().0.clone();

    Ok(match cmd.action {
        DocAction::Collections => {
            ctx.render_success(&kind, doc.list_collections().await?, elapsed(), false)
        }
        DocAction::Find { collection, filter } => {
            let f: serde_json::Value =
                serde_json::from_str(&filter).map_err(|e| Error::Serialization(e.to_string()))?;
            let opts = FindOptions {
                limit: Some(ctx.limit),
                ..Default::default()
            };
            let docs = doc.find(&collection, f.into(), opts).await?;
            let truncated = docs.len() >= ctx.limit;
            ctx.render_success(&kind, docs, elapsed(), truncated)
        }
        DocAction::Insert {
            collection,
            doc: raw_doc,
        } => {
            let d = parse_document(&raw_doc)?;
            let outcome = doc.insert(&collection, vec![d]).await?;
            ctx.render_success(&kind, outcome, elapsed(), false)
        }
        DocAction::Update {
            collection,
            filter,
            update,
        } => {
            let filter = parse_json_value(&filter)?;
            let update = parse_json_value(&update)?;
            let outcome = doc.update(&collection, filter, update).await?;
            ctx.render_success(&kind, outcome, elapsed(), false)
        }
        DocAction::Delete { collection, filter } => {
            let filter = parse_json_value(&filter)?;
            let deleted = doc.delete(&collection, filter).await?;
            ctx.render_success(
                &kind,
                serde_json::json!({ "deleted": deleted }),
                elapsed(),
                false,
            )
        }
        DocAction::Aggregate {
            collection,
            pipeline,
        } => {
            let pipeline = parse_pipeline(&pipeline)?;
            let docs = doc.aggregate(&collection, pipeline).await?;
            let truncated = docs.len() >= ctx.limit;
            ctx.render_success(&kind, docs, elapsed(), truncated)
        }
    })
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

fn parse_json_value(raw: &str) -> Result<Value> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(Value::Json)
        .map_err(|e| Error::Serialization(e.to_string()))
}

fn parse_document(raw: &str) -> Result<Document> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| Error::Serialization(e.to_string()))?;
    match value {
        serde_json::Value::Object(map) => {
            Ok(map.into_iter().map(|(k, v)| (k, Value::Json(v))).collect())
        }
        _ => Err(Error::Serialization("expected JSON object".into())),
    }
}

fn parse_pipeline(raw: &str) -> Result<Vec<Value>> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| Error::Serialization(e.to_string()))?;
    match value {
        serde_json::Value::Array(items) => Ok(items.into_iter().map(Value::Json).collect()),
        _ => Err(Error::Serialization("expected JSON array pipeline".into())),
    }
}

fn pipeline_has_write_stage(pipeline: &[Value]) -> bool {
    pipeline.iter().any(|stage| match stage {
        Value::Json(serde_json::Value::Object(stage)) => {
            stage.contains_key("$out") || stage.contains_key("$merge")
        }
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_write_stages_are_detected() {
        let readonly = parse_pipeline(r#"[{"$match":{"active":true}}]"#).unwrap();
        assert!(!pipeline_has_write_stage(&readonly));

        let out = parse_pipeline(r#"[{"$match":{}},{"$out":"archive"}]"#).unwrap();
        assert!(pipeline_has_write_stage(&out));

        let merge = parse_pipeline(r#"[{"$merge":{"into":"archive"}}]"#).unwrap();
        assert!(pipeline_has_write_stage(&merge));
    }
}
