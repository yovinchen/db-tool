use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{Document, FindOptions, Value},
    service::safety::SafetyGuard,
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
        /// Number of matching documents to skip before returning results.
        #[arg(long)]
        skip: Option<usize>,
        /// JSON sort object, for example '{"created_at":-1}'.
        #[arg(long)]
        sort: Option<String>,
        /// JSON projection object, for example '{"name":1,"_id":0}'.
        #[arg(long)]
        projection: Option<String>,
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
    /// Drop one collection after target-bound confirmation.
    Drop {
        /// Collection name to drop.
        collection: String,
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
    let dsn = ctx.resolve_dsn()?;
    match &cmd.action {
        DocAction::Insert { .. } | DocAction::Update { .. } | DocAction::Delete { .. } => {
            ensure_write_allowed(ctx)?;
        }
        DocAction::Drop { collection } => {
            ensure_write_allowed(ctx)?;
            SafetyGuard::check_destructive_operation(
                "drop_collection",
                collection,
                &ctx.safety_target(&dsn),
                ctx.allow_write,
                ctx.confirm.as_deref(),
            )?;
        }
        DocAction::Aggregate { pipeline, .. } => {
            let pipeline = parse_pipeline(pipeline)?;
            if pipeline_has_write_stage(&pipeline) {
                ensure_write_allowed(ctx)?;
            }
        }
        DocAction::Collections | DocAction::Find { .. } => {}
    }

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
        DocAction::Find {
            collection,
            filter,
            skip,
            sort,
            projection,
        } => {
            let f: serde_json::Value =
                serde_json::from_str(&filter).map_err(|e| Error::Serialization(e.to_string()))?;
            let fetch_limit = limit_plus_one(ctx.limit)?;
            let opts = FindOptions {
                limit: Some(fetch_limit),
                skip,
                sort: parse_optional_json_object(sort.as_deref(), "--sort")?,
                projection: parse_optional_json_object(projection.as_deref(), "--projection")?,
            };
            let docs = doc.find(&collection, f.into(), opts).await?;
            let (docs, truncated) = truncate_documents(docs, ctx.limit);
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
        DocAction::Drop { collection } => {
            doc.drop_collection(&collection).await?;
            ctx.render_success(
                &kind,
                serde_json::json!({ "dropped": true, "collection": collection }),
                elapsed(),
                false,
            )
        }
        DocAction::Aggregate {
            collection,
            pipeline,
        } => {
            let pipeline = parse_pipeline(&pipeline)?;
            let docs = doc
                .aggregate_bounded(&collection, pipeline, limit_plus_one(ctx.limit)?)
                .await?;
            let (docs, truncated) = truncate_documents(docs, ctx.limit);
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

fn parse_optional_json_object(raw: Option<&str>, option: &str) -> Result<Option<Value>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| Error::Serialization(e.to_string()))?;
    if !value.is_object() {
        return Err(Error::Serialization(format!(
            "{option} must be a JSON object"
        )));
    }
    Ok(Some(Value::Json(value)))
}

fn limit_plus_one(limit: usize) -> Result<usize> {
    if limit == 0 {
        return Err(Error::Config("--limit must be greater than zero".into()));
    }
    limit
        .checked_add(1)
        .ok_or_else(|| Error::Config("--limit is too large".into()))
}

fn truncate_documents(mut documents: Vec<Document>, limit: usize) -> (Vec<Document>, bool) {
    let truncated = documents.len() > limit;
    documents.truncate(limit);
    (documents, truncated)
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

    #[test]
    fn find_options_require_json_objects() {
        assert!(
            parse_optional_json_object(Some(r#"{"created_at":-1}"#), "--sort")
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            parse_optional_json_object(Some("[]"), "--projection"),
            Err(Error::Serialization(message)) if message.contains("--projection")
        ));
    }

    #[test]
    fn exact_limit_is_not_reported_as_truncated_without_a_probe_row() {
        let documents = vec![Document::new(), Document::new()];
        let (documents, truncated) = truncate_documents(documents, 2);

        assert_eq!(documents.len(), 2);
        assert!(!truncated);
    }

    #[test]
    fn probe_row_is_removed_and_marks_results_truncated() {
        let documents = vec![Document::new(), Document::new(), Document::new()];
        let (documents, truncated) = truncate_documents(documents, 2);

        assert_eq!(documents.len(), 2);
        assert!(truncated);
    }

    #[test]
    fn zero_limit_is_rejected_before_connecting() {
        assert!(matches!(
            limit_plus_one(0),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
    }
}
