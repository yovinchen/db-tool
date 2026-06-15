use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{error::Error, model::FindOptions, service::formatter::Formatter, Result};

#[derive(Args)]
pub struct DocCmd {
    #[command(subcommand)]
    pub action: DocAction,
}

#[derive(Subcommand)]
pub enum DocAction {
    Collections,
    Find {
        collection: String,
        #[arg(long, default_value = "{}")]
        filter: String,
    },
    Insert {
        collection: String,
        doc: String,
    },
}

pub async fn run(ctx: &Context, cmd: DocCmd) -> Result<String> {
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
            Formatter::success(&kind, doc.list_collections().await?, elapsed(), false)
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
            Formatter::success(&kind, docs, elapsed(), truncated)
        }
        DocAction::Insert {
            collection,
            doc: raw_doc,
        } => {
            let v: serde_json::Value =
                serde_json::from_str(&raw_doc).map_err(|e| Error::Serialization(e.to_string()))?;
            let d: dbtool_core::model::Document = if let serde_json::Value::Object(m) = v {
                m.into_iter()
                    .map(|(k, v)| (k, dbtool_core::model::Value::Json(v)))
                    .collect()
            } else {
                return Err(Error::Serialization("expected JSON object".into()));
            };
            let outcome = doc.insert(&collection, vec![d]).await?;
            Formatter::success(&kind, outcome, elapsed(), false)
        }
    })
}
