use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{error::Error, model::Value, port::capability::SearchOptions, Result};

#[derive(Args)]
#[command(
    about = "Inspect and query OpenSearch/Elasticsearch-compatible indices.",
    long_about = "Search commands use JSON request bodies and the global --limit for hit count. Indexing a document is a write operation and requires --allow-write."
)]
pub struct SearchCmd {
    #[command(subcommand)]
    pub action: SearchAction,
}

#[derive(Subcommand)]
pub enum SearchAction {
    /// List indices from an OpenSearch/Elasticsearch-compatible endpoint.
    Indices,
    /// Run a JSON search query against one index.
    Search {
        /// Index name to query.
        index: String,
        /// JSON query object, for example '{"query":{"match_all":{}}}'.
        #[arg(long)]
        q: String,
        /// Offset into the result set.
        #[arg(long)]
        from: Option<usize>,
        /// Include original _source payloads in hits when supported.
        #[arg(long)]
        source: bool,
    },
    /// Index one JSON document into one index.
    Index {
        /// Index name to write to.
        index: String,
        /// JSON document object to index; requires --allow-write.
        doc: String,
    },
}

pub async fn run(ctx: &Context, cmd: SearchCmd) -> Result<String> {
    if matches!(cmd.action, SearchAction::Index { .. }) {
        ensure_write_allowed(ctx)?;
    }

    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let se = conn
        .as_search()
        .ok_or_else(|| Error::UnsupportedCapability {
            kind: conn.kind().0.clone(),
            needed: "SearchEngine",
        })?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();

    Ok(match cmd.action {
        SearchAction::Indices => ctx.render_success(
            &kind,
            se.list_indices().await?,
            start.elapsed().as_millis() as u64,
            false,
        ),
        SearchAction::Search {
            index,
            q,
            from,
            source,
        } => {
            let query: serde_json::Value =
                serde_json::from_str(&q).map_err(|e| Error::Serialization(e.to_string()))?;
            let opts = SearchOptions {
                size: Some(ctx.limit),
                from,
                source,
            };
            let hits = se.search(&index, query.into(), opts).await?;
            ctx.render_success(&kind, hits, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Index { index, doc } => {
            let doc = parse_json_value(&doc)?;
            se.index_doc(&index, doc).await?;
            ctx.render_success(
                &kind,
                serde_json::json!({"indexed": true}),
                start.elapsed().as_millis() as u64,
                false,
            )
        }
    })
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    if ctx.allow_write {
        Ok(())
    } else {
        Err(Error::WriteNotAllowed)
    }
}

fn parse_json_value(raw: &str) -> Result<Value> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(Value::Json)
        .map_err(|e| Error::Serialization(e.to_string()))
}
