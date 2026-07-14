use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::Value,
    port::capability::{SearchHits, SearchOptions},
    Result,
};

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
            let effective_from = effective_search_from(&query, from);
            let opts = SearchOptions {
                size: Some(ctx.limit),
                from,
                source,
            };
            let hits = se.search(&index, query.into(), opts).await?;
            let truncated = search_results_truncated(&hits, effective_from);
            ctx.render_success(&kind, hits, start.elapsed().as_millis() as u64, truncated)
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
    ctx.ensure_write_allowed()
}

fn parse_json_value(raw: &str) -> Result<Value> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(Value::Json)
        .map_err(|e| Error::Serialization(e.to_string()))
}

fn effective_search_from(query: &serde_json::Value, option_from: Option<usize>) -> u64 {
    option_from
        .and_then(|from| u64::try_from(from).ok())
        .or_else(|| query.get("from").and_then(serde_json::Value::as_u64))
        .unwrap_or_default()
}

fn search_results_truncated(hits: &SearchHits, from: u64) -> bool {
    let returned = u64::try_from(hits.hits.len()).unwrap_or(u64::MAX);
    from.saturating_add(returned) < hits.total
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn explicit_from_option_wins_over_query_body() {
        let query = json!({ "query": { "match_all": {} }, "from": 20 });

        assert_eq!(effective_search_from(&query, Some(7)), 7);
        assert_eq!(effective_search_from(&query, None), 20);
        assert_eq!(effective_search_from(&json!({ "match_all": {} }), None), 0);
    }

    #[test]
    fn truncated_accounts_for_total_offset_and_returned_hits() {
        let hits = SearchHits {
            total: 10,
            hits: vec![json!({}); 3],
        };

        assert!(search_results_truncated(&hits, 0));
        assert!(!search_results_truncated(&hits, 7));
        assert!(!search_results_truncated(&hits, 10));
    }

    #[test]
    fn truncated_is_false_when_the_last_page_is_short() {
        let hits = SearchHits {
            total: 10,
            hits: vec![json!({}); 2],
        };

        assert!(!search_results_truncated(&hits, 8));
        assert!(search_results_truncated(&hits, 7));
    }
}
