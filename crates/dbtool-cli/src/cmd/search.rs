use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::Value,
    port::{
        capability::{SearchHits, SearchOptions},
        CapabilityOperation,
    },
    service::{limiter::ListLimiter, safety::SafetyGuard},
    Result,
};

#[derive(Args)]
#[command(
    about = "Inspect and query OpenSearch/Elasticsearch-compatible indices.",
    long_about = "Search commands use JSON request bodies and the global --limit for hit count. Index, put, update, delete, and delete-index are write operations and require --allow-write; delete-index also requires a target-bound --confirm token."
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
    /// Create or replace one JSON document using a stable caller-provided ID.
    Put {
        /// Index name to write to.
        index: String,
        /// Stable document identifier.
        id: String,
        /// JSON document object to store; requires --allow-write.
        doc: String,
    },
    /// Read one document by stable ID. A missing document returns JSON null.
    Get {
        /// Index name to read from.
        index: String,
        /// Stable document identifier.
        id: String,
    },
    /// Partially update one document by stable ID.
    Update {
        /// Index name containing the document.
        index: String,
        /// Stable document identifier.
        id: String,
        /// JSON patch object. Plain objects are wrapped in a Search `doc` update.
        patch: String,
    },
    /// Delete one document by stable ID.
    Delete {
        /// Index name containing the document.
        index: String,
        /// Stable document identifier.
        id: String,
    },
    /// Delete one complete index after target-bound confirmation.
    DeleteIndex {
        /// Exact index name to delete.
        index: String,
    },
}

pub async fn run(ctx: &Context, cmd: SearchCmd) -> Result<String> {
    if matches!(cmd.action, SearchAction::Indices) {
        ListLimiter::new(ctx.limit).probe_items()?;
    }
    let dsn = ctx.resolve_dsn()?;
    match &cmd.action {
        SearchAction::Index { .. }
        | SearchAction::Put { .. }
        | SearchAction::Update { .. }
        | SearchAction::Delete { .. } => ensure_write_allowed(ctx)?,
        SearchAction::DeleteIndex { index } => {
            ensure_write_allowed(ctx)?;
            SafetyGuard::check_destructive_operation(
                "delete_search_index",
                index,
                &ctx.safety_target(&dsn),
                ctx.allow_write,
                ctx.confirm.as_deref(),
            )?;
        }
        SearchAction::Indices | SearchAction::Search { .. } | SearchAction::Get { .. } => {}
    }

    let conn = ctx.registry.connect(&dsn).await?;
    let operations = conn.operations();
    let kind = conn.kind().0.clone();
    let (operation, needed) = search_operation_for_action(&cmd.action);
    require_search_operation(&operations, operation, &kind, needed)?;
    let se = conn
        .as_search()
        .ok_or_else(|| Error::UnsupportedCapability {
            kind: kind.clone(),
            needed: "SearchEngine",
        })?;
    let start = std::time::Instant::now();

    Ok(match cmd.action {
        SearchAction::Indices => {
            let indices = se.list_indices_bounded(ctx.limit).await?;
            let truncated = indices.truncated;
            ctx.render_success(
                &kind,
                indices.items,
                start.elapsed().as_millis() as u64,
                truncated,
            )
        }
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
            let outcome = se.index_doc(&index, doc).await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Put { index, id, doc } => {
            let doc = parse_json_value(&doc)?;
            let outcome = se.put_doc(&index, &id, doc).await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Get { index, id } => {
            let document = se.get_doc(&index, &id).await?;
            ctx.render_success(&kind, document, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Update { index, id, patch } => {
            let patch = parse_json_value(&patch)?;
            let outcome = se.update_doc(&index, &id, patch).await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Delete { index, id } => {
            let outcome = se.delete_doc(&index, &id).await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::DeleteIndex { index } => {
            let outcome = se.delete_index(&index).await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
    })
}

fn search_operation_for_action(action: &SearchAction) -> (CapabilityOperation, &'static str) {
    match action {
        SearchAction::Indices => (
            CapabilityOperation::SearchListIndicesBounded,
            "SearchEngine.list_indices_bounded",
        ),
        SearchAction::Search { .. } => (CapabilityOperation::SearchSearch, "SearchEngine.search"),
        SearchAction::Index { .. } => (
            CapabilityOperation::SearchIndexDocument,
            "SearchEngine.index_doc",
        ),
        SearchAction::Put { .. } => (
            CapabilityOperation::SearchPutDocument,
            "SearchEngine.put_doc",
        ),
        SearchAction::Get { .. } => (
            CapabilityOperation::SearchGetDocument,
            "SearchEngine.get_doc",
        ),
        SearchAction::Update { .. } => (
            CapabilityOperation::SearchUpdateDocument,
            "SearchEngine.update_doc",
        ),
        SearchAction::Delete { .. } => (
            CapabilityOperation::SearchDeleteDocument,
            "SearchEngine.delete_doc",
        ),
        SearchAction::DeleteIndex { .. } => (
            CapabilityOperation::SearchDeleteIndex,
            "SearchEngine.delete_index",
        ),
    }
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

fn require_search_operation(
    operations: &[CapabilityOperation],
    operation: CapabilityOperation,
    kind: &str,
    needed: &'static str,
) -> Result<()> {
    if operations.contains(&operation) {
        Ok(())
    } else {
        Err(Error::UnsupportedCapability {
            kind: kind.to_owned(),
            needed,
        })
    }
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
    let incomplete_backend_result = hits.total_relation != "eq"
        || hits.timed_out
        || hits
            .extra
            .get("_shards")
            .and_then(|shards| shards.get("failed"))
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|failed| failed > 0);
    let returned = u64::try_from(hits.hits.len()).unwrap_or(u64::MAX);
    incomplete_backend_result || from.saturating_add(returned) < hits.total
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::service::formatter::Format;
    use serde_json::json;

    fn test_context(limit: usize) -> Context {
        Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: Format::Json,
            limit,
            max_bytes: dbtool_core::model::DEFAULT_READ_BYTES,
            throttle_overrides: Default::default(),
            allow_write: false,
            confirm: None,
        }
    }

    #[test]
    fn coarse_search_capability_does_not_authorize_bounded_index_listing() {
        assert!(matches!(
            require_search_operation(
                CapabilityOperation::SEARCH,
                CapabilityOperation::SearchListIndicesBounded,
                "legacy-search",
                "SearchEngine.list_indices_bounded",
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-search" && needed == "SearchEngine.list_indices_bounded"
        ));
    }

    #[tokio::test]
    async fn index_limit_is_rejected_before_dsn_resolution() {
        let error = run(
            &test_context(0),
            SearchCmd {
                action: SearchAction::Indices,
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            Error::Config(message) if message.contains("greater than zero")
        ));
    }

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
            total_relation: "eq".to_owned(),
            hits: vec![json!({}); 3],
            took_ms: 1,
            timed_out: false,
            aggregations: None,
            hits_metadata: Default::default(),
            extra: Default::default(),
        };

        assert!(search_results_truncated(&hits, 0));
        assert!(!search_results_truncated(&hits, 7));
        assert!(!search_results_truncated(&hits, 10));
    }

    #[test]
    fn truncated_is_false_when_the_last_page_is_short() {
        let hits = SearchHits {
            total: 10,
            total_relation: "eq".to_owned(),
            hits: vec![json!({}); 2],
            took_ms: 1,
            timed_out: false,
            aggregations: None,
            hits_metadata: Default::default(),
            extra: Default::default(),
        };

        assert!(!search_results_truncated(&hits, 8));
        assert!(search_results_truncated(&hits, 7));
    }

    #[test]
    fn truncated_is_true_when_total_is_only_a_lower_bound() {
        let hits = SearchHits {
            total: 2,
            total_relation: "gte".to_owned(),
            hits: vec![json!({}); 2],
            took_ms: 1,
            timed_out: false,
            aggregations: None,
            hits_metadata: Default::default(),
            extra: Default::default(),
        };

        assert!(search_results_truncated(&hits, 0));
    }

    #[test]
    fn truncated_is_true_when_search_timed_out() {
        let hits = SearchHits {
            total: 1,
            total_relation: "eq".to_owned(),
            hits: vec![json!({})],
            took_ms: 1,
            timed_out: true,
            aggregations: None,
            hits_metadata: Default::default(),
            extra: Default::default(),
        };

        assert!(search_results_truncated(&hits, 0));
    }

    #[test]
    fn truncated_is_true_when_any_shard_failed() {
        let hits = SearchHits {
            total: 1,
            total_relation: "eq".to_owned(),
            hits: vec![json!({})],
            took_ms: 1,
            timed_out: false,
            aggregations: None,
            hits_metadata: Default::default(),
            extra: [("_shards".to_owned(), json!({ "failed": 1 }))]
                .into_iter()
                .collect(),
        };

        assert!(search_results_truncated(&hits, 0));
    }
}
