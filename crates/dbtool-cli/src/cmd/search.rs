use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{InputBudget, ReadBudget, Value},
    port::{
        capability::{SearchHits, SearchOptions},
        CapabilityOperation,
    },
    service::{safety::SafetyGuard, InputLimiter},
    Result,
};

#[derive(Args)]
#[command(
    about = "Inspect and query OpenSearch/Elasticsearch-compatible indices.",
    long_about = "Search commands use JSON request bodies. Index catalogs and searches apply the global --limit item budget and --max-bytes complete-response budget. Search never enlarges a smaller body size; search/get byte accounting includes _source, aggregations, metadata, and backend-specific fields. Index, put, update, delete, and delete-index are write operations and require --allow-write; delete-index also requires a target-bound --confirm token."
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
    #[command(
        long_about = "Run a JSON search query against one index. The global --limit caps returned hits without enlarging a smaller size in the JSON body. The global --max-bytes bounds the complete response, including _source, aggregations, hit metadata, and backend-specific fields."
    )]
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
    #[command(
        long_about = "Read one document by stable ID. A missing document returns JSON null. A present document always uses a one-item envelope, while the global --max-bytes bounds the complete document including _source and backend-specific fields."
    )]
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
    let read_budget = search_read_budget(&cmd.action, ctx.limit, ctx.max_bytes)?;
    let input_budget = if action_may_mutate(&cmd.action) {
        let budget = ctx.input_budget()?;
        preflight_search_mutation(&cmd.action, budget)?;
        Some(budget)
    } else {
        None
    };
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
                &ctx.confirmation_target(&dsn)?,
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
            let indices = se
                .list_indices_budgeted(read_budget.ok_or_else(|| {
                    Error::Internal("search index-list budget was not initialized".to_owned())
                })?)
                .await?;
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
            let budget = read_budget.ok_or_else(|| {
                Error::Internal("search read budget was not initialized".to_owned())
            })?;
            let query: serde_json::Value =
                serde_json::from_str(&q).map_err(|e| Error::Serialization(e.to_string()))?;
            let effective_from = effective_search_from(&query, from);
            let opts = SearchOptions {
                size: Some(ctx.limit),
                from,
                source,
            };
            let hits = se
                .search_budgeted(&index, query.into(), opts, budget)
                .await?;
            let truncated = search_results_truncated(&hits, effective_from);
            ctx.render_success(&kind, hits, start.elapsed().as_millis() as u64, truncated)
        }
        SearchAction::Index { index, doc } => {
            let doc = parse_json_value(&doc)?;
            let outcome = se
                .index_doc_budgeted(
                    &index,
                    doc,
                    input_budget.expect("index actions construct an input budget"),
                )
                .await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Put { index, id, doc } => {
            let doc = parse_json_value(&doc)?;
            let outcome = se
                .put_doc_budgeted(
                    &index,
                    &id,
                    doc,
                    input_budget.expect("put actions construct an input budget"),
                )
                .await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Get { index, id } => {
            let budget = read_budget.ok_or_else(|| {
                Error::Internal("search get budget was not initialized".to_owned())
            })?;
            let document = se.get_doc_budgeted(&index, &id, budget).await?;
            ctx.render_success(&kind, document, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Update { index, id, patch } => {
            let patch = parse_json_value(&patch)?;
            let outcome = se
                .update_doc_budgeted(
                    &index,
                    &id,
                    patch,
                    input_budget.expect("update actions construct an input budget"),
                )
                .await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::Delete { index, id } => {
            let outcome = se
                .delete_doc_budgeted(
                    &index,
                    &id,
                    input_budget.expect("delete actions construct an input budget"),
                )
                .await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
        SearchAction::DeleteIndex { index } => {
            let outcome = se
                .delete_index_budgeted(
                    &index,
                    input_budget.expect("delete-index actions construct an input budget"),
                )
                .await?;
            ctx.render_success(&kind, outcome, start.elapsed().as_millis() as u64, false)
        }
    })
}

fn search_operation_for_action(action: &SearchAction) -> (CapabilityOperation, &'static str) {
    match action {
        SearchAction::Indices => (
            CapabilityOperation::SearchListIndicesBudgeted,
            "SearchEngine.list_indices_budgeted",
        ),
        SearchAction::Search { .. } => (
            CapabilityOperation::SearchSearchBudgeted,
            "SearchEngine.search_budgeted",
        ),
        SearchAction::Index { .. } => (
            CapabilityOperation::SearchIndexDocumentBudgeted,
            "SearchEngine.index_doc_budgeted",
        ),
        SearchAction::Put { .. } => (
            CapabilityOperation::SearchPutDocumentBudgeted,
            "SearchEngine.put_doc_budgeted",
        ),
        SearchAction::Get { .. } => (
            CapabilityOperation::SearchGetDocumentBudgeted,
            "SearchEngine.get_doc_budgeted",
        ),
        SearchAction::Update { .. } => (
            CapabilityOperation::SearchUpdateDocumentBudgeted,
            "SearchEngine.update_doc_budgeted",
        ),
        SearchAction::Delete { .. } => (
            CapabilityOperation::SearchDeleteDocumentBudgeted,
            "SearchEngine.delete_doc_budgeted",
        ),
        SearchAction::DeleteIndex { .. } => (
            CapabilityOperation::SearchDeleteIndexBudgeted,
            "SearchEngine.delete_index_budgeted",
        ),
    }
}

fn preflight_search_mutation(action: &SearchAction, budget: InputBudget) -> Result<()> {
    let request = match action {
        SearchAction::Index { index, doc } => serde_json::json!({
            "index": index,
            "document": parse_search_object(doc, "index document")?,
        }),
        SearchAction::Put { index, id, doc } => serde_json::json!({
            "index": index,
            "id": id,
            "document": parse_search_object(doc, "put document")?,
        }),
        SearchAction::Update { index, id, patch } => serde_json::json!({
            "index": index,
            "id": id,
            "patch": parse_search_object(patch, "update patch")?,
        }),
        SearchAction::Delete { index, id } => serde_json::json!({
            "index": index,
            "id": id,
        }),
        SearchAction::DeleteIndex { index } => serde_json::json!({ "index": index }),
        SearchAction::Indices | SearchAction::Search { .. } | SearchAction::Get { .. } => {
            return Ok(())
        }
    };
    InputLimiter::new(budget, "CLI search mutation input")?.validate_request(&request)
}

fn parse_search_object(raw: &str, label: &str) -> Result<serde_json::Value> {
    let value = parse_json_value(raw)?.to_plain_json()?;
    if !value.is_object() {
        return Err(Error::Config(format!(
            "search {label} must be a JSON object"
        )));
    }
    Ok(value)
}

pub(crate) fn action_may_mutate(action: &SearchAction) -> bool {
    matches!(
        action,
        SearchAction::Index { .. }
            | SearchAction::Put { .. }
            | SearchAction::Update { .. }
            | SearchAction::Delete { .. }
            | SearchAction::DeleteIndex { .. }
    )
}

fn search_read_budget(
    action: &SearchAction,
    max_items: usize,
    max_bytes: usize,
) -> Result<Option<ReadBudget>> {
    match action {
        SearchAction::Indices | SearchAction::Search { .. } => {
            ReadBudget::new(max_items, max_bytes).map(Some)
        }
        SearchAction::Get { .. } => ReadBudget::new(1, max_bytes).map(Some),
        _ => Ok(None),
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
            max_item_bytes: dbtool_core::model::DEFAULT_INPUT_ITEM_BYTES,
            throttle_overrides: Default::default(),
            allow_write: false,
            confirm: None,
        }
    }

    #[test]
    fn coarse_search_capability_does_not_authorize_budgeted_index_listing() {
        assert!(matches!(
            require_search_operation(
                CapabilityOperation::SEARCH,
                CapabilityOperation::SearchListIndicesBudgeted,
                "legacy-search",
                "SearchEngine.list_indices_budgeted",
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-search" && needed == "SearchEngine.list_indices_budgeted"
        ));
    }

    #[test]
    fn legacy_search_operations_do_not_authorize_complete_read_envelopes() {
        for (operation, needed) in [
            (
                CapabilityOperation::SearchSearchBudgeted,
                "SearchEngine.search_budgeted",
            ),
            (
                CapabilityOperation::SearchGetDocumentBudgeted,
                "SearchEngine.get_doc_budgeted",
            ),
        ] {
            assert!(matches!(
                require_search_operation(
                    CapabilityOperation::SEARCH,
                    operation,
                    "legacy-search",
                    needed,
                ),
                Err(Error::UnsupportedCapability {
                    kind,
                    needed: actual_needed,
                }) if kind == "legacy-search" && actual_needed == needed
            ));
        }
    }

    #[test]
    fn every_search_mutation_selects_an_exact_input_budgeted_operation() {
        let cases = [
            (
                SearchAction::Index {
                    index: "logs".to_owned(),
                    doc: "{}".to_owned(),
                },
                CapabilityOperation::SearchIndexDocumentBudgeted,
                "SearchEngine.index_doc_budgeted",
            ),
            (
                SearchAction::Put {
                    index: "logs".to_owned(),
                    id: "one".to_owned(),
                    doc: "{}".to_owned(),
                },
                CapabilityOperation::SearchPutDocumentBudgeted,
                "SearchEngine.put_doc_budgeted",
            ),
            (
                SearchAction::Update {
                    index: "logs".to_owned(),
                    id: "one".to_owned(),
                    patch: "{}".to_owned(),
                },
                CapabilityOperation::SearchUpdateDocumentBudgeted,
                "SearchEngine.update_doc_budgeted",
            ),
            (
                SearchAction::Delete {
                    index: "logs".to_owned(),
                    id: "one".to_owned(),
                },
                CapabilityOperation::SearchDeleteDocumentBudgeted,
                "SearchEngine.delete_doc_budgeted",
            ),
            (
                SearchAction::DeleteIndex {
                    index: "logs".to_owned(),
                },
                CapabilityOperation::SearchDeleteIndexBudgeted,
                "SearchEngine.delete_index_budgeted",
            ),
        ];
        for (action, operation, needed) in cases {
            assert_eq!(search_operation_for_action(&action), (operation, needed));
            assert!(matches!(
                require_search_operation(&[], operation, "legacy-search", needed),
                Err(Error::UnsupportedCapability {
                    needed: actual_needed,
                    ..
                }) if actual_needed == needed
            ));
        }
    }

    #[tokio::test]
    async fn search_mutation_input_budget_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context(1);
        ctx.allow_write = true;
        ctx.max_item_bytes = 1;
        let error = run(
            &ctx,
            SearchCmd {
                action: SearchAction::Index {
                    index: "logs".to_owned(),
                    doc: r#"{"message":"hello"}"#.to_owned(),
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::InputBudgetExceeded { .. }));
    }

    #[tokio::test]
    async fn search_mutation_shape_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context(1);
        ctx.allow_write = true;
        ctx.dsn = None;
        let error = run(
            &ctx,
            SearchCmd {
                action: SearchAction::Index {
                    index: "logs".to_owned(),
                    doc: "[]".to_owned(),
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("JSON object")));
    }

    #[tokio::test]
    async fn search_hit_budget_is_rejected_before_dsn_resolution() {
        let error = run(
            &test_context(0),
            SearchCmd {
                action: SearchAction::Search {
                    index: "users".to_owned(),
                    q: r#"{"query":{"match_all":{}}}"#.to_owned(),
                    from: None,
                    source: true,
                },
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            Error::Config(message) if message.contains("greater than zero")
        ));
    }

    #[tokio::test]
    async fn get_byte_budget_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context(usize::MAX);
        ctx.max_bytes = 0;
        let error = run(
            &ctx,
            SearchCmd {
                action: SearchAction::Get {
                    index: "users".to_owned(),
                    id: "user-1".to_owned(),
                },
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            Error::Config(message) if message.contains("byte budget")
        ));
    }

    #[test]
    fn get_uses_one_item_even_when_the_global_limit_is_larger() {
        assert_eq!(
            search_read_budget(
                &SearchAction::Get {
                    index: "users".to_owned(),
                    id: "user-1".to_owned(),
                },
                10_000,
                4096,
            )
            .unwrap(),
            Some(ReadBudget::new(1, 4096).unwrap())
        );
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

        let mut ctx = test_context(1);
        ctx.max_bytes = 0;
        let error = run(
            &ctx,
            SearchCmd {
                action: SearchAction::Indices,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("byte budget")));
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
