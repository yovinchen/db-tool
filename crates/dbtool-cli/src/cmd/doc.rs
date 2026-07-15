use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    dsn::Dsn,
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
    /// Run a bounded JSON aggregation pipeline.
    #[command(
        long_about = "Run a bounded JSON aggregation pipeline. Read-only pipelines execute normally. Pipelines containing MongoDB $out or $merge must expose exactly one statically resolvable output namespace and require both --allow-write and a --confirm token bound to the connection, source collection, target namespace, and complete pipeline before dbtool connects."
    )]
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
        DocAction::Aggregate {
            collection,
            pipeline,
        } => {
            let pipeline = parse_pipeline(pipeline)?;
            check_aggregate_safety(ctx, &dsn, collection, &pipeline)?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct AggregateWritePlan {
    operation: String,
    resource: String,
}

fn check_aggregate_safety(
    ctx: &Context,
    dsn: &str,
    source_collection: &str,
    pipeline: &[Value],
) -> Result<()> {
    let parsed_dsn = Dsn::parse(dsn)?;
    let default_database = parsed_dsn.database.as_deref().unwrap_or("admin");
    let Some(plan) = analyze_aggregate_write(pipeline, default_database)? else {
        return Ok(());
    };

    ensure_write_allowed(ctx)?;
    let resolved_target = format!("dsn:{}", parsed_dsn.redacted());
    let connection_target = ctx.safety_target(dsn);
    let confirmation_target = if connection_target == resolved_target {
        resolved_target
    } else {
        format!("{connection_target}|{resolved_target}")
    };
    let confirmation_scope = serde_json::to_string(&(source_collection, pipeline))
        .map_err(|error| Error::Serialization(error.to_string()))?;
    SafetyGuard::check_destructive_operation_with_scope(
        &plan.operation,
        &plan.resource,
        &confirmation_target,
        &confirmation_scope,
        ctx.allow_write,
        ctx.confirm.as_deref(),
    )
}

fn analyze_aggregate_write(
    pipeline: &[Value],
    default_database: &str,
) -> Result<Option<AggregateWritePlan>> {
    let mut write_plan = None;

    for stage in pipeline {
        let Value::Json(serde_json::Value::Object(stage)) = stage else {
            continue;
        };

        if let Some(specification) = stage.get("$out") {
            register_write_plan(
                &mut write_plan,
                parse_out_write_plan(specification, default_database)?,
            )?;
        }
        if let Some(specification) = stage.get("$merge") {
            register_write_plan(
                &mut write_plan,
                parse_merge_write_plan(specification, default_database)?,
            )?;
        }
    }

    Ok(write_plan)
}

fn register_write_plan(
    current: &mut Option<AggregateWritePlan>,
    next: AggregateWritePlan,
) -> Result<()> {
    if current.is_some() {
        return Err(Error::Config(
            "aggregate pipeline contains multiple $out/$merge write targets; refusing to choose one confirmation target"
                .into(),
        ));
    }
    *current = Some(next);
    Ok(())
}

fn parse_out_write_plan(
    specification: &serde_json::Value,
    default_database: &str,
) -> Result<AggregateWritePlan> {
    let is_timeseries = match specification {
        serde_json::Value::Object(options) => validate_out_timeseries(options.get("timeseries"))?,
        _ => false,
    };
    Ok(AggregateWritePlan {
        operation: if is_timeseries {
            "mongo_aggregate_out_timeseries"
        } else {
            "mongo_aggregate_out"
        }
        .to_owned(),
        resource: parse_output_namespace(
            specification,
            default_database,
            "$out",
            &["db", "coll", "timeseries"],
        )?,
    })
}

fn parse_merge_write_plan(
    specification: &serde_json::Value,
    default_database: &str,
) -> Result<AggregateWritePlan> {
    let (resource, when_matched, when_not_matched) = match specification {
        serde_json::Value::String(_) => (
            parse_output_namespace(
                specification,
                default_database,
                "$merge.into",
                &["db", "coll"],
            )?,
            "merge",
            "insert",
        ),
        serde_json::Value::Object(options) => {
            ensure_known_fields(
                options,
                &["into", "on", "let", "whenMatched", "whenNotMatched"],
                "$merge",
            )?;
            let target = options.get("into").ok_or_else(|| {
                Error::Config(
                    "cannot statically resolve $merge target: object form requires 'into'".into(),
                )
            })?;
            validate_merge_on(options.get("on"))?;
            validate_merge_let(options.get("let"))?;
            (
                parse_output_namespace(
                    target,
                    default_database,
                    "$merge.into",
                    &["db", "coll"],
                )?,
                parse_when_matched(options.get("whenMatched"))?,
                parse_when_not_matched(options.get("whenNotMatched"))?,
            )
        }
        _ => {
            return Err(Error::Config(
                "cannot statically resolve $merge target: expected a collection string or options object"
                    .into(),
            ))
        }
    };

    Ok(AggregateWritePlan {
        operation: format!("mongo_aggregate_merge_{when_matched}_{when_not_matched}"),
        resource,
    })
}

fn parse_output_namespace(
    value: &serde_json::Value,
    default_database: &str,
    field: &str,
    allowed_object_fields: &[&str],
) -> Result<String> {
    let (database, collection) = match value {
        serde_json::Value::String(collection) => (
            validate_static_namespace_name(default_database, "database", field)?,
            validate_static_namespace_name(collection, "collection", field)?,
        ),
        serde_json::Value::Object(namespace) => {
            ensure_known_fields(namespace, allowed_object_fields, field)?;
            let database = namespace.get("db").ok_or_else(|| {
                Error::Config(format!(
                    "cannot statically resolve {field} target: object form requires string 'db' and 'coll'"
                ))
            })?;
            let collection = namespace.get("coll").ok_or_else(|| {
                Error::Config(format!(
                    "cannot statically resolve {field} target: object form requires string 'db' and 'coll'"
                ))
            })?;
            (
                static_namespace_value(database, "database", field)?,
                static_namespace_value(collection, "collection", field)?,
            )
        }
        _ => {
            return Err(Error::Config(format!(
                "cannot statically resolve {field} target: expected a collection string or {{\"db\",\"coll\"}} object"
            )))
        }
    };

    Ok(format!("{database}.{collection}"))
}

fn validate_out_timeseries(value: Option<&serde_json::Value>) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    let options = value.as_object().ok_or_else(|| {
        Error::Config("cannot statically validate $out timeseries: expected an object".into())
    })?;
    ensure_known_fields(
        options,
        &[
            "timeField",
            "metaField",
            "granularity",
            "bucketMaxSpanSeconds",
            "bucketRoundingSeconds",
        ],
        "$out timeseries",
    )?;
    static_namespace_value(
        options.get("timeField").ok_or_else(|| {
            Error::Config(
                "cannot statically validate $out timeseries: 'timeField' is required".into(),
            )
        })?,
        "timeField",
        "$out timeseries",
    )?;
    if let Some(meta_field) = options.get("metaField") {
        static_namespace_value(meta_field, "metaField", "$out timeseries")?;
    }

    let granularity = options.get("granularity");
    if let Some(granularity) = granularity {
        if !matches!(granularity.as_str(), Some("seconds" | "minutes" | "hours")) {
            return Err(Error::Config(
                "cannot statically validate $out timeseries granularity".into(),
            ));
        }
    }
    let bucket_max = optional_positive_u64(options.get("bucketMaxSpanSeconds"))?;
    let bucket_rounding = optional_positive_u64(options.get("bucketRoundingSeconds"))?;
    if granularity.is_some() && (bucket_max.is_some() || bucket_rounding.is_some()) {
        return Err(Error::Config(
            "cannot statically validate $out timeseries: granularity cannot be combined with bucket span options"
                .into(),
        ));
    }
    if bucket_max.is_some() != bucket_rounding.is_some() || bucket_max != bucket_rounding {
        return Err(Error::Config(
            "cannot statically validate $out timeseries: bucketMaxSpanSeconds and bucketRoundingSeconds must both be present and equal"
                .into(),
        ));
    }
    Ok(true)
}

fn optional_positive_u64(value: Option<&serde_json::Value>) -> Result<Option<u64>> {
    value
        .map(|value| {
            value.as_u64().filter(|value| *value > 0).ok_or_else(|| {
                Error::Config(
                    "cannot statically validate $out timeseries bucket span: expected a positive integer"
                        .into(),
                )
            })
        })
        .transpose()
}

fn static_namespace_value<'a>(
    value: &'a serde_json::Value,
    kind: &str,
    field: &str,
) -> Result<&'a str> {
    let value = value.as_str().ok_or_else(|| {
        Error::Config(format!(
            "cannot statically resolve {field} target: {kind} must be a string"
        ))
    })?;
    validate_static_namespace_name(value, kind, field)
}

fn validate_static_namespace_name<'a>(value: &'a str, kind: &str, field: &str) -> Result<&'a str> {
    if value.trim().is_empty() || value.contains('\0') {
        return Err(Error::Config(format!(
            "cannot statically resolve {field} target: {kind} must be a non-empty string without NUL bytes"
        )));
    }
    Ok(value)
}

fn ensure_known_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    allowed: &[&str],
    field: &str,
) -> Result<()> {
    if let Some(unknown) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(Error::Config(format!(
            "cannot statically validate {field} write stage: unsupported field '{unknown}'"
        )));
    }
    Ok(())
}

fn parse_when_matched(value: Option<&serde_json::Value>) -> Result<&'static str> {
    let Some(value) = value else {
        return Ok("merge");
    };
    if let Some(mode) = value.as_str() {
        return match mode {
            "replace" => Ok("replace"),
            "keepExisting" => Ok("keep_existing"),
            "merge" => Ok("merge"),
            "fail" => Ok("fail"),
            _ => Err(Error::Config(format!(
                "cannot statically validate $merge whenMatched mode '{mode}'"
            ))),
        };
    }

    let serde_json::Value::Array(pipeline) = value else {
        return Err(Error::Config(
            "cannot statically validate $merge whenMatched: expected a supported mode or update pipeline"
                .into(),
        ));
    };
    for stage in pipeline {
        let serde_json::Value::Object(stage) = stage else {
            return Err(Error::Config(
                "cannot statically validate $merge whenMatched pipeline stage".into(),
            ));
        };
        if stage.len() != 1
            || !stage.keys().all(|operator| {
                matches!(
                    operator.as_str(),
                    "$addFields" | "$set" | "$project" | "$unset" | "$replaceRoot" | "$replaceWith"
                )
            })
        {
            return Err(Error::Config(
                "cannot statically validate $merge whenMatched pipeline: unsupported update stage"
                    .into(),
            ));
        }
    }
    Ok("pipeline")
}

fn parse_when_not_matched(value: Option<&serde_json::Value>) -> Result<&'static str> {
    let Some(value) = value else {
        return Ok("insert");
    };
    match value.as_str() {
        Some("insert") => Ok("insert"),
        Some("discard") => Ok("discard"),
        Some("fail") => Ok("fail"),
        Some(mode) => Err(Error::Config(format!(
            "cannot statically validate $merge whenNotMatched mode '{mode}'"
        ))),
        None => Err(Error::Config(
            "cannot statically validate $merge whenNotMatched: expected insert, discard, or fail"
                .into(),
        )),
    }
}

fn validate_merge_on(value: Option<&serde_json::Value>) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    let valid = match value {
        serde_json::Value::String(field) => !field.trim().is_empty(),
        serde_json::Value::Array(fields) => {
            !fields.is_empty()
                && fields
                    .iter()
                    .all(|field| field.as_str().is_some_and(|field| !field.trim().is_empty()))
        }
        _ => false,
    };
    if !valid {
        return Err(Error::Config(
            "cannot statically validate $merge 'on': expected a field string or non-empty string array"
                .into(),
        ));
    }
    Ok(())
}

fn validate_merge_let(value: Option<&serde_json::Value>) -> Result<()> {
    if value.is_some_and(|value| !value.is_object()) {
        return Err(Error::Config(
            "cannot statically validate $merge 'let': expected an object".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::service::formatter::Format;

    fn test_context(allow_write: bool, dsn: &str) -> Context {
        Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: Some(dsn.to_owned()),
            format: Format::Json,
            limit: 100,
            throttle_overrides: Default::default(),
            allow_write,
            confirm: None,
        }
    }

    #[test]
    fn aggregate_write_targets_include_default_and_explicit_databases() {
        let readonly = parse_pipeline(r#"[{"$match":{"active":true}}]"#).unwrap();
        assert_eq!(analyze_aggregate_write(&readonly, "app").unwrap(), None);

        let out = parse_pipeline(r#"[{"$match":{}},{"$out":"archive"}]"#).unwrap();
        let out = analyze_aggregate_write(&out, "app").unwrap().unwrap();
        assert_eq!(out.operation, "mongo_aggregate_out");
        assert_eq!(out.resource, "app.archive");

        let timeseries = parse_pipeline(
            r#"[{"$out":{"db":"audit","coll":"events","timeseries":{"timeField":"recorded_at","granularity":"minutes"}}}]"#,
        )
        .unwrap();
        let timeseries = analyze_aggregate_write(&timeseries, "app")
            .unwrap()
            .unwrap();
        assert_eq!(timeseries.operation, "mongo_aggregate_out_timeseries");
        assert_eq!(timeseries.resource, "audit.events");

        let merge = parse_pipeline(
            r#"[{"$merge":{"into":{"db":"audit","coll":"archive"},"whenMatched":"replace","whenNotMatched":"discard"}}]"#,
        )
        .unwrap();
        let merge = analyze_aggregate_write(&merge, "app").unwrap().unwrap();
        assert_eq!(merge.operation, "mongo_aggregate_merge_replace_discard");
        assert_eq!(merge.resource, "audit.archive");
    }

    #[test]
    fn aggregate_write_analysis_fails_closed_for_multiple_or_dynamic_targets() {
        let multiple =
            parse_pipeline(r#"[{"$out":"archive"},{"$merge":{"into":"other"}}]"#).unwrap();
        assert!(matches!(
            analyze_aggregate_write(&multiple, "app"),
            Err(Error::Config(message)) if message.contains("multiple")
        ));

        let dynamic = parse_pipeline(
            r#"[{"$merge":{"into":{"db":"audit","coll":{"$concat":["archive","-2026"]}}}}]"#,
        )
        .unwrap();
        assert!(matches!(
            analyze_aggregate_write(&dynamic, "app"),
            Err(Error::Config(message)) if message.contains("statically")
        ));

        let unknown_when_matched =
            parse_pipeline(r#"[{"$merge":{"into":"archive","whenMatched":{"mode":"replace"}}}]"#)
                .unwrap();
        assert!(matches!(
            analyze_aggregate_write(&unknown_when_matched, "app"),
            Err(Error::Config(message)) if message.contains("whenMatched")
        ));
    }

    #[test]
    fn aggregate_confirmation_is_bound_to_dsn_target_and_merge_semantics() {
        let dsn = "mongodb://localhost:27017/app";
        let pipeline =
            parse_pipeline(r#"[{"$merge":{"into":"archive","whenMatched":"replace"}}]"#).unwrap();
        let mut ctx = test_context(true, dsn);
        let error = check_aggregate_safety(&ctx, dsn, "users", &pipeline).unwrap_err();
        let token = match error {
            Error::ConfirmRequired {
                confirm_token,
                impact,
            } => {
                assert_eq!(impact["resource"], "app.archive");
                assert_eq!(impact["op"], "MONGO_AGGREGATE_MERGE_REPLACE_INSERT");
                assert!(impact["target"]
                    .as_str()
                    .is_some_and(|target| target.contains("mongodb://localhost:27017/app")));
                confirm_token
            }
            other => panic!("expected confirmation requirement, got {other:?}"),
        };

        ctx.confirm = Some(token);
        assert!(check_aggregate_safety(&ctx, dsn, "users", &pipeline).is_ok());

        let other_target =
            parse_pipeline(r#"[{"$merge":{"into":"other","whenMatched":"replace"}}]"#).unwrap();
        assert!(matches!(
            check_aggregate_safety(&ctx, dsn, "users", &other_target),
            Err(Error::Internal(message)) if message.contains("mismatch")
        ));
        assert!(matches!(
            check_aggregate_safety(
                &ctx,
                "mongodb://other-host:27017/app",
                "users",
                &pipeline
            ),
            Err(Error::Internal(message)) if message.contains("mismatch")
        ));

        let other_semantics =
            parse_pipeline(r#"[{"$merge":{"into":"archive","whenMatched":"merge"}}]"#).unwrap();
        assert!(matches!(
            check_aggregate_safety(&ctx, dsn, "users", &other_semantics),
            Err(Error::Internal(message)) if message.contains("mismatch")
        ));
    }

    #[test]
    fn aggregate_confirmation_binds_source_and_complete_pipeline() {
        let dsn = "mongodb://localhost:27017/app";
        let pipeline = parse_pipeline(
            r#"[{"$match":{"tenant":"one"}},{"$merge":{"into":"archive","on":"external_id","let":{"source":"$tenant"},"whenMatched":[{"$set":{"status":"one"}}]}}]"#,
        )
        .unwrap();
        let mut ctx = test_context(true, dsn);
        let token = match check_aggregate_safety(&ctx, dsn, "users", &pipeline).unwrap_err() {
            Error::ConfirmRequired { confirm_token, .. } => confirm_token,
            other => panic!("expected confirmation requirement, got {other:?}"),
        };
        ctx.confirm = Some(token);
        assert!(check_aggregate_safety(&ctx, dsn, "users", &pipeline).is_ok());

        let different_source = check_aggregate_safety(&ctx, dsn, "orders", &pipeline);
        assert!(matches!(
            different_source,
            Err(Error::Internal(message)) if message.contains("mismatch")
        ));

        for changed_pipeline in [
            r#"[{"$match":{"tenant":"two"}},{"$merge":{"into":"archive","on":"external_id","let":{"source":"$tenant"},"whenMatched":[{"$set":{"status":"one"}}]}}]"#,
            r#"[{"$match":{"tenant":"one"}},{"$merge":{"into":"archive","on":"alternate_id","let":{"source":"$tenant"},"whenMatched":[{"$set":{"status":"one"}}]}}]"#,
            r#"[{"$match":{"tenant":"one"}},{"$merge":{"into":"archive","on":"external_id","let":{"source":"$other"},"whenMatched":[{"$set":{"status":"one"}}]}}]"#,
            r#"[{"$match":{"tenant":"one"}},{"$merge":{"into":"archive","on":"external_id","let":{"source":"$tenant"},"whenMatched":[{"$set":{"status":"two"}}]}}]"#,
        ] {
            let changed_pipeline = parse_pipeline(changed_pipeline).unwrap();
            assert!(matches!(
                check_aggregate_safety(&ctx, dsn, "users", &changed_pipeline),
                Err(Error::Internal(message)) if message.contains("mismatch")
            ));
        }
    }

    #[test]
    fn readonly_aggregate_needs_no_write_or_confirmation() {
        let dsn = "mongodb://localhost:27017/app";
        let pipeline = parse_pipeline(r#"[{"$match":{"active":true}}]"#).unwrap();

        assert!(check_aggregate_safety(&test_context(false, dsn), dsn, "users", &pipeline).is_ok());
    }

    #[tokio::test]
    async fn aggregate_write_confirmation_is_required_before_connecting() {
        let dsn = "mongodb://127.0.0.1:1/app";
        let ctx = test_context(true, dsn);
        let error = run(
            &ctx,
            DocCmd {
                action: DocAction::Aggregate {
                    collection: "users".to_owned(),
                    pipeline: r#"[{"$out":"archive"}]"#.to_owned(),
                },
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::ConfirmRequired { .. }));
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
