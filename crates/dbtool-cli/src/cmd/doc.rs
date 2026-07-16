use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::{Document, FindOptions, Value},
    port::CapabilityOperation,
    service::{limiter::ListLimiter, safety::SafetyGuard},
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
    /// Update one document by default, or every match with --many and confirmation.
    Update {
        /// Collection name.
        collection: String,
        /// JSON filter object.
        #[arg(long)]
        filter: String,
        /// JSON update document; plain objects are wrapped in `$set` by MongoDB adapter.
        #[arg(long)]
        update: String,
        /// Update every matching document after target/content-bound confirmation.
        #[arg(long)]
        many: bool,
    },
    /// Delete one document by default, or every match with --many and confirmation.
    Delete {
        /// Collection name.
        collection: String,
        /// JSON filter object.
        #[arg(long)]
        filter: String,
        /// Delete every matching document after target/content-bound confirmation.
        #[arg(long)]
        many: bool,
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
    if matches!(cmd.action, DocAction::Collections) {
        ListLimiter::new(ctx.limit).probe_items()?;
    }
    let read_budget = match &cmd.action {
        DocAction::Find { .. } | DocAction::Aggregate { .. } => Some(ctx.read_budget()?),
        _ => None,
    };
    let dsn = ctx.resolve_dsn()?;
    match &cmd.action {
        DocAction::Insert { .. } => ensure_write_allowed(ctx)?,
        DocAction::Update {
            collection,
            filter,
            update,
            many,
        } => check_update_safety(ctx, &dsn, collection, filter, update, *many)?,
        DocAction::Delete {
            collection,
            filter,
            many,
        } => check_delete_safety(ctx, &dsn, collection, filter, *many)?,
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
    let operations = conn.operations();
    let kind = conn.kind().0.clone();
    if let Some((operation, needed)) = document_operation_for_action(&cmd.action) {
        require_document_operation(&operations, operation, &kind, needed)?;
    }
    let doc = conn
        .as_document()
        .ok_or_else(|| Error::UnsupportedCapability {
            kind: kind.clone(),
            needed: "DocumentStore",
        })?;
    let start = std::time::Instant::now();
    let elapsed = || start.elapsed().as_millis() as u64;

    Ok(match cmd.action {
        DocAction::Collections => {
            let collections = doc.list_collections_bounded(ctx.limit).await?;
            let truncated = collections.truncated;
            ctx.render_success(&kind, collections.items, elapsed(), truncated)
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
            let opts = FindOptions {
                limit: None,
                skip,
                sort: parse_optional_json_object(sort.as_deref(), "--sort")?,
                projection: parse_optional_json_object(projection.as_deref(), "--projection")?,
            };
            let result = doc
                .find_budgeted(
                    &collection,
                    f.into(),
                    opts,
                    read_budget.expect("find actions construct a read budget"),
                )
                .await?;
            ctx.render_success(&kind, result.items, elapsed(), result.truncated)
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
            many,
        } => {
            let filter = parse_nonempty_json_object(&filter, "--filter")?.into();
            let update = parse_json_object(&update, "--update")?.into();
            let outcome = if many {
                doc.update_many(&collection, filter, update).await?
            } else {
                doc.update_one(&collection, filter, update).await?
            };
            ctx.render_success(&kind, outcome, elapsed(), false)
        }
        DocAction::Delete {
            collection,
            filter,
            many,
        } => {
            let filter = parse_nonempty_json_object(&filter, "--filter")?.into();
            let deleted = if many {
                doc.delete_many(&collection, filter).await?
            } else {
                doc.delete_one(&collection, filter).await?
            };
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
            let result = doc
                .aggregate_budgeted(
                    &collection,
                    pipeline,
                    read_budget.expect("aggregate actions construct a read budget"),
                )
                .await?;
            ctx.render_success(&kind, result.items, elapsed(), result.truncated)
        }
    })
}

fn document_operation_for_action(
    action: &DocAction,
) -> Option<(CapabilityOperation, &'static str)> {
    match action {
        DocAction::Collections => Some((
            CapabilityOperation::DocumentListCollectionsBounded,
            "DocumentStore.list_collections_bounded",
        )),
        DocAction::Update { many: true, .. } => Some((
            CapabilityOperation::DocumentUpdateMany,
            "DocumentStore.update_many",
        )),
        DocAction::Update { many: false, .. } => Some((
            CapabilityOperation::DocumentUpdateOne,
            "DocumentStore.update_one",
        )),
        DocAction::Delete { many: true, .. } => Some((
            CapabilityOperation::DocumentDeleteMany,
            "DocumentStore.delete_many",
        )),
        DocAction::Delete { many: false, .. } => Some((
            CapabilityOperation::DocumentDeleteOne,
            "DocumentStore.delete_one",
        )),
        DocAction::Drop { .. } => Some((
            CapabilityOperation::DocumentDropCollection,
            "DocumentStore.drop_collection",
        )),
        DocAction::Aggregate { .. } => Some((
            CapabilityOperation::DocumentAggregateBudgeted,
            "DocumentStore.aggregate_budgeted",
        )),
        DocAction::Find { .. } => Some((
            CapabilityOperation::DocumentFindBudgeted,
            "DocumentStore.find_budgeted",
        )),
        DocAction::Insert { .. } => {
            Some((CapabilityOperation::DocumentInsert, "DocumentStore.insert"))
        }
    }
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

fn check_update_safety(
    ctx: &Context,
    dsn: &str,
    collection: &str,
    filter: &str,
    update: &str,
    many: bool,
) -> Result<()> {
    ensure_write_allowed(ctx)?;
    validate_mutation_collection(collection)?;
    let filter = parse_nonempty_json_object(filter, "--filter")?;
    let update = parse_json_object(update, "--update")?;
    check_many_confirmation(
        ctx,
        dsn,
        "document_update",
        collection,
        &filter,
        Some(&update),
        many,
    )
}

fn check_delete_safety(
    ctx: &Context,
    dsn: &str,
    collection: &str,
    filter: &str,
    many: bool,
) -> Result<()> {
    ensure_write_allowed(ctx)?;
    validate_mutation_collection(collection)?;
    let filter = parse_nonempty_json_object(filter, "--filter")?;
    check_many_confirmation(ctx, dsn, "document_delete", collection, &filter, None, many)
}

fn check_many_confirmation(
    ctx: &Context,
    dsn: &str,
    operation: &str,
    collection: &str,
    filter: &serde_json::Value,
    update: Option<&serde_json::Value>,
    many: bool,
) -> Result<()> {
    if !many {
        if ctx.confirm.is_some() {
            return Err(Error::Config(
                "--confirm is not accepted for single-document update/delete; omit it or use --many"
                    .into(),
            ));
        }
        return Ok(());
    }

    let normalized_filter = normalize_document_for_confirmation(filter);
    let normalized_update = update.map(normalize_document_for_confirmation);
    let scope = SafetyGuard::confirmation_scope_digest(&(
        "document-mutation-v1",
        operation,
        collection,
        normalized_filter,
        normalized_update,
        "many",
    ))?;
    SafetyGuard::check_destructive_operation_with_scope(
        &format!("{operation}_many"),
        collection,
        &ctx.safety_target(dsn),
        &scope,
        ctx.allow_write,
        ctx.confirm.as_deref(),
    )
}

fn normalize_document_for_confirmation(value: &serde_json::Value) -> serde_json::Value {
    let serde_json::Value::Object(map) = value else {
        return value.clone();
    };
    let sorted = map
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    serde_json::Value::Object(sorted.into_iter().collect())
}

fn validate_mutation_collection(collection: &str) -> Result<()> {
    if collection.trim().is_empty() || collection.contains('\0') {
        return Err(Error::Config(
            "document mutation collection must be non-empty and contain no NUL bytes".into(),
        ));
    }
    Ok(())
}

fn parse_nonempty_json_object(raw: &str, option: &str) -> Result<serde_json::Value> {
    let value = parse_json_object(raw, option)?;
    if value.as_object().is_some_and(serde_json::Map::is_empty) {
        return Err(Error::Config(format!(
            "{option} must be a non-empty JSON object"
        )));
    }
    Ok(value)
}

fn parse_json_object(raw: &str, option: &str) -> Result<serde_json::Value> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| Error::Serialization(e.to_string()))?;
    if !value.is_object() {
        return Err(Error::Config(format!("{option} must be a JSON object")));
    }
    Ok(value)
}

fn require_document_operation(
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
            max_bytes: dbtool_core::model::DEFAULT_READ_BYTES,
            throttle_overrides: Default::default(),
            allow_write,
            confirm: None,
        }
    }

    #[test]
    fn mutation_filters_are_validated_offline_and_must_be_nonempty_objects() {
        let ctx = test_context(true, "mongodb://127.0.0.1:1/app");

        for filter in ["{}", "[]", "null"] {
            assert!(check_update_safety(
                &ctx,
                "mongodb://127.0.0.1:1/app",
                "users",
                filter,
                r#"{"active":true}"#,
                false,
            )
            .is_err());
            assert!(
                check_delete_safety(&ctx, "mongodb://127.0.0.1:1/app", "users", filter, true,)
                    .is_err()
            );
        }
        assert!(matches!(
            check_update_safety(
                &ctx,
                "mongodb://127.0.0.1:1/app",
                "users",
                r#"{"id":1}"#,
                "[]",
                false,
            ),
            Err(Error::Config(message)) if message.contains("--update")
        ));
    }

    #[test]
    fn confirmation_normalization_preserves_nested_document_order() {
        let root_first = parse_json_object(
            r#"{"z":9,"embedded":{"first":1,"second":2},"a":1}"#,
            "--filter",
        )
        .unwrap();
        let root_reordered = parse_json_object(
            r#"{"a":1,"embedded":{"first":1,"second":2},"z":9}"#,
            "--filter",
        )
        .unwrap();
        let nested_reordered = parse_json_object(
            r#"{"a":1,"embedded":{"second":2,"first":1},"z":9}"#,
            "--filter",
        )
        .unwrap();

        let digest = |value: &serde_json::Value| {
            SafetyGuard::confirmation_scope_digest(&normalize_document_for_confirmation(value))
                .unwrap()
        };
        assert_eq!(digest(&root_first), digest(&root_reordered));
        assert_ne!(digest(&root_first), digest(&nested_reordered));
    }

    #[test]
    fn many_confirmation_is_normalized_target_bound_and_not_reusable() {
        let dsn = "mongodb://dbtool:top-secret@localhost:27017/app";
        let mut ctx = test_context(true, dsn);
        let error = check_update_safety(
            &ctx,
            dsn,
            "users",
            r#"{"b":2,"a":1}"#,
            r#"{"$set":{"status":"ready","count":2}}"#,
            true,
        )
        .unwrap_err();
        let token = match error {
            Error::ConfirmRequired {
                confirm_token,
                impact,
            } => {
                assert_eq!(impact["op"], "DOCUMENT_UPDATE_MANY");
                assert_eq!(impact["resource"], "users");
                let target = impact["target"].as_str().unwrap();
                assert!(!target.contains("top-secret"));
                assert!(!confirm_token.contains("mongodb://"));
                assert!(!confirm_token.contains("top-secret"));
                confirm_token
            }
            other => panic!("expected confirmation requirement, got {other:?}"),
        };
        ctx.confirm = Some(token);

        assert!(check_update_safety(
            &ctx,
            dsn,
            "users",
            r#" { "a": 1, "b": 2 } "#,
            r#"{"$set":{"status":"ready","count":2}}"#,
            true,
        )
        .is_ok());

        for changed in [
            check_update_safety(
                &ctx,
                dsn,
                "users",
                r#"{"a":9,"b":2}"#,
                r#"{"$set":{"status":"ready","count":2}}"#,
                true,
            ),
            check_update_safety(
                &ctx,
                dsn,
                "users",
                r#"{"a":1,"b":2}"#,
                r#"{"$set":{"status":"ready","count":3}}"#,
                true,
            ),
            check_update_safety(
                &ctx,
                dsn,
                "other",
                r#"{"a":1,"b":2}"#,
                r#"{"$set":{"status":"ready","count":2}}"#,
                true,
            ),
            check_update_safety(
                &ctx,
                "mongodb://localhost:27017/other",
                "users",
                r#"{"a":1,"b":2}"#,
                r#"{"$set":{"status":"ready","count":2}}"#,
                true,
            ),
            check_delete_safety(&ctx, dsn, "users", r#"{"a":1,"b":2}"#, true),
        ] {
            assert!(matches!(
                changed,
                Err(Error::Internal(message)) if message.contains("mismatch")
            ));
        }

        assert!(matches!(
            check_update_safety(
                &ctx,
                dsn,
                "users",
                r#"{"a":1,"b":2}"#,
                r#"{"$set":{"status":"ready","count":2}}"#,
                false,
            ),
            Err(Error::Config(message)) if message.contains("single-document")
        ));
    }

    #[test]
    fn coarse_document_capability_does_not_authorize_optional_cardinality_methods() {
        let coarse = CapabilityOperation::DOCUMENT;
        assert!(matches!(
            require_document_operation(
                coarse,
                CapabilityOperation::DocumentUpdateOne,
                "legacy-document",
                "DocumentStore.update_one",
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-document" && needed == "DocumentStore.update_one"
        ));

        let mut explicit = coarse.to_vec();
        explicit.push(CapabilityOperation::DocumentDeleteMany);
        assert!(require_document_operation(
            &explicit,
            CapabilityOperation::DocumentDeleteMany,
            "mongodb",
            "DocumentStore.delete_many",
        )
        .is_ok());

        assert!(matches!(
            require_document_operation(
                coarse,
                CapabilityOperation::DocumentDropCollection,
                "legacy-document",
                "DocumentStore.drop_collection",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "DocumentStore.drop_collection"
        ));

        let aggregate = DocAction::Aggregate {
            collection: "events".to_owned(),
            pipeline: "[]".to_owned(),
        };
        assert_eq!(
            document_operation_for_action(&aggregate),
            Some((
                CapabilityOperation::DocumentAggregateBudgeted,
                "DocumentStore.aggregate_budgeted"
            ))
        );
        assert!(matches!(
            require_document_operation(
                coarse,
                CapabilityOperation::DocumentFindBudgeted,
                "legacy-document",
                "DocumentStore.find_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "DocumentStore.find_budgeted"
        ));
        assert!(matches!(
            require_document_operation(
                coarse,
                CapabilityOperation::DocumentAggregateBudgeted,
                "legacy-document",
                "DocumentStore.aggregate_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "DocumentStore.aggregate_budgeted"
        ));
    }

    #[test]
    fn coarse_document_capability_does_not_authorize_bounded_collection_listing() {
        assert!(matches!(
            require_document_operation(
                CapabilityOperation::DOCUMENT,
                CapabilityOperation::DocumentListCollectionsBounded,
                "legacy-document",
                "DocumentStore.list_collections_bounded",
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-document"
                    && needed == "DocumentStore.list_collections_bounded"
        ));
    }

    #[tokio::test]
    async fn collection_limit_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context(false, "mongodb://127.0.0.1:1/app");
        ctx.dsn = None;
        ctx.limit = 0;

        let error = run(
            &ctx,
            DocCmd {
                action: DocAction::Collections,
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
    async fn mutation_validation_and_many_confirmation_happen_before_connecting() {
        let dsn = "mongodb://127.0.0.1:1/app";
        let ctx = test_context(true, dsn);

        let empty = run(
            &ctx,
            DocCmd {
                action: DocAction::Update {
                    collection: "users".to_owned(),
                    filter: "{}".to_owned(),
                    update: r#"{"active":true}"#.to_owned(),
                    many: false,
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(empty, Error::Config(message) if message.contains("non-empty")));

        let confirmation = run(
            &ctx,
            DocCmd {
                action: DocAction::Delete {
                    collection: "users".to_owned(),
                    filter: r#"{"active":true}"#.to_owned(),
                    many: true,
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(confirmation, Error::ConfirmRequired { .. }));
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

    #[tokio::test]
    async fn document_read_budget_is_rejected_before_dsn_resolution() {
        for action in [
            DocAction::Find {
                collection: "events".to_owned(),
                filter: "{}".to_owned(),
                skip: None,
                sort: None,
                projection: None,
            },
            DocAction::Aggregate {
                collection: "events".to_owned(),
                pipeline: "[]".to_owned(),
            },
        ] {
            let mut ctx = test_context(false, "mongodb://127.0.0.1:1/app");
            ctx.dsn = None;
            ctx.max_bytes = 0;
            let error = run(&ctx, DocCmd { action }).await.unwrap_err();
            assert!(matches!(error, Error::Config(message) if message.contains("byte budget")));
        }
    }
}
