use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::Value,
    port::CapabilityOperation,
    service::safety::{SafetyGuard, StatementKind},
    Result,
};

#[derive(Args)]
#[command(
    about = "Run SQL queries, writes, and schema inspection commands.",
    long_about = "SQL commands use the shared safety path: read queries run directly, writes require --allow-write, and destructive statements may return a target-bound confirmation token. Table and schema lists honor both the global --limit item budget and --max-bytes response budget, and report truncation in JSON metadata."
)]
pub struct SqlCmd {
    #[command(subcommand)]
    pub action: SqlAction,
}

#[derive(Subcommand)]
pub enum SqlAction {
    /// Execute a SELECT and return results
    Query {
        /// SQL SELECT or read-only statement to execute.
        sql: String,
        /// JSON array of bound values: null, bool, i64, finite float, string,
        /// {"$bytes":[0,255]}, {"$timestamp":<epoch_ms>}, or {"$json":...}.
        #[arg(long, default_value = "[]", value_name = "JSON_ARRAY")]
        params: String,
    },
    /// Execute a non-SELECT statement
    Exec {
        /// SQL write or DDL statement; requires --allow-write.
        sql: String,
        /// JSON array of bound values: null, bool, i64, finite float, string,
        /// {"$bytes":[0,255]}, {"$timestamp":<epoch_ms>}, or {"$json":...}.
        #[arg(long, default_value = "[]", value_name = "JSON_ARRAY")]
        params: String,
    },
    /// List a bounded set of tables and views in the current database / schema
    Tables {
        /// Optional schema/database qualifier.
        #[arg(long)]
        schema: Option<String>,
    },
    /// Describe a table's columns and indexes
    Schema {
        /// Table name, optionally schema-qualified where the backend supports it.
        table: String,
    },
    /// List a bounded set of schemas (databases) available on the backend
    Schemas,
}

pub async fn run(ctx: &Context, cmd: SqlCmd) -> Result<String> {
    let read_budget = match &cmd.action {
        SqlAction::Query { .. } | SqlAction::Tables { .. } | SqlAction::Schemas => {
            Some(ctx.read_budget()?)
        }
        _ => None,
    };
    let metadata_budget = match &cmd.action {
        SqlAction::Query { .. } | SqlAction::Tables { .. } | SqlAction::Schemas => None,
        SqlAction::Schema { .. } => Some(ctx.metadata_budget()?),
        SqlAction::Exec { .. } => None,
    };
    let dsn = ctx.resolve_dsn()?;
    let target = ctx.confirmation_target(&dsn)?;

    let parsed_params = match &cmd.action {
        SqlAction::Query { params, .. } | SqlAction::Exec { params, .. } => {
            Some(parse_sql_params(params)?)
        }
        SqlAction::Tables { .. } | SqlAction::Schema { .. } | SqlAction::Schemas => None,
    };

    match &cmd.action {
        SqlAction::Query { sql, .. } => ensure_readonly_query(sql)?,
        SqlAction::Exec { sql, .. } => {
            let kind = SafetyGuard::check_with_target(
                sql,
                &target,
                ctx.allow_write,
                ctx.confirm.as_deref(),
            )?;
            if kind != StatementKind::Read {
                ctx.ensure_write_allowed()?;
            }
        }
        SqlAction::Tables { .. } | SqlAction::Schema { .. } | SqlAction::Schemas => {}
    }

    let conn = ctx.registry.connect(&dsn).await?;
    let operations = conn.operations();
    let kind = conn.kind().0.clone();
    if let Some((operation, needed)) = sql_operation_for_action(&cmd.action) {
        require_sql_operation(&operations, operation, &kind, needed)?;
    }
    let sql_engine = conn.as_sql().ok_or_else(|| Error::UnsupportedCapability {
        kind: kind.clone(),
        needed: "SqlEngine",
    })?;

    let start = std::time::Instant::now();

    let output = match cmd.action {
        SqlAction::Query { sql, .. } => {
            let rs = sql_engine
                .query_budgeted(
                    &sql,
                    parsed_params.as_deref().unwrap_or_default(),
                    read_budget.expect("query actions construct a read budget"),
                )
                .await?;
            let truncated = rs.truncated;
            ctx.render_success(
                conn.kind().0.as_str(),
                rs,
                start.elapsed().as_millis() as u64,
                truncated,
            )
        }
        SqlAction::Exec { sql, .. } => {
            let outcome = sql_engine
                .execute(&sql, parsed_params.as_deref().unwrap_or_default())
                .await?;
            ctx.render_success(
                conn.kind().0.as_str(),
                outcome,
                start.elapsed().as_millis() as u64,
                false,
            )
        }
        SqlAction::Tables { schema } => {
            let tables = sql_engine
                .list_tables_budgeted(
                    schema.as_deref(),
                    read_budget.expect("table-list actions construct a read budget"),
                )
                .await?;
            let truncated = tables.truncated;
            ctx.render_success(
                conn.kind().0.as_str(),
                tables.items,
                start.elapsed().as_millis() as u64,
                truncated,
            )
        }
        SqlAction::Schema { table } => {
            let schema = sql_engine
                .describe_table_bounded(
                    &table,
                    metadata_budget.expect("schema actions construct a metadata budget"),
                )
                .await?;
            ctx.render_success(
                conn.kind().0.as_str(),
                schema,
                start.elapsed().as_millis() as u64,
                false,
            )
        }
        SqlAction::Schemas => {
            let schemas = sql_engine
                .list_schemas_budgeted(
                    read_budget.expect("schema-list actions construct a read budget"),
                )
                .await?;
            let truncated = schemas.truncated;
            ctx.render_success(
                conn.kind().0.as_str(),
                schemas.items,
                start.elapsed().as_millis() as u64,
                truncated,
            )
        }
    };

    Ok(output)
}

fn sql_operation_for_action(action: &SqlAction) -> Option<(CapabilityOperation, &'static str)> {
    match action {
        SqlAction::Query { .. } => Some((
            CapabilityOperation::SqlQueryBudgeted,
            "SqlEngine.query_budgeted",
        )),
        SqlAction::Tables { .. } => Some((
            CapabilityOperation::SqlListTablesBudgeted,
            "SqlEngine.list_tables_budgeted",
        )),
        SqlAction::Schemas => Some((
            CapabilityOperation::SqlListSchemasBudgeted,
            "SqlEngine.list_schemas_budgeted",
        )),
        SqlAction::Exec { .. } => Some((CapabilityOperation::SqlExecute, "SqlEngine.execute")),
        SqlAction::Schema { .. } => Some((
            CapabilityOperation::SqlDescribeTableBounded,
            "SqlEngine.describe_table_bounded",
        )),
    }
}

fn require_sql_operation(
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

fn parse_sql_params(raw: &str) -> Result<Vec<Value>> {
    let json: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| Error::Config(format!("invalid --params JSON: {error}")))?;
    let params = json.as_array().ok_or_else(|| {
        Error::Config("--params must be a JSON array (for example: [1,\"alice\",null])".into())
    })?;

    params
        .iter()
        .enumerate()
        .map(|(index, value)| parse_sql_param(index, value))
        .collect()
}

fn ensure_readonly_query(sql: &str) -> Result<()> {
    match SafetyGuard::check(sql, false, None) {
        Ok(StatementKind::Read) => Ok(()),
        _ => Err(Error::WriteNotAllowed),
    }
}

fn parse_sql_param(index: usize, value: &serde_json::Value) -> Result<Value> {
    let position = index + 1;
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if value.as_u64().is_some() {
                Err(Error::Config(format!(
                    "--params item {position} is outside the supported i64 integer range"
                )))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(Error::Config(format!(
                    "--params item {position} is not a supported finite number"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::Text(value.clone())),
        serde_json::Value::Object(object)
            if object.len() == 1 && object.contains_key("$bytes") =>
        {
            let bytes = object["$bytes"].as_array().ok_or_else(|| {
                Error::Config(format!(
                    "--params item {position} must encode bytes as {{\"$bytes\":[0,255]}}"
                ))
            })?;
            bytes
                .iter()
                .enumerate()
                .map(|(byte_index, byte)| {
                    byte.as_u64()
                        .filter(|byte| *byte <= u8::MAX as u64)
                        .map(|byte| byte as u8)
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "--params item {position} byte {} must be an integer from 0 to 255",
                                byte_index + 1
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()
                .map(Value::Bytes)
        }
        serde_json::Value::Object(object)
            if object.len() == 1 && object.contains_key("$timestamp") =>
        {
            object["$timestamp"]
                .as_i64()
                .map(Value::Timestamp)
                .ok_or_else(|| {
                    Error::Config(format!(
                        "--params item {position} must encode a timestamp as {{\"$timestamp\":<epoch_ms_i64>}}"
                    ))
                })
        }
        serde_json::Value::Object(object)
            if object.len() == 1 && object.contains_key("$json") =>
        {
            Ok(Value::Json(object["$json"].clone()))
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(Error::Config(format!(
            "--params item {position} has an unsupported structured value; use a tagged {{\"$json\":...}} wrapper"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_params_parse_all_supported_json_types() {
        let params = parse_sql_params(
            r#"[null,true,-42,3.5,"O'Reilly",{"$bytes":[0,127,255]},{"$timestamp":1700000000123},{"$json":{"source":"test"}}]"#,
        )
        .unwrap();

        assert_eq!(
            params,
            vec![
                Value::Null,
                Value::Bool(true),
                Value::Int(-42),
                Value::Float(3.5),
                Value::Text("O'Reilly".into()),
                Value::Bytes(vec![0, 127, 255]),
                Value::Timestamp(1_700_000_000_123),
                Value::Json(serde_json::json!({"source": "test"})),
            ]
        );
    }

    #[test]
    fn sql_params_require_an_array_and_reject_ambiguous_structures() {
        for raw in [
            r#"{"id":1}"#,
            r#"[[1]]"#,
            r#"[{"bytes":[1]}]"#,
            r#"[{"$bytes":"AQI="}]"#,
            r#"[{"$bytes":[-1]}]"#,
            r#"[{"$bytes":[256]}]"#,
            r#"[{"$timestamp":"now"}]"#,
            r#"[{"$timestamp":18446744073709551615}]"#,
            r#"[{"$json":{},"extra":true}]"#,
            r#"[18446744073709551615]"#,
        ] {
            assert!(parse_sql_params(raw).is_err(), "{raw} should be rejected");
        }
    }

    #[test]
    fn metadata_lists_require_explicit_budgeted_operations_without_legacy_fallback() {
        let legacy_only = CapabilityOperation::SQL;
        assert!(matches!(
            require_sql_operation(
                legacy_only,
                CapabilityOperation::SqlListTablesBudgeted,
                "legacy-sql",
                "SqlEngine.list_tables_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "SqlEngine.list_tables_budgeted"
        ));

        let mut explicit = legacy_only.to_vec();
        explicit.extend([
            CapabilityOperation::SqlListSchemasBudgeted,
            CapabilityOperation::SqlListTablesBudgeted,
        ]);
        assert!(require_sql_operation(
            &explicit,
            CapabilityOperation::SqlListSchemasBudgeted,
            "sqlite",
            "SqlEngine.list_schemas_budgeted",
        )
        .is_ok());

        assert!(matches!(
            require_sql_operation(
                legacy_only,
                CapabilityOperation::SqlDescribeTableBounded,
                "legacy-sql",
                "SqlEngine.describe_table_bounded",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "SqlEngine.describe_table_bounded"
        ));
        assert_eq!(
            sql_operation_for_action(&SqlAction::Schema {
                table: "app.users".to_owned(),
            }),
            Some((
                CapabilityOperation::SqlDescribeTableBounded,
                "SqlEngine.describe_table_bounded",
            ))
        );
    }

    #[test]
    fn budgeted_sql_query_is_selected_before_engine_access() {
        let action = SqlAction::Query {
            sql: "select 1".to_owned(),
            params: "[]".to_owned(),
        };
        assert_eq!(
            sql_operation_for_action(&action),
            Some((
                CapabilityOperation::SqlQueryBudgeted,
                "SqlEngine.query_budgeted"
            ))
        );
        assert!(matches!(
            require_sql_operation(
                &[CapabilityOperation::SqlQuery],
                CapabilityOperation::SqlQueryBudgeted,
                "legacy-sql",
                "SqlEngine.query_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "SqlEngine.query_budgeted"
        ));
    }

    #[tokio::test]
    async fn sql_query_budget_is_rejected_before_dsn_resolution() {
        let ctx = Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: dbtool_core::service::formatter::Format::Json,
            limit: 1,
            max_bytes: 0,
            throttle_overrides: Default::default(),
            allow_write: false,
            confirm: None,
        };
        let error = run(
            &ctx,
            SqlCmd {
                action: SqlAction::Query {
                    sql: "select 1".to_owned(),
                    params: "[]".to_owned(),
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("byte budget")));
    }

    #[tokio::test]
    async fn sql_catalog_byte_budget_is_rejected_before_dsn_resolution() {
        let ctx = Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: dbtool_core::service::formatter::Format::Json,
            limit: 1,
            max_bytes: 0,
            throttle_overrides: Default::default(),
            allow_write: false,
            confirm: None,
        };
        let error = run(
            &ctx,
            SqlCmd {
                action: SqlAction::Schemas,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("byte budget")));
    }

    #[tokio::test]
    async fn sql_schema_budget_is_rejected_before_dsn_resolution() {
        let ctx = Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: dbtool_core::service::formatter::Format::Json,
            limit: usize::MAX,
            max_bytes: dbtool_core::model::DEFAULT_READ_BYTES,
            throttle_overrides: Default::default(),
            allow_write: false,
            confirm: None,
        };
        let error = run(
            &ctx,
            SqlCmd {
                action: SqlAction::Schema {
                    table: "app.users".to_owned(),
                },
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::Config(message) if message.contains("too large")));
    }
}
