use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    dsn::Dsn,
    error::Error,
    model::Value,
    service::{
        limiter::ResultLimiter,
        safety::{SafetyGuard, StatementKind},
    },
    Result,
};

#[derive(Args)]
#[command(
    about = "Run SQL queries, writes, and schema inspection commands.",
    long_about = "SQL commands use the shared safety path: read queries run directly, writes require --allow-write, and destructive statements may return a target-bound confirmation token."
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
        /// Optional schema/database qualifier (currently unsupported for query execution).
        #[arg(long)]
        schema: Option<String>,
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
    /// List tables in the current database / schema
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
    /// List schemas (databases) available on the backend
    Schemas,
}

pub async fn run(ctx: &Context, cmd: SqlCmd) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let target = ctx.safety_target(&dsn);

    let parsed_params = match &cmd.action {
        SqlAction::Query { params, .. } | SqlAction::Exec { params, .. } => {
            Some(parse_sql_params(params)?)
        }
        SqlAction::Tables { .. } | SqlAction::Schema { .. } | SqlAction::Schemas => None,
    };

    match &cmd.action {
        SqlAction::Query { sql, .. } | SqlAction::Exec { sql, .. } => {
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

    if matches!(
        &cmd.action,
        SqlAction::Query {
            schema: Some(_),
            ..
        }
    ) {
        return Err(Error::UnsupportedCapability {
            kind: Dsn::parse(&dsn)?.scheme,
            needed: "SqlQuerySchema",
        });
    }

    let conn = ctx.registry.connect(&dsn).await?;
    let sql_engine = conn.as_sql().ok_or_else(|| Error::UnsupportedCapability {
        kind: conn.kind().0.clone(),
        needed: "SqlEngine",
    })?;

    let start = std::time::Instant::now();

    let output = match cmd.action {
        SqlAction::Query { sql, .. } => {
            let rs = sql_engine
                .query(&sql, parsed_params.as_deref().unwrap_or_default())
                .await?;
            let rs = ResultLimiter::new(ctx.limit).apply(rs);
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
            let tables = sql_engine.list_tables(schema.as_deref()).await?;
            ctx.render_success(
                conn.kind().0.as_str(),
                tables,
                start.elapsed().as_millis() as u64,
                false,
            )
        }
        SqlAction::Schema { table } => {
            let schema = sql_engine.describe_table(&table).await?;
            ctx.render_success(
                conn.kind().0.as_str(),
                schema,
                start.elapsed().as_millis() as u64,
                false,
            )
        }
        SqlAction::Schemas => {
            let schemas = sql_engine.list_schemas().await?;
            ctx.render_success(
                conn.kind().0.as_str(),
                schemas,
                start.elapsed().as_millis() as u64,
                false,
            )
        }
    };

    Ok(output)
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
}
