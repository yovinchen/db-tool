use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    service::{limiter::ResultLimiter, safety::SafetyGuard},
    Result,
};

#[derive(Args)]
pub struct SqlCmd {
    #[command(subcommand)]
    pub action: SqlAction,
}

#[derive(Subcommand)]
pub enum SqlAction {
    /// Execute a SELECT and return results
    Query {
        sql: String,
        #[arg(long)]
        schema: Option<String>,
    },
    /// Execute a non-SELECT statement
    Exec { sql: String },
    /// List tables in the current database / schema
    Tables {
        #[arg(long)]
        schema: Option<String>,
    },
    /// Describe a table's columns and indexes
    Schema { table: String },
}

pub async fn run(ctx: &Context, cmd: SqlCmd) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let target = ctx.safety_target(&dsn);

    match &cmd.action {
        SqlAction::Query { sql, .. } | SqlAction::Exec { sql } => {
            SafetyGuard::check_with_target(sql, &target, ctx.allow_write, ctx.confirm.as_deref())?;
        }
        SqlAction::Tables { .. } | SqlAction::Schema { .. } => {}
    }

    let conn = ctx.registry.connect(&dsn).await?;
    let sql_engine = conn.as_sql().ok_or_else(|| Error::UnsupportedCapability {
        kind: conn.kind().0.clone(),
        needed: "SqlEngine",
    })?;

    let start = std::time::Instant::now();

    let output = match cmd.action {
        SqlAction::Query { sql, .. } => {
            let rs = sql_engine.query(&sql, &[]).await?;
            let rs = ResultLimiter::new(ctx.limit).apply(rs);
            let truncated = rs.truncated;
            ctx.render_success(
                conn.kind().0.as_str(),
                rs,
                start.elapsed().as_millis() as u64,
                truncated,
            )
        }
        SqlAction::Exec { sql } => {
            let outcome = sql_engine.execute(&sql, &[]).await?;
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
    };

    Ok(output)
}
