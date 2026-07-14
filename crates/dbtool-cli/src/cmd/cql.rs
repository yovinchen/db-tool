use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    service::{
        limiter::ResultLimiter,
        safety::{SafetyGuard, StatementKind},
    },
    Result,
};

#[derive(Args)]
#[command(
    about = "Run Cassandra/ScyllaDB CQL commands.",
    long_about = "CQL commands expose Cassandra-specific keyspace/table wording while reusing dbtool's JSON output, result limits, timeouts, and safety path. CQL writes and DDL use cql exec, require --allow-write, and destructive statements require target-bound confirmation."
)]
pub struct CqlCmd {
    #[command(subcommand)]
    pub action: CqlAction,
}

#[derive(Subcommand)]
pub enum CqlAction {
    /// Execute a read-only CQL query and return rows.
    Query {
        /// CQL SELECT statement to execute.
        cql: String,
    },
    /// Execute a CQL write or DDL statement.
    Exec {
        /// CQL write or DDL statement; requires --allow-write.
        cql: String,
    },
    /// List keyspaces.
    Keyspaces,
    /// List tables, optionally within one keyspace.
    Tables {
        /// Keyspace to inspect. Defaults to the DSN keyspace when present.
        #[arg(long)]
        keyspace: Option<String>,
    },
    /// Describe a table's CQL columns.
    Schema {
        /// Table name. Use keyspace.table or pass --keyspace.
        table: String,
        /// Keyspace qualifier for an unqualified table name.
        #[arg(long)]
        keyspace: Option<String>,
    },
}

pub async fn run(ctx: &Context, cmd: CqlCmd) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let target = ctx.safety_target(&dsn);

    match &cmd.action {
        CqlAction::Query { cql } => ensure_readonly_query(cql)?,
        CqlAction::Exec { cql } => ensure_exec_allowed(ctx, cql, &target)?,
        CqlAction::Keyspaces | CqlAction::Tables { .. } | CqlAction::Schema { .. } => {}
    }

    let conn = ctx.registry.connect(&dsn).await?;
    let cql = conn.as_cql().ok_or_else(|| Error::UnsupportedCapability {
        kind: conn.kind().0.clone(),
        needed: "CqlEngine",
    })?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();
    let elapsed = || start.elapsed().as_millis() as u64;

    Ok(match cmd.action {
        CqlAction::Query { cql: query } => {
            let result = cql.query_cql(&query).await?;
            let result = ResultLimiter::new(ctx.limit).apply(result);
            let truncated = result.truncated;
            ctx.render_success(&kind, result, elapsed(), truncated)
        }
        CqlAction::Exec { cql: statement } => {
            let outcome = cql.execute_cql(&statement).await?;
            ctx.render_success(&kind, outcome, elapsed(), false)
        }
        CqlAction::Keyspaces => {
            let keyspaces = cql.list_keyspaces().await?;
            ctx.render_success(&kind, keyspaces, elapsed(), false)
        }
        CqlAction::Tables { keyspace } => {
            let tables = cql.list_cql_tables(keyspace.as_deref()).await?;
            ctx.render_success(&kind, tables, elapsed(), false)
        }
        CqlAction::Schema { table, keyspace } => {
            let table = cql_table_ref(&table, keyspace.as_deref())?;
            let schema = cql.describe_cql_table(&table).await?;
            ctx.render_success(&kind, schema, elapsed(), false)
        }
    })
}

fn cql_table_ref(table: &str, keyspace: Option<&str>) -> Result<String> {
    if table.contains('.') || keyspace.is_none() {
        return Ok(table.to_owned());
    }

    let keyspace = keyspace.expect("checked is_some");
    if keyspace.contains('.') {
        return Err(Error::Config(
            "CQL keyspace must not contain a dot when --keyspace is used".to_owned(),
        ));
    }
    Ok(format!("{keyspace}.{table}"))
}

fn ensure_exec_allowed(ctx: &Context, cql: &str, target: &str) -> Result<()> {
    ctx.ensure_write_allowed()?;
    SafetyGuard::check_with_target(cql, target, ctx.allow_write, ctx.confirm.as_deref())?;
    Ok(())
}

fn ensure_readonly_query(cql: &str) -> Result<()> {
    match SafetyGuard::check(cql, false, None) {
        Ok(StatementKind::Read) => Ok(()),
        _ => Err(Error::WriteNotAllowed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::service::formatter::Format;

    fn test_context(allow_write: bool) -> Context {
        Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: Format::Json,
            limit: 100,
            throttle_overrides: Default::default(),
            allow_write,
            confirm: None,
        }
    }

    #[test]
    fn cql_exec_requires_write_flag() {
        assert!(matches!(
            ensure_exec_allowed(
                &test_context(false),
                "insert into app.users (id) values (1)",
                "dsn:cassandra://localhost/app"
            ),
            Err(Error::WriteNotAllowed)
        ));
        assert!(ensure_exec_allowed(
            &test_context(true),
            "insert into app.users (id) values (1)",
            "dsn:cassandra://localhost/app"
        )
        .is_ok());
    }

    #[test]
    fn cql_destructive_exec_requires_target_bound_confirmation() {
        let target = "dsn:cassandra://localhost/app";
        let mut ctx = test_context(true);
        let token = match ensure_exec_allowed(&ctx, "drop table app.users", target).unwrap_err() {
            Error::ConfirmRequired { confirm_token, .. } => confirm_token,
            other => panic!("expected confirmation requirement, got {other:?}"),
        };

        ctx.confirm = Some(token);
        assert!(ensure_exec_allowed(&ctx, "drop table app.users", target).is_ok());
        assert!(matches!(
            ensure_exec_allowed(&ctx, "drop table app.users", "dsn:cassandra://other/app"),
            Err(Error::Internal(_))
        ));
    }

    #[test]
    fn cql_query_rejects_write_and_ddl_statements() {
        assert!(ensure_readonly_query("select * from system.local").is_ok());
        assert!(matches!(
            ensure_readonly_query("insert into app.users (id) values (1)"),
            Err(Error::WriteNotAllowed)
        ));
        assert!(matches!(
            ensure_readonly_query("/* hidden */ truncate app.users"),
            Err(Error::WriteNotAllowed)
        ));
    }

    #[test]
    fn cql_schema_target_accepts_keyspace_or_qualified_table() {
        assert_eq!(
            cql_table_ref("users", Some("app")).unwrap(),
            "app.users".to_owned()
        );
        assert_eq!(
            cql_table_ref("app.users", Some("ignored")).unwrap(),
            "app.users".to_owned()
        );
        assert!(matches!(
            cql_table_ref("users", Some("bad.keyspace")),
            Err(Error::Config(message)) if message.contains("must not contain a dot")
        ));
    }
}
