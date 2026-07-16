use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    port::CapabilityOperation,
    service::{
        limiter::ListLimiter,
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
    let read_budget = match &cmd.action {
        CqlAction::Query { .. } => Some(ctx.read_budget()?),
        _ => None,
    };
    let metadata_budget = match &cmd.action {
        CqlAction::Query { .. } => None,
        CqlAction::Keyspaces | CqlAction::Tables { .. } => {
            ListLimiter::new(ctx.limit).probe_items()?;
            None
        }
        CqlAction::Schema { .. } => Some(ctx.metadata_budget()?),
        CqlAction::Exec { .. } => None,
    };
    let dsn = ctx.resolve_dsn()?;
    let target = ctx.safety_target(&dsn);

    match &cmd.action {
        CqlAction::Query { cql } => ensure_readonly_query(cql)?,
        CqlAction::Exec { cql } => ensure_exec_allowed(ctx, cql, &target)?,
        CqlAction::Keyspaces | CqlAction::Tables { .. } | CqlAction::Schema { .. } => {}
    }

    let conn = ctx.registry.connect(&dsn).await?;
    let operations = conn.operations();
    let kind = conn.kind().0.clone();
    if let Some((operation, needed)) = cql_operation_for_action(&cmd.action) {
        require_cql_operation(&operations, operation, &kind, needed)?;
    }
    let cql = conn.as_cql().ok_or_else(|| Error::UnsupportedCapability {
        kind: kind.clone(),
        needed: "CqlEngine",
    })?;
    let start = std::time::Instant::now();
    let elapsed = || start.elapsed().as_millis() as u64;

    Ok(match cmd.action {
        CqlAction::Query { cql: query } => {
            let result = cql
                .query_cql_budgeted(
                    &query,
                    read_budget.expect("query actions construct a read budget"),
                )
                .await?;
            let truncated = result.truncated;
            ctx.render_success(&kind, result, elapsed(), truncated)
        }
        CqlAction::Exec { cql: statement } => {
            let outcome = cql.execute_cql(&statement).await?;
            ctx.render_success(&kind, outcome, elapsed(), false)
        }
        CqlAction::Keyspaces => {
            let keyspaces = cql.list_keyspaces_bounded(ctx.limit).await?;
            let truncated = keyspaces.truncated;
            ctx.render_success(&kind, keyspaces.items, elapsed(), truncated)
        }
        CqlAction::Tables { keyspace } => {
            let tables = cql
                .list_cql_tables_bounded(keyspace.as_deref(), ctx.limit)
                .await?;
            let truncated = tables.truncated;
            ctx.render_success(&kind, tables.items, elapsed(), truncated)
        }
        CqlAction::Schema { table, keyspace } => {
            let table = cql_table_ref(&table, keyspace.as_deref())?;
            let schema = cql
                .describe_cql_table_bounded(
                    &table,
                    metadata_budget.expect("schema actions construct a metadata budget"),
                )
                .await?;
            ctx.render_success(&kind, schema, elapsed(), false)
        }
    })
}

fn cql_operation_for_action(action: &CqlAction) -> Option<(CapabilityOperation, &'static str)> {
    match action {
        CqlAction::Query { .. } => Some((
            CapabilityOperation::CqlQueryBudgeted,
            "CqlEngine.query_cql_budgeted",
        )),
        CqlAction::Keyspaces => Some((
            CapabilityOperation::CqlListKeyspacesBounded,
            "CqlEngine.list_keyspaces_bounded",
        )),
        CqlAction::Tables { .. } => Some((
            CapabilityOperation::CqlListTablesBounded,
            "CqlEngine.list_cql_tables_bounded",
        )),
        CqlAction::Exec { .. } => Some((CapabilityOperation::CqlExecute, "CqlEngine.execute_cql")),
        CqlAction::Schema { .. } => Some((
            CapabilityOperation::CqlDescribeTableBounded,
            "CqlEngine.describe_cql_table_bounded",
        )),
    }
}

fn require_cql_operation(
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
            max_bytes: dbtool_core::model::DEFAULT_READ_BYTES,
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

    #[test]
    fn cql_catalogs_require_explicit_bounded_operations_without_fallback() {
        assert!(matches!(
            require_cql_operation(
                CapabilityOperation::CQL,
                CapabilityOperation::CqlListKeyspacesBounded,
                "legacy-cql",
                "CqlEngine.list_keyspaces_bounded",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "CqlEngine.list_keyspaces_bounded"
        ));

        let mut operations = CapabilityOperation::CQL.to_vec();
        operations.push(CapabilityOperation::CqlListTablesBounded);
        assert!(require_cql_operation(
            &operations,
            CapabilityOperation::CqlListTablesBounded,
            "cassandra",
            "CqlEngine.list_cql_tables_bounded",
        )
        .is_ok());

        assert!(matches!(
            require_cql_operation(
                CapabilityOperation::CQL,
                CapabilityOperation::CqlDescribeTableBounded,
                "legacy-cql",
                "CqlEngine.describe_cql_table_bounded",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "CqlEngine.describe_cql_table_bounded"
        ));
        assert_eq!(
            cql_operation_for_action(&CqlAction::Schema {
                table: "users".to_owned(),
                keyspace: Some("app".to_owned()),
            }),
            Some((
                CapabilityOperation::CqlDescribeTableBounded,
                "CqlEngine.describe_cql_table_bounded",
            ))
        );
    }

    #[tokio::test]
    async fn cql_schema_budget_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context(false);
        ctx.limit = usize::MAX;
        let error = run(
            &ctx,
            CqlCmd {
                action: CqlAction::Schema {
                    table: "users".to_owned(),
                    keyspace: Some("app".to_owned()),
                },
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::Config(message) if message.contains("too large")));
    }

    #[test]
    fn budgeted_cql_query_is_selected_before_engine_access() {
        let action = CqlAction::Query {
            cql: "select now() from system.local".to_owned(),
        };
        assert_eq!(
            cql_operation_for_action(&action),
            Some((
                CapabilityOperation::CqlQueryBudgeted,
                "CqlEngine.query_cql_budgeted"
            ))
        );
        assert!(matches!(
            require_cql_operation(
                &[CapabilityOperation::CqlQuery],
                CapabilityOperation::CqlQueryBudgeted,
                "legacy-cql",
                "CqlEngine.query_cql_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "CqlEngine.query_cql_budgeted"
        ));
    }

    #[tokio::test]
    async fn cql_query_budget_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context(false);
        ctx.max_bytes = 0;
        let error = run(
            &ctx,
            CqlCmd {
                action: CqlAction::Query {
                    cql: "select now() from system.local".to_owned(),
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("byte budget")));
    }
}
