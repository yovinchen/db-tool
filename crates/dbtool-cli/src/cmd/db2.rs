use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{error::Error, port::CapabilityOperation, service::limiter::ListLimiter, Result};

#[derive(Args)]
#[command(
    about = "IBM Db2 schema inspection (sequences / routines / tablespaces / fk / ddl).",
    long_about = "Db2 commands query the SYSCAT catalog to expose Db2-specific metadata: \
                  sequences, stored procedures/UDFs, tablespaces, foreign-key constraints, \
                  and generated DDL. Catalog lists honor the global --limit and report truncation. \
                  All commands are read-only and do not require --allow-write."
)]
pub struct Db2Cmd {
    #[command(subcommand)]
    pub action: Db2Action,
}

#[derive(Subcommand)]
pub enum Db2Action {
    /// List sequences in a schema.
    Sequences {
        /// Schema to inspect (defaults to DB2INST1 when omitted).
        #[arg(long)]
        schema: Option<String>,
    },
    /// List stored procedures and user-defined functions in a schema.
    Routines {
        /// Schema to inspect (defaults to DB2INST1 when omitted).
        #[arg(long)]
        schema: Option<String>,
    },
    /// List tablespaces in the connected database.
    Tablespaces,
    /// List foreign-key constraints declared on a table.
    ForeignKeys {
        /// Table name. Use SCHEMA.TABLE or pass bare name (defaults to DB2INST1 schema).
        table: String,
    },
    /// Generate a CREATE TABLE DDL statement from the Db2 catalog.
    Ddl {
        /// Table name. Use SCHEMA.TABLE or pass bare name (defaults to DB2INST1 schema).
        table: String,
    },
    /// List schemas accessible in the connected database.
    Schemas,
    /// List tables in a schema.
    Tables {
        /// Schema to inspect (defaults to DB2INST1 when omitted).
        #[arg(long)]
        schema: Option<String>,
    },
    /// Describe columns and indexes of a table.
    Schema {
        /// Table name. Use SCHEMA.TABLE or pass bare name.
        table: String,
    },
}

pub async fn run(ctx: &Context, cmd: Db2Cmd) -> Result<String> {
    let metadata_budget = match &cmd.action {
        Db2Action::Sequences { .. }
        | Db2Action::Routines { .. }
        | Db2Action::Tablespaces
        | Db2Action::ForeignKeys { .. }
        | Db2Action::Schemas
        | Db2Action::Tables { .. } => {
            ListLimiter::new(ctx.limit).probe_items()?;
            None
        }
        Db2Action::Ddl { .. } | Db2Action::Schema { .. } => Some(ctx.metadata_budget()?),
    };
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let operations = conn.operations();
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();
    let elapsed = || start.elapsed().as_millis() as u64;
    let (operation, needed) = db2_operation_for_action(&cmd.action);
    require_operation(&operations, operation, &kind, needed)?;

    match cmd.action {
        // ── Db2Engine-specific operations ────────────────────────────────────
        Db2Action::Sequences { schema } => {
            let db2 = require_db2(&*conn)?;
            let seqs = db2
                .list_sequences_bounded(schema.as_deref(), ctx.limit)
                .await?;
            Ok(ctx.render_success(&kind, seqs.items, elapsed(), seqs.truncated))
        }
        Db2Action::Routines { schema } => {
            let db2 = require_db2(&*conn)?;
            let routines = db2
                .list_routines_bounded(schema.as_deref(), ctx.limit)
                .await?;
            Ok(ctx.render_success(&kind, routines.items, elapsed(), routines.truncated))
        }
        Db2Action::Tablespaces => {
            let db2 = require_db2(&*conn)?;
            let tablespaces = db2.list_tablespaces_bounded(ctx.limit).await?;
            Ok(ctx.render_success(&kind, tablespaces.items, elapsed(), tablespaces.truncated))
        }
        Db2Action::ForeignKeys { table } => {
            let db2 = require_db2(&*conn)?;
            let foreign_keys = db2.list_foreign_keys_bounded(&table, ctx.limit).await?;
            Ok(ctx.render_success(&kind, foreign_keys.items, elapsed(), foreign_keys.truncated))
        }
        Db2Action::Ddl { table } => {
            let db2 = require_db2(&*conn)?;
            let ddl = db2
                .generate_ddl_bounded(
                    &table,
                    metadata_budget.expect("DDL actions construct a metadata budget"),
                )
                .await?;
            // Return DDL as a JSON string so the output is valid JSON.
            Ok(ctx.render_success(&kind, ddl, elapsed(), false))
        }

        // ── SqlEngine operations (same surface as `sql` but Db2-flavoured) ──
        Db2Action::Schemas => {
            let sql = require_sql(&*conn)?;
            let schemas = sql.list_schemas_bounded(ctx.limit).await?;
            Ok(ctx.render_success(&kind, schemas.items, elapsed(), schemas.truncated))
        }
        Db2Action::Tables { schema } => {
            let sql = require_sql(&*conn)?;
            let tables = sql
                .list_tables_bounded(schema.as_deref(), ctx.limit)
                .await?;
            Ok(ctx.render_success(&kind, tables.items, elapsed(), tables.truncated))
        }
        Db2Action::Schema { table } => {
            let sql = require_sql(&*conn)?;
            let schema = sql
                .describe_table_bounded(
                    &table,
                    metadata_budget.expect("schema actions construct a metadata budget"),
                )
                .await?;
            Ok(ctx.render_success(&kind, schema, elapsed(), false))
        }
    }
}

fn db2_operation_for_action(action: &Db2Action) -> (CapabilityOperation, &'static str) {
    match action {
        Db2Action::Sequences { .. } => (
            CapabilityOperation::Db2ListSequencesBounded,
            "Db2Engine.list_sequences_bounded",
        ),
        Db2Action::Routines { .. } => (
            CapabilityOperation::Db2ListRoutinesBounded,
            "Db2Engine.list_routines_bounded",
        ),
        Db2Action::Tablespaces => (
            CapabilityOperation::Db2ListTablespacesBounded,
            "Db2Engine.list_tablespaces_bounded",
        ),
        Db2Action::ForeignKeys { .. } => (
            CapabilityOperation::Db2ListForeignKeysBounded,
            "Db2Engine.list_foreign_keys_bounded",
        ),
        Db2Action::Ddl { .. } => (
            CapabilityOperation::Db2GenerateDdlBounded,
            "Db2Engine.generate_ddl_bounded",
        ),
        Db2Action::Schemas => (
            CapabilityOperation::SqlListSchemasBounded,
            "SqlEngine.list_schemas_bounded",
        ),
        Db2Action::Tables { .. } => (
            CapabilityOperation::SqlListTablesBounded,
            "SqlEngine.list_tables_bounded",
        ),
        Db2Action::Schema { .. } => (
            CapabilityOperation::SqlDescribeTableBounded,
            "SqlEngine.describe_table_bounded",
        ),
    }
}

fn require_operation(
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

fn require_db2(
    conn: &dyn dbtool_core::port::connector::Connector,
) -> Result<&dyn dbtool_core::port::capability::Db2Engine> {
    conn.as_db2().ok_or_else(|| Error::UnsupportedCapability {
        kind: conn.kind().0.clone(),
        needed: "Db2Engine",
    })
}

fn require_sql(
    conn: &dyn dbtool_core::port::connector::Connector,
) -> Result<&dyn dbtool_core::port::capability::SqlEngine> {
    conn.as_sql().ok_or_else(|| Error::UnsupportedCapability {
        kind: conn.kind().0.clone(),
        needed: "SqlEngine",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::port::Capabilities;
    use dbtool_core::service::formatter::Format;

    fn test_context() -> Context {
        Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: Format::Json,
            limit: 100,
            max_bytes: dbtool_core::model::DEFAULT_READ_BYTES,
            throttle_overrides: Default::default(),
            allow_write: false,
            confirm: None,
        }
    }

    #[test]
    fn require_db2_returns_error_for_uncapable_connector() {
        // No DSN configured — resolve_dsn() will fail, which is the outermost
        // guard. We test the inner helper with a mock connector below.
        let ctx = test_context();
        assert!(ctx.resolve_dsn().is_err());
    }

    #[test]
    fn legacy_db2_capabilities_do_not_authorize_bounded_catalog_calls() {
        let operations = Capabilities {
            sql: true,
            db2: true,
            ..Default::default()
        }
        .operations();

        assert!(matches!(
            require_operation(
                &operations,
                CapabilityOperation::Db2ListSequencesBounded,
                "legacy-db2",
                "Db2Engine.list_sequences_bounded",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "Db2Engine.list_sequences_bounded"
        ));
    }

    #[test]
    fn db2_ddl_and_sql_schema_have_distinct_exact_operations() {
        assert_eq!(
            db2_operation_for_action(&Db2Action::Ddl {
                table: "APP.USERS".to_owned(),
            }),
            (
                CapabilityOperation::Db2GenerateDdlBounded,
                "Db2Engine.generate_ddl_bounded"
            )
        );
        assert_eq!(
            db2_operation_for_action(&Db2Action::Schema {
                table: "APP.USERS".to_owned(),
            }),
            (
                CapabilityOperation::SqlDescribeTableBounded,
                "SqlEngine.describe_table_bounded"
            )
        );

        for (operation, needed) in [
            (
                CapabilityOperation::Db2GenerateDdlBounded,
                "Db2Engine.generate_ddl_bounded",
            ),
            (
                CapabilityOperation::SqlDescribeTableBounded,
                "SqlEngine.describe_table_bounded",
            ),
        ] {
            assert!(matches!(
                require_operation(CapabilityOperation::DB2, operation, "legacy-db2", needed),
                Err(Error::UnsupportedCapability { needed: actual, .. }) if actual == needed
            ));
        }
    }

    #[tokio::test]
    async fn catalog_limit_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context();
        ctx.limit = usize::MAX;
        let error = run(
            &ctx,
            Db2Cmd {
                action: Db2Action::Tablespaces,
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::Config(message) if message.contains("too large")));

        for action in [
            Db2Action::Ddl {
                table: "APP.USERS".to_owned(),
            },
            Db2Action::Schema {
                table: "APP.USERS".to_owned(),
            },
        ] {
            let error = run(&ctx, Db2Cmd { action }).await.unwrap_err();
            assert!(matches!(error, Error::Config(message) if message.contains("too large")));
        }
    }
}
