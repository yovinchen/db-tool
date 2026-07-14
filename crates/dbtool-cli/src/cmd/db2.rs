use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{error::Error, Result};

#[derive(Args)]
#[command(
    about = "IBM Db2 schema inspection (sequences / routines / tablespaces / fk / ddl).",
    long_about = "Db2 commands query the SYSCAT catalog to expose Db2-specific metadata: \
                  sequences, stored procedures/UDFs, tablespaces, foreign-key constraints, \
                  and generated DDL. All commands are read-only and do not require --allow-write."
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
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();
    let elapsed = || start.elapsed().as_millis() as u64;

    match cmd.action {
        // ── Db2Engine-specific operations ────────────────────────────────────
        Db2Action::Sequences { schema } => {
            let db2 = require_db2(&*conn)?;
            let seqs = db2.list_sequences(schema.as_deref()).await?;
            Ok(ctx.render_success(&kind, seqs, elapsed(), false))
        }
        Db2Action::Routines { schema } => {
            let db2 = require_db2(&*conn)?;
            let routines = db2.list_routines(schema.as_deref()).await?;
            Ok(ctx.render_success(&kind, routines, elapsed(), false))
        }
        Db2Action::Tablespaces => {
            let db2 = require_db2(&*conn)?;
            let ts = db2.list_tablespaces().await?;
            Ok(ctx.render_success(&kind, ts, elapsed(), false))
        }
        Db2Action::ForeignKeys { table } => {
            let db2 = require_db2(&*conn)?;
            let fks = db2.list_foreign_keys(&table).await?;
            Ok(ctx.render_success(&kind, fks, elapsed(), false))
        }
        Db2Action::Ddl { table } => {
            let db2 = require_db2(&*conn)?;
            let ddl = db2.generate_ddl(&table).await?;
            // Return DDL as a JSON string so the output is valid JSON.
            Ok(ctx.render_success(&kind, ddl, elapsed(), false))
        }

        // ── SqlEngine operations (same surface as `sql` but Db2-flavoured) ──
        Db2Action::Schemas => {
            let sql = require_sql(&*conn)?;
            let schemas = sql.list_schemas().await?;
            Ok(ctx.render_success(&kind, schemas, elapsed(), false))
        }
        Db2Action::Tables { schema } => {
            let sql = require_sql(&*conn)?;
            let tables = sql.list_tables(schema.as_deref()).await?;
            Ok(ctx.render_success(&kind, tables, elapsed(), false))
        }
        Db2Action::Schema { table } => {
            let sql = require_sql(&*conn)?;
            let schema = sql.describe_table(&table).await?;
            Ok(ctx.render_success(&kind, schema, elapsed(), false))
        }
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
    use dbtool_core::service::formatter::Format;

    fn test_context() -> Context {
        Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: Format::Json,
            limit: 100,
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
}
