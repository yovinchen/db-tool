use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{error::Error, model::TimeRange, Result};

#[derive(Args)]
pub struct TsCmd {
    #[command(subcommand)]
    pub action: TsAction,
}

#[derive(Subcommand)]
pub enum TsAction {
    /// List metric names from a Prometheus-compatible backend.
    Measurements,
    /// Run a range query over the last N minutes.
    Query {
        query: String,
        #[arg(long, default_value = "60")]
        last_minutes: i64,
    },
}

pub async fn run(ctx: &Context, cmd: TsCmd) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let ts = conn
        .as_timeseries()
        .ok_or_else(|| Error::UnsupportedCapability {
            kind: conn.kind().0.clone(),
            needed: "TimeSeriesStore",
        })?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();

    Ok(match cmd.action {
        TsAction::Measurements => ctx.render_success(
            &kind,
            ts.list_measurements().await?,
            start.elapsed().as_millis() as u64,
            false,
        ),
        TsAction::Query {
            query,
            last_minutes,
        } => {
            let range = TimeRange::last_n_minutes(last_minutes);
            let result = ts.query_range(&query, range).await?;
            let truncated = result.truncated;
            ctx.render_success(&kind, result, start.elapsed().as_millis() as u64, truncated)
        }
    })
}
