use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error, port::capability::SearchOptions, service::formatter::Formatter, Result,
};

#[derive(Args)]
pub struct SearchCmd {
    #[command(subcommand)]
    pub action: SearchAction,
}

#[derive(Subcommand)]
pub enum SearchAction {
    Indices,
    Search {
        index: String,
        #[arg(long)]
        q: String,
    },
}

pub async fn run(ctx: &Context, cmd: SearchCmd) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let se = conn
        .as_search()
        .ok_or_else(|| Error::UnsupportedCapability {
            kind: conn.kind().0.clone(),
            needed: "SearchEngine",
        })?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();

    Ok(match cmd.action {
        SearchAction::Indices => Formatter::success(
            &kind,
            se.list_indices().await?,
            start.elapsed().as_millis() as u64,
            false,
        ),
        SearchAction::Search { index, q } => {
            let query: serde_json::Value =
                serde_json::from_str(&q).map_err(|e| Error::Serialization(e.to_string()))?;
            let opts = SearchOptions {
                size: Some(ctx.limit),
                ..Default::default()
            };
            let hits = se.search(&index, query.into(), opts).await?;
            Formatter::success(&kind, hits, start.elapsed().as_millis() as u64, false)
        }
    })
}
