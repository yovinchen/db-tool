use super::Context;
use dbtool_core::{service::formatter::Formatter, Result};

pub async fn run(ctx: &Context) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let start = std::time::Instant::now();
    conn.ping().await?;
    let elapsed = start.elapsed().as_millis() as u64;
    Ok(Formatter::success(
        conn.kind().0.as_str(),
        serde_json::json!({"status":"ok"}),
        elapsed,
        false,
    ))
}
