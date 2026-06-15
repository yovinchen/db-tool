use super::Context;
use dbtool_core::Result;

pub async fn run(ctx: &Context) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let caps = conn.capabilities();
    Ok(ctx.render_success(conn.kind().0.as_str(), caps, 0, false))
}
