mod app;
mod state;
mod ui;

use app::App;
use dbtool_registry::build_registry;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let registry = Arc::new(build_registry());
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{}", App::help_text());
        return Ok(());
    }
    let mut app = App::new(registry);
    if args.iter().any(|arg| arg == "--smoke") {
        println!("{}", app.smoke_summary());
        return Ok(());
    }
    app.run().await
}
