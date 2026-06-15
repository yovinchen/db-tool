mod app;
mod state;
mod ui;

use app::App;
use dbtool_registry::build_registry;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let registry = Arc::new(build_registry());
    let mut app = App::new(registry);
    app.run().await
}
