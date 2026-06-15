use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    config::{env::discover_env_connections, ConnectionConfig},
    dsn::redact_dsn,
    Result,
};

#[derive(Args)]
pub struct ConnCmd {
    #[command(subcommand)]
    pub action: ConnAction,
}

#[derive(Subcommand)]
pub enum ConnAction {
    /// List all configured connections
    List,
}

pub async fn run(ctx: &Context, cmd: ConnCmd) -> Result<String> {
    Ok(match cmd.action {
        ConnAction::List => {
            let schemes = ctx.registry.supported_schemes();
            let config_path = ConnectionConfig::default_path();
            let config = ConnectionConfig::load(&config_path)?;
            let mut env_connections: Vec<_> = discover_env_connections().keys().cloned().collect();
            env_connections.sort();

            let mut file_connections: Vec<_> = config
                .connections
                .iter()
                .map(|(name, entry)| {
                    serde_json::json!({
                        "name": name,
                        "dsn": redact_dsn(&entry.dsn),
                        "readonly": entry.readonly.unwrap_or(false),
                    })
                })
                .collect();
            file_connections.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

            ctx.render_success(
                "registry",
                serde_json::json!({
                    "supported_schemes": schemes,
                    "config_path": config_path,
                    "env_connections": env_connections,
                    "file_connections": file_connections,
                }),
                0,
                false,
            )
        }
    })
}
