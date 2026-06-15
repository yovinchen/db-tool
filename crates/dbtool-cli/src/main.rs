mod cmd;

use clap::{Parser, Subcommand};
use dbtool_core::service::formatter::{Format, Formatter};
use dbtool_registry::build_registry;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "dbtool",
    about = "Unified data & message connection tool",
    version
)]
struct Cli {
    /// Connection name (looks up env DBTOOL_CONN_<NAME> then connections.toml)
    #[arg(long, global = true)]
    conn: Option<String>,

    /// Raw DSN (takes precedence over --conn)
    #[arg(long, global = true)]
    dsn: Option<String>,

    /// Output format: json | table | ndjson
    #[arg(long, global = true, default_value = "json")]
    format: String,

    /// Maximum rows/messages to return
    #[arg(long, global = true, default_value = "100")]
    limit: usize,

    /// Allow write operations (INSERT / UPDATE / DELETE)
    #[arg(long, global = true)]
    allow_write: bool,

    /// Confirmation token for destructive operations (see CONFIRM_REQUIRED response)
    #[arg(long, global = true)]
    confirm: Option<String>,

    /// Enable verbose tracing output
    #[arg(long, global = true, short = 'v')]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Verify connectivity to a backend
    Ping,
    /// Show capabilities of a connection
    Caps,
    /// SQL operations (query / exec / tables / schema)
    Sql(cmd::sql::SqlCmd),
    /// Key-value operations (get / set / scan / raw)
    Kv(cmd::kv::KvCmd),
    /// Document store operations (find / insert / collections / aggregate)
    Doc(cmd::doc::DocCmd),
    /// Time-series operations (query / measurements)
    Ts(cmd::ts::TsCmd),
    /// Full-text search operations (search / indices)
    Search(cmd::search::SearchCmd),
    /// Message queue operations (produce / consume / topics / lag)
    Mq(cmd::mq::MqCmd),
    /// Manage named connections
    Conn(cmd::conn::ConnCmd),
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    let filter = if cli.verbose {
        "dbtool=debug,info"
    } else {
        "warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_writer(std::io::stderr)
        .init();

    let registry = build_registry();
    let ctx = cmd::Context {
        registry,
        conn: cli.conn,
        dsn: cli.dsn,
        format: cli.format.parse().unwrap_or(Format::Json),
        limit: cli.limit,
        allow_write: cli.allow_write,
        confirm: cli.confirm,
    };

    let result = match cli.command {
        Commands::Ping => cmd::ping::run(&ctx).await,
        Commands::Caps => cmd::caps::run(&ctx).await,
        Commands::Sql(sub) => cmd::sql::run(&ctx, sub).await,
        Commands::Kv(sub) => cmd::kv::run(&ctx, sub).await,
        Commands::Doc(sub) => cmd::doc::run(&ctx, sub).await,
        Commands::Ts(sub) => cmd::ts::run(&ctx, sub).await,
        Commands::Search(sub) => cmd::search::run(&ctx, sub).await,
        Commands::Mq(sub) => cmd::mq::run(&ctx, sub).await,
        Commands::Conn(sub) => cmd::conn::run(&ctx, sub).await,
    };

    match result {
        Ok(output) => println!("{output}"),
        Err(e) => {
            let output = Formatter::error(&e);
            eprintln!("{output}");
            std::process::exit(1);
        }
    }
}
