mod cmd;

use clap::{Parser, Subcommand};
use dbtool_core::config::LimitsConfig;
use dbtool_core::service::{formatter::Format, FlowControl};
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

    /// Maximum in-process concurrent operations for this command
    #[arg(long, global = true)]
    max_concurrency: Option<usize>,

    /// Token-bucket rate limit, e.g. 50/s or 120/min
    #[arg(long, global = true)]
    rate: Option<String>,

    /// Maximum time to wait for rate/concurrency admission
    #[arg(long, global = true)]
    acquire_timeout: Option<String>,

    /// Per-request timeout, e.g. 500ms, 5s, or 1m
    #[arg(long, global = true)]
    request_timeout: Option<String>,

    /// Overall command deadline including admission and execution
    #[arg(long, global = true)]
    deadline: Option<String>,

    /// Retry budget for retry-capable embedded/core flows
    #[arg(long, global = true)]
    max_retries: Option<u32>,

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
        throttle_overrides: LimitsConfig {
            max_concurrency: cli.max_concurrency,
            rate: cli.rate,
            acquire_timeout: cli.acquire_timeout,
            request_timeout: cli.request_timeout,
            overall_deadline: cli.deadline,
            max_retries: cli.max_retries,
        },
        allow_write: cli.allow_write,
        confirm: cli.confirm,
    };

    let result = match cli.command {
        Commands::Conn(sub) => cmd::conn::run(&ctx, sub).await,
        command => match ctx.throttle_config() {
            Ok(config) => {
                FlowControl::new(config)
                    .run_single(run_data_command(&ctx, command))
                    .await
            }
            Err(err) => Err(err),
        },
    };

    match result {
        Ok(output) => println!("{output}"),
        Err(e) => {
            let output = ctx.render_error(&e);
            eprintln!("{output}");
            std::process::exit(1);
        }
    }
}

async fn run_data_command(ctx: &cmd::Context, command: Commands) -> dbtool_core::Result<String> {
    match command {
        Commands::Ping => cmd::ping::run(ctx).await,
        Commands::Caps => cmd::caps::run(ctx).await,
        Commands::Sql(sub) => cmd::sql::run(ctx, sub).await,
        Commands::Kv(sub) => cmd::kv::run(ctx, sub).await,
        Commands::Doc(sub) => cmd::doc::run(ctx, sub).await,
        Commands::Ts(sub) => cmd::ts::run(ctx, sub).await,
        Commands::Search(sub) => cmd::search::run(ctx, sub).await,
        Commands::Mq(sub) => cmd::mq::run(ctx, sub).await,
        Commands::Conn(_) => Err(dbtool_core::Error::Internal(
            "conn command is not a data operation".to_owned(),
        )),
    }
}
