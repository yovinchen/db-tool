mod artifacts;
mod cmd;

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use dbtool_core::config::LimitsConfig;
use dbtool_core::model::{DEFAULT_INPUT_ITEM_BYTES, DEFAULT_READ_BYTES};
use dbtool_core::service::{formatter::Format, FlowControl};
use dbtool_registry::build_registry;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "dbtool",
    about = "Unified data & message connection tool",
    version,
    long_about = "dbtool provides one JSON-first CLI for SQL, CQL, key-value, document, messaging, search, and time-series backends. It resolves raw DSNs, named connections from connections.toml, and DBTOOL_CONN_* environment entries while keeping writes behind explicit flags.",
    after_help = "Examples:\n  dbtool --dsn sqlite::memory: ping\n  dbtool --conn local sql query \"select 1 as id\"\n  dbtool --dsn cassandra://localhost:9042/app cql tables --keyspace app\n  dbtool --dsn redis://localhost:6379 kv scan \"user:*\"\n  dbtool --conn local export sql --query \"select * from people\" --out people.json\n  dbtool --allow-write --conn local import sql --table people_copy --input people.json\n  dbtool --allow-write --dsn opensearch://localhost:9200 search index logs '{\"message\":\"ok\"}'"
)]
struct Cli {
    /// Connection name (looks up env DBTOOL_CONN_<NAME> then connections.toml)
    #[arg(long, global = true)]
    conn: Option<String>,

    /// Raw DSN (takes precedence over --conn)
    #[arg(long, global = true)]
    dsn: Option<String>,

    /// Output format: json | table | ndjson
    #[arg(long, global = true, default_value = "json", value_enum)]
    format: OutputFormat,

    /// Maximum rows/messages to return (must be greater than zero)
    #[arg(long, global = true, default_value = "100")]
    limit: usize,

    /// Maximum cumulative bytes in one bounded read response or write input batch
    #[arg(long, global = true, default_value_t = DEFAULT_READ_BYTES)]
    max_bytes: usize,

    /// Maximum bytes in one logical item of a portable mutation input
    #[arg(long, global = true, default_value_t = DEFAULT_INPUT_ITEM_BYTES)]
    max_item_bytes: usize,

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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Json,
    Table,
    Ndjson,
}

impl From<OutputFormat> for Format {
    fn from(value: OutputFormat) -> Self {
        match value {
            OutputFormat::Json => Self::Json,
            OutputFormat::Table => Self::Table,
            OutputFormat::Ndjson => Self::Ndjson,
        }
    }
}

#[derive(Args)]
struct GenerateArtifactsCmd {
    /// Output directory for completions/ and man/ artifacts.
    #[arg(long)]
    out_dir: PathBuf,
}

#[derive(Subcommand)]
enum Commands {
    /// Verify connectivity to a backend
    Ping,
    /// Show legacy capability families and exact method-level operation names
    Caps,
    /// SQL operations (query / exec / tables / schema)
    #[command(
        about = "SQL operations (query / exec / tables / schema)",
        long_about = "SQL commands use the shared safety path: read queries run directly, writes require --allow-write, and destructive statements may return a target-bound confirmation token."
    )]
    Sql(cmd::sql::SqlCmd),
    /// Cassandra/ScyllaDB CQL operations (query / exec / keyspaces / tables / schema)
    #[command(
        about = "CQL operations (query / exec / keyspaces / tables / schema)",
        long_about = "CQL commands expose Cassandra-specific keyspace and table operations while reusing dbtool's JSON output, result limits, timeouts, and write gate. CQL writes and DDL use cql exec and require --allow-write."
    )]
    Cql(cmd::cql::CqlCmd),
    /// IBM Db2 schema inspection (sequences / routines / tablespaces / fk / ddl)
    #[command(
        about = "IBM Db2 schema inspection (sequences / routines / tablespaces / fk / ddl)",
        long_about = "Db2 commands query the SYSCAT catalog to expose IBM Db2-specific metadata: sequences, stored procedures/UDFs, tablespaces, foreign-key constraints, and generated CREATE TABLE DDL. Use --dsn 'db2://user:pass@host:50000/DBNAME' or a named --conn."
    )]
    Db2(cmd::db2::Db2Cmd),
    /// Key-value operations (get / set / scan / raw)
    Kv(cmd::kv::KvCmd),
    /// Document store operations (find / insert / collections / aggregate)
    Doc(cmd::doc::DocCmd),
    /// Export rows, keys, or documents to a JSON artifact
    Export(cmd::transfer::ExportCmd),
    /// Import a dbtool JSON artifact
    Import(cmd::transfer::ImportCmd),
    /// Time-series operations (query / measurements)
    #[command(
        about = "Time-series operations (query / measurements / write)",
        long_about = "Time-series commands list metric names, run bounded range queries, and write single samples through Prometheus remote write behind --allow-write."
    )]
    Ts(cmd::ts::TsCmd),
    /// Full-text search operations and document/index lifecycle
    #[command(
        about = "Search operations (indices / search / index / put / get / update / delete / delete-index)",
        long_about = "Search commands use JSON request bodies and the global --limit against OpenSearch/Elasticsearch-compatible endpoints. Each document mutation requires --allow-write; delete-index additionally requires a target-bound confirmation token."
    )]
    Search(cmd::search::SearchCmd),
    /// Message queue operations (produce / consume / inspect / delete)
    #[command(
        about = "Message queue operations (produce / consume / inspect / delete)",
        long_about = "Message commands cover Kafka-compatible topics, AMQP/RabbitMQ queues, Redis Streams/PubSub, and NATS/JetStream where the selected connector exposes those capabilities. Produce requires --allow-write; AMQP consume also requires write permission because it ACKs deliveries; persistent resource deletion additionally requires a target-bound confirmation token."
    )]
    Mq(cmd::mq::MqCmd),
    /// Manage named connections
    #[command(
        about = "Manage named connections",
        long_about = "Connection commands list DBTOOL_CONN_* environment entries and atomically add, replace, or remove file-managed entries in the default connections.toml. Commands redact DSNs in every output; configuration writes require --allow-write, while replacement and removal also require target- and content-bound confirmation."
    )]
    Conn(cmd::conn::ConnCmd),
    /// Generate release artifact files from clap command metadata
    #[command(name = "generate-artifacts", hide = true)]
    GenerateArtifacts(GenerateArtifactsCmd),
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    if let Commands::GenerateArtifacts(cmd) = &cli.command {
        if let Err(err) = artifacts::write_cli_artifacts(Cli::command(), &cmd.out_dir) {
            eprintln!("failed to generate CLI artifacts: {err}");
            std::process::exit(1);
        }
        for path in artifacts::artifact_paths(&cmd.out_dir) {
            println!("{}", path.display());
        }
        return;
    }

    let filter = if cli.verbose {
        "dbtool=debug,info"
    } else {
        // JSON is the default machine contract. Dependency warnings written
        // before the final envelope would make stderr impossible to parse.
        // Operators who want diagnostic logs opt in with --verbose.
        "off"
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
        format: cli.format.into(),
        limit: cli.limit,
        max_bytes: cli.max_bytes,
        max_item_bytes: cli.max_item_bytes,
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
                let flow = FlowControl::new(config);
                let mutation_timeout_subject = mutation_timeout_subject(&command);
                let operation = run_data_command(&ctx, command);
                if let Some(subject) = mutation_timeout_subject {
                    flow.run_single_mutation(subject, operation).await
                } else {
                    flow.run_single(operation).await
                }
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

fn mutation_timeout_subject(command: &Commands) -> Option<&'static str> {
    match command {
        Commands::Sql(cmd::sql::SqlCmd { action }) if cmd::sql::action_may_mutate(action) => {
            Some("SQL execute")
        }
        Commands::Cql(cmd::cql::CqlCmd {
            action: cmd::cql::CqlAction::Exec { .. },
        }) => Some("CQL execute"),
        Commands::Kv(cmd::kv::KvCmd { action }) if cmd::kv::action_may_mutate(action) => {
            Some("key-value mutation")
        }
        Commands::Doc(cmd::doc::DocCmd { action }) if cmd::doc::action_may_mutate(action) => {
            Some("document mutation")
        }
        Commands::Import(_) => Some("data import"),
        Commands::Ts(cmd::ts::TsCmd {
            action: cmd::ts::TsAction::Write { .. },
        }) => Some("time-series write"),
        Commands::Search(cmd::search::SearchCmd { action })
            if cmd::search::action_may_mutate(action) =>
        {
            Some("search mutation")
        }
        Commands::Mq(cmd::mq::MqCmd { action }) if cmd::mq::action_may_mutate(action) => {
            Some("message mutation")
        }
        _ => None,
    }
}

async fn run_data_command(ctx: &cmd::Context, command: Commands) -> dbtool_core::Result<String> {
    ctx.ensure_positive_limit()?;
    ctx.ensure_read_byte_budget()?;
    match command {
        Commands::Ping => cmd::ping::run(ctx).await,
        Commands::Caps => cmd::caps::run(ctx).await,
        Commands::Sql(sub) => cmd::sql::run(ctx, sub).await,
        Commands::Cql(sub) => cmd::cql::run(ctx, sub).await,
        Commands::Db2(sub) => cmd::db2::run(ctx, sub).await,
        Commands::Kv(sub) => cmd::kv::run(ctx, sub).await,
        Commands::Doc(sub) => cmd::doc::run(ctx, sub).await,
        Commands::Export(sub) => cmd::transfer::run_export(ctx, sub).await,
        Commands::Import(sub) => cmd::transfer::run_import(ctx, sub).await,
        Commands::Ts(sub) => cmd::ts::run(ctx, sub).await,
        Commands::Search(sub) => cmd::search::run(ctx, sub).await,
        Commands::Mq(sub) => cmd::mq::run(ctx, sub).await,
        Commands::GenerateArtifacts(_) => Err(dbtool_core::Error::Internal(
            "generate-artifacts command is not a data operation".to_owned(),
        )),
        Commands::Conn(_) => Err(dbtool_core::Error::Internal(
            "conn command is not a data operation".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed_command(args: &[&str]) -> Commands {
        Cli::try_parse_from(args)
            .expect("test CLI should parse")
            .command
    }

    #[test]
    fn global_input_item_budget_is_documented_and_parsed() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("--max-item-bytes"));
        assert!(help.contains("logical item"));

        let parsed = Cli::try_parse_from([
            "dbtool",
            "--max-item-bytes",
            "4096",
            "--dsn",
            "sqlite::memory:",
            "ping",
        ])
        .unwrap();
        assert_eq!(parsed.max_item_bytes, 4096);
        assert_eq!(parsed.limit, 100);
        assert_eq!(parsed.max_bytes, DEFAULT_READ_BYTES);
    }

    #[test]
    fn every_mutation_surface_uses_indeterminate_outer_timeout_semantics() {
        let produce = parsed_command(&[
            "dbtool",
            "--dsn",
            "kafka://127.0.0.1:9092",
            "mq",
            "produce",
            "events",
            "payload",
        ]);
        assert_eq!(mutation_timeout_subject(&produce), Some("message mutation"));

        let consume = parsed_command(&[
            "dbtool",
            "--dsn",
            "kafka://127.0.0.1:9092",
            "mq",
            "consume",
            "events",
        ]);
        assert_eq!(mutation_timeout_subject(&consume), None);

        let sql_write = parsed_command(&[
            "dbtool",
            "--dsn",
            "sqlite::memory:",
            "sql",
            "exec",
            "INSERT INTO events VALUES (1)",
        ]);
        assert_eq!(mutation_timeout_subject(&sql_write), Some("SQL execute"));

        let sql_read = parsed_command(&[
            "dbtool",
            "--dsn",
            "sqlite::memory:",
            "sql",
            "exec",
            "SELECT 1",
        ]);
        assert_eq!(mutation_timeout_subject(&sql_read), None);

        for args in [
            vec![
                "dbtool",
                "--dsn",
                "redis://127.0.0.1:6379",
                "kv",
                "set",
                "key",
                "value",
            ],
            vec![
                "dbtool",
                "--dsn",
                "mongodb://127.0.0.1/app",
                "doc",
                "insert",
                "users",
                "{}",
            ],
            vec![
                "dbtool",
                "--dsn",
                "prometheus://127.0.0.1:9090",
                "ts",
                "write",
                "metric",
                "1",
            ],
            vec![
                "dbtool",
                "--dsn",
                "opensearch://127.0.0.1:9200",
                "search",
                "index",
                "logs",
                "{}",
            ],
        ] {
            assert!(mutation_timeout_subject(&parsed_command(&args)).is_some());
        }

        let raw_read = parsed_command(&[
            "dbtool",
            "--dsn",
            "redis://127.0.0.1:6379",
            "kv",
            "raw",
            "GET",
            "key",
        ]);
        assert_eq!(mutation_timeout_subject(&raw_read), None);

        let raw_write = parsed_command(&[
            "dbtool",
            "--dsn",
            "redis://127.0.0.1:6379",
            "kv",
            "raw",
            "SET",
            "key",
            "value",
        ]);
        assert_eq!(
            mutation_timeout_subject(&raw_write),
            Some("key-value mutation")
        );

        for args in [
            vec![
                "dbtool",
                "--dsn",
                "kafka://127.0.0.1:9092",
                "mq",
                "delete",
                "--kind",
                "kafka-topic",
                "events",
            ],
            vec![
                "dbtool",
                "--dsn",
                "kafka://127.0.0.1:9092",
                "mq",
                "consume",
                "events",
                "--group",
                "workers",
                "--ack",
                "none",
            ],
            vec![
                "dbtool",
                "--dsn",
                "amqp://127.0.0.1:5672",
                "mq",
                "consume",
                "events",
                "--ack",
                "on-success",
            ],
        ] {
            assert_eq!(
                mutation_timeout_subject(&parsed_command(&args)),
                Some("message mutation")
            );
        }

        let readonly_aggregate = parsed_command(&[
            "dbtool",
            "--dsn",
            "mongodb://127.0.0.1/app",
            "doc",
            "aggregate",
            "users",
            "[]",
        ]);
        assert_eq!(mutation_timeout_subject(&readonly_aggregate), None);
    }
}
