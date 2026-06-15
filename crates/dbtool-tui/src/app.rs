use crate::{
    state::{AppState, ConnectionItem, StateAction},
    ui,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use dbtool_core::{
    config::{env::discover_env_connections, ConnectionConfig},
    dsn::Dsn,
    error::Error,
    model::{FindOptions, TimeRange, Value},
    registry::Registry,
    service::{safety::SafetyGuard, ConnectionManager},
    Result,
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::sync::Arc;

pub struct App {
    _manager: Arc<ConnectionManager>,
    state: AppState,
}

impl App {
    pub fn new(registry: Arc<Registry>) -> Self {
        let manager = Arc::new(ConnectionManager::new(registry));
        let connections = load_connection_items();
        Self {
            _manager: Arc::clone(&manager),
            state: AppState::with_connections(connections),
        }
    }

    pub fn help_text() -> &'static str {
        "dbtool-tui\n\nUsage: dbtool-tui [--smoke]\n\nKeys: Tab changes panel, Enter runs a query command, y confirms a pending write, n cancels it, q quits.\nCommands: ping, caps, sql <query>, sql exec <statement>, tables, schema <table>, kv get/scan/set/del, doc collections/find, search indices/index/query, ts measurements/query."
    }

    pub fn smoke_summary(&self) -> String {
        format!(
            "ok: loaded {} connection(s); selected panel: {:?}",
            self.state.connections.len(),
            self.state.active_panel
        )
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut term = Terminal::new(backend)?;

        loop {
            term.draw(|f| ui::render(f, &self.state))?;

            if event::poll(std::time::Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => break,
                        other => match self.state.handle_key(other) {
                            StateAction::None => {}
                            StateAction::Execute => self.execute_current(false).await,
                            StateAction::ConfirmWrite => self.execute_current(true).await,
                        },
                    }
                }
            }
        }

        disable_raw_mode()?;
        execute!(term.backend_mut(), LeaveAlternateScreen)?;
        Ok(())
    }

    async fn execute_current(&mut self, confirmed_write: bool) {
        let command = if confirmed_write {
            self.state
                .pending_write
                .clone()
                .unwrap_or_else(|| self.state.query_input.clone())
        } else {
            self.state.query_input.trim().to_owned()
        };
        if command.trim().is_empty() {
            self.state.result_text = "Enter a command first".to_owned();
            return;
        }

        if !confirmed_write && command_requires_write(&command) {
            self.state.pending_write = Some(command);
            self.state.result_text =
                "Write command pending. Press y to execute once, or n to cancel.".to_owned();
            return;
        }

        let Some(connection) = self.state.selected_connection().cloned() else {
            self.state.result_text = "No configured connections found".to_owned();
            return;
        };

        if confirmed_write && connection.readonly {
            self.state.pending_write = None;
            self.state.result_text = "Selected connection is readonly".to_owned();
            return;
        }

        let output =
            execute_tui_command(&self._manager, &connection, &command, self.state.limit).await;
        self.state.pending_write = None;
        self.state.result_text = match output {
            Ok(value) => value,
            Err(err) => format_error(&err),
        };
    }
}

fn load_connection_items() -> Vec<ConnectionItem> {
    let mut connections = Vec::new();

    if let Ok(config) = ConnectionConfig::load(&ConnectionConfig::default_path()) {
        connections.extend(
            config
                .connections
                .into_iter()
                .map(|(name, entry)| ConnectionItem {
                    name,
                    dsn: entry.dsn,
                    readonly: entry.readonly.unwrap_or(false),
                }),
        );
    }

    connections.extend(
        discover_env_connections()
            .into_iter()
            .map(|(name, dsn)| ConnectionItem {
                name: format!("env:{name}"),
                dsn,
                readonly: false,
            }),
    );

    connections.sort_by(|a, b| a.name.cmp(&b.name));
    connections.dedup_by(|a, b| a.name == b.name);
    if connections.is_empty() {
        connections.push(ConnectionItem::default());
    }
    connections
}

async fn execute_tui_command(
    manager: &ConnectionManager,
    connection: &ConnectionItem,
    command: &str,
    limit: usize,
) -> Result<String> {
    let conn = manager.get_or_connect(&connection.dsn).await?;
    let connector = conn.as_ref().as_ref();
    let mut parts = command.split_whitespace();
    let head = parts
        .next()
        .ok_or_else(|| Error::Config("empty TUI command".into()))?;

    match head {
        "ping" => {
            connector.ping().await?;
            render_json(serde_json::json!({
                "status": "ok",
                "kind": connector.kind().0,
                "capabilities": connector.capabilities()
            }))
        }
        "caps" => render_json(connector.capabilities()),
        "tables" => {
            let sql = connector
                .as_sql()
                .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
            render_json(sql.list_tables(None).await?)
        }
        "schema" => {
            let table = parts
                .next()
                .ok_or_else(|| Error::Config("schema requires a table name".into()))?;
            let sql = connector
                .as_sql()
                .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
            render_json(sql.describe_table(table).await?)
        }
        "sql" => {
            let rest = command
                .strip_prefix("sql")
                .map(str::trim)
                .unwrap_or_default();
            run_sql_command(connector, connection, rest, limit).await
        }
        "exec" => {
            let sql = command
                .strip_prefix("exec")
                .map(str::trim)
                .unwrap_or_default();
            run_sql_exec(connector, connection, sql).await
        }
        "kv" => run_kv_command(connector, command.strip_prefix("kv").unwrap_or("").trim(), limit).await,
        "doc" => run_doc_command(connector, command.strip_prefix("doc").unwrap_or("").trim(), limit).await,
        "search" => {
            run_search_command(
                connector,
                command.strip_prefix("search").unwrap_or("").trim(),
                limit,
            )
            .await
        }
        "ts" => run_ts_command(connector, command.strip_prefix("ts").unwrap_or("").trim()).await,
        _ => Err(Error::Config(format!(
            "unknown TUI command '{head}'; try ping, caps, sql, tables, schema, kv, doc, search, or ts"
        ))),
    }
}

async fn run_sql_command(
    connector: &dyn dbtool_core::port::Connector,
    connection: &ConnectionItem,
    command: &str,
    limit: usize,
) -> Result<String> {
    if let Some(sql) = command.strip_prefix("query ").map(str::trim) {
        return run_sql_query(connector, sql, limit).await;
    }
    if let Some(sql) = command.strip_prefix("exec ").map(str::trim) {
        return run_sql_exec(connector, connection, sql).await;
    }
    run_sql_query(connector, command, limit).await
}

async fn run_sql_query(
    connector: &dyn dbtool_core::port::Connector,
    sql_text: &str,
    limit: usize,
) -> Result<String> {
    let sql = connector
        .as_sql()
        .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
    let mut result = sql.query(sql_text, &[]).await?;
    if result.rows.len() > limit {
        result.rows.truncate(limit);
        result.truncated = true;
    }
    render_json(result)
}

async fn run_sql_exec(
    connector: &dyn dbtool_core::port::Connector,
    connection: &ConnectionItem,
    sql_text: &str,
) -> Result<String> {
    let sql = connector
        .as_sql()
        .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
    SafetyGuard::check_with_target(sql_text, &safety_target(&connection.dsn), true, None)?;
    render_json(sql.execute(sql_text, &[]).await?)
}

async fn run_kv_command(
    connector: &dyn dbtool_core::port::Connector,
    command: &str,
    limit: usize,
) -> Result<String> {
    let kv = connector
        .as_kv()
        .ok_or_else(|| unsupported(connector, "KeyValueStore"))?;
    let mut parts = command.split_whitespace();
    match parts.next() {
        Some("get") => {
            let key = parts
                .next()
                .ok_or_else(|| Error::Config("kv get requires a key".into()))?;
            let value = kv
                .get(key)
                .await?
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
            render_json(serde_json::json!({ "key": key, "value": value }))
        }
        Some("scan") => {
            let pattern = parts.next().unwrap_or("*");
            render_json(kv.scan(pattern, limit).await?)
        }
        Some("set") => {
            let key = parts
                .next()
                .ok_or_else(|| Error::Config("kv set requires a key".into()))?;
            let value = parts.collect::<Vec<_>>().join(" ");
            kv.set(
                key,
                value.as_bytes(),
                dbtool_core::port::capability::SetOptions::default(),
            )
            .await?;
            render_json(serde_json::json!({ "ok": true }))
        }
        Some("del") => {
            let keys = parts.map(str::to_owned).collect::<Vec<_>>();
            if keys.is_empty() {
                return Err(Error::Config("kv del requires at least one key".into()));
            }
            render_json(serde_json::json!({ "deleted": kv.delete(&keys).await? }))
        }
        _ => Err(Error::Config(
            "kv command must be get, scan, set, or del".into(),
        )),
    }
}

async fn run_doc_command(
    connector: &dyn dbtool_core::port::Connector,
    command: &str,
    limit: usize,
) -> Result<String> {
    let doc = connector
        .as_document()
        .ok_or_else(|| unsupported(connector, "DocumentStore"))?;
    if command == "collections" {
        return render_json(doc.list_collections().await?);
    }
    if let Some(rest) = command.strip_prefix("find ").map(str::trim) {
        let (collection, filter) = rest
            .split_once(' ')
            .map(|(collection, filter)| (collection, filter.trim()))
            .unwrap_or((rest, "{}"));
        let filter = parse_json_value(filter)?;
        let result = doc
            .find(
                collection,
                filter,
                FindOptions {
                    limit: Some(limit),
                    ..Default::default()
                },
            )
            .await?;
        return render_json(result);
    }
    Err(Error::Config(
        "doc command must be collections or find <collection> [filter]".into(),
    ))
}

async fn run_search_command(
    connector: &dyn dbtool_core::port::Connector,
    command: &str,
    limit: usize,
) -> Result<String> {
    let search = connector
        .as_search()
        .ok_or_else(|| unsupported(connector, "SearchEngine"))?;
    if command == "indices" {
        return render_json(search.list_indices().await?);
    }
    if let Some(rest) = command.strip_prefix("index ").map(str::trim) {
        let (index, doc) = rest
            .split_once(' ')
            .ok_or_else(|| Error::Config("search index requires index and JSON doc".into()))?;
        search
            .index_doc(index, parse_json_value(doc.trim())?)
            .await?;
        return render_json(serde_json::json!({ "indexed": true }));
    }
    let (index, query) = command
        .split_once(' ')
        .ok_or_else(|| Error::Config("search requires index and JSON query".into()))?;
    let result = search
        .search(
            index,
            parse_json_value(query.trim())?,
            dbtool_core::port::capability::SearchOptions {
                size: Some(limit),
                from: None,
                source: false,
            },
        )
        .await?;
    render_json(result)
}

async fn run_ts_command(
    connector: &dyn dbtool_core::port::Connector,
    command: &str,
) -> Result<String> {
    let ts = connector
        .as_timeseries()
        .ok_or_else(|| unsupported(connector, "TimeSeriesStore"))?;
    if command == "measurements" {
        return render_json(ts.list_measurements().await?);
    }
    let query = command
        .strip_prefix("query ")
        .map(str::trim)
        .ok_or_else(|| Error::Config("ts command must be measurements or query <expr>".into()))?;
    render_json(ts.query_range(query, TimeRange::last_n_minutes(60)).await?)
}

fn command_requires_write(command: &str) -> bool {
    let command = command.trim().to_ascii_lowercase();
    command.starts_with("exec ")
        || command.starts_with("sql exec ")
        || command.starts_with("kv set ")
        || command.starts_with("kv del ")
        || command.starts_with("doc insert ")
        || command.starts_with("doc update ")
        || command.starts_with("doc delete ")
        || command.starts_with("search index ")
}

fn parse_json_value(raw: &str) -> Result<Value> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(Value::Json)
        .map_err(|e| Error::Serialization(e.to_string()))
}

fn safety_target(raw_dsn: &str) -> String {
    Dsn::parse(raw_dsn)
        .map(|dsn| format!("dsn:{}", dsn.redacted()))
        .unwrap_or_else(|_| "dsn:<unparsed>".to_owned())
}

fn unsupported(connector: &dyn dbtool_core::port::Connector, needed: &'static str) -> Error {
    Error::UnsupportedCapability {
        kind: connector.kind().0,
        needed,
    }
}

fn render_json<T: serde::Serialize>(value: T) -> Result<String> {
    serde_json::to_string_pretty(&value).map_err(|e| Error::Serialization(e.to_string()))
}

fn format_error(err: &Error) -> String {
    serde_json::json!({
        "error": {
            "code": err.code(),
            "message": err.to_string()
        }
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_registry::build_registry;

    #[test]
    fn detects_write_commands() {
        assert!(command_requires_write("exec insert into t values (1)"));
        assert!(command_requires_write(
            "sql exec update users set name = 'a'"
        ));
        assert!(command_requires_write("kv set key value"));
        assert!(command_requires_write("search index users {}"));
        assert!(!command_requires_write("sql select 1"));
        assert!(!command_requires_write("kv get key"));
    }

    #[test]
    fn exposes_noninteractive_help_and_smoke_summary() {
        let registry = Arc::new(build_registry());
        let app = App::new(registry);

        assert!(App::help_text().contains("Usage: dbtool-tui"));
        assert!(app.smoke_summary().contains("loaded"));
    }

    #[tokio::test]
    async fn dispatches_ping_and_sql_query() {
        let registry = Arc::new(build_registry());
        let manager = ConnectionManager::new(registry);
        let connection = ConnectionItem {
            name: "sqlite".to_owned(),
            dsn: "sqlite::memory:".to_owned(),
            readonly: false,
        };

        let ping = execute_tui_command(&manager, &connection, "ping", 100)
            .await
            .unwrap();
        assert!(ping.contains("\"status\": \"ok\""));

        let query = execute_tui_command(&manager, &connection, "sql select 1 as id", 100)
            .await
            .unwrap();
        assert!(query.contains("\"id\""));
        assert!(query.contains("1"));
    }

    #[tokio::test]
    async fn readonly_connection_refuses_confirmed_write_before_connecting() {
        let registry = Arc::new(build_registry());
        let mut app = App {
            _manager: Arc::new(ConnectionManager::new(registry)),
            state: AppState {
                pending_write: Some("exec insert into t values (1)".to_owned()),
                ..AppState::with_connections(vec![ConnectionItem {
                    name: "readonly".to_owned(),
                    dsn: "sqlite::memory:".to_owned(),
                    readonly: true,
                }])
            },
        };

        app.execute_current(true).await;

        assert!(app.state.result_text.contains("readonly"));
        assert!(app.state.pending_write.is_none());
    }
}
