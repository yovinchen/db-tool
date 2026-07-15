use crate::{
    state::{AppState, ConnectionItem, StateAction},
    terminal::{CrosstermLifecycle, TerminalLifecycle, TerminalSession},
    ui,
};
use crossterm::event::{self, Event, KeyCode};
use dbtool_core::{
    config::{env::discover_env_connections, ConnectionConfig},
    dsn::Dsn,
    error::Error,
    model::{BoundedList, FindOptions, Point, TimeRange, Value},
    port::CapabilityOperation,
    registry::Registry,
    service::{
        safety::{SafetyGuard, StatementKind},
        ConnectionManager, ListLimiter,
    },
    Result,
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{collections::HashMap, io, sync::Arc, time::Duration};

trait TuiRuntime {
    fn draw(&mut self, state: &AppState) -> io::Result<()>;
    fn poll(&mut self, timeout: Duration) -> io::Result<bool>;
    fn read(&mut self) -> io::Result<Event>;
}

struct CrosstermRuntime {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl CrosstermRuntime {
    fn new() -> io::Result<Self> {
        let backend = CrosstermBackend::new(io::stdout());
        Ok(Self {
            terminal: Terminal::new(backend)?,
        })
    }
}

impl TuiRuntime for CrosstermRuntime {
    fn draw(&mut self, state: &AppState) -> io::Result<()> {
        self.terminal.draw(|frame| ui::render(frame, state))?;
        Ok(())
    }

    fn poll(&mut self, timeout: Duration) -> io::Result<bool> {
        event::poll(timeout)
    }

    fn read(&mut self) -> io::Result<Event> {
        event::read()
    }
}

pub struct App {
    _manager: Arc<ConnectionManager>,
    state: AppState,
    startup_error: Option<String>,
}

impl App {
    pub fn new(registry: Arc<Registry>) -> Self {
        Self::from_connection_load(registry, load_connection_items())
    }

    fn from_connection_load(
        registry: Arc<Registry>,
        connections: Result<Vec<ConnectionItem>>,
    ) -> Self {
        let manager = Arc::new(ConnectionManager::new(registry));
        match connections {
            Ok(connections) => Self {
                _manager: Arc::clone(&manager),
                state: AppState::with_connections(connections),
                startup_error: None,
            },
            Err(error) => {
                let startup_error = format_error(&error);
                let mut state = AppState::with_connections(Vec::new());
                state.result_text = startup_error.clone();
                Self {
                    _manager: Arc::clone(&manager),
                    state,
                    startup_error: Some(startup_error),
                }
            }
        }
    }

    pub fn help_text() -> &'static str {
        "dbtool-tui\n\nUsage: dbtool-tui [--smoke]\n\nKeys: Tab changes panel, Enter runs a query command, Up/Down recall command history in the query panel, F2 changes the capability form, F3 changes form field, F4 applies the form, y confirms a pending write, n cancels it, q quits.\nCommands: ping, caps, sql <query>, sql exec <statement>, schemas, tables [schema], schema <table>, kv get/scan/set/del, doc collections/find, search indices/index/query, ts measurements/query/write."
    }

    pub fn smoke_summary(&self) -> String {
        if let Some(error) = &self.startup_error {
            return format!("error: configuration load failed; {error}");
        }
        format!(
            "ok: loaded {} connection(s); selected panel: {:?}",
            self.state.connections.len(),
            self.state.active_panel
        )
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.run_with_terminal(CrosstermLifecycle, CrosstermRuntime::new)
            .await
    }

    async fn run_with_terminal<L, R, F>(
        &mut self,
        lifecycle: L,
        make_runtime: F,
    ) -> anyhow::Result<()>
    where
        L: TerminalLifecycle,
        R: TuiRuntime,
        F: FnOnce() -> io::Result<R>,
    {
        let mut session = TerminalSession::enter(lifecycle)?;
        let run_result = match make_runtime() {
            Ok(mut runtime) => self.run_event_loop(&mut runtime).await,
            Err(error) => Err(error.into()),
        };
        let restore_result = session.restore();

        run_result?;
        restore_result?;
        Ok(())
    }

    async fn run_event_loop<R: TuiRuntime>(&mut self, runtime: &mut R) -> anyhow::Result<()> {
        loop {
            runtime.draw(&self.state)?;

            if runtime.poll(Duration::from_millis(100))? {
                if let Event::Key(key) = runtime.read()? {
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
        Ok(())
    }

    async fn execute_current(&mut self, confirmed_write: bool) {
        if let Some(error) = &self.startup_error {
            self.state.pending_write = None;
            self.state.result_text = error.clone();
            return;
        }

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

        let Some(connection) = self.state.selected_connection().cloned() else {
            self.state.result_text = "No configured connections found".to_owned();
            return;
        };

        let requires_write = match command_requires_write(&command) {
            Ok(requires_write) => requires_write,
            Err(error) => {
                self.state.pending_write = None;
                self.state.result_text = format_error(&error);
                return;
            }
        };

        if requires_write && connection.readonly {
            self.state.pending_write = None;
            self.state.result_text = "Selected connection is readonly".to_owned();
            return;
        }

        if requires_write && !confirmed_write {
            self.state.pending_write = Some(command);
            self.state.result_text =
                "Write command pending. Press y to execute once, or n to cancel.".to_owned();
            return;
        }

        let output = execute_tui_command(
            &self._manager,
            &connection,
            &command,
            self.state.limit,
            confirmed_write,
        )
        .await;
        self.state.pending_write = None;
        self.state.record_command(&command);
        self.state.result_text = match output {
            Ok(value) => value,
            Err(err) => format_error(&err),
        };
    }
}

fn load_connection_items() -> Result<Vec<ConnectionItem>> {
    load_connection_items_from(&ConnectionConfig::default_path())
}

fn load_connection_items_from(path: &std::path::Path) -> Result<Vec<ConnectionItem>> {
    let mut connections = Vec::new();

    let config = ConnectionConfig::load(path)?;
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
    Ok(connections)
}

async fn execute_tui_command(
    manager: &ConnectionManager,
    connection: &ConnectionItem,
    command: &str,
    limit: usize,
    confirmed_write: bool,
) -> Result<String> {
    authorize_tui_command(connection, command, confirmed_write)?;
    validate_bounded_catalog_limit(command, limit)?;
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
            require_operation(
                connector,
                CapabilityOperation::SqlListTablesBounded,
                "SqlEngine.list_tables_bounded",
            )?;
            let schema = parts.next();
            if parts.next().is_some() {
                return Err(Error::Config(
                    "tables accepts at most one schema name".into(),
                ));
            }
            let sql = connector
                .as_sql()
                .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
            render_bounded_list(sql.list_tables_bounded(schema, limit).await?)
        }
        "schemas" => {
            if parts.next().is_some() {
                return Err(Error::Config("schemas does not accept arguments".into()));
            }
            require_operation(
                connector,
                CapabilityOperation::SqlListSchemasBounded,
                "SqlEngine.list_schemas_bounded",
            )?;
            let sql = connector
                .as_sql()
                .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
            render_bounded_list(sql.list_schemas_bounded(limit).await?)
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
            run_sql_command(connector, connection, rest, limit, confirmed_write).await
        }
        "exec" => {
            let sql = command
                .strip_prefix("exec")
                .map(str::trim)
                .unwrap_or_default();
            run_sql_exec(connector, connection, sql, confirmed_write).await
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
        "ts" => {
            run_ts_command(
                connector,
                command.strip_prefix("ts").unwrap_or("").trim(),
                limit,
            )
            .await
        }
        _ => Err(Error::Config(format!(
            "unknown TUI command '{head}'; try ping, caps, sql, schemas, tables, schema, kv, doc, search, or ts"
        ))),
    }
}

async fn run_sql_command(
    connector: &dyn dbtool_core::port::Connector,
    connection: &ConnectionItem,
    command: &str,
    limit: usize,
    confirmed_write: bool,
) -> Result<String> {
    if let Some(sql) = command.strip_prefix("query ").map(str::trim) {
        return run_sql_query(connector, connection, sql, limit, confirmed_write).await;
    }
    if let Some(sql) = command.strip_prefix("exec ").map(str::trim) {
        return run_sql_exec(connector, connection, sql, confirmed_write).await;
    }
    run_sql_query(connector, connection, command, limit, confirmed_write).await
}

async fn run_sql_query(
    connector: &dyn dbtool_core::port::Connector,
    connection: &ConnectionItem,
    sql_text: &str,
    limit: usize,
    confirmed_write: bool,
) -> Result<String> {
    authorize_sql_statement(connection, sql_text, confirmed_write)?;
    let sql = connector
        .as_sql()
        .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
    let result = sql.query_bounded(sql_text, &[], limit).await?;
    render_json(result)
}

async fn run_sql_exec(
    connector: &dyn dbtool_core::port::Connector,
    connection: &ConnectionItem,
    sql_text: &str,
    confirmed_write: bool,
) -> Result<String> {
    authorize_sql_statement(connection, sql_text, confirmed_write)?;
    let sql = connector
        .as_sql()
        .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
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
        require_operation(
            connector,
            CapabilityOperation::DocumentListCollectionsBounded,
            "DocumentStore.list_collections_bounded",
        )?;
        return render_bounded_list(doc.list_collections_bounded(limit).await?);
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
        require_operation(
            connector,
            CapabilityOperation::SearchListIndicesBounded,
            "SearchEngine.list_indices_bounded",
        )?;
        return render_bounded_list(search.list_indices_bounded(limit).await?);
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
    limit: usize,
) -> Result<String> {
    let ts = connector
        .as_timeseries()
        .ok_or_else(|| unsupported(connector, "TimeSeriesStore"))?;
    if command == "measurements" {
        require_operation(
            connector,
            CapabilityOperation::TimeSeriesListMeasurementsBounded,
            "TimeSeriesStore.list_measurements_bounded",
        )?;
        return render_bounded_list(ts.list_measurements_bounded(limit).await?);
    }
    if let Some(rest) = command.strip_prefix("write ").map(str::trim) {
        let point = parse_tui_ts_point(rest)?;
        ts.write_points(vec![point]).await?;
        return render_json(serde_json::json!({
            "written_points": 1,
            "written_samples": 1
        }));
    }
    let query = command
        .strip_prefix("query ")
        .map(str::trim)
        .ok_or_else(|| Error::Config("ts command must be measurements, query <expr>, or write <measurement> <value> [field=<name>] [tag=value...]".into()))?;
    render_json(
        ts.query_range(query, TimeRange::last_n_minutes(60)?)
            .await?,
    )
}

fn parse_tui_ts_point(raw: &str) -> Result<Point> {
    let mut parts = raw.split_whitespace();
    let measurement = parts
        .next()
        .ok_or_else(|| Error::Config("ts write requires a measurement".into()))?
        .to_owned();
    let value = parts
        .next()
        .ok_or_else(|| Error::Config("ts write requires a value".into()))?
        .parse::<f64>()
        .map_err(|e| Error::Config(format!("invalid ts write value: {e}")))?;
    let mut field = "value".to_owned();
    let mut tags = HashMap::new();

    for token in parts {
        let (key, value) = token
            .split_once('=')
            .ok_or_else(|| Error::Config(format!("invalid ts write option '{token}'")))?;
        if key == "field" {
            field = value.to_owned();
        } else {
            tags.insert(key.to_owned(), value.to_owned());
        }
    }

    Ok(Point {
        measurement,
        tags,
        fields: HashMap::from([(field, value)]),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64,
    })
}

fn validate_bounded_catalog_limit(command: &str, limit: usize) -> Result<()> {
    let parts = command.split_whitespace().collect::<Vec<_>>();
    if matches!(
        parts.as_slice(),
        ["doc", "collections"] | ["search", "indices"] | ["ts", "measurements"]
    ) {
        ListLimiter::new(limit).probe_items()?;
    }
    Ok(())
}

fn command_requires_write(command: &str) -> Result<bool> {
    if let Some(sql) = sql_statement_from_command(command) {
        return sql_requires_write(sql);
    }

    let command = command.trim().to_ascii_lowercase();
    Ok(command.starts_with("kv set ")
        || command.starts_with("kv del ")
        || command.starts_with("doc insert ")
        || command.starts_with("doc update ")
        || command.starts_with("doc delete ")
        || command.starts_with("search index ")
        || command.starts_with("ts write "))
}

fn sql_statement_from_command(command: &str) -> Option<&str> {
    let command = command.trim();
    if command == "sql" || command == "exec" {
        return Some("");
    }
    if let Some(rest) = command.strip_prefix("sql ").map(str::trim_start) {
        if rest == "query" || rest == "exec" {
            return Some("");
        }
        if let Some(sql) = rest.strip_prefix("query ").map(str::trim) {
            return Some(sql);
        }
        if let Some(sql) = rest.strip_prefix("exec ").map(str::trim) {
            return Some(sql);
        }
        return Some(rest);
    }
    command.strip_prefix("exec ").map(str::trim)
}

fn sql_requires_write(sql: &str) -> Result<bool> {
    if sql.trim().is_empty() {
        return Err(Error::Config("SQL command requires a statement".to_owned()));
    }

    match SafetyGuard::check(sql, true, None) {
        Ok(StatementKind::Read) => Ok(false),
        Ok(StatementKind::Write | StatementKind::Destructive)
        | Err(Error::ConfirmRequired { .. }) => Ok(true),
        Err(error) => Err(error),
    }
}

fn authorize_tui_command(
    connection: &ConnectionItem,
    command: &str,
    confirmed_write: bool,
) -> Result<()> {
    if command_requires_write(command)? {
        if connection.readonly {
            return Err(Error::ReadOnly);
        }
        if !confirmed_write {
            return Err(Error::WriteNotAllowed);
        }
    }
    Ok(())
}

fn authorize_sql_statement(
    connection: &ConnectionItem,
    sql: &str,
    confirmed_write: bool,
) -> Result<StatementKind> {
    if sql.trim().is_empty() {
        return Err(Error::Config("SQL command requires a statement".to_owned()));
    }

    let target = safety_target(&connection.dsn);
    match SafetyGuard::check_with_target(sql, &target, true, None) {
        Ok(StatementKind::Read) => Ok(StatementKind::Read),
        Ok(StatementKind::Write) => {
            if connection.readonly {
                Err(Error::ReadOnly)
            } else if confirmed_write {
                Ok(StatementKind::Write)
            } else {
                Err(Error::WriteNotAllowed)
            }
        }
        Ok(StatementKind::Destructive) => unreachable!("destructive SQL requires a token"),
        Err(Error::ConfirmRequired {
            confirm_token,
            impact: _,
        }) => {
            if connection.readonly {
                return Err(Error::ReadOnly);
            }
            if !confirmed_write {
                return Err(Error::WriteNotAllowed);
            }
            SafetyGuard::check_with_target(sql, &target, true, Some(&confirm_token))
        }
        Err(error) => Err(error),
    }
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

fn require_operation(
    connector: &dyn dbtool_core::port::Connector,
    operation: CapabilityOperation,
    needed: &'static str,
) -> Result<()> {
    require_declared_operation(
        &connector.operations(),
        operation,
        connector.kind().0,
        needed,
    )
}

fn require_declared_operation(
    operations: &[CapabilityOperation],
    operation: CapabilityOperation,
    kind: String,
    needed: &'static str,
) -> Result<()> {
    if operations.contains(&operation) {
        Ok(())
    } else {
        Err(Error::UnsupportedCapability { kind, needed })
    }
}

fn render_bounded_list<T: serde::Serialize>(list: BoundedList<T>) -> Result<String> {
    render_json(serde_json::json!({
        "data": list.items,
        "meta": { "truncated": list.truncated }
    }))
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
    use crossterm::event::{KeyEvent, KeyModifiers};
    use dbtool_registry::build_registry;
    use std::sync::Mutex;

    #[test]
    fn detects_write_commands() {
        assert!(command_requires_write("exec insert into t values (1)").unwrap());
        assert!(command_requires_write("sql exec update users set name = 'a'").unwrap());
        assert!(command_requires_write("kv set key value").unwrap());
        assert!(command_requires_write("search index users {}").unwrap());
        assert!(command_requires_write("ts write requests_total 1").unwrap());
        assert!(!command_requires_write("sql select 1").unwrap());
        assert!(!command_requires_write("sql query select 1").unwrap());
        assert!(!command_requires_write("sql exec select 1").unwrap());
        assert!(!command_requires_write("kv get key").unwrap());
    }

    #[test]
    fn sql_query_and_fallback_routes_classify_mutations_with_safety_guard() {
        for statement in [
            "delete from users where id = 1",
            "drop table users",
            "insert into users values (1)",
            "update users set name = 'alice' where id = 1",
        ] {
            assert!(command_requires_write(&format!("sql query {statement}")).unwrap());
            assert!(command_requires_write(&format!("sql {statement}")).unwrap());
        }
    }

    #[test]
    fn parses_tui_time_series_write_point() {
        let point =
            parse_tui_ts_point("requests_total 2.5 field=count job=api instance=local").unwrap();

        assert_eq!(point.measurement, "requests_total");
        assert_eq!(point.fields["count"], 2.5);
        assert_eq!(point.tags["job"], "api");
        assert_eq!(point.tags["instance"], "local");
    }

    #[test]
    fn exposes_noninteractive_help_and_smoke_summary() {
        let registry = Arc::new(build_registry());
        let app = App::new(registry);

        assert!(App::help_text().contains("Usage: dbtool-tui"));
        assert!(App::help_text().contains("Up/Down recall command history"));
        assert!(App::help_text().contains("F2 changes the capability form"));
        assert!(app.smoke_summary().contains("loaded"));
    }

    #[test]
    fn sql_catalog_commands_reject_legacy_only_declarations_without_fallback() {
        assert!(matches!(
            require_declared_operation(
                CapabilityOperation::SQL,
                CapabilityOperation::SqlListTablesBounded,
                "legacy-sql".into(),
                "SqlEngine.list_tables_bounded",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "SqlEngine.list_tables_bounded"
        ));
    }

    #[test]
    fn invalid_connection_file_is_propagated_instead_of_ignored() {
        let path = std::env::temp_dir().join(format!(
            "dbtool-tui-invalid-config-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            r#"
[connections.prod]
dsn = "postgres://127.0.0.1:1/app"
readonli = true
"#,
        )
        .unwrap();

        let error = load_connection_items_from(&path).unwrap_err();
        std::fs::remove_file(path).ok();

        assert!(matches!(
            error,
            Error::Config(message) if message.contains("readonli")
        ));
    }

    #[tokio::test]
    async fn configuration_error_is_rendered_and_blocks_execution() {
        let registry = Arc::new(build_registry());
        let mut app = App::from_connection_load(
            registry,
            Err(Error::Config("unknown field `readonli`".to_owned())),
        );

        assert!(app.state.connections.is_empty());
        let rendered: serde_json::Value = serde_json::from_str(&app.state.result_text).unwrap();
        assert_eq!(rendered["error"]["code"], "CONFIG_ERROR");
        assert!(rendered["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("readonli")));
        assert!(app.smoke_summary().contains("configuration load failed"));
        assert!(app.smoke_summary().contains("CONFIG_ERROR"));

        let original_error = app.state.result_text.clone();
        app.state.query_input = "ping".to_owned();
        app.execute_current(false).await;
        assert_eq!(app.state.result_text, original_error);
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

        let ping = execute_tui_command(&manager, &connection, "ping", 100, false)
            .await
            .unwrap();
        assert!(ping.contains("\"status\": \"ok\""));

        let query = execute_tui_command(&manager, &connection, "sql select 1 as id", 100, false)
            .await
            .unwrap();
        assert!(query.contains("\"id\""));
        assert!(query.contains("1"));
    }

    #[tokio::test]
    async fn sql_catalog_commands_use_bounded_operations_and_expose_truncation() {
        let registry = Arc::new(build_registry());
        let manager = ConnectionManager::new(registry);
        let connection = ConnectionItem {
            name: "sqlite".to_owned(),
            dsn: "sqlite::memory:".to_owned(),
            readonly: false,
        };

        for table in ["alpha", "beta", "gamma"] {
            execute_tui_command(
                &manager,
                &connection,
                &format!("sql query create table {table} (id integer)"),
                100,
                true,
            )
            .await
            .unwrap();
        }

        let tables: serde_json::Value = serde_json::from_str(
            &execute_tui_command(&manager, &connection, "tables main", 2, false)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(tables["data"].as_array().map(Vec::len), Some(2));
        assert_eq!(tables["data"][0]["schema"], "main");
        assert_eq!(tables["data"][0]["name"], "alpha");
        assert_eq!(tables["data"][1]["name"], "beta");
        assert_eq!(tables["meta"]["truncated"], true);

        let schemas: serde_json::Value = serde_json::from_str(
            &execute_tui_command(&manager, &connection, "schemas", 1, false)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(schemas["data"], serde_json::json!(["main"]));
        assert_eq!(schemas["meta"]["truncated"], false);

        for limit in [0, usize::MAX] {
            assert!(matches!(
                execute_tui_command(&manager, &connection, "tables main", limit, false).await,
                Err(Error::Config(_))
            ));
        }
    }

    #[test]
    fn document_search_and_time_series_catalogs_require_explicit_bounded_operations() {
        for (operations, operation, kind, needed) in [
            (
                CapabilityOperation::DOCUMENT,
                CapabilityOperation::DocumentListCollectionsBounded,
                "legacy-document",
                "DocumentStore.list_collections_bounded",
            ),
            (
                CapabilityOperation::SEARCH,
                CapabilityOperation::SearchListIndicesBounded,
                "legacy-search",
                "SearchEngine.list_indices_bounded",
            ),
            (
                CapabilityOperation::TIME_SERIES,
                CapabilityOperation::TimeSeriesListMeasurementsBounded,
                "legacy-time-series",
                "TimeSeriesStore.list_measurements_bounded",
            ),
        ] {
            assert!(matches!(
                require_declared_operation(operations, operation, kind.to_owned(), needed),
                Err(Error::UnsupportedCapability { kind: actual_kind, needed: actual_needed })
                    if actual_kind == kind && actual_needed == needed
            ));
        }
    }

    #[tokio::test]
    async fn bounded_catalog_limits_are_rejected_before_tui_connection() {
        let registry = Arc::new(build_registry());
        let manager = ConnectionManager::new(registry);
        let connection = ConnectionItem {
            name: "unreachable".to_owned(),
            dsn: "mongodb://127.0.0.1:1/dbtool".to_owned(),
            readonly: false,
        };

        for command in ["doc collections", "search indices", "ts measurements"] {
            assert!(matches!(
                execute_tui_command(&manager, &connection, command, 0, false).await,
                Err(Error::Config(message)) if message.contains("greater than zero")
            ));
        }
    }

    #[tokio::test]
    async fn query_style_sql_mutations_require_confirmation_before_execution() {
        let registry = Arc::new(build_registry());
        let manager = ConnectionManager::new(registry);
        let connection = ConnectionItem {
            name: "sqlite".to_owned(),
            dsn: "sqlite::memory:".to_owned(),
            readonly: false,
        };

        execute_tui_command(
            &manager,
            &connection,
            "sql query create table tui_safety (id integer primary key, name text)",
            100,
            true,
        )
        .await
        .unwrap();

        for statement in [
            "insert into tui_safety values (1, 'alice')",
            "update tui_safety set name = 'bob' where id = 1",
            "delete from tui_safety where id = 1",
            "drop table tui_safety",
        ] {
            for command in [format!("sql query {statement}"), format!("sql {statement}")] {
                assert!(matches!(
                    execute_tui_command(&manager, &connection, &command, 100, false).await,
                    Err(Error::WriteNotAllowed)
                ));
            }
        }

        execute_tui_command(
            &manager,
            &connection,
            "sql query insert into tui_safety values (1, 'alice')",
            100,
            true,
        )
        .await
        .unwrap();
        execute_tui_command(
            &manager,
            &connection,
            "sql query update tui_safety set name = 'bob' where id = 1",
            100,
            true,
        )
        .await
        .unwrap();

        let selected = execute_tui_command(
            &manager,
            &connection,
            "sql query select name from tui_safety where id = 1",
            100,
            false,
        )
        .await
        .unwrap();
        assert!(selected.contains("bob"));

        execute_tui_command(
            &manager,
            &connection,
            "sql query delete from tui_safety where id = 1",
            100,
            true,
        )
        .await
        .unwrap();
        execute_tui_command(
            &manager,
            &connection,
            "sql query drop table tui_safety",
            100,
            true,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn readonly_rejects_query_style_writes_but_allows_select() {
        let registry = Arc::new(build_registry());
        let manager = ConnectionManager::new(registry);
        let writable = ConnectionItem {
            name: "writable".to_owned(),
            dsn: "sqlite::memory:".to_owned(),
            readonly: false,
        };
        let readonly = ConnectionItem {
            name: "readonly".to_owned(),
            dsn: writable.dsn.clone(),
            readonly: true,
        };

        execute_tui_command(
            &manager,
            &writable,
            "sql query create table tui_readonly (id integer primary key)",
            100,
            true,
        )
        .await
        .unwrap();

        assert!(matches!(
            execute_tui_command(
                &manager,
                &readonly,
                "sql query insert into tui_readonly values (1)",
                100,
                true,
            )
            .await,
            Err(Error::ReadOnly)
        ));

        let selected = execute_tui_command(
            &manager,
            &readonly,
            "sql query select count(*) as count from tui_readonly",
            100,
            false,
        )
        .await
        .unwrap();
        assert!(selected.contains("\"count\""));
        assert!(selected.contains('0'));
    }

    #[tokio::test]
    async fn readonly_connection_refuses_confirmed_write_before_connecting() {
        let registry = Arc::new(build_registry());
        let mut app = App {
            _manager: Arc::new(ConnectionManager::new(registry)),
            startup_error: None,
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

    #[tokio::test]
    async fn executed_commands_are_recorded_in_history() {
        let registry = Arc::new(build_registry());
        let mut app = App {
            _manager: Arc::new(ConnectionManager::new(registry)),
            startup_error: None,
            state: AppState {
                active_panel: crate::state::Panel::QueryInput,
                query_input: "ping".to_owned(),
                ..AppState::with_connections(vec![ConnectionItem {
                    name: "sqlite".to_owned(),
                    dsn: "sqlite::memory:".to_owned(),
                    readonly: false,
                }])
            },
        };

        app.execute_current(false).await;

        assert_eq!(app.state.command_history, vec!["ping"]);
    }

    #[tokio::test]
    async fn pending_write_is_recorded_only_after_confirmation() {
        let registry = Arc::new(build_registry());
        let mut app = App {
            _manager: Arc::new(ConnectionManager::new(registry)),
            startup_error: None,
            state: AppState {
                active_panel: crate::state::Panel::QueryInput,
                query_input: "exec create table tui_history (id integer primary key)".to_owned(),
                ..AppState::with_connections(vec![ConnectionItem {
                    name: "sqlite".to_owned(),
                    dsn: "sqlite::memory:".to_owned(),
                    readonly: false,
                }])
            },
        };

        app.execute_current(false).await;
        assert!(app.state.command_history.is_empty());
        assert!(app.state.pending_write.is_some());

        app.execute_current(true).await;

        assert_eq!(
            app.state.command_history,
            vec!["exec create table tui_history (id integer primary key)"]
        );
        assert!(app.state.pending_write.is_none());
    }

    #[derive(Clone, Copy)]
    enum RuntimeFailure {
        None,
        Draw,
        Poll,
        Read,
    }

    struct FakeRuntime {
        failure: RuntimeFailure,
    }

    impl TuiRuntime for FakeRuntime {
        fn draw(&mut self, _state: &AppState) -> io::Result<()> {
            if matches!(self.failure, RuntimeFailure::Draw) {
                Err(io::Error::other("draw failed"))
            } else {
                Ok(())
            }
        }

        fn poll(&mut self, _timeout: Duration) -> io::Result<bool> {
            if matches!(self.failure, RuntimeFailure::Poll) {
                Err(io::Error::other("poll failed"))
            } else {
                Ok(true)
            }
        }

        fn read(&mut self) -> io::Result<Event> {
            if matches!(self.failure, RuntimeFailure::Read) {
                Err(io::Error::other("read failed"))
            } else {
                Ok(Event::Key(KeyEvent::new(
                    KeyCode::Char('q'),
                    KeyModifiers::NONE,
                )))
            }
        }
    }

    struct RecordingLifecycle {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl RecordingLifecycle {
        fn record(&self, event: &'static str) {
            self.events.lock().unwrap().push(event);
        }
    }

    impl TerminalLifecycle for RecordingLifecycle {
        fn enable_raw_mode(&mut self) -> io::Result<()> {
            self.record("enable_raw");
            Ok(())
        }

        fn enter_alternate_screen(&mut self) -> io::Result<()> {
            self.record("enter_alternate");
            Ok(())
        }

        fn leave_alternate_screen(&mut self) -> io::Result<()> {
            self.record("leave_alternate");
            Ok(())
        }

        fn disable_raw_mode(&mut self) -> io::Result<()> {
            self.record("disable_raw");
            Ok(())
        }
    }

    fn lifecycle_events() -> Arc<Mutex<Vec<&'static str>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    fn assert_restored(events: &Arc<Mutex<Vec<&'static str>>>) {
        assert_eq!(
            *events.lock().unwrap(),
            [
                "enable_raw",
                "enter_alternate",
                "leave_alternate",
                "disable_raw"
            ]
        );
    }

    #[tokio::test]
    async fn draw_poll_and_read_errors_restore_terminal() {
        for failure in [
            RuntimeFailure::Draw,
            RuntimeFailure::Poll,
            RuntimeFailure::Read,
        ] {
            let events = lifecycle_events();
            let mut app = App::new(Arc::new(build_registry()));
            let result = app
                .run_with_terminal(
                    RecordingLifecycle {
                        events: Arc::clone(&events),
                    },
                    || Ok(FakeRuntime { failure }),
                )
                .await;

            assert!(result.is_err());
            assert_restored(&events);
        }
    }

    #[tokio::test]
    async fn runtime_creation_error_and_normal_early_exit_restore_terminal() {
        let creation_events = lifecycle_events();
        let mut app = App::new(Arc::new(build_registry()));
        let creation_result = app
            .run_with_terminal(
                RecordingLifecycle {
                    events: Arc::clone(&creation_events),
                },
                || -> io::Result<FakeRuntime> { Err(io::Error::other("terminal init failed")) },
            )
            .await;
        assert!(creation_result.is_err());
        assert_restored(&creation_events);

        let exit_events = lifecycle_events();
        let exit_result = app
            .run_with_terminal(
                RecordingLifecycle {
                    events: Arc::clone(&exit_events),
                },
                || {
                    Ok(FakeRuntime {
                        failure: RuntimeFailure::None,
                    })
                },
            )
            .await;
        assert!(exit_result.is_ok());
        assert_restored(&exit_events);
    }
}
