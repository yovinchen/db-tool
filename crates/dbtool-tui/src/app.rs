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
    model::{
        BoundedList, FindOptions, InputBudget, MetadataBudget, Point, ReadBudget, SqlExecuteInput,
        TimeRange, TimeSeriesReadBudget, Value,
    },
    port::{capability::SetOptions, CapabilityOperation, CapabilityReport},
    registry::Registry,
    service::{
        safety::{SafetyGuard, StatementKind},
        ConnectionManager, InputLimiter,
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

enum PreparedMutation {
    SqlExecute {
        sql: String,
    },
    KeyValueSet {
        key: String,
        value: Vec<u8>,
        options: SetOptions,
    },
    KeyValueDelete {
        keys: Vec<String>,
    },
    SearchIndexDocument {
        index: String,
        document: Value,
    },
    TimeSeriesWritePoints {
        points: Vec<Point>,
    },
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

fn prepare_tui_mutation(
    connection: &ConnectionItem,
    command: &str,
    confirmed_write: bool,
) -> Result<Option<PreparedMutation>> {
    let command = command.trim();
    if let Some(sql) = sql_statement_from_command(command) {
        let kind = authorize_sql_statement(connection, sql, confirmed_write)?;
        let explicit_execute = command == "exec"
            || command.starts_with("exec ")
            || command == "sql exec"
            || command.starts_with("sql exec ");
        if explicit_execute || kind != StatementKind::Read {
            return prepare_sql_execute(sql).map(Some);
        }
        return Ok(None);
    }

    if command == "kv set" || command.starts_with("kv set ") {
        let rest = command.strip_prefix("kv set").unwrap_or_default().trim();
        let mut parts = rest.split_whitespace();
        let key = parts
            .next()
            .ok_or_else(|| Error::Config("kv set requires a key".into()))?
            .to_owned();
        let value = parts.collect::<Vec<_>>().join(" ").into_bytes();
        return prepare_key_value_set(key, value, SetOptions::default()).map(Some);
    }

    if command == "kv del" || command.starts_with("kv del ") {
        let keys = command
            .strip_prefix("kv del")
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        return prepare_key_value_delete(keys).map(Some);
    }

    if command == "search index" || command.starts_with("search index ") {
        let rest = command
            .strip_prefix("search index")
            .unwrap_or_default()
            .trim();
        let (index, document) = rest
            .split_once(' ')
            .ok_or_else(|| Error::Config("search index requires index and JSON doc".into()))?;
        return prepare_search_index(index.to_owned(), parse_json_value(document.trim())?)
            .map(Some);
    }

    if command == "ts write" || command.starts_with("ts write ") {
        let raw = command.strip_prefix("ts write").unwrap_or_default().trim();
        return prepare_time_series_write(vec![parse_tui_ts_point(raw)?]).map(Some);
    }

    Ok(None)
}

fn prepare_sql_execute(sql: &str) -> Result<PreparedMutation> {
    let params: &[Value] = &[];
    let limiter = InputLimiter::new(InputBudget::default(), "TUI SQL execute input")?;
    limiter.validate_request(&SqlExecuteInput { sql, params })?;
    Ok(PreparedMutation::SqlExecute {
        sql: sql.to_owned(),
    })
}

fn prepare_key_value_set(
    key: String,
    value: Vec<u8>,
    options: SetOptions,
) -> Result<PreparedMutation> {
    InputLimiter::new(InputBudget::default(), "TUI key-value set input")?.validate_request(&(
        &key,
        value.as_slice(),
        &options,
    ))?;
    Ok(PreparedMutation::KeyValueSet {
        key,
        value,
        options,
    })
}

fn prepare_key_value_delete(keys: Vec<String>) -> Result<PreparedMutation> {
    if keys.is_empty() {
        return Err(Error::Config("kv del requires at least one key".into()));
    }
    InputLimiter::new(InputBudget::default(), "TUI key-value delete input")?
        .validate_batch(&keys)?;
    Ok(PreparedMutation::KeyValueDelete { keys })
}

fn prepare_search_index(index: String, document: Value) -> Result<PreparedMutation> {
    let body = document.to_plain_json()?;
    if !body.is_object() {
        return Err(Error::Config(
            "search index document body must be a JSON object".to_owned(),
        ));
    }
    InputLimiter::new(InputBudget::default(), "TUI search index input")?
        .validate_request(&serde_json::json!({ "index": &index, "document": &body }))?;
    Ok(PreparedMutation::SearchIndexDocument { index, document })
}

fn prepare_time_series_write(points: Vec<Point>) -> Result<PreparedMutation> {
    InputLimiter::new(InputBudget::default(), "TUI time-series write input")?
        .validate_batch(&points)?;
    Ok(PreparedMutation::TimeSeriesWritePoints { points })
}

async fn execute_prepared_mutation(
    connector: &dyn dbtool_core::port::Connector,
    mutation: PreparedMutation,
) -> Result<String> {
    match mutation {
        PreparedMutation::SqlExecute { sql } => {
            require_operation(
                connector,
                CapabilityOperation::SqlExecuteBudgeted,
                "SqlEngine.execute_budgeted",
            )?;
            let engine = connector
                .as_sql()
                .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
            render_json(
                engine
                    .execute_budgeted(&sql, &[], InputBudget::default())
                    .await?,
            )
        }
        PreparedMutation::KeyValueSet {
            key,
            value,
            options,
        } => {
            require_operation(
                connector,
                CapabilityOperation::KeyValueSetBudgeted,
                "KeyValueStore.set_budgeted",
            )?;
            let store = connector
                .as_kv()
                .ok_or_else(|| unsupported(connector, "KeyValueStore"))?;
            store
                .set_budgeted(&key, &value, options, InputBudget::default())
                .await?;
            render_json(serde_json::json!({ "ok": true }))
        }
        PreparedMutation::KeyValueDelete { keys } => {
            require_operation(
                connector,
                CapabilityOperation::KeyValueDeleteBudgeted,
                "KeyValueStore.delete_budgeted",
            )?;
            let store = connector
                .as_kv()
                .ok_or_else(|| unsupported(connector, "KeyValueStore"))?;
            render_json(serde_json::json!({
                "deleted": store.delete_budgeted(&keys, InputBudget::default()).await?
            }))
        }
        PreparedMutation::SearchIndexDocument { index, document } => {
            require_operation(
                connector,
                CapabilityOperation::SearchIndexDocumentBudgeted,
                "SearchEngine.index_doc_budgeted",
            )?;
            let engine = connector
                .as_search()
                .ok_or_else(|| unsupported(connector, "SearchEngine"))?;
            engine
                .index_doc_budgeted(&index, document, InputBudget::default())
                .await?;
            render_json(serde_json::json!({ "indexed": true }))
        }
        PreparedMutation::TimeSeriesWritePoints { points } => {
            require_operation(
                connector,
                CapabilityOperation::TimeSeriesWritePointsBudgeted,
                "TimeSeriesStore.write_points_budgeted",
            )?;
            let store = connector
                .as_timeseries()
                .ok_or_else(|| unsupported(connector, "TimeSeriesStore"))?;
            let written_points = points.len();
            store
                .write_points_budgeted(points, InputBudget::default())
                .await?;
            render_json(serde_json::json!({
                "written_points": written_points,
                "written_samples": written_points
            }))
        }
    }
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
    let mutation = prepare_tui_mutation(connection, command, confirmed_write)?;
    let conn = manager.get_or_connect(&connection.dsn).await?;
    let connector = conn.as_ref().as_ref();
    if let Some(mutation) = mutation {
        return execute_prepared_mutation(connector, mutation).await;
    }
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
                "capabilities": CapabilityReport::new(
                    connector.capabilities(),
                    connector.operations()
                )
            }))
        }
        "caps" => render_json(CapabilityReport::new(
            connector.capabilities(),
            connector.operations(),
        )),
        "tables" => {
            require_operation(
                connector,
                CapabilityOperation::SqlListTablesBudgeted,
                "SqlEngine.list_tables_budgeted",
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
            render_bounded_list(
                sql.list_tables_budgeted(schema, ReadBudget::with_default_bytes(limit)?)
                    .await?,
            )
        }
        "schemas" => {
            if parts.next().is_some() {
                return Err(Error::Config("schemas does not accept arguments".into()));
            }
            require_operation(
                connector,
                CapabilityOperation::SqlListSchemasBudgeted,
                "SqlEngine.list_schemas_budgeted",
            )?;
            let sql = connector
                .as_sql()
                .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
            render_bounded_list(
                sql.list_schemas_budgeted(ReadBudget::with_default_bytes(limit)?)
                    .await?,
            )
        }
        "schema" => {
            let table = parts
                .next()
                .ok_or_else(|| Error::Config("schema requires a table name".into()))?;
            require_operation(
                connector,
                CapabilityOperation::SqlDescribeTableBounded,
                "SqlEngine.describe_table_bounded",
            )?;
            let sql = connector
                .as_sql()
                .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
            render_json(
                sql.describe_table_bounded(table, MetadataBudget::with_default_bytes(limit)?)
                    .await?,
            )
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
    let kind = authorize_sql_statement(connection, sql_text, confirmed_write)?;
    if kind != StatementKind::Read {
        return execute_prepared_mutation(connector, prepare_sql_execute(sql_text)?).await;
    }
    require_operation(
        connector,
        CapabilityOperation::SqlQueryBudgeted,
        "SqlEngine.query_budgeted",
    )?;
    let sql = connector
        .as_sql()
        .ok_or_else(|| unsupported(connector, "SqlEngine"))?;
    let result = sql
        .query_budgeted(sql_text, &[], ReadBudget::with_default_bytes(limit)?)
        .await?;
    render_json(result)
}

async fn run_sql_exec(
    connector: &dyn dbtool_core::port::Connector,
    connection: &ConnectionItem,
    sql_text: &str,
    confirmed_write: bool,
) -> Result<String> {
    authorize_sql_statement(connection, sql_text, confirmed_write)?;
    execute_prepared_mutation(connector, prepare_sql_execute(sql_text)?).await
}

async fn run_kv_command(
    connector: &dyn dbtool_core::port::Connector,
    command: &str,
    limit: usize,
) -> Result<String> {
    let mut parts = command.split_whitespace();
    match parts.next() {
        Some("get") => {
            require_operation(
                connector,
                CapabilityOperation::KeyValueGetBounded,
                "KeyValueStore.get_bounded",
            )?;
            let kv = connector
                .as_kv()
                .ok_or_else(|| unsupported(connector, "KeyValueStore"))?;
            let key = parts
                .next()
                .ok_or_else(|| Error::Config("kv get requires a key".into()))?;
            let value = kv
                .get_bounded(key, ReadBudget::with_default_bytes(1)?)
                .await?
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned());
            render_json(serde_json::json!({ "key": key, "value": value }))
        }
        Some("scan") => {
            require_operation(
                connector,
                CapabilityOperation::KeyValueScanBounded,
                "KeyValueStore.scan_bounded",
            )?;
            let kv = connector
                .as_kv()
                .ok_or_else(|| unsupported(connector, "KeyValueStore"))?;
            let pattern = parts.next().unwrap_or("*");
            render_bounded_list(
                kv.scan_bounded(pattern, ReadBudget::with_default_bytes(limit)?)
                    .await?,
            )
        }
        Some("set") => {
            let key = parts
                .next()
                .ok_or_else(|| Error::Config("kv set requires a key".into()))?
                .to_owned();
            let value = parts.collect::<Vec<_>>().join(" ").into_bytes();
            execute_prepared_mutation(
                connector,
                prepare_key_value_set(key, value, SetOptions::default())?,
            )
            .await
        }
        Some("del") => {
            let keys = parts.map(str::to_owned).collect::<Vec<_>>();
            execute_prepared_mutation(connector, prepare_key_value_delete(keys)?).await
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
    if command == "collections" {
        require_operation(
            connector,
            CapabilityOperation::DocumentListCollectionsBudgeted,
            "DocumentStore.list_collections_budgeted",
        )?;
        let doc = connector
            .as_document()
            .ok_or_else(|| unsupported(connector, "DocumentStore"))?;
        return render_bounded_list(
            doc.list_collections_budgeted(ReadBudget::with_default_bytes(limit)?)
                .await?,
        );
    }
    if let Some(rest) = command.strip_prefix("find ").map(str::trim) {
        require_operation(
            connector,
            CapabilityOperation::DocumentFindBudgeted,
            "DocumentStore.find_budgeted",
        )?;
        let doc = connector
            .as_document()
            .ok_or_else(|| unsupported(connector, "DocumentStore"))?;
        let (collection, filter) = rest
            .split_once(' ')
            .map(|(collection, filter)| (collection, filter.trim()))
            .unwrap_or((rest, "{}"));
        let filter = parse_json_value(filter)?;
        let result = doc
            .find_budgeted(
                collection,
                filter,
                FindOptions {
                    limit: None,
                    ..Default::default()
                },
                ReadBudget::with_default_bytes(limit)?,
            )
            .await?;
        return render_bounded_list(result);
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
    if command == "indices" {
        require_operation(
            connector,
            CapabilityOperation::SearchListIndicesBudgeted,
            "SearchEngine.list_indices_budgeted",
        )?;
        let search = connector
            .as_search()
            .ok_or_else(|| unsupported(connector, "SearchEngine"))?;
        return render_bounded_list(
            search
                .list_indices_budgeted(ReadBudget::with_default_bytes(limit)?)
                .await?,
        );
    }
    if let Some(rest) = command.strip_prefix("index ").map(str::trim) {
        let (index, doc) = rest
            .split_once(' ')
            .ok_or_else(|| Error::Config("search index requires index and JSON doc".into()))?;
        return execute_prepared_mutation(
            connector,
            prepare_search_index(index.to_owned(), parse_json_value(doc.trim())?)?,
        )
        .await;
    }
    require_operation(
        connector,
        CapabilityOperation::SearchSearchBudgeted,
        "SearchEngine.search_budgeted",
    )?;
    let search = connector
        .as_search()
        .ok_or_else(|| unsupported(connector, "SearchEngine"))?;
    let (index, query) = command
        .split_once(' ')
        .ok_or_else(|| Error::Config("search requires index and JSON query".into()))?;
    let result = search
        .search_budgeted(
            index,
            parse_json_value(query.trim())?,
            dbtool_core::port::capability::SearchOptions {
                size: Some(limit),
                from: None,
                source: false,
            },
            ReadBudget::with_default_bytes(limit)?,
        )
        .await?;
    render_json(result)
}

async fn run_ts_command(
    connector: &dyn dbtool_core::port::Connector,
    command: &str,
    limit: usize,
) -> Result<String> {
    if command == "measurements" {
        require_operation(
            connector,
            CapabilityOperation::TimeSeriesListMeasurementsBudgeted,
            "TimeSeriesStore.list_measurements_budgeted",
        )?;
        let ts = connector
            .as_timeseries()
            .ok_or_else(|| unsupported(connector, "TimeSeriesStore"))?;
        return render_bounded_list(
            ts.list_measurements_budgeted(ReadBudget::with_default_bytes(limit)?)
                .await?,
        );
    }
    if let Some(rest) = command.strip_prefix("write ").map(str::trim) {
        let point = parse_tui_ts_point(rest)?;
        return execute_prepared_mutation(connector, prepare_time_series_write(vec![point])?).await;
    }
    require_operation(
        connector,
        CapabilityOperation::TimeSeriesQueryRangeBounded,
        "TimeSeriesStore.query_range_bounded",
    )?;
    let ts = connector
        .as_timeseries()
        .ok_or_else(|| unsupported(connector, "TimeSeriesStore"))?;
    let query = command
        .strip_prefix("query ")
        .map(str::trim)
        .ok_or_else(|| Error::Config("ts command must be measurements, query <expr>, or write <measurement> <value> [field=<name>] [tag=value...]".into()))?;
    render_json(
        ts.query_range_bounded(
            query,
            TimeRange::last_n_minutes(60)?,
            TimeSeriesReadBudget::with_default_bytes(limit, limit)?,
        )
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
        ["schemas"]
            | ["tables"]
            | ["tables", _]
            | ["doc", "collections"]
            | ["search", "indices"]
            | ["ts", "measurements"]
    ) {
        ReadBudget::with_default_bytes(limit)?;
    }
    if matches!(parts.as_slice(), ["schema", _]) {
        MetadataBudget::with_default_bytes(limit)?;
    }
    if matches!(parts.as_slice(), ["ts", "query", ..]) {
        TimeSeriesReadBudget::with_default_bytes(limit, limit)?;
    }
    let is_sql_read = parts.first() == Some(&"sql") && parts.get(1) != Some(&"exec");
    let is_search_read = parts.first() == Some(&"search")
        && !matches!(parts.get(1), None | Some(&"indices") | Some(&"index"));
    if is_sql_read
        || is_search_read
        || matches!(
            parts.as_slice(),
            ["doc", "find", ..] | ["kv", "get", _] | ["kv", "scan", ..]
        )
    {
        ReadBudget::with_default_bytes(limit)?;
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

    let target = safety_target(&connection.dsn)?;
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

fn safety_target(raw_dsn: &str) -> Result<String> {
    match Dsn::parse(raw_dsn) {
        Ok(dsn) => {
            let display = format!("dsn:{}", dsn.redacted());
            SafetyGuard::bind_target_scope(&display, &dsn.raw)
        }
        Err(_) => SafetyGuard::bind_target_scope("dsn:<unparsed>", raw_dsn),
    }
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
    use async_trait::async_trait;
    use crossterm::event::{KeyEvent, KeyModifiers};
    use dbtool_core::{
        model::{
            ExecOutcome, IndexInfo, ResultSet, SearchDeleteIndexOutcome, SearchDocument,
            SearchHits, SearchWriteOutcome, SeriesSet, TableInfo, TableSchema,
        },
        port::{
            Capabilities, Connector, ConnectorKind, KeyValueStore, SearchEngine, SqlEngine,
            TimeSeriesStore,
        },
    };
    use dbtool_registry::build_registry;
    use std::sync::Mutex;

    struct MutationProbe {
        advertise_exact: bool,
        calls: Mutex<Vec<&'static str>>,
    }

    impl MutationProbe {
        fn new(advertise_exact: bool) -> Self {
            Self {
                advertise_exact,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn record(&self, operation: &'static str) {
            self.calls.lock().unwrap().push(operation);
        }

        fn recorded(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Connector for MutationProbe {
        fn kind(&self) -> ConnectorKind {
            ConnectorKind("tui-mutation-probe".to_owned())
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities {
                sql: true,
                key_value: true,
                time_series: true,
                search: true,
                ..Default::default()
            }
        }

        fn operations(&self) -> Vec<CapabilityOperation> {
            let mut operations = self.capabilities().operations();
            if self.advertise_exact {
                operations.extend([
                    CapabilityOperation::SqlExecuteBudgeted,
                    CapabilityOperation::KeyValueSetBudgeted,
                    CapabilityOperation::KeyValueDeleteBudgeted,
                    CapabilityOperation::SearchIndexDocumentBudgeted,
                    CapabilityOperation::TimeSeriesWritePointsBudgeted,
                ]);
            }
            operations
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        fn as_sql(&self) -> Option<&dyn SqlEngine> {
            Some(self)
        }

        fn as_kv(&self) -> Option<&dyn KeyValueStore> {
            Some(self)
        }

        fn as_timeseries(&self) -> Option<&dyn TimeSeriesStore> {
            Some(self)
        }

        fn as_search(&self) -> Option<&dyn SearchEngine> {
            Some(self)
        }
    }

    #[async_trait]
    impl SqlEngine for MutationProbe {
        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<ResultSet> {
            unreachable!("mutation probe does not execute SQL reads")
        }

        async fn query_bounded(
            &self,
            _sql: &str,
            _params: &[Value],
            _max_rows: usize,
        ) -> Result<ResultSet> {
            unreachable!("mutation probe does not execute SQL reads")
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<ExecOutcome> {
            self.record("sql.execute");
            Ok(ExecOutcome {
                rows_affected: 1,
                last_insert_id: None,
            })
        }

        async fn execute_budgeted(
            &self,
            sql: &str,
            _params: &[Value],
            budget: InputBudget,
        ) -> Result<ExecOutcome> {
            assert_eq!(budget, InputBudget::default());
            self.record("sql.execute_budgeted");
            if sql == "indeterminate" {
                return Err(Error::OutcomeIndeterminate(
                    "probe cannot prove the remote write outcome".to_owned(),
                ));
            }
            Ok(ExecOutcome {
                rows_affected: 1,
                last_insert_id: None,
            })
        }

        async fn list_schemas(&self) -> Result<Vec<String>> {
            unreachable!("mutation probe does not inspect SQL catalogs")
        }

        async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
            unreachable!("mutation probe does not inspect SQL catalogs")
        }

        async fn describe_table(&self, _table: &str) -> Result<TableSchema> {
            unreachable!("mutation probe does not inspect SQL catalogs")
        }
    }

    #[async_trait]
    impl KeyValueStore for MutationProbe {
        async fn get(&self, _key: &str) -> Result<Option<bytes::Bytes>> {
            unreachable!("mutation probe does not execute key-value reads")
        }

        async fn set(&self, _key: &str, _value: &[u8], _options: SetOptions) -> Result<()> {
            self.record("kv.set");
            Ok(())
        }

        async fn set_budgeted(
            &self,
            _key: &str,
            _value: &[u8],
            _options: SetOptions,
            budget: InputBudget,
        ) -> Result<()> {
            assert_eq!(budget, InputBudget::default());
            self.record("kv.set_budgeted");
            Ok(())
        }

        async fn delete(&self, _keys: &[String]) -> Result<u64> {
            self.record("kv.delete");
            Ok(1)
        }

        async fn delete_budgeted(&self, _keys: &[String], budget: InputBudget) -> Result<u64> {
            assert_eq!(budget, InputBudget::default());
            self.record("kv.delete_budgeted");
            Ok(1)
        }

        async fn scan(&self, _pattern: &str, _limit: usize) -> Result<Vec<String>> {
            unreachable!("mutation probe does not execute key-value reads")
        }

        async fn raw_command(&self, _args: &[String]) -> Result<Value> {
            unreachable!("mutation probe does not execute raw commands")
        }
    }

    #[async_trait]
    impl SearchEngine for MutationProbe {
        async fn list_indices(&self) -> Result<Vec<IndexInfo>> {
            unreachable!("mutation probe does not inspect search indices")
        }

        async fn search(
            &self,
            _index: &str,
            _query: Value,
            _options: dbtool_core::port::capability::SearchOptions,
        ) -> Result<SearchHits> {
            unreachable!("mutation probe does not execute search reads")
        }

        async fn index_doc(&self, index: &str, _doc: Value) -> Result<SearchWriteOutcome> {
            self.record("search.index_doc");
            Ok(search_write_outcome(index))
        }

        async fn index_doc_budgeted(
            &self,
            index: &str,
            _doc: Value,
            budget: InputBudget,
        ) -> Result<SearchWriteOutcome> {
            assert_eq!(budget, InputBudget::default());
            self.record("search.index_doc_budgeted");
            Ok(search_write_outcome(index))
        }

        async fn put_doc(
            &self,
            _index: &str,
            _id: &str,
            _doc: Value,
        ) -> Result<SearchWriteOutcome> {
            unreachable!("mutation probe does not execute search put")
        }

        async fn get_doc(&self, _index: &str, _id: &str) -> Result<Option<SearchDocument>> {
            unreachable!("mutation probe does not execute search reads")
        }

        async fn update_doc(
            &self,
            _index: &str,
            _id: &str,
            _patch: Value,
        ) -> Result<SearchWriteOutcome> {
            unreachable!("mutation probe does not execute search updates")
        }

        async fn delete_doc(&self, _index: &str, _id: &str) -> Result<SearchWriteOutcome> {
            unreachable!("mutation probe does not execute search deletes")
        }

        async fn delete_index(&self, _index: &str) -> Result<SearchDeleteIndexOutcome> {
            unreachable!("mutation probe does not delete search indices")
        }
    }

    #[async_trait]
    impl TimeSeriesStore for MutationProbe {
        async fn list_measurements(&self) -> Result<Vec<String>> {
            unreachable!("mutation probe does not inspect measurements")
        }

        async fn write_points(&self, _points: Vec<Point>) -> Result<()> {
            self.record("time_series.write_points");
            Ok(())
        }

        async fn write_points_budgeted(
            &self,
            _points: Vec<Point>,
            budget: InputBudget,
        ) -> Result<()> {
            assert_eq!(budget, InputBudget::default());
            self.record("time_series.write_points_budgeted");
            Ok(())
        }

        async fn query_range(&self, _query: &str, _range: TimeRange) -> Result<SeriesSet> {
            unreachable!("mutation probe does not execute time-series reads")
        }
    }

    fn search_write_outcome(index: &str) -> SearchWriteOutcome {
        SearchWriteOutcome {
            index: index.to_owned(),
            id: "generated".to_owned(),
            result: "created".to_owned(),
            version: Some(1),
            seq_no: None,
            primary_term: None,
            extra: serde_json::Map::new(),
        }
    }

    fn mutation_samples() -> Vec<PreparedMutation> {
        vec![
            prepare_sql_execute("insert into events values (1)").unwrap(),
            prepare_key_value_set("key".to_owned(), b"value".to_vec(), SetOptions::default())
                .unwrap(),
            prepare_key_value_delete(vec!["key".to_owned()]).unwrap(),
            prepare_search_index(
                "events".to_owned(),
                Value::Json(serde_json::json!({ "id": 1 })),
            )
            .unwrap(),
            prepare_time_series_write(vec![Point {
                measurement: "requests_total".to_owned(),
                tags: HashMap::new(),
                fields: HashMap::from([("value".to_owned(), 1.0)]),
                timestamp: 1,
            }])
            .unwrap(),
        ]
    }

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
    fn tui_confirmation_targets_bind_hidden_dsn_identity_without_exposing_it() {
        let first_marker = "TUI_TOKEN_MARKER_ONE";
        let second_marker = "TUI_TOKEN_MARKER_TWO";
        let first = safety_target(&format!(
            "nats://{first_marker}@127.0.0.1:4222?auth=tenant-one"
        ))
        .unwrap();
        let second = safety_target(&format!(
            "nats://{second_marker}@127.0.0.1:4222?auth=tenant-two"
        ))
        .unwrap();
        for target in [&first, &second] {
            assert!(!target.contains(first_marker));
            assert!(!target.contains(second_marker));
            assert!(!target.contains("tenant-one"));
            assert!(!target.contains("tenant-two"));
        }

        let first_error =
            SafetyGuard::check_with_target("drop table events", &first, true, None).unwrap_err();
        let (first_token, first_impact) = match first_error {
            Error::ConfirmRequired {
                confirm_token,
                impact,
            } => (confirm_token, impact),
            other => panic!("expected confirmation requirement, got {other:?}"),
        };
        let second_token =
            match SafetyGuard::check_with_target("drop table events", &second, true, None)
                .unwrap_err()
            {
                Error::ConfirmRequired { confirm_token, .. } => confirm_token,
                other => panic!("expected confirmation requirement, got {other:?}"),
            };
        assert_ne!(first_token, second_token);
        let visible = first_impact["target"].as_str().unwrap();
        assert!(visible.starts_with("dsn:nats://***@127.0.0.1:4222"));
        assert!(!visible.contains(first_marker));
        assert!(!visible.contains("tenant-one"));

        let variable = "DBTOOL_TUI_CONFIRMATION_SCOPE_TOKEN";
        let template = format!("nats://${{{variable}}}@127.0.0.1:4222?auth=tenant");
        std::env::set_var(variable, first_marker);
        let first_expansion = safety_target(&template).unwrap();
        std::env::set_var(variable, second_marker);
        let second_expansion = safety_target(&template).unwrap();
        std::env::remove_var(variable);
        assert_ne!(first_expansion, second_expansion);
        assert!(!first_expansion.contains(first_marker));
        assert!(!second_expansion.contains(second_marker));
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
                &[CapabilityOperation::SqlQuery],
                CapabilityOperation::SqlQueryBudgeted,
                "legacy-sql".into(),
                "SqlEngine.query_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "SqlEngine.query_budgeted"
        ));
        assert!(matches!(
            require_declared_operation(
                CapabilityOperation::SQL,
                CapabilityOperation::SqlDescribeTableBounded,
                "legacy-sql".into(),
                "SqlEngine.describe_table_bounded",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "SqlEngine.describe_table_bounded"
        ));
        assert!(matches!(
            require_declared_operation(
                CapabilityOperation::SQL,
                CapabilityOperation::SqlListTablesBudgeted,
                "legacy-sql".into(),
                "SqlEngine.list_tables_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "SqlEngine.list_tables_budgeted"
        ));
    }

    #[test]
    fn invalid_connection_file_is_propagated_with_secret_safe_diagnostics() {
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
            Error::Config(message)
                if message == "connection config is invalid TOML or contains unsupported fields"
                    && !message.contains("readonli")
                    && !message.contains("postgres://")
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
        let ping: serde_json::Value = serde_json::from_str(&ping).unwrap();
        assert_eq!(ping["capabilities"]["sql"], true);
        assert_eq!(
            ping["capabilities"]["operations"],
            serde_json::json!([
                "sql.describe_table",
                "sql.describe_table_bounded",
                "sql.execute",
                "sql.execute_budgeted",
                "sql.insert_rows_atomic",
                "sql.insert_rows_atomic_budgeted",
                "sql.list_schemas",
                "sql.list_schemas_bounded",
                "sql.list_schemas_budgeted",
                "sql.list_tables",
                "sql.list_tables_bounded",
                "sql.list_tables_budgeted",
                "sql.query",
                "sql.query_bounded",
                "sql.query_budgeted"
            ])
        );

        let caps: serde_json::Value = serde_json::from_str(
            &execute_tui_command(&manager, &connection, "caps", 100, false)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(caps, ping["capabilities"]);
        let operations = caps["operations"].as_array().unwrap();
        assert!(operations.windows(2).all(|pair| {
            pair[0].as_str().expect("operation name") < pair[1].as_str().expect("operation name")
        }));

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
    fn kv_document_search_and_time_series_reads_require_explicit_bounded_operations() {
        for (operations, operation, kind, needed) in [
            (
                CapabilityOperation::KEY_VALUE,
                CapabilityOperation::KeyValueGetBounded,
                "legacy-kv",
                "KeyValueStore.get_bounded",
            ),
            (
                CapabilityOperation::KEY_VALUE,
                CapabilityOperation::KeyValueScanBounded,
                "legacy-kv",
                "KeyValueStore.scan_bounded",
            ),
            (
                CapabilityOperation::DOCUMENT,
                CapabilityOperation::DocumentListCollectionsBudgeted,
                "legacy-document",
                "DocumentStore.list_collections_budgeted",
            ),
            (
                CapabilityOperation::DOCUMENT,
                CapabilityOperation::DocumentFindBudgeted,
                "legacy-document",
                "DocumentStore.find_budgeted",
            ),
            (
                CapabilityOperation::SEARCH,
                CapabilityOperation::SearchListIndicesBudgeted,
                "legacy-search",
                "SearchEngine.list_indices_budgeted",
            ),
            (
                CapabilityOperation::SEARCH,
                CapabilityOperation::SearchSearchBudgeted,
                "legacy-search",
                "SearchEngine.search_budgeted",
            ),
            (
                CapabilityOperation::TIME_SERIES,
                CapabilityOperation::TimeSeriesListMeasurementsBudgeted,
                "legacy-time-series",
                "TimeSeriesStore.list_measurements_budgeted",
            ),
            (
                CapabilityOperation::TIME_SERIES,
                CapabilityOperation::TimeSeriesQueryRangeBounded,
                "legacy-time-series",
                "TimeSeriesStore.query_range_bounded",
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
    async fn mutation_dispatch_negotiates_and_invokes_only_exact_methods() {
        let connector = MutationProbe::new(true);

        for mutation in mutation_samples() {
            execute_prepared_mutation(&connector, mutation)
                .await
                .unwrap();
        }

        assert_eq!(
            connector.recorded(),
            [
                "sql.execute_budgeted",
                "kv.set_budgeted",
                "kv.delete_budgeted",
                "search.index_doc_budgeted",
                "time_series.write_points_budgeted",
            ]
        );
    }

    #[tokio::test]
    async fn legacy_only_mutation_declarations_are_rejected_without_dispatch() {
        let connector = MutationProbe::new(false);

        for mutation in mutation_samples() {
            assert!(matches!(
                execute_prepared_mutation(&connector, mutation).await,
                Err(Error::UnsupportedCapability { kind, .. })
                    if kind == "tui-mutation-probe"
            ));
        }

        assert!(connector.recorded().is_empty());
    }

    #[tokio::test]
    async fn indeterminate_mutation_errors_are_returned_without_retry() {
        let connector = MutationProbe::new(true);

        let error =
            execute_prepared_mutation(&connector, prepare_sql_execute("indeterminate").unwrap())
                .await
                .unwrap_err();

        assert_eq!(error.code(), "OUTCOME_INDETERMINATE");
        assert_eq!(connector.recorded(), ["sql.execute_budgeted"]);
    }

    #[tokio::test]
    async fn mutation_input_budget_is_rejected_before_tui_connection() {
        let registry = Arc::new(build_registry());
        let manager = ConnectionManager::new(registry);
        let connection = ConnectionItem {
            name: "unreachable".to_owned(),
            dsn: "mongodb://127.0.0.1:1/dbtool".to_owned(),
            readonly: false,
        };
        let keys = (0..=InputBudget::default().max_items)
            .map(|index| format!("key-{index}"))
            .collect::<Vec<_>>()
            .join(" ");
        let command = format!("kv del {keys}");

        assert!(matches!(
            execute_tui_command(&manager, &connection, &command, 100, true).await,
            Err(Error::InputBudgetExceeded { unit: "items", .. })
        ));
        assert!(matches!(
            execute_tui_command(
                &manager,
                &connection,
                "search index events []",
                100,
                true,
            )
            .await,
            Err(Error::Config(message)) if message.contains("JSON object")
        ));
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

        for command in [
            "tables main",
            "schemas",
            "schema users",
            "sql query select 1",
            "doc collections",
            "doc find users {}",
            "search indices",
            "search users {}",
            "ts measurements",
        ] {
            assert!(matches!(
                execute_tui_command(&manager, &connection, command, 0, false).await,
                Err(Error::Config(message)) if message.contains("greater than zero")
            ));
        }

        assert!(validate_bounded_catalog_limit("sql exec create table t(id int)", 0).is_ok());
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
