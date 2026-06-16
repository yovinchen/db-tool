use crossterm::event::KeyCode;

const MAX_COMMAND_HISTORY: usize = 50;

#[derive(Default, Clone, Debug, PartialEq)]
pub struct AppState {
    pub active_panel: Panel,
    pub connections: Vec<ConnectionItem>,
    pub selected: usize,
    pub form: CommandFormState,
    pub query_input: String,
    pub result_text: String,
    pub pending_write: Option<String>,
    pub limit: usize,
    pub command_history: Vec<String>,
    pub history_cursor: Option<usize>,
    pub history_draft: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionItem {
    pub name: String,
    pub dsn: String,
    pub readonly: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateAction {
    None,
    Execute,
    ConfirmWrite,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Panel {
    #[default]
    ConnectionList,
    CommandForm,
    QueryInput,
    Results,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandFormState {
    pub kind: CapabilityForm,
    pub fields: Vec<String>,
    pub active_field: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CapabilityForm {
    #[default]
    SqlQuery,
    SqlExec,
    SqlTables,
    SqlSchema,
    KvGet,
    KvSet,
    KvScan,
    KvDel,
    DocCollections,
    DocFind,
    SearchIndices,
    SearchQuery,
    SearchIndex,
    TimeSeriesMeasurements,
    TimeSeriesQuery,
}

impl AppState {
    pub fn with_connections(connections: Vec<ConnectionItem>) -> Self {
        Self {
            connections,
            limit: 100,
            ..Default::default()
        }
    }

    pub fn selected_connection(&self) -> Option<&ConnectionItem> {
        self.connections.get(self.selected)
    }

    pub fn record_command(&mut self, command: &str) {
        let command = command.trim();
        if command.is_empty() {
            return;
        }

        if self
            .command_history
            .last()
            .is_some_and(|last| last == command)
        {
            self.reset_history_navigation();
            return;
        }

        self.command_history.push(command.to_owned());
        if self.command_history.len() > MAX_COMMAND_HISTORY {
            let overflow = self.command_history.len() - MAX_COMMAND_HISTORY;
            self.command_history.drain(0..overflow);
        }
        self.reset_history_navigation();
    }

    pub fn handle_key(&mut self, key: KeyCode) -> StateAction {
        match key {
            KeyCode::Tab => {
                self.active_panel = match self.active_panel {
                    Panel::ConnectionList => Panel::CommandForm,
                    Panel::CommandForm => Panel::QueryInput,
                    Panel::QueryInput => Panel::Results,
                    Panel::Results => Panel::ConnectionList,
                };
                StateAction::None
            }
            KeyCode::F(2) => {
                self.form.next_form();
                StateAction::None
            }
            KeyCode::F(3) => {
                self.form.next_field();
                StateAction::None
            }
            KeyCode::F(4) => {
                self.apply_form_to_query();
                StateAction::None
            }
            KeyCode::Up if self.active_panel == Panel::ConnectionList && self.selected > 0 => {
                self.selected -= 1;
                StateAction::None
            }
            KeyCode::Down
                if self.active_panel == Panel::ConnectionList
                    && self.selected + 1 < self.connections.len() =>
            {
                self.selected += 1;
                StateAction::None
            }
            KeyCode::Up if self.active_panel == Panel::CommandForm => {
                self.form.previous_form();
                StateAction::None
            }
            KeyCode::Down if self.active_panel == Panel::CommandForm => {
                self.form.next_form();
                StateAction::None
            }
            KeyCode::Left if self.active_panel == Panel::CommandForm => {
                self.form.previous_field();
                StateAction::None
            }
            KeyCode::Right if self.active_panel == Panel::CommandForm => {
                self.form.next_field();
                StateAction::None
            }
            KeyCode::Enter if self.active_panel == Panel::CommandForm => {
                self.apply_form_to_query();
                StateAction::None
            }
            KeyCode::Up if self.active_panel == Panel::QueryInput => {
                self.recall_previous_command();
                StateAction::None
            }
            KeyCode::Down if self.active_panel == Panel::QueryInput => {
                self.recall_next_command();
                StateAction::None
            }
            KeyCode::Enter if self.active_panel == Panel::QueryInput => StateAction::Execute,
            KeyCode::Char('y') if self.pending_write.is_some() => StateAction::ConfirmWrite,
            KeyCode::Char('n') if self.pending_write.is_some() => {
                self.pending_write = None;
                self.result_text = "Write command cancelled".to_owned();
                StateAction::None
            }
            KeyCode::Char(c) if self.active_panel == Panel::QueryInput => {
                self.reset_history_navigation();
                self.query_input.push(c);
                StateAction::None
            }
            KeyCode::Char(c) if self.active_panel == Panel::CommandForm => {
                self.form.push_char(c);
                StateAction::None
            }
            KeyCode::Backspace if self.active_panel == Panel::QueryInput => {
                self.reset_history_navigation();
                self.query_input.pop();
                StateAction::None
            }
            KeyCode::Backspace if self.active_panel == Panel::CommandForm => {
                self.form.pop_char();
                StateAction::None
            }
            _ => StateAction::None,
        }
    }

    pub fn apply_form_to_query(&mut self) {
        self.query_input = self.form.command();
        self.active_panel = Panel::QueryInput;
        self.reset_history_navigation();
    }

    fn recall_previous_command(&mut self) {
        if self.command_history.is_empty() {
            return;
        }

        let next_cursor = match self.history_cursor {
            Some(0) => 0,
            Some(index) => index - 1,
            None => {
                self.history_draft = self.query_input.clone();
                self.command_history.len() - 1
            }
        };
        self.history_cursor = Some(next_cursor);
        self.query_input = self.command_history[next_cursor].clone();
    }

    fn recall_next_command(&mut self) {
        let Some(cursor) = self.history_cursor else {
            return;
        };

        if cursor + 1 < self.command_history.len() {
            let next_cursor = cursor + 1;
            self.history_cursor = Some(next_cursor);
            self.query_input = self.command_history[next_cursor].clone();
        } else {
            self.query_input = self.history_draft.clone();
            self.reset_history_navigation();
        }
    }

    fn reset_history_navigation(&mut self) {
        self.history_cursor = None;
        self.history_draft.clear();
    }
}

impl CommandFormState {
    pub fn new(kind: CapabilityForm) -> Self {
        Self {
            kind,
            fields: vec![String::new(); kind.field_labels().len()],
            active_field: 0,
        }
    }

    pub fn active_label(&self) -> Option<&'static str> {
        self.kind.field_labels().get(self.active_field).copied()
    }

    pub fn next_form(&mut self) {
        self.kind = self.kind.next();
        self.reset_fields();
    }

    pub fn previous_form(&mut self) {
        self.kind = self.kind.previous();
        self.reset_fields();
    }

    pub fn next_field(&mut self) {
        if !self.fields.is_empty() {
            self.active_field = (self.active_field + 1) % self.fields.len();
        }
    }

    pub fn previous_field(&mut self) {
        if !self.fields.is_empty() {
            self.active_field = if self.active_field == 0 {
                self.fields.len() - 1
            } else {
                self.active_field - 1
            };
        }
    }

    pub fn push_char(&mut self, c: char) {
        if let Some(field) = self.fields.get_mut(self.active_field) {
            field.push(c);
        }
    }

    pub fn pop_char(&mut self) {
        if let Some(field) = self.fields.get_mut(self.active_field) {
            field.pop();
        }
    }

    pub fn command(&self) -> String {
        self.kind.command(&self.fields)
    }

    fn reset_fields(&mut self) {
        self.fields = vec![String::new(); self.kind.field_labels().len()];
        self.active_field = 0;
    }
}

impl Default for CommandFormState {
    fn default() -> Self {
        Self::new(CapabilityForm::default())
    }
}

impl CapabilityForm {
    pub fn all() -> &'static [Self] {
        &[
            Self::SqlQuery,
            Self::SqlExec,
            Self::SqlTables,
            Self::SqlSchema,
            Self::KvGet,
            Self::KvSet,
            Self::KvScan,
            Self::KvDel,
            Self::DocCollections,
            Self::DocFind,
            Self::SearchIndices,
            Self::SearchQuery,
            Self::SearchIndex,
            Self::TimeSeriesMeasurements,
            Self::TimeSeriesQuery,
        ]
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::SqlQuery => "SQL query",
            Self::SqlExec => "SQL exec",
            Self::SqlTables => "SQL tables",
            Self::SqlSchema => "SQL schema",
            Self::KvGet => "KV get",
            Self::KvSet => "KV set",
            Self::KvScan => "KV scan",
            Self::KvDel => "KV delete",
            Self::DocCollections => "Document collections",
            Self::DocFind => "Document find",
            Self::SearchIndices => "Search indices",
            Self::SearchQuery => "Search query",
            Self::SearchIndex => "Search index",
            Self::TimeSeriesMeasurements => "Time-series measurements",
            Self::TimeSeriesQuery => "Time-series query",
        }
    }

    pub fn field_labels(self) -> &'static [&'static str] {
        match self {
            Self::SqlQuery => &["sql"],
            Self::SqlExec => &["statement"],
            Self::SqlTables => &[],
            Self::SqlSchema => &["table"],
            Self::KvGet => &["key"],
            Self::KvSet => &["key", "value"],
            Self::KvScan => &["pattern"],
            Self::KvDel => &["key"],
            Self::DocCollections => &[],
            Self::DocFind => &["collection", "filter"],
            Self::SearchIndices => &[],
            Self::SearchQuery => &["index", "query"],
            Self::SearchIndex => &["index", "document"],
            Self::TimeSeriesMeasurements => &[],
            Self::TimeSeriesQuery => &["expression"],
        }
    }

    pub fn defaults(self) -> &'static [&'static str] {
        match self {
            Self::SqlQuery => &["select 1"],
            Self::SqlExec => &["create table example (id integer primary key)"],
            Self::SqlTables => &[],
            Self::SqlSchema => &["example"],
            Self::KvGet => &["key"],
            Self::KvSet => &["key", "value"],
            Self::KvScan => &["*"],
            Self::KvDel => &["key"],
            Self::DocCollections => &[],
            Self::DocFind => &["collection", "{}"],
            Self::SearchIndices => &[],
            Self::SearchQuery => &["index", "{}"],
            Self::SearchIndex => &["index", "{}"],
            Self::TimeSeriesMeasurements => &[],
            Self::TimeSeriesQuery => &["up"],
        }
    }

    fn next(self) -> Self {
        let forms = Self::all();
        let index = forms.iter().position(|form| *form == self).unwrap_or(0);
        forms[(index + 1) % forms.len()]
    }

    fn previous(self) -> Self {
        let forms = Self::all();
        let index = forms.iter().position(|form| *form == self).unwrap_or(0);
        forms[(index + forms.len() - 1) % forms.len()]
    }

    fn command(self, fields: &[String]) -> String {
        let value = |index: usize| {
            fields
                .get(index)
                .map(|field| field.trim())
                .filter(|field| !field.is_empty())
                .unwrap_or_else(|| self.defaults()[index])
        };

        match self {
            Self::SqlQuery => format!("sql {}", value(0)),
            Self::SqlExec => format!("sql exec {}", value(0)),
            Self::SqlTables => "tables".to_owned(),
            Self::SqlSchema => format!("schema {}", value(0)),
            Self::KvGet => format!("kv get {}", value(0)),
            Self::KvSet => format!("kv set {} {}", value(0), value(1)),
            Self::KvScan => format!("kv scan {}", value(0)),
            Self::KvDel => format!("kv del {}", value(0)),
            Self::DocCollections => "doc collections".to_owned(),
            Self::DocFind => format!("doc find {} {}", value(0), value(1)),
            Self::SearchIndices => "search indices".to_owned(),
            Self::SearchQuery => format!("search {} {}", value(0), value(1)),
            Self::SearchIndex => format!("search index {} {}", value(0), value(1)),
            Self::TimeSeriesMeasurements => "ts measurements".to_owned(),
            Self::TimeSeriesQuery => format!("ts query {}", value(0)),
        }
    }
}

impl Default for ConnectionItem {
    fn default() -> Self {
        Self {
            name: "scratch-sqlite".to_owned(),
            dsn: "sqlite::memory:".to_owned(),
            readonly: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn navigation_selects_connections_and_panels() {
        let mut state = AppState::with_connections(vec![
            ConnectionItem {
                name: "one".to_owned(),
                dsn: "sqlite::memory:".to_owned(),
                readonly: true,
            },
            ConnectionItem {
                name: "two".to_owned(),
                dsn: "sqlite::memory:".to_owned(),
                readonly: false,
            },
        ]);

        assert_eq!(state.selected_connection().unwrap().name, "one");
        state.handle_key(KeyCode::Down);
        assert_eq!(state.selected_connection().unwrap().name, "two");
        state.handle_key(KeyCode::Tab);
        assert_eq!(state.active_panel, Panel::CommandForm);
        state.handle_key(KeyCode::Tab);
        assert_eq!(state.active_panel, Panel::QueryInput);
        state.handle_key(KeyCode::Char('p'));
        state.handle_key(KeyCode::Char('i'));
        state.handle_key(KeyCode::Char('n'));
        state.handle_key(KeyCode::Char('g'));
        assert_eq!(state.query_input, "ping");
        assert_eq!(state.handle_key(KeyCode::Enter), StateAction::Execute);
    }

    #[test]
    fn pending_write_can_be_confirmed_or_cancelled() {
        let mut state = AppState {
            pending_write: Some("kv set a b".to_owned()),
            ..Default::default()
        };

        assert_eq!(
            state.handle_key(KeyCode::Char('y')),
            StateAction::ConfirmWrite
        );
        state.pending_write = Some("kv set a b".to_owned());
        assert_eq!(state.handle_key(KeyCode::Char('n')), StateAction::None);
        assert!(state.pending_write.is_none());
    }

    #[test]
    fn query_panel_recalls_command_history_without_losing_draft() {
        let mut state = AppState::with_connections(vec![ConnectionItem::default()]);
        state.active_panel = Panel::QueryInput;
        state.record_command("ping");
        state.record_command("sql select 1");
        state.query_input = "caps".to_owned();

        state.handle_key(KeyCode::Up);
        assert_eq!(state.query_input, "sql select 1");
        state.handle_key(KeyCode::Up);
        assert_eq!(state.query_input, "ping");
        state.handle_key(KeyCode::Up);
        assert_eq!(state.query_input, "ping");
        state.handle_key(KeyCode::Down);
        assert_eq!(state.query_input, "sql select 1");
        state.handle_key(KeyCode::Down);
        assert_eq!(state.query_input, "caps");
    }

    #[test]
    fn command_history_is_bounded_and_skips_adjacent_duplicates() {
        let mut state = AppState::default();

        state.record_command(" ping ");
        state.record_command("ping");
        for i in 0..60 {
            state.record_command(&format!("sql select {i}"));
        }

        assert_eq!(state.command_history.len(), MAX_COMMAND_HISTORY);
        assert_eq!(state.command_history.first().unwrap(), "sql select 10");
        assert_eq!(state.command_history.last().unwrap(), "sql select 59");
    }

    #[test]
    fn capability_forms_build_safe_command_strings() {
        let mut form = CommandFormState::new(CapabilityForm::KvSet);
        assert_eq!(form.active_label(), Some("key"));
        form.push_char('u');
        form.push_char('s');
        form.push_char('e');
        form.push_char('r');
        form.next_field();
        assert_eq!(form.active_label(), Some("value"));
        for c in "alice".chars() {
            form.push_char(c);
        }

        assert_eq!(form.command(), "kv set user alice");

        form.next_form();
        assert_eq!(form.kind, CapabilityForm::KvScan);
        assert_eq!(form.command(), "kv scan *");
    }

    #[test]
    fn applying_form_updates_query_input_without_executing() {
        let mut state = AppState::with_connections(vec![ConnectionItem::default()]);
        state.active_panel = Panel::CommandForm;
        state.form = CommandFormState::new(CapabilityForm::DocFind);
        for c in "users".chars() {
            state.form.push_char(c);
        }
        state.form.next_field();
        for c in "{\"active\":true}".chars() {
            state.form.push_char(c);
        }

        assert_eq!(state.handle_key(KeyCode::Enter), StateAction::None);

        assert_eq!(
            state.query_input,
            "doc find users {\"active\":true}".to_owned()
        );
        assert_eq!(state.active_panel, Panel::QueryInput);
    }

    #[test]
    fn form_shortcuts_cycle_and_apply_from_any_panel() {
        let mut state = AppState::with_connections(vec![ConnectionItem::default()]);

        assert_eq!(state.form.kind, CapabilityForm::SqlQuery);
        state.handle_key(KeyCode::F(2));
        assert_eq!(state.form.kind, CapabilityForm::SqlExec);
        state.handle_key(KeyCode::F(4));

        assert_eq!(
            state.query_input,
            "sql exec create table example (id integer primary key)"
        );
        assert_eq!(state.active_panel, Panel::QueryInput);
    }
}
