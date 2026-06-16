use crossterm::event::KeyCode;

const MAX_COMMAND_HISTORY: usize = 50;

#[derive(Default, Clone, Debug, PartialEq)]
pub struct AppState {
    pub active_panel: Panel,
    pub connections: Vec<ConnectionItem>,
    pub selected: usize,
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
    QueryInput,
    Results,
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
                    Panel::ConnectionList => Panel::QueryInput,
                    Panel::QueryInput => Panel::Results,
                    Panel::Results => Panel::ConnectionList,
                };
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
            KeyCode::Backspace if self.active_panel == Panel::QueryInput => {
                self.reset_history_navigation();
                self.query_input.pop();
                StateAction::None
            }
            _ => StateAction::None,
        }
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
}
