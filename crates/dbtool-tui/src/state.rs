use crossterm::event::KeyCode;

#[derive(Default, Clone, Debug, PartialEq)]
pub struct AppState {
    pub active_panel: Panel,
    pub connections: Vec<ConnectionItem>,
    pub selected: usize,
    pub query_input: String,
    pub result_text: String,
    pub pending_write: Option<String>,
    pub limit: usize,
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
            KeyCode::Enter if self.active_panel == Panel::QueryInput => StateAction::Execute,
            KeyCode::Char('y') if self.pending_write.is_some() => StateAction::ConfirmWrite,
            KeyCode::Char('n') if self.pending_write.is_some() => {
                self.pending_write = None;
                self.result_text = "Write command cancelled".to_owned();
                StateAction::None
            }
            KeyCode::Char(c) if self.active_panel == Panel::QueryInput => {
                self.query_input.push(c);
                StateAction::None
            }
            KeyCode::Backspace if self.active_panel == Panel::QueryInput => {
                self.query_input.pop();
                StateAction::None
            }
            _ => StateAction::None,
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
}
