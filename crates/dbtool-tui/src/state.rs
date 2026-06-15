use crossterm::event::KeyCode;

#[derive(Default)]
pub struct AppState {
    pub active_panel: Panel,
    pub connections: Vec<String>,
    pub selected: usize,
    pub query_input: String,
    pub result_text: String,
}

#[derive(Default, Clone, PartialEq)]
pub enum Panel {
    #[default]
    ConnectionList,
    QueryInput,
    Results,
}

impl AppState {
    pub fn handle_key(&mut self, key: KeyCode) {
        match key {
            KeyCode::Tab => {
                self.active_panel = match self.active_panel {
                    Panel::ConnectionList => Panel::QueryInput,
                    Panel::QueryInput => Panel::Results,
                    Panel::Results => Panel::ConnectionList,
                };
            }
            KeyCode::Up if self.active_panel == Panel::ConnectionList && self.selected > 0 => {
                self.selected -= 1;
            }
            KeyCode::Down
                if self.active_panel == Panel::ConnectionList
                    && self.selected + 1 < self.connections.len() =>
            {
                self.selected += 1;
            }
            KeyCode::Char(c) if self.active_panel == Panel::QueryInput => {
                self.query_input.push(c);
            }
            KeyCode::Backspace if self.active_panel == Panel::QueryInput => {
                self.query_input.pop();
            }
            _ => {}
        }
    }
}
