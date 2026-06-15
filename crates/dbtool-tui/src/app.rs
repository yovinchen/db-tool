use crate::{state::AppState, ui};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use dbtool_core::registry::Registry;
use dbtool_core::service::ConnectionManager;
use ratatui::{backend::CrosstermBackend, Terminal};
use std::sync::Arc;

pub struct App {
    _manager: Arc<ConnectionManager>,
    state: AppState,
}

impl App {
    pub fn new(registry: Arc<Registry>) -> Self {
        let manager = Arc::new(ConnectionManager::new(registry));
        Self {
            _manager: manager,
            state: AppState::default(),
        }
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
                        other => self.state.handle_key(other),
                    }
                }
            }
        }

        disable_raw_mode()?;
        execute!(term.backend_mut(), LeaveAlternateScreen)?;
        Ok(())
    }
}
