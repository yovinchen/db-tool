use crate::state::{AppState, Panel};
use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame,
};

pub fn render(f: &mut Frame, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
        .split(f.area());

    // Connection list panel
    let items: Vec<ListItem> = state
        .connections
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let style = if i == state.selected {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            let mode = if c.readonly { "ro" } else { "rw" };
            ListItem::new(format!("{} ({mode})", c.name)).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title("Connections [Tab]")
            .borders(Borders::ALL)
            .border_style(panel_style(&state.active_panel, Panel::ConnectionList)),
    );
    f.render_widget(list, chunks[0]);

    // Right: query + results
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(chunks[1]);

    let input_title = if state.pending_write.is_some() {
        "Query [y confirm / n cancel]"
    } else {
        "Query [Enter, Up/Down history]"
    };
    let input = Paragraph::new(state.query_input.as_str()).block(
        Block::default()
            .title(input_title)
            .borders(Borders::ALL)
            .border_style(panel_style(&state.active_panel, Panel::QueryInput)),
    );
    f.render_widget(input, right[0]);

    let results = Paragraph::new(state.result_text.as_str()).block(
        Block::default()
            .title("Results [Tab]")
            .borders(Borders::ALL)
            .border_style(panel_style(&state.active_panel, Panel::Results)),
    );
    f.render_widget(results, right[1]);
}

fn panel_style(active: &Panel, target: Panel) -> Style {
    if *active == target {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    }
}
