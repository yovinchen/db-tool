use crate::state::{AppState, CommandFormState, Panel};
use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
    Frame,
};

pub fn render(f: &mut Frame, state: &AppState) {
    let screen = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(f.area());

    let header = Paragraph::new(status_text(state)).block(
        Block::default()
            .title("dbtool TUI")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(header, screen[0]);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
        .split(screen[1]);

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

    // Right: command form + query + results
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(chunks[1]);

    let form = Paragraph::new(form_text(&state.form))
        .block(
            Block::default()
                .title("Form [F2/F3/F4]")
                .borders(Borders::ALL)
                .border_style(panel_style(&state.active_panel, Panel::CommandForm)),
        )
        .wrap(Wrap { trim: false });
    f.render_widget(form, right[0]);

    let input_title = if state.pending_write.is_some() {
        "Query [y confirm / n cancel]"
    } else {
        "Query [Enter, Up/Down history]"
    };
    let input = Paragraph::new(state.query_input.as_str())
        .block(
            Block::default()
                .title(input_title)
                .borders(Borders::ALL)
                .border_style(panel_style(&state.active_panel, Panel::QueryInput)),
        )
        .wrap(Wrap { trim: false });
    f.render_widget(input, right[1]);

    let results = Paragraph::new(state.result_text.as_str())
        .block(
            Block::default()
                .title("Results [Tab]")
                .borders(Borders::ALL)
                .border_style(panel_style(&state.active_panel, Panel::Results)),
        )
        .wrap(Wrap { trim: false });
    f.render_widget(results, right[2]);

    let footer = Paragraph::new(footer_text(state)).style(Style::default().fg(Color::DarkGray));
    f.render_widget(footer, screen[2]);
}

fn status_text(state: &AppState) -> String {
    let (connection, mode) = state
        .selected_connection()
        .map(|connection| {
            let mode = if connection.readonly {
                "readonly"
            } else {
                "read-write"
            };
            (connection.name.as_str(), mode)
        })
        .unwrap_or(("none", "unknown"));

    format!(
        "connection: {connection} | mode: {mode} | limit: {} | history: {} | form: {} | panel: {:?}",
        state.limit,
        state.command_history.len(),
        state.form.kind.label(),
        state.active_panel
    )
}

fn form_text(form: &CommandFormState) -> String {
    let labels = form.kind.field_labels();
    if labels.is_empty() {
        return format!("{} -> {}", form.kind.label(), form.command());
    }

    let active = form.active_label().unwrap_or("none");
    let fields = labels
        .iter()
        .enumerate()
        .map(|(index, label)| {
            let marker = if index == form.active_field { ">" } else { " " };
            let value = form
                .fields
                .get(index)
                .map(|field| field.as_str())
                .filter(|field| !field.is_empty())
                .unwrap_or("<default>");
            format!("{marker} {label}: {value}")
        })
        .collect::<Vec<_>>()
        .join(" | ");

    format!(
        "{} -> {}\nactive: {active} | {fields}",
        form.kind.label(),
        form.command()
    )
}

fn footer_text(state: &AppState) -> String {
    match &state.pending_write {
        Some(command) => format!("pending write: awaiting confirmation | command: {command}"),
        None => format!(
            "pending write: none | result bytes: {}",
            state.result_text.len()
        ),
    }
}

fn panel_style(active: &Panel, target: Panel) -> Style {
    if *active == target {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::ConnectionItem;
    use ratatui::{backend::TestBackend, buffer::Buffer, Terminal};

    #[test]
    fn status_text_summarizes_selected_runtime_state() {
        let mut state = AppState::with_connections(vec![ConnectionItem {
            name: "primary".to_owned(),
            dsn: "sqlite::memory:".to_owned(),
            readonly: true,
        }]);
        state.limit = 42;
        state.command_history.push("ping".to_owned());
        state.active_panel = Panel::Results;

        let status = status_text(&state);

        assert!(status.contains("connection: primary"));
        assert!(status.contains("mode: readonly"));
        assert!(status.contains("limit: 42"));
        assert!(status.contains("history: 1"));
        assert!(status.contains("form: SQL query"));
        assert!(status.contains("panel: Results"));
    }

    #[test]
    fn render_exposes_status_and_footer_in_full_screen_layout() {
        let backend = TestBackend::new(96, 18);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut state = AppState::with_connections(vec![ConnectionItem {
            name: "primary".to_owned(),
            dsn: "sqlite::memory:".to_owned(),
            readonly: false,
        }]);
        state.command_history.push("ping".to_owned());
        state.result_text = "{\"rows\":[{\"id\":1}]}".to_owned();

        terminal.draw(|f| render(f, &state)).unwrap();

        let rendered = buffer_text(terminal.backend().buffer());
        assert!(rendered.contains("dbtool TUI"));
        assert!(rendered.contains("Form"));
        assert!(rendered.contains("connection: primary"));
        assert!(rendered.contains("mode: read-write"));
        assert!(rendered.contains("history: 1"));
        assert!(rendered.contains("SQL query"));
        assert!(rendered.contains("pending write: none"));
        assert!(rendered.contains("result bytes: 19"));
    }

    #[test]
    fn form_text_renders_active_field_and_generated_command() {
        let mut form = CommandFormState::new(crate::state::CapabilityForm::SearchQuery);
        for c in "users".chars() {
            form.push_char(c);
        }
        form.next_field();
        for c in "{\"query\":{\"match_all\":{}}}".chars() {
            form.push_char(c);
        }

        let rendered = form_text(&form);

        assert!(rendered.contains("Search query"));
        assert!(rendered.contains("search users {\"query\":{\"match_all\":{}}}"));
        assert!(rendered.contains("active: query"));
        assert!(rendered.contains("> query: {\"query\":{\"match_all\":{}}}"));
    }

    fn buffer_text(buffer: &Buffer) -> String {
        buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<Vec<_>>()
            .join("")
    }
}
