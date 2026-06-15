use crate::Error;
use serde::Serialize;
use std::collections::BTreeSet;

use serde_json::{json, Value as JVal};

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum Format {
    #[default]
    Json,
    Table,
    Ndjson,
}

impl std::str::FromStr for Format {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "json" => Ok(Format::Json),
            "table" => Ok(Format::Table),
            "ndjson" => Ok(Format::Ndjson),
            other => Err(format!("unknown format: {other}")),
        }
    }
}

pub struct Formatter;

impl Formatter {
    pub fn success<T: Serialize>(kind: &str, data: T, elapsed_ms: u64, truncated: bool) -> String {
        Self::success_as(Format::Json, kind, data, elapsed_ms, truncated)
    }

    pub fn success_as<T: Serialize>(
        format: Format,
        kind: &str,
        data: T,
        elapsed_ms: u64,
        truncated: bool,
    ) -> String {
        let data = serde_json::to_value(data).unwrap_or_else(|e| {
            json!({
                "serialization_error": e.to_string()
            })
        });
        let envelope = json!({
            "ok":   true,
            "kind": kind,
            "data": data,
            "meta": { "elapsed_ms": elapsed_ms, "truncated": truncated }
        });
        render_success(format, &envelope, truncated)
    }

    pub fn error(err: &Error) -> String {
        Self::error_as(Format::Json, err)
    }

    pub fn error_as(format: Format, err: &Error) -> String {
        if let Error::ConfirmRequired {
            confirm_token,
            impact,
        } = err
        {
            return Self::confirm_required_as(format, confirm_token, impact);
        }

        let envelope = json!({
            "ok":    false,
            "error": {
                "code":    err.code(),
                "message": err.to_string(),
            }
        });
        render_value(format, &envelope, false)
    }

    pub fn confirm_required(token: &str, impact: &JVal) -> String {
        Self::confirm_required_as(Format::Json, token, impact)
    }

    pub fn confirm_required_as(format: Format, token: &str, impact: &JVal) -> String {
        let envelope = json!({
            "ok":    false,
            "error": {
                "code":          "CONFIRM_REQUIRED",
                "message":       "destructive operation requires confirmation",
                "confirm_token": token,
                "impact":        impact,
            }
        });
        render_value(format, &envelope, false)
    }
}

fn render_success(format: Format, envelope: &JVal, truncated: bool) -> String {
    match format {
        Format::Json => serde_json::to_string(envelope).unwrap_or_default(),
        Format::Table => render_table_value(&envelope["data"], truncated),
        Format::Ndjson => render_ndjson_value(&envelope["data"]),
    }
}

fn render_value(format: Format, value: &JVal, truncated: bool) -> String {
    match format {
        Format::Json => serde_json::to_string(value).unwrap_or_default(),
        Format::Table => render_table_value(value, truncated),
        Format::Ndjson => render_ndjson_value(value),
    }
}

fn render_table_value(value: &JVal, truncated: bool) -> String {
    let mut rendered = if let Some((headers, rows)) = result_set_table(value) {
        render_table(&headers, &rows)
    } else {
        render_generic_table(value)
    };

    if truncated {
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str("# truncated");
    }

    rendered
}

fn result_set_table(value: &JVal) -> Option<(Vec<String>, Vec<Vec<String>>)> {
    let columns = value.get("columns")?.as_array()?;
    let rows = value.get("rows")?.as_array()?;
    let headers = columns
        .iter()
        .map(|column| {
            column
                .get("name")
                .and_then(JVal::as_str)
                .unwrap_or("column")
                .to_owned()
        })
        .collect::<Vec<_>>();
    let rows = rows
        .iter()
        .map(|row| {
            row.as_array()
                .map(|cells| cells.iter().map(cell_text).collect::<Vec<_>>())
        })
        .collect::<Option<Vec<_>>>()?;

    Some((headers, rows))
}

fn render_generic_table(value: &JVal) -> String {
    match value {
        JVal::Array(items) => render_array_table(items),
        JVal::Object(map) => {
            let rows = map
                .iter()
                .map(|(key, value)| vec![key.clone(), cell_text(value)])
                .collect::<Vec<_>>();
            render_table(&["key".to_owned(), "value".to_owned()], &rows)
        }
        scalar => render_table(&["value".to_owned()], &[vec![cell_text(scalar)]]),
    }
}

fn render_array_table(items: &[JVal]) -> String {
    if items.is_empty() {
        return render_table(&["value".to_owned()], &[]);
    }

    if items.iter().all(JVal::is_object) {
        let headers = items
            .iter()
            .flat_map(|item| item.as_object().into_iter().flat_map(|map| map.keys()))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let rows = items
            .iter()
            .map(|item| {
                let map = item.as_object().expect("objects were checked above");
                headers
                    .iter()
                    .map(|header| map.get(header).map(cell_text).unwrap_or_default())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        return render_table(&headers, &rows);
    }

    let rows = items
        .iter()
        .map(|item| vec![cell_text(item)])
        .collect::<Vec<_>>();
    render_table(&["value".to_owned()], &rows)
}

fn render_table(headers: &[String], rows: &[Vec<String>]) -> String {
    let headers = if headers.is_empty() {
        vec!["value".to_owned()]
    } else {
        headers.to_vec()
    };
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();

    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if index >= widths.len() {
                widths.push(0);
            }
            widths[index] = widths[index].max(escape_cell(cell).len());
        }
    }

    let mut lines = Vec::with_capacity(rows.len() + 2);
    lines.push(render_table_row(&headers, &widths));
    lines.push(render_separator(&widths));
    lines.extend(rows.iter().map(|row| render_table_row(row, &widths)));
    lines.join("\n")
}

fn render_table_row(cells: &[String], widths: &[usize]) -> String {
    let cells = widths
        .iter()
        .enumerate()
        .map(|(index, width)| {
            let value = cells.get(index).map(String::as_str).unwrap_or_default();
            format!(" {:width$} ", escape_cell(value), width = width)
        })
        .collect::<Vec<_>>();
    format!("|{}|", cells.join("|"))
}

fn render_separator(widths: &[usize]) -> String {
    let cells = widths
        .iter()
        .map(|width| format!(" {} ", "-".repeat(*width)))
        .collect::<Vec<_>>();
    format!("|{}|", cells.join("|"))
}

fn render_ndjson_value(value: &JVal) -> String {
    if let Some(rows) = result_set_ndjson_rows(value) {
        return rows
            .iter()
            .map(|row| serde_json::to_string(row).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
    }

    match value {
        JVal::Array(items) => items
            .iter()
            .map(|item| serde_json::to_string(item).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n"),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn result_set_ndjson_rows(value: &JVal) -> Option<Vec<JVal>> {
    let columns = value.get("columns")?.as_array()?;
    let rows = value.get("rows")?.as_array()?;
    let headers = columns
        .iter()
        .map(|column| {
            column
                .get("name")
                .and_then(JVal::as_str)
                .unwrap_or("column")
                .to_owned()
        })
        .collect::<Vec<_>>();

    rows.iter()
        .map(|row| {
            let cells = row.as_array()?;
            let mut map = serde_json::Map::new();
            for (index, header) in headers.iter().enumerate() {
                map.insert(
                    header.clone(),
                    cells.get(index).cloned().unwrap_or(JVal::Null),
                );
            }
            Some(JVal::Object(map))
        })
        .collect()
}

fn cell_text(value: &JVal) -> String {
    match value {
        JVal::Null => String::new(),
        JVal::Bool(value) => value.to_string(),
        JVal::Number(value) => value.to_string(),
        JVal::String(value) => value.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn escape_cell(value: &str) -> String {
    value.replace('\n', "\\n").replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirm_required_error_exposes_token_and_impact() {
        let rendered = Formatter::error(&Error::ConfirmRequired {
            confirm_token: "abc123".to_owned(),
            impact: json!({ "op": "DROP", "target": "conn:prod" }),
        });

        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], "CONFIRM_REQUIRED");
        assert_eq!(value["error"]["confirm_token"], "abc123");
        assert_eq!(value["error"]["impact"]["target"], "conn:prod");
    }

    #[test]
    fn table_format_renders_result_sets_as_rows() {
        let data = json!({
            "columns": [
                { "name": "id", "type_name": "int", "nullable": false },
                { "name": "name", "type_name": "text", "nullable": true }
            ],
            "rows": [
                [1, "alice"],
                [2, "bob"]
            ],
            "truncated": false
        });

        let rendered = Formatter::success_as(Format::Table, "sqlite", data, 1, false);

        assert!(rendered.contains("| id | name  |"));
        assert!(rendered.contains("| 1  | alice |"));
        assert!(rendered.contains("| 2  | bob   |"));
    }

    #[test]
    fn ndjson_format_renders_result_sets_as_row_objects() {
        let data = json!({
            "columns": [
                { "name": "id", "type_name": "int", "nullable": false },
                { "name": "name", "type_name": "text", "nullable": true }
            ],
            "rows": [
                [1, "alice"],
                [2, "bob"]
            ],
            "truncated": false
        });

        let rendered = Formatter::success_as(Format::Ndjson, "sqlite", data, 1, false);
        let lines = rendered.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 2);
        assert_eq!(serde_json::from_str::<JVal>(lines[0]).unwrap()["id"], 1);
        assert_eq!(
            serde_json::from_str::<JVal>(lines[1]).unwrap()["name"],
            "bob"
        );
    }

    #[test]
    fn table_format_marks_truncated_output() {
        let rendered = Formatter::success_as(Format::Table, "redis", vec!["a", "b"], 1, true);

        assert!(rendered.ends_with("# truncated"));
    }
}
