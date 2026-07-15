use crate::Error;
use serde::Serialize;
use std::collections::BTreeSet;

use serde_json::{json, Value as JVal};

type TableData = (Vec<String>, Vec<Vec<String>>);

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
        let data = match serde_json::to_value(data) {
            Ok(data) => data,
            Err(error) => {
                return Self::error_as(format, &Error::Serialization(error.to_string()));
            }
        };
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
    let rendered = match format {
        Format::Json => serde_json::to_string(envelope),
        Format::Table => render_table_value(&envelope["data"], truncated),
        Format::Ndjson => render_success_ndjson(envelope),
    };
    render_or_serialization_error(format, rendered)
}

fn render_value(format: Format, value: &JVal, truncated: bool) -> String {
    let rendered = match format {
        Format::Json | Format::Ndjson => serde_json::to_string(value),
        Format::Table => render_table_value(value, truncated),
    };
    render_or_serialization_error(format, rendered)
}

fn render_or_serialization_error(
    format: Format,
    rendered: Result<String, serde_json::Error>,
) -> String {
    match rendered {
        Ok(rendered) => rendered,
        Err(error) => render_serialization_error(format, &error),
    }
}

fn render_serialization_error(format: Format, error: &serde_json::Error) -> String {
    const FALLBACK: &str = r#"{"ok":false,"error":{"code":"SERIALIZATION_ERROR","message":"serialization error while rendering formatter output"}}"#;

    let envelope = json!({
        "ok": false,
        "error": {
            "code": "SERIALIZATION_ERROR",
            "message": format!("serialization error: {error}"),
        }
    });

    match format {
        Format::Json | Format::Ndjson => {
            serde_json::to_string(&envelope).unwrap_or_else(|_| FALLBACK.to_owned())
        }
        Format::Table => render_table_value(&envelope, false).unwrap_or_else(|_| {
            "SERIALIZATION_ERROR: formatter output could not be rendered".to_owned()
        }),
    }
}

fn render_table_value(value: &JVal, truncated: bool) -> Result<String, serde_json::Error> {
    let mut rendered = if let Some((headers, rows)) = result_set_table(value)? {
        render_table(&headers, &rows)
    } else {
        render_generic_table(value)?
    };

    if truncated {
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str("# truncated");
    }

    Ok(rendered)
}

fn result_set_table(value: &JVal) -> Result<Option<TableData>, serde_json::Error> {
    let Some(columns) = value.get("columns").and_then(JVal::as_array) else {
        return Ok(None);
    };
    let Some(rows) = value.get("rows").and_then(JVal::as_array) else {
        return Ok(None);
    };
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
    let mut rendered_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(cells) = row.as_array() else {
            return Ok(None);
        };
        rendered_rows.push(cells.iter().map(cell_text).collect::<Result<Vec<_>, _>>()?);
    }

    Ok(Some((headers, rendered_rows)))
}

fn render_generic_table(value: &JVal) -> Result<String, serde_json::Error> {
    match value {
        JVal::Array(items) => render_array_table(items),
        JVal::Object(map) => {
            let rows = map
                .iter()
                .map(|(key, value)| Ok(vec![key.clone(), cell_text(value)?]))
                .collect::<Result<Vec<_>, serde_json::Error>>()?;
            Ok(render_table(&["key".to_owned(), "value".to_owned()], &rows))
        }
        scalar => Ok(render_table(
            &["value".to_owned()],
            &[vec![cell_text(scalar)?]],
        )),
    }
}

fn render_array_table(items: &[JVal]) -> Result<String, serde_json::Error> {
    if items.is_empty() {
        return Ok(render_table(&["value".to_owned()], &[]));
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
                    .map(|header| match map.get(header) {
                        Some(value) => cell_text(value),
                        None => Ok(String::new()),
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(render_table(&headers, &rows));
    }

    let rows = items
        .iter()
        .map(|item| Ok(vec![cell_text(item)?]))
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    Ok(render_table(&["value".to_owned()], &rows))
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

// NDJSON uses one self-contained envelope per line. Result sets start with a
// schema record, followed by ordered row arrays. Keeping rows positional avoids
// losing values when a SQL query returns duplicate column names.
fn render_success_ndjson(envelope: &JVal) -> Result<String, serde_json::Error> {
    let data = &envelope["data"];
    let records = if let Some((schema, rows)) = result_set_ndjson_records(data) {
        let mut records = Vec::with_capacity(rows.len() + 1);
        records.push(ndjson_record(envelope, "schema", schema));
        records.extend(
            rows.iter()
                .cloned()
                .map(|row| ndjson_record(envelope, "row", row)),
        );
        records
    } else if let Some(items) = data.as_array() {
        if items.is_empty() {
            vec![ndjson_record(envelope, "data", data.clone())]
        } else {
            items
                .iter()
                .cloned()
                .map(|item| ndjson_record(envelope, "item", item))
                .collect()
        }
    } else {
        vec![ndjson_record(envelope, "data", data.clone())]
    };

    records
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .map(|lines| lines.join("\n"))
}

fn result_set_ndjson_records(value: &JVal) -> Option<(JVal, &[JVal])> {
    let object = value.as_object()?;
    object.get("columns")?.as_array()?;
    let rows = object.get("rows")?.as_array()?;

    let mut schema = object.clone();
    schema.remove("rows");
    Some((JVal::Object(schema), rows))
}

fn ndjson_record(envelope: &JVal, record: &str, data: JVal) -> JVal {
    json!({
        "ok": envelope["ok"].clone(),
        "kind": envelope["kind"].clone(),
        "record": record,
        "data": data,
        "meta": envelope["meta"].clone(),
    })
}

fn cell_text(value: &JVal) -> Result<String, serde_json::Error> {
    match value {
        JVal::Null => Ok(String::new()),
        JVal::Bool(value) => Ok(value.to_string()),
        JVal::Number(value) => Ok(value.to_string()),
        JVal::String(value) => Ok(value.clone()),
        other => serde_json::to_string(other),
    }
}

fn escape_cell(value: &str) -> String {
    value.replace('\n', "\\n").replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SerializationFailure;

    impl Serialize for SerializationFailure {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(<S::Error as serde::ser::Error>::custom(
                "intentional formatter test failure",
            ))
        }
    }

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
    fn ndjson_format_renders_self_describing_result_set_records() {
        let data = json!({
            "columns": [
                { "name": "id", "type_name": "int", "nullable": false },
                { "name": "name", "type_name": "text", "nullable": true }
            ],
            "rows": [
                [1, "alice"],
                [2, "bob"]
            ],
            "truncated": true
        });

        let rendered = Formatter::success_as(Format::Ndjson, "sqlite", data, 7, true);
        let records = rendered
            .lines()
            .map(|line| serde_json::from_str::<JVal>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(records.len(), 3);
        for record in &records {
            assert_eq!(record["ok"], true);
            assert_eq!(record["kind"], "sqlite");
            assert_eq!(record["meta"]["elapsed_ms"], 7);
            assert_eq!(record["meta"]["truncated"], true);
        }
        assert_eq!(records[0]["record"], "schema");
        assert_eq!(records[0]["data"]["columns"][0]["name"], "id");
        assert_eq!(records[1]["record"], "row");
        assert_eq!(records[1]["data"], json!([1, "alice"]));
        assert_eq!(records[2]["data"], json!([2, "bob"]));
    }

    #[test]
    fn ndjson_result_set_preserves_duplicate_columns_and_positional_values() {
        let data = json!({
            "columns": [
                { "name": "value", "type_name": "int", "nullable": false },
                { "name": "value", "type_name": "int", "nullable": false }
            ],
            "rows": [[1, 2]],
            "truncated": false
        });

        let rendered = Formatter::success_as(Format::Ndjson, "sqlite", data, 1, false);
        let records = rendered
            .lines()
            .map(|line| serde_json::from_str::<JVal>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["data"]["columns"].as_array().unwrap().len(), 2);
        assert_eq!(records[0]["data"]["columns"][0]["name"], "value");
        assert_eq!(records[0]["data"]["columns"][1]["name"], "value");
        assert_eq!(records[1]["record"], "row");
        assert_eq!(records[1]["data"], json!([1, 2]));
    }

    #[test]
    fn ndjson_empty_result_set_still_emits_schema_and_metadata() {
        let data = json!({
            "columns": [{ "name": "id", "type_name": "int", "nullable": false }],
            "rows": [],
            "truncated": false
        });

        let rendered = Formatter::success_as(Format::Ndjson, "postgres", data, 3, false);
        let record: JVal = serde_json::from_str(&rendered).unwrap();

        assert_eq!(record["record"], "schema");
        assert_eq!(record["kind"], "postgres");
        assert_eq!(record["meta"]["elapsed_ms"], 3);
        assert_eq!(record["meta"]["truncated"], false);
    }

    #[test]
    fn serialization_failure_is_never_rendered_as_success_or_empty_output() {
        for format in [Format::Json, Format::Ndjson] {
            let rendered = Formatter::success_as(format, "test", SerializationFailure, 1, false);
            assert!(!rendered.is_empty());
            let value: JVal = serde_json::from_str(&rendered).unwrap();
            assert_eq!(value["ok"], false);
            assert_eq!(value["error"]["code"], "SERIALIZATION_ERROR");
        }

        let rendered = Formatter::success_as(Format::Table, "test", SerializationFailure, 1, false);
        assert!(!rendered.is_empty());
        assert!(rendered.contains("SERIALIZATION_ERROR"));
        assert!(!rendered.contains("| ok    | true"));
    }

    #[test]
    fn table_format_marks_truncated_output() {
        let rendered = Formatter::success_as(Format::Table, "redis", vec!["a", "b"], 1, true);

        assert!(rendered.ends_with("# truncated"));
    }
}
