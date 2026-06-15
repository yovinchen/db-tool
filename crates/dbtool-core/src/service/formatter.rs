use crate::Error;
use serde::Serialize;
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
        let envelope = json!({
            "ok":   true,
            "kind": kind,
            "data": data,
            "meta": { "elapsed_ms": elapsed_ms, "truncated": truncated }
        });
        serde_json::to_string(&envelope).unwrap_or_default()
    }

    pub fn error(err: &Error) -> String {
        let envelope = json!({
            "ok":    false,
            "error": {
                "code":    err.code(),
                "message": err.to_string(),
            }
        });
        serde_json::to_string(&envelope).unwrap_or_default()
    }

    pub fn confirm_required(token: &str, impact: &JVal) -> String {
        let envelope = json!({
            "ok":    false,
            "error": {
                "code":          "CONFIRM_REQUIRED",
                "message":       "destructive operation requires confirmation",
                "confirm_token": token,
                "impact":        impact,
            }
        });
        serde_json::to_string(&envelope).unwrap_or_default()
    }
}
