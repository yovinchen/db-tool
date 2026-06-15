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
        if let Error::ConfirmRequired {
            confirm_token,
            impact,
        } = err
        {
            return Self::confirm_required(confirm_token, impact);
        }

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
}
