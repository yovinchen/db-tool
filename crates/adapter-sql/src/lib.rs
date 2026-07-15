mod identifier;
pub mod mysql;
pub mod postgres;
pub mod sqlite;
mod value;

pub use mysql::mysql_factory;
pub use postgres::postgres_factory;
pub use sqlite::sqlite_factory;

use chrono::{DateTime, Utc};
use dbtool_core::{
    error::{Error, Result},
    model::{IndexInfo, Value},
};

pub(crate) fn timestamp_utc(value: i64, position: usize, backend: &str) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value).ok_or_else(|| {
        Error::Query(format!(
            "{backend} SQL parameter {position} is outside the supported timestamp range"
        ))
    })
}

pub(crate) fn structured_json(value: &Value) -> Result<serde_json::Value> {
    match value {
        Value::Json(value) => Ok(value.clone()),
        Value::Array(_) | Value::Map(_) => value.to_plain_json(),
        _ => Err(Error::Internal(
            "structured_json called for a scalar SQL parameter".into(),
        )),
    }
}

/// Fold a flat stream of `(index_name, unique, primary, column_name)` rows
/// (already sorted by `index_name`) into a `Vec<IndexInfo>`.
pub(crate) fn group_index_rows(
    rows: impl Iterator<Item = (String, bool, bool, String)>,
) -> Vec<IndexInfo> {
    let mut indexes: Vec<IndexInfo> = Vec::new();
    for (name, unique, primary, col) in rows {
        match indexes.last_mut() {
            Some(idx) if idx.name == name => idx.columns.push(col),
            _ => indexes.push(IndexInfo {
                name,
                columns: vec![col],
                unique,
                primary,
            }),
        }
    }
    indexes
}
