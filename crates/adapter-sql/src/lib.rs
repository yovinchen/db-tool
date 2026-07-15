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
    service::limiter::ListLimiter,
};
use std::collections::BTreeSet;

use crate::identifier::{parse_table_ref, validate_identifier, TableRef};

/// Validate a caller's catalog budget and convert its N+1 probe to the signed
/// integer accepted by SQL LIMIT parameters. Callers use this before acquiring
/// a connection or issuing any backend request.
pub(crate) fn bounded_catalog_limit(max_items: usize, backend: &str) -> Result<(ListLimiter, i64)> {
    let limiter = ListLimiter::new(max_items);
    let probe_items = limiter.probe_items()?;
    let sql_limit = i64::try_from(probe_items).map_err(|_| {
        Error::Config(format!(
            "{backend} catalog limit is too large for a SQL LIMIT parameter"
        ))
    })?;
    Ok((limiter, sql_limit))
}

pub(crate) fn validate_atomic_insert(
    table: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Result<TableRef> {
    let table = parse_table_ref(table)?;
    if columns.is_empty() {
        return Err(Error::Query(
            "atomic SQL insert requires at least one target column".into(),
        ));
    }

    let mut normalized = BTreeSet::new();
    for column in columns {
        validate_identifier(column)?;
        if !normalized.insert(column.to_ascii_lowercase()) {
            return Err(Error::Query(format!(
                "duplicate SQL insert column: {column}"
            )));
        }
    }

    for (index, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(Error::Query(format!(
                "atomic SQL insert row {} has {} values but {} columns were supplied",
                index + 1,
                row.len(),
                columns.len()
            )));
        }
    }

    Ok(table)
}

pub(crate) fn quoted_identifier(identifier: &str, quote: char) -> String {
    debug_assert!(matches!(quote, '"' | '`'));
    format!("{quote}{identifier}{quote}")
}

pub(crate) fn quoted_table(table: &TableRef, quote: char) -> String {
    match &table.schema {
        Some(schema) => format!(
            "{}.{}",
            quoted_identifier(schema, quote),
            quoted_identifier(&table.name, quote)
        ),
        None => quoted_identifier(&table.name, quote),
    }
}

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

#[cfg(test)]
mod bounded_catalog_tests {
    use super::*;

    #[test]
    fn catalog_limit_rejects_zero_probe_overflow_and_sql_parameter_overflow() {
        assert!(matches!(
            bounded_catalog_limit(0, "test"),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            bounded_catalog_limit(usize::MAX, "test"),
            Err(Error::Config(_))
        ));
        if usize::MAX > i64::MAX as usize {
            assert!(matches!(
                bounded_catalog_limit(i64::MAX as usize, "test"),
                Err(Error::Config(_))
            ));
        }
        assert_eq!(bounded_catalog_limit(2, "test").unwrap().1, 3);
    }
}
