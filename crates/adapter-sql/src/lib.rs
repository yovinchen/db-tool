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
    model::{ColumnMeta, IndexInfo, ReadBudget, ResultSet, Value},
    service::limiter::{ListLimiter, MetadataLimiter, ReadLimiter},
};
use std::collections::BTreeSet;

use crate::identifier::{parse_table_ref, validate_identifier, TableRef};

/// Builds one SQL result under the shared row-and-recursive-byte envelope.
///
/// SQLx exposes column metadata on the first decoded row, so adapters create
/// this object before backend access, register that complete metadata vector
/// once, and then charge every decoded row before it is retained. An empty
/// SQLx result has no row from which portable column metadata can be derived
/// and therefore preserves the existing `ResultSet::empty()` representation.
pub(crate) struct SqlReadEnvelope {
    limiter: ReadLimiter,
    columns: Option<Vec<ColumnMeta>>,
    rows: Vec<Vec<Value>>,
}

impl SqlReadEnvelope {
    pub(crate) fn new(budget: ReadBudget, backend: &str) -> Result<Self> {
        Ok(Self {
            limiter: ReadLimiter::new(budget, format!("{backend} SQL query result"))?,
            columns: None,
            rows: Vec::new(),
        })
    }

    pub(crate) fn probe_rows(&self) -> Result<usize> {
        self.limiter.probe_items()
    }

    pub(crate) fn observe_columns(&mut self, columns: Vec<ColumnMeta>) -> Result<()> {
        if self.columns.is_some() {
            return Err(Error::Internal(
                "SQL read envelope observed column metadata more than once".into(),
            ));
        }
        self.limiter.observe_header(&columns)?;
        self.columns = Some(columns);
        Ok(())
    }

    pub(crate) fn column_count(&self) -> usize {
        self.columns.as_ref().map_or(0, Vec::len)
    }

    pub(crate) fn observe_row(&mut self, row: Vec<Value>) -> Result<()> {
        if self.columns.is_none() {
            return Err(Error::Internal(
                "SQL read envelope observed a row before column metadata".into(),
            ));
        }
        self.limiter.retain_item(row, &mut self.rows)
    }

    pub(crate) fn observed_rows(&self) -> usize {
        self.limiter.observed_items()
    }

    pub(crate) fn finish(self) -> Result<ResultSet> {
        let Self {
            limiter,
            columns,
            rows,
        } = self;
        let columns = columns.unwrap_or_default();
        limiter.finish_with(rows, move |rows, truncated| ResultSet {
            columns,
            rows,
            truncated,
        })
    }
}

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

/// Convert the remaining complete-metadata N+1 probe to a SQL LIMIT value.
///
/// Unlike [`bounded_catalog_limit`], the limiter is shared across columns,
/// index identities, and index-column memberships. Each metadata phase calls
/// this immediately before backend access so earlier phases reduce the next
/// query's protocol-side limit.
pub(crate) fn bounded_metadata_limit(limiter: &MetadataLimiter, backend: &str) -> Result<i64> {
    let probe_items = limiter.probe_items()?;
    i64::try_from(probe_items).map_err(|_| {
        Error::Config(format!(
            "{backend} metadata budget is too large for a SQL LIMIT parameter"
        ))
    })
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
    use dbtool_core::model::{MetadataBudget, DEFAULT_METADATA_BYTES};

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

    #[test]
    fn metadata_limit_tracks_the_remaining_cross_phase_budget() {
        let budget = MetadataBudget::new(3, DEFAULT_METADATA_BYTES).unwrap();
        let mut limiter = MetadataLimiter::new(budget, "test schema").unwrap();

        assert_eq!(bounded_metadata_limit(&limiter, "test").unwrap(), 4);
        limiter.observe("column").unwrap();
        assert_eq!(bounded_metadata_limit(&limiter, "test").unwrap(), 3);
        limiter.observe("index").unwrap();
        limiter.observe("index-column").unwrap();
        assert_eq!(bounded_metadata_limit(&limiter, "test").unwrap(), 1);
    }

    #[test]
    fn metadata_limit_rejects_sql_parameter_overflow() {
        if usize::MAX > i64::MAX as usize {
            let budget = MetadataBudget::new(i64::MAX as usize, 1).unwrap();
            let limiter = MetadataLimiter::new(budget, "test schema").unwrap();
            assert!(matches!(
                bounded_metadata_limit(&limiter, "test"),
                Err(Error::Config(_))
            ));
        }
    }
}

#[cfg(test)]
mod read_envelope_tests {
    use super::*;
    use dbtool_core::model::ReadBudget;

    fn test_columns() -> Vec<ColumnMeta> {
        vec![ColumnMeta {
            name: "payload".into(),
            type_name: "JSON".into(),
            nullable: true,
            primary_key: false,
            default_value: None,
        }]
    }

    fn envelope(max_items: usize, max_bytes: usize) -> SqlReadEnvelope {
        let mut envelope =
            SqlReadEnvelope::new(ReadBudget::new(max_items, max_bytes).unwrap(), "test").unwrap();
        envelope.observe_columns(test_columns()).unwrap();
        envelope
    }

    #[test]
    fn sql_read_envelope_distinguishes_n_from_n_plus_one() {
        let mut exact = envelope(2, 4096);
        exact.observe_row(vec![Value::Int(1)]).unwrap();
        exact.observe_row(vec![Value::Int(2)]).unwrap();
        let exact = exact.finish().unwrap();
        assert_eq!(exact.rows.len(), 2);
        assert!(!exact.truncated);

        let mut probed = envelope(2, 4096);
        probed.observe_row(vec![Value::Int(1)]).unwrap();
        probed.observe_row(vec![Value::Int(2)]).unwrap();
        probed.observe_row(vec![Value::Int(3)]).unwrap();
        let probed = probed.finish().unwrap();
        assert_eq!(probed.rows, [vec![Value::Int(1)], vec![Value::Int(2)]]);
        assert!(probed.truncated);
    }

    #[test]
    fn sql_read_envelope_accepts_exact_resultset_bytes_and_rejects_n_minus_one() {
        let row = vec![Value::Json(serde_json::json!({
            "nested": [{"value": "recursive"}]
        }))];
        let expected = ResultSet {
            columns: test_columns(),
            rows: vec![row.clone()],
            truncated: false,
        };
        let required = serde_json::to_vec(&expected).unwrap().len();

        let mut exact = envelope(1, required);
        exact.observe_row(row.clone()).unwrap();
        assert_eq!(
            serde_json::to_vec(&exact.finish().unwrap()).unwrap().len(),
            required
        );

        let mut short = envelope(1, required - 1);
        let error = match short.observe_row(row) {
            Ok(()) => short.finish().unwrap_err(),
            Err(error) => error,
        };
        assert!(matches!(error, Error::ReadBudgetExceeded { .. }));
    }

    #[test]
    fn sql_read_envelope_fails_closed_for_large_recursive_value_variants() {
        for value in [
            Value::Text("x".repeat(4096)),
            Value::Bytes(vec![0x5a; 4096]),
            Value::Json(serde_json::json!({
                "outer": [{"inner": "x".repeat(4096)}]
            })),
        ] {
            let mut envelope = envelope(1, 512);
            assert!(matches!(
                envelope.observe_row(vec![value]),
                Err(Error::ReadBudgetExceeded { .. })
            ));
        }
    }
}
