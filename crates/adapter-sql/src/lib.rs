mod identifier;
pub mod mysql;
pub mod postgres;
pub mod sqlite;
mod value;

pub use mysql::mysql_factory;
pub use postgres::postgres_factory;
pub use sqlite::sqlite_factory;

use dbtool_core::model::IndexInfo;

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
