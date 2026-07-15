use super::{ColumnMeta, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSet {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<Value>>,
    /// True when one or more rows were omitted because of a caller row budget.
    pub truncated: bool,
}

impl ResultSet {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            truncated: false,
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecOutcome {
    pub rows_affected: u64,
    pub last_insert_id: Option<u64>,
}
