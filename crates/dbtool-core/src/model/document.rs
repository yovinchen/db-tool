use super::Value;
use serde::{Deserialize, Serialize};

pub type Document = std::collections::BTreeMap<String, Value>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FindOptions {
    pub limit: Option<usize>,
    pub skip: Option<usize>,
    pub sort: Option<Value>,
    pub projection: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertOutcome {
    pub inserted: u64,
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateOutcome {
    pub matched: u64,
    pub modified: u64,
}
