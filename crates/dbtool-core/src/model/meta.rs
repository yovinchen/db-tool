use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: Option<String>,
    pub name: String,
    pub kind: TableKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableKind {
    Table,
    View,
    MaterializedView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnMeta>,
    pub indexes: Vec<IndexInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replicas: i16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDetail {
    pub info: TopicInfo,
    pub config: std::collections::HashMap<String, String>,
    pub watermarks: Vec<PartitionWatermark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionWatermark {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LagInfo {
    pub topic: String,
    pub partition: i32,
    pub group: String,
    pub committed: i64,
    pub latest: i64,
    pub lag: i64,
}
