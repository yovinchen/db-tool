use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    /// True when this column is part of the primary key.
    #[serde(default)]
    pub primary_key: bool,
    /// Column default expression, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: Option<String>,
    pub name: String,
    pub kind: TableKind,
}

impl TableInfo {
    /// Return a dotted schema-qualified representation of this table.
    ///
    /// SQL adapters accept this value in `describe_table` when both components
    /// use the portable unquoted-identifier grammar.
    pub fn qualified_name(&self) -> String {
        match self.schema.as_deref() {
            Some(schema) => format!("{schema}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableKind {
    Table,
    View,
    #[serde(rename = "materialized-view")]
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
    /// True when this is the primary key index.
    #[serde(default)]
    pub primary: bool,
}

// ── DB2-specific metadata types ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceInfo {
    pub schema: String,
    pub name: String,
    pub data_type: String,
    pub start: String,
    pub increment: String,
    pub min_value: String,
    pub max_value: String,
    pub cycle: bool,
    pub cache: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RoutineKind {
    Procedure,
    Function,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineInfo {
    pub schema: String,
    pub name: String,
    pub kind: RoutineKind,
    pub language: String,
    pub parms: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablespaceInfo {
    pub name: String,
    pub kind: String,
    pub page_size: i64,
    pub extent_size: i64,
    pub prefetch_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub constraint_name: String,
    pub columns: Vec<String>,
    pub ref_schema: String,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub update_rule: String,
    pub delete_rule: String,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_info_reconstructs_the_describe_identifier() {
        let qualified = TableInfo {
            schema: Some("analytics".to_owned()),
            name: "daily_totals".to_owned(),
            kind: TableKind::MaterializedView,
        };
        assert_eq!(qualified.qualified_name(), "analytics.daily_totals");
        assert_eq!(
            serde_json::to_value(qualified.kind).unwrap(),
            "materialized-view"
        );

        let unqualified = TableInfo {
            schema: None,
            name: "users".to_owned(),
            kind: TableKind::Table,
        };
        assert_eq!(unqualified.qualified_name(), "users");
    }
}
