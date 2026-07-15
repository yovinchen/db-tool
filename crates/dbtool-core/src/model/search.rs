use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

/// One document returned by the Search get-by-id API.
///
/// Backend-specific fields are flattened into the serialized result so a
/// compatible backend can add metadata without dbtool silently discarding it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchDocument {
    #[serde(alias = "_index")]
    pub index: String,
    #[serde(alias = "_id")]
    pub id: String,
    #[serde(default = "default_true")]
    pub found: bool,
    #[serde(default, alias = "_version", skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(default, alias = "_seq_no", skip_serializing_if = "Option::is_none")]
    pub seq_no: Option<u64>,
    #[serde(
        default,
        alias = "_primary_term",
        skip_serializing_if = "Option::is_none"
    )]
    pub primary_term: Option<u64>,
    #[serde(default, alias = "_source", skip_serializing_if = "Option::is_none")]
    pub source: Option<JsonValue>,
    #[serde(default, flatten)]
    pub extra: Map<String, JsonValue>,
}

/// Normalized result of an index, put, update, or delete document operation.
///
/// `id`, `result`, and `version` are stable dbtool fields. Other response
/// metadata remains available through the flattened `extra` map.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchWriteOutcome {
    #[serde(alias = "_index")]
    pub index: String,
    #[serde(alias = "_id")]
    pub id: String,
    pub result: String,
    #[serde(default, alias = "_version", skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(default, alias = "_seq_no", skip_serializing_if = "Option::is_none")]
    pub seq_no: Option<u64>,
    #[serde(
        default,
        alias = "_primary_term",
        skip_serializing_if = "Option::is_none"
    )]
    pub primary_term: Option<u64>,
    #[serde(default, flatten)]
    pub extra: Map<String, JsonValue>,
}

/// Result of deleting a complete search index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchDeleteIndexOutcome {
    pub acknowledged: bool,
    #[serde(default, flatten)]
    pub extra: Map<String, JsonValue>,
}

/// Search result metadata and raw hits.
///
/// Known cross-product fields are normalized while unknown top-level and
/// `hits`-container metadata are retained instead of silently dropped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHits {
    pub total: u64,
    pub total_relation: String,
    pub hits: Vec<JsonValue>,
    pub took_ms: u64,
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub hits_metadata: Map<String, JsonValue>,
    #[serde(default, flatten)]
    pub extra: Map<String, JsonValue>,
}

fn default_true() -> bool {
    true
}
