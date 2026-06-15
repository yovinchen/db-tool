use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, f64>,
    /// Epoch millis.
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesSet {
    pub series: Vec<Series>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Series {
    pub name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    /// Epoch millis; None = open.
    pub start: Option<i64>,
    pub end: Option<i64>,
}

impl TimeRange {
    pub fn last_n_minutes(n: i64) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            start: Some(now - n * 60 * 1000),
            end: Some(now),
        }
    }
}
