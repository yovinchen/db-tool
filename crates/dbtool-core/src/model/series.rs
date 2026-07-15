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
    /// Construct an explicit, closed interval. Time-series adapters must not
    /// invent a missing endpoint because repeated requests with the same value
    /// would otherwise observe different ranges.
    pub fn closed(start: i64, end: i64) -> crate::Result<Self> {
        if start > end {
            return Err(crate::Error::Config(
                "time range start must be less than or equal to end".into(),
            ));
        }
        Ok(Self {
            start: Some(start),
            end: Some(end),
        })
    }

    /// Return exact endpoints or fail before a backend request. The optional
    /// fields remain deserializable for wire compatibility, but the current
    /// range-query contract requires both endpoints.
    pub fn require_closed(&self) -> crate::Result<(i64, i64)> {
        match (self.start, self.end) {
            (Some(start), Some(end)) if start <= end => Ok((start, end)),
            (Some(_), Some(_)) => Err(crate::Error::Config(
                "time range start must be less than or equal to end".into(),
            )),
            _ => Err(crate::Error::Config(
                "time range requires both start and end epoch milliseconds".into(),
            )),
        }
    }

    pub fn last_n_minutes(n: i64) -> crate::Result<Self> {
        if n <= 0 {
            return Err(crate::Error::Config(
                "time range minutes must be greater than zero".into(),
            ));
        }
        let now = chrono::Utc::now().timestamp_millis();
        let window = n
            .checked_mul(60_000)
            .ok_or_else(|| crate::Error::Config("time range minutes overflow".into()))?;
        let start = now
            .checked_sub(window)
            .ok_or_else(|| crate::Error::Config("time range start overflow".into()))?;
        Self::closed(start, now)
    }
}

#[cfg(test)]
mod tests {
    use super::TimeRange;

    #[test]
    fn closed_ranges_require_two_ordered_endpoints() {
        assert_eq!(
            TimeRange::closed(10, 20).unwrap().require_closed().unwrap(),
            (10, 20)
        );
        assert!(TimeRange::closed(20, 10).is_err());
        assert!(TimeRange {
            start: Some(10),
            end: None,
        }
        .require_closed()
        .is_err());
    }

    #[test]
    fn relative_ranges_reject_non_positive_and_overflowing_windows() {
        assert!(TimeRange::last_n_minutes(0).is_err());
        assert!(TimeRange::last_n_minutes(-1).is_err());
        assert!(TimeRange::last_n_minutes(i64::MAX).is_err());
        let range = TimeRange::last_n_minutes(5).unwrap();
        let (start, end) = range.require_closed().unwrap();
        assert_eq!(end - start, 300_000);
    }
}
