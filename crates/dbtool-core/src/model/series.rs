use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::bounded::{DEFAULT_READ_BYTES, MAX_READ_BYTES};
use crate::{Error, Result};

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

/// Caller-owned structure and byte envelope for one time-series range read.
///
/// `max_series` bounds retained series headers, while `max_samples` is shared
/// across every retained series rather than being reset for each one. Both
/// dimensions reserve one additional observation to prove truncation.
/// `max_bytes` covers the complete compact JSON representation of the returned
/// [`SeriesSet`] plus the one non-retained series-header or sample probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeSeriesReadBudget {
    pub max_series: usize,
    pub max_samples: usize,
    pub max_bytes: usize,
}

impl TimeSeriesReadBudget {
    pub fn new(max_series: usize, max_samples: usize, max_bytes: usize) -> Result<Self> {
        let budget = Self {
            max_series,
            max_samples,
            max_bytes,
        };
        budget.validate()
    }

    pub fn with_default_bytes(max_series: usize, max_samples: usize) -> Result<Self> {
        Self::new(max_series, max_samples, DEFAULT_READ_BYTES)
    }

    /// Validate deserialized or directly constructed budgets before a backend
    /// connection is opened.
    pub fn validate(self) -> Result<Self> {
        if self.max_series == 0 {
            return Err(Error::Config(
                "time-series series budget must be greater than zero".to_owned(),
            ));
        }
        if self.max_series.checked_add(1).is_none() {
            return Err(Error::Config(
                "time-series series budget is too large to reserve a probe series".to_owned(),
            ));
        }
        if self.max_samples == 0 {
            return Err(Error::Config(
                "time-series sample budget must be greater than zero".to_owned(),
            ));
        }
        if self.max_samples.checked_add(1).is_none() {
            return Err(Error::Config(
                "time-series sample budget is too large to reserve a probe sample".to_owned(),
            ));
        }
        if self.max_bytes == 0 {
            return Err(Error::Config(
                "time-series read byte budget must be greater than zero".to_owned(),
            ));
        }
        if self.max_bytes > MAX_READ_BYTES {
            return Err(Error::Config(format!(
                "time-series read byte budget exceeds the hard {MAX_READ_BYTES}-byte ceiling"
            )));
        }
        Ok(self)
    }
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
    use super::{TimeRange, TimeSeriesReadBudget};
    use crate::{model::bounded::MAX_READ_BYTES, Error};

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

    #[test]
    fn time_series_read_budget_rejects_invalid_dimensions_and_bytes() {
        for result in [
            TimeSeriesReadBudget::new(0, 1, 1024),
            TimeSeriesReadBudget::new(1, 0, 1024),
            TimeSeriesReadBudget::new(usize::MAX, 1, 1024),
            TimeSeriesReadBudget::new(1, usize::MAX, 1024),
            TimeSeriesReadBudget::new(1, 1, 0),
            TimeSeriesReadBudget::new(1, 1, MAX_READ_BYTES + 1),
        ] {
            assert!(matches!(result, Err(Error::Config(_))));
        }
    }

    #[test]
    fn time_series_read_budget_uses_shared_default_and_round_trips() {
        let budget = TimeSeriesReadBudget::with_default_bytes(7, 11).unwrap();
        assert_eq!(budget.max_series, 7);
        assert_eq!(budget.max_samples, 11);
        assert_eq!(budget.max_bytes, super::DEFAULT_READ_BYTES);
        assert_eq!(
            serde_json::from_value::<TimeSeriesReadBudget>(serde_json::to_value(budget).unwrap())
                .unwrap(),
            budget
        );
    }
}
