use crate::{model::ResultSet, Error, Result};

/// Limits result rows to prevent flooding the caller's context.
/// Distinct from FlowControl (which limits request rate/concurrency).
pub struct ResultLimiter {
    pub max_rows: usize,
}

impl ResultLimiter {
    pub fn new(max_rows: usize) -> Self {
        Self { max_rows }
    }

    /// Return the number of rows an adapter must observe to distinguish an
    /// exact-size result from a truncated result.
    ///
    /// The extra row is deliberately calculated with `checked_add`: using a
    /// saturating addition would make `usize::MAX` indistinguishable from a
    /// valid probe and would prevent an adapter from reporting truncation
    /// accurately.
    pub fn probe_rows(&self) -> Result<usize> {
        if self.max_rows == 0 {
            return Err(Error::Config(
                "result row limit must be greater than zero".to_owned(),
            ));
        }

        self.max_rows.checked_add(1).ok_or_else(|| {
            Error::Config(
                "result row limit is too large to reserve a truncation probe row".to_owned(),
            )
        })
    }

    pub fn apply(&self, mut rs: ResultSet) -> ResultSet {
        if rs.rows.len() > self.max_rows {
            rs.rows.truncate(self.max_rows);
            rs.truncated = true;
        }
        rs
    }
}

impl Default for ResultLimiter {
    fn default() -> Self {
        Self::new(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Value;

    #[test]
    fn truncates_rows_over_limit() {
        let rs = ResultSet {
            columns: vec![],
            rows: vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
            ],
            truncated: false,
        };

        let limited = ResultLimiter::new(2).apply(rs);

        assert_eq!(limited.rows.len(), 2);
        assert!(limited.truncated);
    }

    #[test]
    fn exact_limit_is_not_truncated() {
        let rs = ResultSet {
            columns: vec![],
            rows: vec![vec![Value::Int(1)], vec![Value::Int(2)]],
            truncated: false,
        };

        let limited = ResultLimiter::new(2).apply(rs);

        assert_eq!(limited.rows.len(), 2);
        assert!(!limited.truncated);
    }

    #[test]
    fn probe_rows_rejects_zero_and_overflow() {
        assert!(matches!(
            ResultLimiter::new(0).probe_rows(),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            ResultLimiter::new(usize::MAX).probe_rows(),
            Err(Error::Config(_))
        ));
        assert_eq!(
            ResultLimiter::new(usize::MAX - 1).probe_rows().unwrap(),
            usize::MAX
        );
    }
}
