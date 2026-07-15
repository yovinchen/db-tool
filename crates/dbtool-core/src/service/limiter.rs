use crate::{
    model::{BoundedList, ResultSet},
    Error, Result,
};

fn checked_probe_limit(limit: usize, subject: &str) -> Result<usize> {
    if limit == 0 {
        return Err(Error::Config(format!(
            "{subject} limit must be greater than zero"
        )));
    }

    limit.checked_add(1).ok_or_else(|| {
        Error::Config(format!(
            "{subject} limit is too large to reserve a truncation probe item"
        ))
    })
}

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
        checked_probe_limit(self.max_rows, "result row")
    }

    pub fn apply(&self, mut rs: ResultSet) -> ResultSet {
        if rs.rows.len() > self.max_rows {
            rs.rows.truncate(self.max_rows);
            rs.truncated = true;
        }
        rs
    }
}

/// Validates and finalizes an N+1 catalog read without hiding completeness.
///
/// Adapters must call [`Self::probe_items`] before backend access and pass that
/// exact value as the remote query/page limit. Once the backend returns, pass
/// the observed items to [`Self::finish`] to retain at most the caller's
/// budget and set `truncated` only when the probe item exists.
pub struct ListLimiter {
    pub max_items: usize,
}

impl ListLimiter {
    pub fn new(max_items: usize) -> Self {
        Self { max_items }
    }

    /// Return the remote limit needed to distinguish N items from N+1.
    ///
    /// Zero and `usize::MAX` are rejected before callers contact a backend.
    pub fn probe_items(&self) -> Result<usize> {
        checked_probe_limit(self.max_items, "catalog item")
    }

    /// Finalize items read with the N+1 probe limit.
    pub fn finish<T>(&self, mut items: Vec<T>) -> BoundedList<T> {
        let truncated = items.len() > self.max_items;
        if truncated {
            items.truncate(self.max_items);
        }
        BoundedList { items, truncated }
    }
}

impl Default for ListLimiter {
    fn default() -> Self {
        Self::new(100)
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

    #[test]
    fn list_limiter_distinguishes_n_from_n_plus_one() {
        let limiter = ListLimiter::new(2);
        assert_eq!(limiter.probe_items().unwrap(), 3);

        let exact = limiter.finish(vec!["one", "two"]);
        assert_eq!(exact.items, vec!["one", "two"]);
        assert!(!exact.truncated);

        let probed = limiter.finish(vec!["one", "two", "probe"]);
        assert_eq!(probed.items, vec!["one", "two"]);
        assert!(probed.truncated);
    }

    #[test]
    fn list_probe_rejects_zero_and_overflow_before_backend_use() {
        assert!(matches!(
            ListLimiter::new(0).probe_items(),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            ListLimiter::new(usize::MAX).probe_items(),
            Err(Error::Config(_))
        ));
        assert_eq!(
            ListLimiter::new(usize::MAX - 1).probe_items().unwrap(),
            usize::MAX
        );
    }
}
