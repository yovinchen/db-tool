use crate::{
    model::{BoundedList, MetadataBudget, ResultSet},
    Error, Result,
};
use serde::Serialize;

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

/// Enforces a budget while building one semantically complete metadata object.
///
/// Unlike [`ListLimiter`], this limiter never returns a truncated value. Every
/// nested item is observed before insertion; an N+1 probe or byte overflow
/// fails closed so callers cannot mistake a partial schema/detail for a
/// complete one.
pub struct MetadataLimiter {
    budget: MetadataBudget,
    subject: String,
    observed_items: usize,
    observed_bytes: usize,
}

impl MetadataLimiter {
    pub fn new(budget: MetadataBudget, subject: impl Into<String>) -> Result<Self> {
        Ok(Self {
            budget: budget.validate()?,
            subject: subject.into(),
            observed_items: 0,
            observed_bytes: 0,
        })
    }

    /// Return the remaining item budget plus one probe item.
    ///
    /// Adapters pass this value to their next backend query/page. Even after
    /// the item budget is exactly consumed, a one-item probe is required to
    /// prove that no additional nested metadata exists.
    pub fn probe_items(&self) -> Result<usize> {
        self.budget
            .max_items
            .saturating_sub(self.observed_items)
            .checked_add(1)
            .ok_or_else(|| {
                Error::Config(format!(
                    "{} item budget is too large to reserve a probe item",
                    self.subject
                ))
            })
    }

    /// Account for one nested item before retaining it.
    pub fn observe<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        if self.observed_items >= self.budget.max_items {
            return Err(Error::MetadataBudgetExceeded {
                subject: self.subject.clone(),
                unit: "items",
                limit: self.budget.max_items,
            });
        }
        let encoded =
            serde_json::to_vec(item).map_err(|error| Error::Serialization(error.to_string()))?;
        self.observed_bytes = self
            .observed_bytes
            .checked_add(encoded.len())
            .ok_or_else(|| Error::Query(format!("{} metadata size overflow", self.subject)))?;
        if self.observed_bytes > self.budget.max_bytes {
            return Err(Error::MetadataBudgetExceeded {
                subject: self.subject.clone(),
                unit: "bytes",
                limit: self.budget.max_bytes,
            });
        }
        self.observed_items += 1;
        Ok(())
    }

    /// Verify the complete serialized object, including container overhead.
    pub fn ensure_complete<T: Serialize + ?Sized>(&self, value: &T) -> Result<()> {
        let encoded =
            serde_json::to_vec(value).map_err(|error| Error::Serialization(error.to_string()))?;
        if encoded.len() > self.budget.max_bytes {
            return Err(Error::MetadataBudgetExceeded {
                subject: self.subject.clone(),
                unit: "bytes",
                limit: self.budget.max_bytes,
            });
        }
        Ok(())
    }

    pub fn observed_items(&self) -> usize {
        self.observed_items
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

    #[test]
    fn metadata_limiter_requires_complete_items_and_bytes() {
        let budget = MetadataBudget::new(2, 64).unwrap();
        let mut limiter = MetadataLimiter::new(budget, "table schema").unwrap();
        assert_eq!(limiter.probe_items().unwrap(), 3);

        limiter.observe("id").unwrap();
        assert_eq!(limiter.probe_items().unwrap(), 2);
        limiter.observe("name").unwrap();
        assert_eq!(limiter.probe_items().unwrap(), 1);
        assert!(matches!(
            limiter.observe("probe"),
            Err(Error::MetadataBudgetExceeded { unit: "items", .. })
        ));
        assert_eq!(limiter.observed_items(), 2);

        limiter.ensure_complete(&vec!["id", "name"]).unwrap();
    }

    #[test]
    fn metadata_limiter_fails_closed_on_byte_budget() {
        let budget = MetadataBudget::new(3, 8).unwrap();
        let mut limiter = MetadataLimiter::new(budget, "topic detail").unwrap();
        assert!(matches!(
            limiter.observe("0123456789"),
            Err(Error::MetadataBudgetExceeded { unit: "bytes", .. })
        ));
        assert_eq!(limiter.observed_items(), 0);
    }
}
