use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Absolute ceiling for a single complete metadata object.
///
/// Callers may choose a smaller byte budget, but cannot opt out of this guard.
/// It protects schema, DDL, topic-detail, and lag responses whose individual
/// fields may be much larger than their item count suggests.
pub const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;

/// Default byte ceiling used by CLI and TUI metadata reads.
pub const DEFAULT_METADATA_BYTES: usize = 8 * 1024 * 1024;

/// Caller budget for a semantically complete metadata object.
///
/// `max_items` counts nested collection entries (for example columns plus
/// index-column memberships, or topic config entries plus partition
/// watermarks). `max_bytes` bounds the complete serialized object. Exceeding
/// either budget is an error: complete metadata is never silently truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataBudget {
    pub max_items: usize,
    pub max_bytes: usize,
}

impl MetadataBudget {
    pub fn new(max_items: usize, max_bytes: usize) -> Result<Self> {
        let budget = Self {
            max_items,
            max_bytes,
        };
        budget.validate()?;
        Ok(budget)
    }

    pub fn with_default_bytes(max_items: usize) -> Result<Self> {
        Self::new(max_items, DEFAULT_METADATA_BYTES)
    }

    /// Validate deserialized or directly constructed budgets at the boundary.
    pub fn validate(self) -> Result<Self> {
        if self.max_items == 0 {
            return Err(Error::Config(
                "metadata item budget must be greater than zero".to_owned(),
            ));
        }
        if self.max_items.checked_add(1).is_none() {
            return Err(Error::Config(
                "metadata item budget is too large to reserve a probe item".to_owned(),
            ));
        }
        if self.max_bytes == 0 {
            return Err(Error::Config(
                "metadata byte budget must be greater than zero".to_owned(),
            ));
        }
        if self.max_bytes > MAX_METADATA_BYTES {
            return Err(Error::Config(format!(
                "metadata byte budget exceeds the hard {MAX_METADATA_BYTES}-byte ceiling"
            )));
        }
        Ok(self)
    }
}

/// A caller-budgeted list whose completeness is reported explicitly.
///
/// `truncated=true` means the backend observed at least one additional item
/// beyond the returned `items`. Adapters must determine that fact while
/// reading from the backend; constructing this value after loading an
/// unbounded catalog does not satisfy the bounded-list contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundedList<T> {
    pub items: Vec<T>,
    pub truncated: bool,
}

impl<T> BoundedList<T> {
    pub fn complete(items: Vec<T>) -> Self {
        Self {
            items,
            truncated: false,
        }
    }
}

impl<T> Default for BoundedList<T> {
    fn default() -> Self {
        Self::complete(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_list_round_trips_completeness() {
        let list = BoundedList {
            items: vec!["one".to_owned(), "two".to_owned()],
            truncated: true,
        };

        let encoded = serde_json::to_value(&list).unwrap();
        assert_eq!(encoded["items"], serde_json::json!(["one", "two"]));
        assert_eq!(encoded["truncated"], true);
        assert_eq!(
            serde_json::from_value::<BoundedList<String>>(encoded).unwrap(),
            list
        );
    }

    #[test]
    fn metadata_budget_rejects_unbounded_or_invalid_inputs() {
        assert!(matches!(
            MetadataBudget::new(0, DEFAULT_METADATA_BYTES),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            MetadataBudget::new(usize::MAX, DEFAULT_METADATA_BYTES),
            Err(Error::Config(_))
        ));
        assert!(matches!(MetadataBudget::new(1, 0), Err(Error::Config(_))));
        assert!(matches!(
            MetadataBudget::new(1, MAX_METADATA_BYTES + 1),
            Err(Error::Config(_))
        ));

        let budget = MetadataBudget::with_default_bytes(7).unwrap();
        assert_eq!(budget.max_items, 7);
        assert_eq!(budget.max_bytes, DEFAULT_METADATA_BYTES);
    }
}
