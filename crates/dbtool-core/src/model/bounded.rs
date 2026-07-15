use serde::{Deserialize, Serialize};

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
}
