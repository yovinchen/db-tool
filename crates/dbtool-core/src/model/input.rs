use serde::{Deserialize, Serialize};

use crate::{model::Value, Error, Result};

/// Absolute number of items accepted by one portable mutation request.
pub const MAX_INPUT_ITEMS: usize = 100_000;

/// Absolute byte ceiling for one item or one complete mutation request.
pub const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;

/// Finite default item count for portable mutation requests.
pub const DEFAULT_INPUT_ITEMS: usize = 1_000;

/// Finite default byte ceiling for one complete mutation item.
pub const DEFAULT_INPUT_ITEM_BYTES: usize = 8 * 1024 * 1024;

/// Finite default byte ceiling for one complete mutation request or batch.
pub const DEFAULT_INPUT_BATCH_BYTES: usize = 8 * 1024 * 1024;

/// Canonical portable input envelope for one SQL execute request.
///
/// Every first-party SQL adapter and caller serializes this same structure
/// before connection or dispatch. Keeping the field names and shape in core
/// prevents byte-boundary drift between SQLx, SQL Server, Db2, Cassandra's SQL
/// compatibility surface, CLI, TUI, and embedded integrations.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SqlExecuteInput<'a> {
    pub sql: &'a str,
    pub params: &'a [Value],
}

/// Caller-owned item-and-byte envelope for a portable mutation input.
///
/// `max_items` limits the logical values in a batch. `max_item_bytes` charges
/// every complete item independently, while `max_batch_bytes` charges the
/// compact JSON representation of the complete request or batch, including
/// its field names and delimiters. The complete request must include every
/// caller-controlled input, including protocol resource names such as keys,
/// tables, collections, indices, and document identifiers. Fitting this
/// portable envelope does not replace adapter-specific syntax or fixed-limit
/// validation for those targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputBudget {
    pub max_items: usize,
    pub max_item_bytes: usize,
    pub max_batch_bytes: usize,
}

impl InputBudget {
    pub fn new(max_items: usize, max_item_bytes: usize, max_batch_bytes: usize) -> Result<Self> {
        Self {
            max_items,
            max_item_bytes,
            max_batch_bytes,
        }
        .validate()
    }

    /// Revalidate a deserialized or directly constructed budget at a trust
    /// boundary. No field may disable its process-level hard ceiling.
    pub fn validate(self) -> Result<Self> {
        if self.max_items == 0 {
            return Err(Error::Config(
                "input item budget must be greater than zero".to_owned(),
            ));
        }
        if self.max_items > MAX_INPUT_ITEMS {
            return Err(Error::Config(format!(
                "input item budget exceeds the hard {MAX_INPUT_ITEMS}-item ceiling"
            )));
        }
        validate_input_byte_budget("per-item", self.max_item_bytes)?;
        validate_input_byte_budget("batch", self.max_batch_bytes)?;
        Ok(self)
    }
}

impl Default for InputBudget {
    fn default() -> Self {
        Self {
            max_items: DEFAULT_INPUT_ITEMS,
            max_item_bytes: DEFAULT_INPUT_ITEM_BYTES,
            max_batch_bytes: DEFAULT_INPUT_BATCH_BYTES,
        }
    }
}

fn validate_input_byte_budget(label: &str, value: usize) -> Result<()> {
    if value == 0 {
        return Err(Error::Config(format!(
            "input {label} byte budget must be greater than zero"
        )));
    }
    if value > MAX_INPUT_BYTES {
        return Err(Error::Config(format!(
            "input {label} byte budget exceeds the hard {MAX_INPUT_BYTES}-byte ceiling"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_finite_and_inside_portable_hard_ceilings() {
        let budget = InputBudget::default().validate().unwrap();
        assert!(budget.max_items <= MAX_INPUT_ITEMS);
        assert!(budget.max_item_bytes <= MAX_INPUT_BYTES);
        assert!(budget.max_batch_bytes <= MAX_INPUT_BYTES);
    }

    #[test]
    fn sql_execute_envelope_has_one_stable_wire_shape() {
        let params = [Value::Int(1), Value::Text("note".to_owned())];
        let encoded = serde_json::to_string(&SqlExecuteInput {
            sql: "select ?, ?",
            params: &params,
        })
        .unwrap();

        assert_eq!(encoded, r#"{"sql":"select ?, ?","params":[1,"note"]}"#);
    }

    #[test]
    fn rejects_zero_and_above_ceiling_dimensions() {
        for budget in [
            InputBudget {
                max_items: 0,
                ..InputBudget::default()
            },
            InputBudget {
                max_items: MAX_INPUT_ITEMS + 1,
                ..InputBudget::default()
            },
            InputBudget {
                max_item_bytes: 0,
                ..InputBudget::default()
            },
            InputBudget {
                max_item_bytes: MAX_INPUT_BYTES + 1,
                ..InputBudget::default()
            },
            InputBudget {
                max_batch_bytes: 0,
                ..InputBudget::default()
            },
            InputBudget {
                max_batch_bytes: MAX_INPUT_BYTES + 1,
                ..InputBudget::default()
            },
        ] {
            assert!(matches!(budget.validate(), Err(Error::Config(_))));
        }

        assert_eq!(
            InputBudget::new(MAX_INPUT_ITEMS, MAX_INPUT_BYTES, MAX_INPUT_BYTES).unwrap(),
            InputBudget {
                max_items: MAX_INPUT_ITEMS,
                max_item_bytes: MAX_INPUT_BYTES,
                max_batch_bytes: MAX_INPUT_BYTES,
            }
        );
    }
}
