use crate::{
    model::{BoundedList, MetadataBudget, ReadBudget, ResultSet},
    Error, Result,
};
use serde::Serialize;
use std::io::Write;

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

/// Enforces one reusable item-and-byte envelope while a read is retained.
///
/// Headers contribute their compact JSON byte length without consuming an
/// item. Each item is serialized into the same kind of counting writer before
/// it can be retained, so nested values are visited without allocating a
/// second encoded copy. The first `max_items` values are retained; one
/// additional value is charged and observed only to establish
/// `truncated=true`. Callers must stop after that N+1 probe.
pub struct ReadLimiter {
    budget: ReadBudget,
    subject: String,
    observed_items: usize,
    observed_bytes: usize,
    probe_bytes: usize,
    truncated: bool,
}

impl ReadLimiter {
    pub fn new(budget: ReadBudget, subject: impl Into<String>) -> Result<Self> {
        Ok(Self {
            budget: budget.validate()?,
            subject: subject.into(),
            observed_items: 0,
            observed_bytes: 0,
            probe_bytes: 0,
            truncated: false,
        })
    }

    /// Return the exact N+1 item count needed to prove truncation.
    pub fn probe_items(&self) -> Result<usize> {
        checked_probe_limit(self.budget.max_items, "read item")
    }

    /// Charge a complete serialized header without consuming an item slot.
    ///
    /// SQL/CQL adapters use this for the complete column metadata vector.
    pub fn observe_header<T: Serialize + ?Sized>(&mut self, header: &T) -> Result<()> {
        self.charge_serialized(header).map(|_| ())
    }

    /// Charge one complete item before deciding whether it may be retained.
    ///
    /// Returns `true` for the first N items and `false` for the N+1 truncation
    /// probe. Calling this method again after the probe is a caller bug.
    pub fn observe_item<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<bool> {
        if self.truncated {
            return Err(Error::Internal(format!(
                "{} read limiter observed more than its N+1 probe",
                self.subject
            )));
        }

        let charged = self.charge_serialized(item)?;
        let retain = self.observed_items < self.budget.max_items;
        self.observed_items += 1;
        self.truncated = !retain;
        if !retain {
            self.probe_bytes = charged;
        }
        Ok(retain)
    }

    /// Charge `item` and append it only when it is inside the retention limit.
    pub fn retain_item<T: Serialize>(&mut self, item: T, retained: &mut Vec<T>) -> Result<()> {
        if self.observe_item(&item)? {
            retained.push(item);
        }
        Ok(())
    }

    /// Finalize a caller-visible collection after the N or N+1 read.
    pub fn finish<T: Serialize>(self, items: Vec<T>) -> Result<BoundedList<T>> {
        self.finish_with(items, |items, truncated| BoundedList { items, truncated })
    }

    /// Finalize a caller-visible response and charge its complete serialized
    /// envelope plus the N+1 probe item, when present.
    ///
    /// Incremental header/item charging prevents an oversized unit from being
    /// retained. This final pass additionally includes object field names,
    /// delimiters, and completeness markers that only the concrete response
    /// type can define. The counting writer does not allocate another encoded
    /// copy of the response.
    pub fn finish_with<T, O, F>(self, items: Vec<T>, build: F) -> Result<O>
    where
        O: Serialize,
        F: FnOnce(Vec<T>, bool) -> O,
    {
        let expected = self.observed_items.min(self.budget.max_items);
        if items.len() != expected {
            return Err(Error::Internal(format!(
                "{} read limiter retained {} items after observing {}; expected {expected}",
                self.subject,
                items.len(),
                self.observed_items
            )));
        }
        let response = build(items, self.truncated);
        let visible_limit = self
            .budget
            .max_bytes
            .checked_sub(self.probe_bytes)
            .ok_or_else(|| self.byte_budget_error())?;
        Self::measure_serialized(&response, visible_limit)
            .map_err(|error| self.map_counting_error(error))?;
        Ok(response)
    }

    pub fn observed_items(&self) -> usize {
        self.observed_items
    }

    pub fn observed_bytes(&self) -> usize {
        self.observed_bytes
    }

    pub fn is_truncated(&self) -> bool {
        self.truncated
    }

    fn charge_serialized<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<usize> {
        let remaining = self
            .budget
            .max_bytes
            .checked_sub(self.observed_bytes)
            .expect("observed read bytes never exceed the validated budget");
        let written = Self::measure_serialized(value, remaining)
            .map_err(|error| self.map_counting_error(error))?;
        self.observed_bytes += written;
        Ok(written)
    }

    fn measure_serialized<T: Serialize + ?Sized>(
        value: &T,
        limit: usize,
    ) -> std::result::Result<usize, CountingError> {
        let mut writer = SerializedByteCounter::new(limit);
        match serde_json::to_writer(&mut writer, value) {
            Ok(()) => Ok(writer.written),
            Err(_) if writer.exceeded => Err(CountingError::BudgetExceeded),
            Err(error) => Err(CountingError::Serialization(error)),
        }
    }

    fn map_counting_error(&self, error: CountingError) -> Error {
        match error {
            CountingError::BudgetExceeded => self.byte_budget_error(),
            CountingError::Serialization(error) => Error::Serialization(error.to_string()),
        }
    }

    fn byte_budget_error(&self) -> Error {
        Error::ReadBudgetExceeded {
            subject: self.subject.clone(),
            unit: "bytes",
            limit: self.budget.max_bytes,
        }
    }
}

enum CountingError {
    BudgetExceeded,
    Serialization(serde_json::Error),
}

struct SerializedByteCounter {
    limit: usize,
    written: usize,
    exceeded: bool,
}

impl SerializedByteCounter {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            written: 0,
            exceeded: false,
        }
    }
}

impl Write for SerializedByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(next) = self.written.checked_add(bytes.len()) else {
            self.exceeded = true;
            return Err(std::io::Error::other("serialized read size overflow"));
        };
        if next > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other("read byte budget exceeded"));
        }
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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
    use std::collections::BTreeMap;

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

    #[test]
    fn read_limiter_distinguishes_n_from_n_plus_one_after_accounting() {
        let budget = ReadBudget::new(2, 1024).unwrap();
        let mut exact = ReadLimiter::new(budget, "query result").unwrap();
        exact.observe_header(&["id"]).unwrap();
        let mut exact_rows = Vec::new();
        exact
            .retain_item(vec![Value::Int(1)], &mut exact_rows)
            .unwrap();
        exact
            .retain_item(vec![Value::Int(2)], &mut exact_rows)
            .unwrap();
        let exact = exact.finish(exact_rows).unwrap();
        assert_eq!(exact.items.len(), 2);
        assert!(!exact.truncated);

        let mut probed = ReadLimiter::new(budget, "query result").unwrap();
        assert_eq!(probed.probe_items().unwrap(), 3);
        probed.observe_header(&["id"]).unwrap();
        let mut rows = Vec::new();
        for value in 1..=3 {
            probed
                .retain_item(vec![Value::Int(value)], &mut rows)
                .unwrap();
        }
        assert_eq!(probed.observed_items(), 3);
        assert!(probed.is_truncated());
        let probed = probed.finish(rows).unwrap();
        assert_eq!(probed.items.len(), 2);
        assert!(probed.truncated);
    }

    #[test]
    fn read_limiter_accounts_recursive_values_before_retention_and_fails_closed() {
        let header = ["payload"];
        let row = vec![Value::Map(BTreeMap::from([(
            "nested".to_owned(),
            Value::Array(vec![
                Value::Text("large value".repeat(4)),
                Value::Json(serde_json::json!({"deep": [1, 2, 3]})),
            ]),
        )]))];
        let required =
            serde_json::to_vec(&header).unwrap().len() + serde_json::to_vec(&row).unwrap().len();

        let mut exact =
            ReadLimiter::new(ReadBudget::new(1, required).unwrap(), "query result").unwrap();
        exact.observe_header(&header).unwrap();
        let mut retained = Vec::new();
        exact.retain_item(row.clone(), &mut retained).unwrap();
        assert_eq!(exact.observed_bytes(), required);
        assert_eq!(retained.as_slice(), std::slice::from_ref(&row));

        let mut too_small =
            ReadLimiter::new(ReadBudget::new(1, required - 1).unwrap(), "query result").unwrap();
        too_small.observe_header(&header).unwrap();
        let mut retained = Vec::new();
        let error = too_small.retain_item(row, &mut retained).unwrap_err();
        assert!(matches!(
            error,
            Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == required - 1
        ));
        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
        assert!(retained.is_empty());
        assert_eq!(too_small.observed_items(), 0);
    }

    #[test]
    fn read_limiter_rejects_an_oversized_complete_header() {
        let mut limiter =
            ReadLimiter::new(ReadBudget::new(1, 16).unwrap(), "query result").unwrap();
        let error = limiter
            .observe_header(&["column-name".repeat(32)])
            .unwrap_err();

        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
        assert_eq!(limiter.observed_bytes(), 0);
        assert_eq!(limiter.observed_items(), 0);
    }

    #[test]
    fn read_limiter_charges_the_complete_visible_envelope_and_probe() {
        let columns = vec![crate::model::ColumnMeta {
            name: "id".to_owned(),
            type_name: "integer".to_owned(),
            nullable: false,
            primary_key: true,
            default_value: None,
        }];
        let retained_row = vec![Value::Int(1)];
        let probe_row = vec![Value::Int(2)];
        let expected = ResultSet {
            columns: columns.clone(),
            rows: vec![retained_row.clone()],
            truncated: true,
        };
        let required = serde_json::to_vec(&expected).unwrap().len()
            + serde_json::to_vec(&probe_row).unwrap().len();

        let mut exact =
            ReadLimiter::new(ReadBudget::new(1, required).unwrap(), "query result").unwrap();
        exact.observe_header(&columns).unwrap();
        let mut retained = Vec::new();
        exact
            .retain_item(retained_row.clone(), &mut retained)
            .unwrap();
        exact.retain_item(probe_row.clone(), &mut retained).unwrap();
        let result = exact
            .finish_with(retained, |rows, truncated| ResultSet {
                columns: columns.clone(),
                rows,
                truncated,
            })
            .unwrap();
        assert_eq!(result.columns.len(), expected.columns.len());
        assert_eq!(result.columns[0].name, expected.columns[0].name);
        assert_eq!(result.rows, expected.rows);
        assert_eq!(result.truncated, expected.truncated);

        let mut short =
            ReadLimiter::new(ReadBudget::new(1, required - 1).unwrap(), "query result").unwrap();
        short.observe_header(&columns).unwrap();
        let mut retained = Vec::new();
        short.retain_item(retained_row, &mut retained).unwrap();
        short.retain_item(probe_row, &mut retained).unwrap();
        assert!(matches!(
            short.finish_with(retained, |rows, truncated| ResultSet {
                columns,
                rows,
                truncated,
            }),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == required - 1
        ));
    }
}
