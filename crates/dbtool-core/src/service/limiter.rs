use crate::{
    model::{
        series::Series, BoundedList, ConsumeOptions, InputBudget, Message, MetadataBudget,
        ProduceBudget, ReadBudget, ResultSet, SearchDocument, SearchHits, SeriesSet,
        TimeSeriesReadBudget,
    },
    Error, Result,
};
use serde::Serialize;
use serde_json::Value as JsonValue;
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

    /// Validate and return one optional scalar response.
    ///
    /// A present value consumes one item slot; an absent value consumes none,
    /// but its complete serialized `None` response is still byte-accounted.
    /// This is intended for endpoints such as bounded key-value GET. It must
    /// not be used to hide a collection-shaped raw response inside one scalar
    /// item; those collection members must be observed separately.
    pub fn finish_optional<T: Serialize>(mut self, value: Option<T>) -> Result<Option<T>> {
        let mut retained = Vec::with_capacity(usize::from(value.is_some()));
        if let Some(value) = value {
            self.retain_item(value, &mut retained)?;
        }
        self.finish_with(retained, |mut values, truncated| {
            debug_assert!(!truncated, "an optional scalar cannot produce a probe item");
            values.pop()
        })
    }

    /// Validate and return one semantically atomic scalar response.
    ///
    /// The value consumes one item slot and the final pass verifies its whole
    /// serialized form. Collection-shaped responses must instead account each
    /// caller-visible member before finalization.
    pub fn finish_single<T: Serialize>(mut self, value: T) -> Result<T> {
        let mut retained = Vec::with_capacity(1);
        self.retain_item(value, &mut retained)?;
        self.finish_with(retained, |mut values, truncated| {
            debug_assert!(!truncated, "a single scalar cannot produce a probe item");
            values
                .pop()
                .expect("one scalar is retained after successful accounting")
        })
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

/// Enforces a fail-closed item-and-byte envelope for search reads.
///
/// Search results have no portable truncation marker, so observing more than
/// `budget.max_items` hits is an error rather than a partial success. Each hit
/// is charged before retention. [`Self::finish`] then charges the complete
/// [`SearchHits`] value so `_source`, aggregations, hit-container metadata, and
/// backend-specific top-level fields all share the caller's byte budget.
pub struct SearchReadLimiter {
    hits: ReadLimiter,
    subject: String,
    max_items: usize,
}

impl SearchReadLimiter {
    pub fn new(budget: ReadBudget, subject: impl Into<String>) -> Result<Self> {
        let subject = subject.into();
        Ok(Self {
            hits: ReadLimiter::new(budget, subject.clone())?,
            subject,
            max_items: budget.max_items,
        })
    }

    /// Return the maximum number of hits the backend may return.
    ///
    /// Unlike collection reads with a portable `truncated` field, search reads
    /// must not request an N+1 probe because they cannot return that probe as a
    /// successful partial result.
    pub fn max_items(&self) -> usize {
        self.max_items
    }

    /// Charge and retain one complete raw hit.
    ///
    /// An extra hit fails immediately with `READ_BUDGET_EXCEEDED`; the retained
    /// prefix must never be returned as a successful search result.
    pub fn retain_hit(&mut self, hit: JsonValue, retained: &mut Vec<JsonValue>) -> Result<()> {
        if !self.hits.observe_item(&hit)? {
            return Err(self.item_budget_error());
        }
        retained.push(hit);
        Ok(())
    }

    /// Enforce the envelope on an already decoded portable search response.
    ///
    /// This convenience path is intended for adapters whose transport has
    /// already bounded the HTTP body before JSON parsing. Any retained prefix
    /// is dropped if a later hit or the final response envelope exceeds the
    /// caller's budget.
    pub fn apply(mut self, mut response: SearchHits) -> Result<SearchHits> {
        let source_hits = std::mem::take(&mut response.hits);
        let mut retained = Vec::with_capacity(source_hits.len().min(self.max_items));
        for hit in source_hits {
            self.retain_hit(hit, &mut retained)?;
        }
        response.hits = retained;
        self.finish(response)
    }

    /// Validate the complete response after all retained hits were observed.
    pub fn finish(self, mut response: SearchHits) -> Result<SearchHits> {
        if self.hits.is_truncated() {
            return Err(self.item_budget_error());
        }
        let retained = std::mem::take(&mut response.hits);
        self.hits.finish_with(retained, |hits, truncated| {
            debug_assert!(!truncated, "search reads fail instead of truncating hits");
            response.hits = hits;
            response
        })
    }

    /// Validate one optional get-by-id response as a complete scalar.
    ///
    /// A present document consumes one item. A missing document consumes zero
    /// items, while the serialized `None` envelope still consumes bytes.
    pub fn finish_optional_document(
        budget: ReadBudget,
        subject: impl Into<String>,
        document: Option<SearchDocument>,
    ) -> Result<Option<SearchDocument>> {
        ReadLimiter::new(budget, subject)?.finish_optional(document)
    }

    fn item_budget_error(&self) -> Error {
        Error::ReadBudgetExceeded {
            subject: self.subject.clone(),
            unit: "items",
            limit: self.max_items,
        }
    }
}

#[derive(Serialize)]
struct TimeSeriesHeader<'a> {
    name: &'a str,
    columns: &'a [String],
}

/// Applies independent series and cumulative-sample N+1 limits to a time-series
/// range response while sharing one serialized-byte envelope.
///
/// Adapters pass one lazily converted series at a time to
/// [`Self::retain_series`]. A retained series charges its complete name and
/// columns before any sample is retained. Samples then share one counter across
/// all series. The first extra series header or sample row is charged as the
/// sole truncation probe, after which the caller must stop. [`Self::finish`]
/// measures the complete [`SeriesSet`] and reserves the bytes consumed by that
/// non-visible probe, so a byte failure never returns a partial response.
pub struct TimeSeriesReadLimiter {
    budget: TimeSeriesReadBudget,
    subject: String,
    observed_series: usize,
    observed_samples: usize,
    observed_bytes: usize,
    probe_bytes: usize,
    truncated: bool,
}

impl TimeSeriesReadLimiter {
    pub fn new(budget: TimeSeriesReadBudget, subject: impl Into<String>) -> Result<Self> {
        Ok(Self {
            budget: budget.validate()?,
            subject: subject.into(),
            observed_series: 0,
            observed_samples: 0,
            observed_bytes: 0,
            probe_bytes: 0,
            truncated: false,
        })
    }

    /// Return the maximum number of series headers needed to distinguish an
    /// exact result from a series-truncated result.
    pub fn probe_series(&self) -> Result<usize> {
        checked_probe_limit(self.budget.max_series, "time-series series")
    }

    /// Return the maximum cumulative number of sample rows needed to
    /// distinguish an exact result from a sample-truncated result.
    pub fn probe_samples(&self) -> Result<usize> {
        checked_probe_limit(self.budget.max_samples, "time-series sample")
    }

    /// Account and retain one series without eagerly converting samples beyond
    /// the first truncation probe.
    ///
    /// The iterator is fallible so protocol adapters can convert one backend
    /// sample at a time. `Ok(true)` means the caller may provide another series;
    /// `Ok(false)` means this call observed the one permitted series or sample
    /// probe and the caller must stop.
    pub fn retain_series<I>(
        &mut self,
        name: String,
        columns: Vec<String>,
        samples: I,
        retained: &mut Vec<Series>,
    ) -> Result<bool>
    where
        I: IntoIterator<Item = Result<Vec<serde_json::Value>>>,
    {
        if self.truncated {
            return Err(Error::Internal(format!(
                "{} time-series limiter observed data after its N+1 probe",
                self.subject
            )));
        }

        let header = TimeSeriesHeader {
            name: &name,
            columns: &columns,
        };
        let header_bytes = self.charge_serialized(&header)?;
        let retain_series = self.observed_series < self.budget.max_series;
        self.observed_series += 1;
        if !retain_series {
            self.probe_bytes = header_bytes;
            self.truncated = true;
            return Ok(false);
        }

        let mut values = Vec::new();
        for sample in samples {
            let sample = sample?;
            let sample_bytes = self.charge_serialized(&sample)?;
            let retain_sample = self.observed_samples < self.budget.max_samples;
            self.observed_samples += 1;
            if retain_sample {
                values.push(sample);
            } else {
                self.probe_bytes = sample_bytes;
                self.truncated = true;
                break;
            }
        }

        retained.push(Series {
            name,
            columns,
            values,
        });
        Ok(!self.truncated)
    }

    /// Finalize the complete portable response and include the non-visible
    /// series-header or sample probe in the same byte budget.
    pub fn finish(self, series: Vec<Series>) -> Result<SeriesSet> {
        let expected_series = self.observed_series.min(self.budget.max_series);
        if series.len() != expected_series {
            return Err(Error::Internal(format!(
                "{} time-series limiter retained {} series after observing {}; expected {expected_series}",
                self.subject,
                series.len(),
                self.observed_series
            )));
        }

        let retained_samples = series.iter().try_fold(0usize, |total, item| {
            total.checked_add(item.values.len()).ok_or_else(|| {
                Error::Internal(format!(
                    "{} retained time-series sample count overflow",
                    self.subject
                ))
            })
        })?;
        let expected_samples = self.observed_samples.min(self.budget.max_samples);
        if retained_samples != expected_samples {
            return Err(Error::Internal(format!(
                "{} time-series limiter retained {retained_samples} samples after observing {}; expected {expected_samples}",
                self.subject, self.observed_samples
            )));
        }

        let response = SeriesSet {
            series,
            truncated: self.truncated,
        };
        let visible_limit = self
            .budget
            .max_bytes
            .checked_sub(self.probe_bytes)
            .ok_or_else(|| self.byte_budget_error())?;
        ReadLimiter::measure_serialized(&response, visible_limit)
            .map_err(|error| self.map_counting_error(error))?;
        Ok(response)
    }

    pub fn observed_series(&self) -> usize {
        self.observed_series
    }

    pub fn observed_samples(&self) -> usize {
        self.observed_samples
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
            .expect("observed time-series bytes never exceed the validated budget");
        let written = ReadLimiter::measure_serialized(value, remaining)
            .map_err(|error| self.map_counting_error(error))?;
        self.observed_bytes += written;
        Ok(written)
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

/// Applies the two consume byte limits before a protocol acknowledges or
/// commits any delivery.
///
/// Each message is measured independently as a complete [`Message`], then
/// charged to one cumulative batch limiter. [`Self::finish`] additionally
/// measures the complete caller-visible `Vec<Message>` envelope, including
/// delimiters that are not present in the sum of individual items.
pub struct MessageReadLimiter {
    single_budget: ReadBudget,
    batch: ReadLimiter,
    subject: String,
    max_items: usize,
}

impl MessageReadLimiter {
    pub fn new(options: &ConsumeOptions, subject: impl Into<String>) -> Result<Self> {
        let subject = subject.into();
        Ok(Self {
            single_budget: ReadBudget::new(1, options.max_message_bytes)?,
            batch: ReadLimiter::new(
                ReadBudget::new(options.max, options.max_batch_bytes)?,
                format!("{subject} batch"),
            )?,
            subject,
            max_items: options.max,
        })
    }

    /// Charge one fully converted message before any acknowledgement action.
    pub fn observe(&mut self, message: &Message) -> Result<()> {
        let mut single = ReadLimiter::new(self.single_budget, format!("{} message", self.subject))?;
        if !single.observe_item(message)? {
            return Err(Error::Internal(
                "single-message limiter unexpectedly produced a probe item".into(),
            ));
        }
        if !self.batch.observe_item(message)? {
            return Err(Error::ReadBudgetExceeded {
                subject: format!("{} batch", self.subject),
                unit: "items",
                limit: self.max_items,
            });
        }
        Ok(())
    }

    /// Verify the final visible batch envelope before returning or confirming
    /// any of its messages.
    pub fn finish(self, messages: Vec<Message>) -> Result<Vec<Message>> {
        self.batch.finish_with(messages, |messages, truncated| {
            debug_assert!(!truncated, "consume batches never retain a probe item");
            messages
        })
    }
}

/// Prevalidates a complete portable mutation input before remote side effects.
///
/// A scalar request is charged as one complete item and again as its complete
/// request envelope. A batch is rejected when empty, charges every item before
/// acceptance, and then charges the caller-supplied complete request or batch.
/// Compact JSON is streamed into a checked counting writer, so validation does
/// not allocate a second encoded copy. The complete request passed to this
/// limiter must include every caller-controlled target/resource name; adapters
/// must additionally enforce protocol syntax and fixed limits.
pub struct InputLimiter {
    budget: InputBudget,
    subject: String,
}

impl InputLimiter {
    pub fn new(budget: InputBudget, subject: impl Into<String>) -> Result<Self> {
        Ok(Self {
            budget: budget.validate()?,
            subject: subject.into(),
        })
    }

    /// Validate one complete scalar mutation request.
    ///
    /// The same serialized value is checked against both the per-item and the
    /// complete-request ceilings. This performs no remote access.
    pub fn validate_request<T: Serialize + ?Sized>(&self, request: &T) -> Result<()> {
        self.validate_item(request, 0)?;
        self.validate_complete(request, "request")
    }

    /// Validate a non-empty batch whose complete request is the batch itself.
    pub fn validate_batch<T: Serialize>(&self, items: &[T]) -> Result<()> {
        self.validate_items_with_request(items, items)
    }

    /// Validate non-empty logical items and their complete enclosing request.
    ///
    /// Adapters use this when the wire request contains context in addition to
    /// the items, for example SQL text plus bound parameters or a collection
    /// target plus document filters and an update expression. The complete
    /// request must include all caller-controlled targets; fitting the byte
    /// envelope does not replace their protocol-specific validation.
    pub fn validate_items_with_request<T, R>(&self, items: &[T], request: &R) -> Result<()>
    where
        T: Serialize,
        R: Serialize + ?Sized,
    {
        if items.is_empty() {
            return Err(Error::Config(
                "input batch must contain at least one item".to_owned(),
            ));
        }
        if items.len() > self.budget.max_items {
            return Err(self.budget_error("batch", "items", self.budget.max_items));
        }
        for (index, item) in items.iter().enumerate() {
            self.validate_item(item, index)?;
        }
        self.validate_complete(request, "batch")
    }

    fn validate_item<T: Serialize + ?Sized>(&self, item: &T, index: usize) -> Result<()> {
        ReadLimiter::measure_serialized(item, self.budget.max_item_bytes)
            .map(|_| ())
            .map_err(|error| {
                self.map_counting_error(error, &format!("item {index}"), self.budget.max_item_bytes)
            })
    }

    fn validate_complete<T: Serialize + ?Sized>(&self, value: &T, scope: &str) -> Result<()> {
        ReadLimiter::measure_serialized(value, self.budget.max_batch_bytes)
            .map(|_| ())
            .map_err(|error| self.map_counting_error(error, scope, self.budget.max_batch_bytes))
    }

    fn map_counting_error(&self, error: CountingError, scope: &str, limit: usize) -> Error {
        match error {
            CountingError::BudgetExceeded => self.budget_error(scope, "bytes", limit),
            CountingError::Serialization(error) => Error::Serialization(error.to_string()),
        }
    }

    fn budget_error(&self, scope: &str, unit: &'static str, limit: usize) -> Error {
        Error::InputBudgetExceeded {
            subject: format!("{} {scope}", self.subject),
            unit,
            limit,
        }
    }
}

/// Prevalidates one complete message write batch before remote side effects.
///
/// Every message is measured independently so an oversized unit cannot hide
/// inside an otherwise small batch. The complete `Vec<Message>` is then
/// measured again to include array delimiters and all message fields in the
/// caller's cumulative envelope. The counting writer uses checked arithmetic
/// and never allocates a second encoded copy. The target/resource name is not
/// a [`Message`] field and must be validated separately by each protocol
/// adapter before it performs resource creation or send work.
pub struct MessageWriteLimiter {
    budget: ProduceBudget,
    subject: String,
}

impl MessageWriteLimiter {
    pub fn new(budget: ProduceBudget, subject: impl Into<String>) -> Result<Self> {
        Ok(Self {
            budget: budget.validate()?,
            subject: subject.into(),
        })
    }

    /// Validate the count, each complete message, and the complete batch.
    /// This method performs no remote access and retains no partial result.
    pub fn validate(&self, messages: &[Message]) -> Result<()> {
        if messages.is_empty() {
            return Err(Error::Config(
                "message produce batch must contain at least one message".to_owned(),
            ));
        }
        if messages.len() > self.budget.max_messages {
            return Err(self.budget_error("batch", "messages", self.budget.max_messages));
        }

        for message in messages {
            ReadLimiter::measure_serialized(message, self.budget.max_message_bytes)
                .map_err(|error| self.map_counting_error(error, "message"))?;
        }

        ReadLimiter::measure_serialized(messages, self.budget.max_batch_bytes)
            .map_err(|error| self.map_counting_error(error, "batch"))?;
        Ok(())
    }

    fn map_counting_error(&self, error: CountingError, scope: &str) -> Error {
        match error {
            CountingError::BudgetExceeded => {
                let limit = if scope == "message" {
                    self.budget.max_message_bytes
                } else {
                    self.budget.max_batch_bytes
                };
                self.budget_error(scope, "bytes", limit)
            }
            CountingError::Serialization(error) => Error::Serialization(error.to_string()),
        }
    }

    fn budget_error(&self, scope: &str, unit: &'static str, limit: usize) -> Error {
        Error::InputBudgetExceeded {
            subject: format!("{} {scope}", self.subject),
            unit,
            limit,
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
    use bytes::Bytes;
    use std::collections::{BTreeMap, HashMap};

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

    #[test]
    fn read_limiter_finishes_optional_and_single_kv_values_at_exact_bytes() {
        let bytes = Bytes::from_static(&[0, 0xff, b'Z']);
        let present_bytes = serde_json::to_vec(&Some(bytes.clone())).unwrap().len();
        let present =
            ReadLimiter::new(ReadBudget::new(1, present_bytes).unwrap(), "bounded KV get")
                .unwrap()
                .finish_optional(Some(bytes.clone()))
                .unwrap();
        assert_eq!(present, Some(bytes.clone()));

        let error = ReadLimiter::new(
            ReadBudget::new(1, present_bytes - 1).unwrap(),
            "bounded KV get",
        )
        .unwrap()
        .finish_optional(Some(bytes))
        .unwrap_err();
        assert!(matches!(
            error,
            Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == present_bytes - 1
        ));

        let absent_bytes = serde_json::to_vec(&Option::<Bytes>::None).unwrap().len();
        assert_eq!(
            ReadLimiter::new(ReadBudget::new(1, absent_bytes).unwrap(), "bounded KV miss",)
                .unwrap()
                .finish_optional(Option::<Bytes>::None)
                .unwrap(),
            None
        );
        assert!(matches!(
            ReadLimiter::new(
                ReadBudget::new(1, absent_bytes - 1).unwrap(),
                "bounded KV miss",
            )
            .unwrap()
            .finish_optional(Option::<Bytes>::None),
            Err(Error::ReadBudgetExceeded { unit: "bytes", .. })
        ));

        let snapshot = crate::model::KeyValueSnapshot {
            value: Bytes::from_static(b"snapshot"),
            expiry: crate::model::KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123),
        };
        let snapshot_bytes = serde_json::to_vec(&Some(snapshot.clone())).unwrap().len();
        assert_eq!(
            ReadLimiter::new(
                ReadBudget::new(1, snapshot_bytes).unwrap(),
                "bounded KV snapshot",
            )
            .unwrap()
            .finish_optional(Some(snapshot.clone()))
            .unwrap(),
            Some(snapshot)
        );

        let raw = Value::Map(BTreeMap::from([(
            "payload".to_owned(),
            Value::Bytes(vec![0, 1, 2]),
        )]));
        let raw_bytes = serde_json::to_vec(&raw).unwrap().len();
        assert_eq!(
            ReadLimiter::new(
                ReadBudget::new(1, raw_bytes).unwrap(),
                "bounded scalar raw response",
            )
            .unwrap()
            .finish_single(raw.clone())
            .unwrap(),
            raw
        );
    }

    #[test]
    fn read_limiter_bounds_kv_scan_keys_with_one_exact_probe() {
        let retained = vec!["app:one".to_owned(), "app:two".to_owned()];
        let probe = "app:probe".to_owned();
        let expected = BoundedList {
            items: retained.clone(),
            truncated: true,
        };
        let required = serde_json::to_vec(&expected).unwrap().len()
            + serde_json::to_vec(&probe).unwrap().len();

        let mut exact =
            ReadLimiter::new(ReadBudget::new(2, required).unwrap(), "bounded KV scan").unwrap();
        let mut keys = Vec::new();
        for key in retained
            .clone()
            .into_iter()
            .chain(std::iter::once(probe.clone()))
        {
            exact.retain_item(key, &mut keys).unwrap();
        }
        assert_eq!(exact.finish(keys).unwrap(), expected);

        let mut short =
            ReadLimiter::new(ReadBudget::new(2, required - 1).unwrap(), "bounded KV scan").unwrap();
        let mut keys = Vec::new();
        for key in retained.into_iter().chain(std::iter::once(probe)) {
            short.retain_item(key, &mut keys).unwrap();
        }
        assert!(matches!(
            short.finish(keys),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == required - 1
        ));
    }

    fn complete_search_hits() -> SearchHits {
        SearchHits {
            total: 2,
            total_relation: "eq".to_owned(),
            hits: vec![
                serde_json::json!({
                    "_index": "users",
                    "_id": "user-1",
                    "_score": 1.0,
                    "_source": {"name": "Alice", "roles": ["admin", "reader"]}
                }),
                serde_json::json!({
                    "_index": "users",
                    "_id": "user-2",
                    "_score": 0.5,
                    "_source": {"name": "Bob", "roles": ["reader"]}
                }),
            ],
            took_ms: 7,
            timed_out: false,
            aggregations: Some(serde_json::json!({
                "roles": {"buckets": [{"key": "reader", "doc_count": 2}]}
            })),
            hits_metadata: serde_json::Map::from_iter([(
                "max_score".to_owned(),
                serde_json::json!(1.0),
            )]),
            extra: serde_json::Map::from_iter([(
                "_shards".to_owned(),
                serde_json::json!({"total": 1, "successful": 1, "failed": 0}),
            )]),
        }
    }

    #[test]
    fn search_limiter_fails_closed_instead_of_returning_a_hit_prefix() {
        let mut response = complete_search_hits();
        let source_hits = std::mem::take(&mut response.hits);
        let mut limiter =
            SearchReadLimiter::new(ReadBudget::new(1, 4096).unwrap(), "search response").unwrap();
        assert_eq!(limiter.max_items(), 1);

        let mut retained = Vec::new();
        limiter
            .retain_hit(source_hits[0].clone(), &mut retained)
            .unwrap();
        let error = limiter
            .retain_hit(source_hits[1].clone(), &mut retained)
            .unwrap_err();
        assert!(matches!(
            error,
            Error::ReadBudgetExceeded {
                unit: "items",
                limit: 1,
                ..
            }
        ));
        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
        assert_eq!(retained, vec![source_hits[0].clone()]);

        response.hits = retained;
        assert!(matches!(
            limiter.finish(response),
            Err(Error::ReadBudgetExceeded {
                unit: "items",
                limit: 1,
                ..
            })
        ));
        assert!(matches!(
            SearchReadLimiter::new(ReadBudget::new(1, 4096).unwrap(), "decoded search response",)
                .unwrap()
                .apply(complete_search_hits()),
            Err(Error::ReadBudgetExceeded {
                unit: "items",
                limit: 1,
                ..
            })
        ));
    }

    #[test]
    fn search_limiter_charges_the_complete_hits_envelope_at_n_and_n_minus_one_bytes() {
        let expected = complete_search_hits();
        let required = serde_json::to_vec(&expected).unwrap().len();
        let run = |max_bytes| {
            SearchReadLimiter::new(ReadBudget::new(2, max_bytes)?, "search response")?
                .apply(expected.clone())
        };

        assert_eq!(run(required).unwrap(), expected);
        let error = run(required - 1).unwrap_err();
        assert!(matches!(
            error,
            Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == required - 1
        ));
        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
    }

    #[test]
    fn search_limiter_charges_an_aggregation_only_response() {
        let response = SearchHits {
            total: 0,
            total_relation: "eq".to_owned(),
            hits: Vec::new(),
            took_ms: 3,
            timed_out: false,
            aggregations: Some(serde_json::json!({
                "status": {
                    "buckets": [
                        {"key": "active", "doc_count": 17},
                        {"key": "disabled", "doc_count": 2}
                    ]
                }
            })),
            hits_metadata: serde_json::Map::new(),
            extra: serde_json::Map::new(),
        };
        let required = serde_json::to_vec(&response).unwrap().len();

        assert_eq!(
            SearchReadLimiter::new(
                ReadBudget::new(1, required).unwrap(),
                "aggregation-only search",
            )
            .unwrap()
            .apply(response.clone())
            .unwrap(),
            response
        );
        assert!(matches!(
            SearchReadLimiter::new(
                ReadBudget::new(1, required - 1).unwrap(),
                "aggregation-only search",
            )
            .unwrap()
            .apply(response),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == required - 1
        ));
    }

    #[test]
    fn search_limiter_charges_present_and_absent_get_documents_completely() {
        let document = SearchDocument {
            index: "users".to_owned(),
            id: "user-1".to_owned(),
            found: true,
            version: Some(4),
            seq_no: Some(9),
            primary_term: Some(2),
            source: Some(serde_json::json!({
                "name": "Alice",
                "profile": {"bio": "complete source", "tags": ["one", "two"]}
            })),
            extra: serde_json::Map::from_iter([(
                "_routing".to_owned(),
                serde_json::json!("tenant-a"),
            )]),
        };
        let present_bytes = serde_json::to_vec(&Some(document.clone())).unwrap().len();

        assert_eq!(
            SearchReadLimiter::finish_optional_document(
                ReadBudget::new(1, present_bytes).unwrap(),
                "search get document",
                Some(document.clone()),
            )
            .unwrap(),
            Some(document.clone())
        );
        let error = SearchReadLimiter::finish_optional_document(
            ReadBudget::new(1, present_bytes - 1).unwrap(),
            "search get document",
            Some(document),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == present_bytes - 1
        ));
        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");

        let absent_bytes = serde_json::to_vec(&Option::<SearchDocument>::None)
            .unwrap()
            .len();
        assert_eq!(
            SearchReadLimiter::finish_optional_document(
                ReadBudget::new(1, absent_bytes).unwrap(),
                "search get miss",
                None,
            )
            .unwrap(),
            None
        );
        assert!(matches!(
            SearchReadLimiter::finish_optional_document(
                ReadBudget::new(1, absent_bytes - 1).unwrap(),
                "search get miss",
                None,
            ),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == absent_bytes - 1
        ));
    }

    fn sample(timestamp: i64, value: i64) -> Vec<serde_json::Value> {
        vec![serde_json::json!(timestamp), serde_json::json!(value)]
    }

    #[test]
    fn time_series_limiter_distinguishes_exact_series_and_cumulative_samples() {
        let budget = TimeSeriesReadBudget::new(2, 3, 4096).unwrap();
        let mut limiter = TimeSeriesReadLimiter::new(budget, "Prometheus range").unwrap();
        assert_eq!(limiter.probe_series().unwrap(), 3);
        assert_eq!(limiter.probe_samples().unwrap(), 4);

        let mut retained = Vec::new();
        assert!(limiter
            .retain_series(
                "cpu".to_owned(),
                vec!["timestamp".to_owned(), "value".to_owned()],
                vec![Ok(sample(1, 10)), Ok(sample(2, 20))],
                &mut retained,
            )
            .unwrap());
        assert!(limiter
            .retain_series(
                "memory".to_owned(),
                vec!["timestamp".to_owned(), "value".to_owned()],
                vec![Ok(sample(3, 30))],
                &mut retained,
            )
            .unwrap());
        assert_eq!(limiter.observed_series(), 2);
        assert_eq!(limiter.observed_samples(), 3);

        let result = limiter.finish(retained).unwrap();
        assert_eq!(result.series.len(), 2);
        assert_eq!(
            result
                .series
                .iter()
                .map(|series| series.values.len())
                .sum::<usize>(),
            3
        );
        assert!(!result.truncated);
    }

    #[test]
    fn time_series_limiter_stops_after_one_sample_or_series_probe() {
        let mut sample_limiter = TimeSeriesReadLimiter::new(
            TimeSeriesReadBudget::new(3, 2, 4096).unwrap(),
            "Prometheus range",
        )
        .unwrap();
        let mut retained = Vec::new();
        assert!(sample_limiter
            .retain_series(
                "cpu".to_owned(),
                vec!["timestamp".to_owned(), "value".to_owned()],
                vec![Ok(sample(1, 10))],
                &mut retained,
            )
            .unwrap());
        assert!(!sample_limiter
            .retain_series(
                "memory".to_owned(),
                vec!["timestamp".to_owned(), "value".to_owned()],
                vec![Ok(sample(2, 20)), Ok(sample(3, 30)), Ok(sample(4, 40))],
                &mut retained,
            )
            .unwrap());
        assert_eq!(sample_limiter.observed_series(), 2);
        assert_eq!(sample_limiter.observed_samples(), 3);
        assert!(sample_limiter.is_truncated());
        let sample_limited = sample_limiter.finish(retained).unwrap();
        assert_eq!(sample_limited.series.len(), 2);
        assert_eq!(sample_limited.series[1].values, vec![sample(2, 20)]);
        assert!(sample_limited.truncated);

        let mut series_limiter = TimeSeriesReadLimiter::new(
            TimeSeriesReadBudget::new(1, 10, 4096).unwrap(),
            "Prometheus range",
        )
        .unwrap();
        let mut retained = Vec::new();
        assert!(series_limiter
            .retain_series(
                "cpu".to_owned(),
                vec!["timestamp".to_owned(), "value".to_owned()],
                vec![Ok(sample(1, 10))],
                &mut retained,
            )
            .unwrap());
        assert!(!series_limiter
            .retain_series(
                "probe".to_owned(),
                vec!["timestamp".to_owned(), "value".to_owned()],
                std::iter::once_with(|| -> Result<Vec<serde_json::Value>> {
                    panic!("series probe must not convert any sample")
                }),
                &mut retained,
            )
            .unwrap());
        assert_eq!(series_limiter.observed_series(), 2);
        assert_eq!(series_limiter.observed_samples(), 1);
        let series_limited = series_limiter.finish(retained).unwrap();
        assert_eq!(series_limited.series.len(), 1);
        assert!(series_limited.truncated);
    }

    #[test]
    fn time_series_limiter_charges_complete_envelope_and_probe_bytes() {
        let columns = vec!["timestamp".to_owned(), "value".to_owned()];
        let retained_sample = sample(1, 10);
        let probe_sample = sample(2, 20);
        let expected = SeriesSet {
            series: vec![Series {
                name: "cpu".to_owned(),
                columns: columns.clone(),
                values: vec![retained_sample.clone()],
            }],
            truncated: true,
        };
        let required = serde_json::to_vec(&expected).unwrap().len()
            + serde_json::to_vec(&probe_sample).unwrap().len();

        let run = |max_bytes| {
            let budget = TimeSeriesReadBudget::new(1, 1, max_bytes).unwrap();
            let mut limiter = TimeSeriesReadLimiter::new(budget, "Prometheus range").unwrap();
            let mut retained = Vec::new();
            limiter.retain_series(
                "cpu".to_owned(),
                columns.clone(),
                vec![Ok(retained_sample.clone()), Ok(probe_sample.clone())],
                &mut retained,
            )?;
            limiter.finish(retained)
        };

        let exact = run(required).unwrap();
        assert_eq!(
            serde_json::to_value(exact).unwrap(),
            serde_json::to_value(expected).unwrap()
        );

        assert!(matches!(
            run(required - 1),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == required - 1
        ));

        let complete = SeriesSet {
            series: vec![Series {
                name: "cpu".to_owned(),
                columns: columns.clone(),
                values: vec![retained_sample.clone()],
            }],
            truncated: true,
        };
        let probe_name = "memory".to_owned();
        let probe_columns = columns.clone();
        let probe_header = TimeSeriesHeader {
            name: &probe_name,
            columns: &probe_columns,
        };
        let series_required = serde_json::to_vec(&complete).unwrap().len()
            + serde_json::to_vec(&probe_header).unwrap().len();
        let run_series_probe = |max_bytes| {
            let mut limiter = TimeSeriesReadLimiter::new(
                TimeSeriesReadBudget::new(1, 10, max_bytes).unwrap(),
                "Prometheus range",
            )
            .unwrap();
            let mut retained = Vec::new();
            limiter.retain_series(
                "cpu".to_owned(),
                columns.clone(),
                vec![Ok(retained_sample.clone())],
                &mut retained,
            )?;
            limiter.retain_series(
                probe_name.clone(),
                probe_columns.clone(),
                Vec::<Result<Vec<serde_json::Value>>>::new(),
                &mut retained,
            )?;
            limiter.finish(retained)
        };

        assert_eq!(
            serde_json::to_value(run_series_probe(series_required).unwrap()).unwrap(),
            serde_json::to_value(complete).unwrap()
        );
        assert!(matches!(
            run_series_probe(series_required - 1),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == series_required - 1
        ));
    }

    #[test]
    fn time_series_limiter_rejects_oversized_headers_before_retention() {
        let mut limiter = TimeSeriesReadLimiter::new(
            TimeSeriesReadBudget::new(1, 1, 32).unwrap(),
            "Prometheus range",
        )
        .unwrap();
        let mut retained = Vec::new();
        let error = limiter
            .retain_series(
                "oversized-series-name".repeat(8),
                vec!["timestamp".to_owned(), "value".to_owned()],
                Vec::<Result<Vec<serde_json::Value>>>::new(),
                &mut retained,
            )
            .unwrap_err();

        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
        assert!(retained.is_empty());
        assert_eq!(limiter.observed_series(), 0);
        assert_eq!(limiter.observed_samples(), 0);
        assert_eq!(limiter.observed_bytes(), 0);
    }

    #[test]
    fn message_read_limiter_charges_each_complete_message_and_the_batch_envelope() {
        let message = Message {
            key: Some(Bytes::from_static(b"key")),
            payload: Bytes::from_static(b"payload"),
            headers: HashMap::from([("trace".to_owned(), "abc".to_owned())]),
            partition: Some(1),
            offset: Some(2),
            timestamp: Some(3),
            cursor: Some(crate::model::MessageCursor::Kafka {
                topic: "events".to_owned(),
                partition: 1,
                offset: 2,
            }),
            metadata: Some(crate::model::MessageMetadata::Amqp {
                delivery_tag: 7,
                redelivered: true,
                exchange: "events".to_owned(),
                routing_key: "orders".to_owned(),
            }),
        };
        let message_bytes = serde_json::to_vec(&message).unwrap().len();
        let batch_bytes = serde_json::to_vec(&vec![message.clone(), message.clone()])
            .unwrap()
            .len();

        let exact = ConsumeOptions {
            max: 2,
            max_message_bytes: message_bytes,
            max_batch_bytes: batch_bytes,
            ..Default::default()
        };
        let mut limiter = MessageReadLimiter::new(&exact, "Kafka consume").unwrap();
        limiter.observe(&message).unwrap();
        limiter.observe(&message).unwrap();
        assert_eq!(
            limiter
                .finish(vec![message.clone(), message.clone()])
                .unwrap()
                .len(),
            2
        );

        let too_small_message = ConsumeOptions {
            max_message_bytes: message_bytes - 1,
            ..exact.clone()
        };
        assert!(matches!(
            MessageReadLimiter::new(&too_small_message, "Kafka consume")
                .unwrap()
                .observe(&message),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == message_bytes - 1
        ));

        let too_small_batch = ConsumeOptions {
            max_batch_bytes: batch_bytes - 1,
            ..exact
        };
        let mut limiter = MessageReadLimiter::new(&too_small_batch, "Kafka consume").unwrap();
        limiter.observe(&message).unwrap();
        limiter.observe(&message).unwrap();
        assert!(matches!(
            limiter.finish(vec![message.clone(), message]),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == batch_bytes - 1
        ));
    }

    #[test]
    fn message_write_limiter_charges_count_each_message_and_complete_batch() {
        let message = Message {
            key: Some(Bytes::from_static(b"key")),
            payload: Bytes::from_static(b"payload"),
            headers: HashMap::from([("trace".to_owned(), "abc".to_owned())]),
            partition: Some(1),
            offset: None,
            timestamp: Some(3),
            cursor: None,
            metadata: None,
        };
        let message_bytes = serde_json::to_vec(&message).unwrap().len();
        let messages = vec![message.clone(), message.clone()];
        let batch_bytes = serde_json::to_vec(&messages).unwrap().len();
        let exact = ProduceBudget::new(2, message_bytes, batch_bytes).unwrap();
        MessageWriteLimiter::new(exact, "Kafka produce")
            .unwrap()
            .validate(&messages)
            .unwrap();

        let empty_error = MessageWriteLimiter::new(exact, "Kafka produce")
            .unwrap()
            .validate(&[])
            .unwrap_err();
        assert!(matches!(empty_error, Error::Config(message) if message.contains("at least one")));

        assert!(matches!(
            MessageWriteLimiter::new(
                ProduceBudget {
                    max_messages: 0,
                    ..ProduceBudget::default()
                },
                "Kafka produce",
            ),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));

        let count_error = MessageWriteLimiter::new(
            ProduceBudget::new(1, message_bytes, batch_bytes).unwrap(),
            "Kafka produce",
        )
        .unwrap()
        .validate(&messages)
        .unwrap_err();
        assert!(matches!(
            count_error,
            Error::InputBudgetExceeded {
                unit: "messages",
                limit: 1,
                ..
            }
        ));

        let message_error = MessageWriteLimiter::new(
            ProduceBudget::new(2, message_bytes - 1, batch_bytes).unwrap(),
            "Kafka produce",
        )
        .unwrap()
        .validate(&messages)
        .unwrap_err();
        assert!(matches!(
            message_error,
            Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == message_bytes - 1
        ));

        let batch_error = MessageWriteLimiter::new(
            ProduceBudget::new(2, message_bytes, batch_bytes - 1).unwrap(),
            "Kafka produce",
        )
        .unwrap()
        .validate(&messages)
        .unwrap_err();
        assert!(matches!(
            batch_error,
            Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == batch_bytes - 1
        ));
    }

    #[test]
    fn input_limiter_enforces_exact_n_n_plus_one_and_byte_boundaries() {
        let items = vec!["alpha".to_owned(), "bravo".to_owned()];
        let item_bytes = items
            .iter()
            .map(|item| serde_json::to_vec(item).unwrap().len())
            .max()
            .unwrap();
        let batch_bytes = serde_json::to_vec(&items).unwrap().len();
        let exact = InputBudget::new(2, item_bytes, batch_bytes).unwrap();

        InputLimiter::new(exact, "document insert")
            .unwrap()
            .validate_batch(&items)
            .unwrap();

        let too_many = vec!["alpha".to_owned(), "bravo".to_owned(), "charlie".to_owned()];
        assert!(matches!(
            InputLimiter::new(exact, "document insert")
                .unwrap()
                .validate_batch(&too_many),
            Err(Error::InputBudgetExceeded {
                unit: "items",
                limit: 2,
                ..
            })
        ));

        let item_error = InputLimiter::new(
            InputBudget::new(2, item_bytes - 1, batch_bytes).unwrap(),
            "document insert",
        )
        .unwrap()
        .validate_batch(&items)
        .unwrap_err();
        assert!(matches!(
            item_error,
            Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == item_bytes - 1
        ));

        let batch_error = InputLimiter::new(
            InputBudget::new(2, item_bytes, batch_bytes - 1).unwrap(),
            "document insert",
        )
        .unwrap()
        .validate_batch(&items)
        .unwrap_err();
        assert!(matches!(
            batch_error,
            Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            } if limit == batch_bytes - 1
        ));
    }

    #[test]
    fn input_limiter_charges_complete_requests_and_rejects_empty_batches() {
        let items = vec![serde_json::json!({ "id": 1 })];
        let request = serde_json::json!({
            "collection": "users",
            "filter": { "tenant": "one" },
            "updates": items,
        });
        let item_bytes = serde_json::to_vec(&items[0]).unwrap().len();
        let request_bytes = serde_json::to_vec(&request).unwrap().len();

        InputLimiter::new(
            InputBudget::new(1, item_bytes, request_bytes).unwrap(),
            "document update",
        )
        .unwrap()
        .validate_items_with_request(&items, &request)
        .unwrap();

        assert!(matches!(
            InputLimiter::new(
                InputBudget::new(1, item_bytes, request_bytes - 1).unwrap(),
                "document update",
            )
            .unwrap()
            .validate_items_with_request(&items, &request),
            Err(Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == request_bytes - 1
        ));

        let scalar = serde_json::json!({ "statement": "DELETE FROM t" });
        let scalar_bytes = serde_json::to_vec(&scalar).unwrap().len();
        InputLimiter::new(
            InputBudget::new(1, scalar_bytes, scalar_bytes).unwrap(),
            "SQL execute",
        )
        .unwrap()
        .validate_request(&scalar)
        .unwrap();
        assert!(matches!(
            InputLimiter::new(
                InputBudget::new(1, scalar_bytes, scalar_bytes - 1).unwrap(),
                "SQL execute",
            )
            .unwrap()
            .validate_request(&scalar),
            Err(Error::InputBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == scalar_bytes - 1
        ));

        let empty: Vec<serde_json::Value> = Vec::new();
        assert!(matches!(
            InputLimiter::new(InputBudget::default(), "empty mutation")
                .unwrap()
                .validate_batch(&empty),
            Err(Error::Config(message)) if message.contains("at least one item")
        ));
    }
}
