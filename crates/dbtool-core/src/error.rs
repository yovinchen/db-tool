use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // ── Configuration / DSN ───────────────────────────────────────────────────
    #[error("config error: {0}")]
    Config(String),
    #[error("invalid DSN: {0}")]
    Dsn(String),
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),

    // ── Capability ────────────────────────────────────────────────────────────
    #[error("{kind:?} does not support capability: {needed}")]
    UnsupportedCapability { kind: String, needed: &'static str },

    // ── Connectivity ──────────────────────────────────────────────────────────
    #[error("connection error: {0}")]
    Connection(String),
    #[error("authentication failed: {0}")]
    Auth(String),

    // ── Query / execution ─────────────────────────────────────────────────────
    #[error("query error: {0}")]
    Query(String),
    /// A complete metadata object exceeded its caller item or byte budget.
    /// Callers may retry with a larger permitted budget; no partial value was
    /// returned and no remote mutation occurred.
    #[error("{subject} exceeds the metadata {unit} budget of {limit}")]
    MetadataBudgetExceeded {
        subject: String,
        unit: &'static str,
        limit: usize,
    },
    /// A caller-visible read exceeded its cumulative serialized byte envelope.
    /// No partial collection or result set is returned.
    #[error("{subject} exceeds the read {unit} budget of {limit}")]
    ReadBudgetExceeded {
        subject: String,
        unit: &'static str,
        limit: usize,
    },
    /// A complete caller-supplied write input exceeded its declared item or
    /// byte envelope. The failure occurs before any remote mutation begins.
    #[error("{subject} exceeds the input {unit} budget of {limit}")]
    InputBudgetExceeded {
        subject: String,
        unit: &'static str,
        limit: usize,
    },
    /// The client submitted an irreversible protocol operation but could not
    /// prove whether the remote system applied it. Callers must inspect remote
    /// state before retrying instead of treating this as a normal transient
    /// failure.
    #[error("remote outcome is indeterminate: {0}")]
    OutcomeIndeterminate(String),

    // ── Safety guard ──────────────────────────────────────────────────────────
    #[error("destructive operation blocked; call again with --confirm {confirm_token}")]
    ConfirmRequired {
        confirm_token: String,
        impact: serde_json::Value,
    },
    #[error("readonly connection: write operations are disabled")]
    ReadOnly,
    #[error("write operations require --allow-write flag")]
    WriteNotAllowed,

    // ── Flow control ──────────────────────────────────────────────────────────
    #[error("rate limit exceeded")]
    RateLimited,
    #[error("concurrency limit reached; try later")]
    Overloaded,
    #[error("request timed out")]
    Timeout,
    #[error("overall deadline exceeded")]
    DeadlineExceeded,

    // ── Serialization ─────────────────────────────────────────────────────────
    #[error("serialization error: {0}")]
    Serialization(String),

    // ── Internal ─────────────────────────────────────────────────────────────
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Connection(_) | Error::Timeout)
    }

    pub fn code(&self) -> &'static str {
        match self {
            Error::Config(_) => "CONFIG_ERROR",
            Error::Dsn(_) => "INVALID_DSN",
            Error::UnsupportedScheme(_) => "UNSUPPORTED_SCHEME",
            Error::UnsupportedCapability { .. } => "UNSUPPORTED_CAPABILITY",
            Error::Connection(_) => "CONNECTION_ERROR",
            Error::Auth(_) => "AUTH_ERROR",
            Error::Query(_) => "QUERY_ERROR",
            Error::MetadataBudgetExceeded { .. } => "METADATA_BUDGET_EXCEEDED",
            Error::ReadBudgetExceeded { .. } => "READ_BUDGET_EXCEEDED",
            Error::InputBudgetExceeded { .. } => "INPUT_BUDGET_EXCEEDED",
            Error::OutcomeIndeterminate(_) => "OUTCOME_INDETERMINATE",
            Error::ConfirmRequired { .. } => "CONFIRM_REQUIRED",
            Error::ReadOnly => "READ_ONLY",
            Error::WriteNotAllowed => "WRITE_NOT_ALLOWED",
            Error::RateLimited => "RATE_LIMITED",
            Error::Overloaded => "OVERLOADED",
            Error::Timeout => "TIMEOUT",
            Error::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Error::Serialization(_) => "SERIALIZATION_ERROR",
            Error::Internal(_) => "INTERNAL_ERROR",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn indeterminate_remote_outcomes_are_stable_and_never_retryable() {
        let error = Error::OutcomeIndeterminate("inspect remote state".into());
        assert_eq!(error.code(), "OUTCOME_INDETERMINATE");
        assert!(!error.is_retryable());
        assert!(error.to_string().contains("inspect remote state"));
    }

    #[test]
    fn metadata_budget_errors_have_a_stable_machine_code() {
        let error = Error::MetadataBudgetExceeded {
            subject: "table schema".to_owned(),
            unit: "items",
            limit: 100,
        };
        assert_eq!(error.code(), "METADATA_BUDGET_EXCEEDED");
        assert!(!error.is_retryable());
        assert_eq!(
            error.to_string(),
            "table schema exceeds the metadata items budget of 100"
        );
    }

    #[test]
    fn read_budget_errors_have_a_stable_machine_code() {
        let error = Error::ReadBudgetExceeded {
            subject: "SQL query result".to_owned(),
            unit: "bytes",
            limit: 1024,
        };
        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
        assert!(!error.is_retryable());
        assert_eq!(
            error.to_string(),
            "SQL query result exceeds the read bytes budget of 1024"
        );
    }

    #[test]
    fn input_budget_errors_have_a_stable_machine_code_and_are_not_retryable() {
        let error = Error::InputBudgetExceeded {
            subject: "Kafka produce message".to_owned(),
            unit: "bytes",
            limit: 1024,
        };
        assert_eq!(error.code(), "INPUT_BUDGET_EXCEEDED");
        assert!(!error.is_retryable());
        assert_eq!(
            error.to_string(),
            "Kafka produce message exceeds the input bytes budget of 1024"
        );
    }
}
