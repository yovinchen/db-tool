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
}
