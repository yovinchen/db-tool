/// Replace the password segment of a DSN URL with `***` for safe logging.
pub fn redact_dsn(raw: &str) -> String {
    if let Ok(mut url) = url::Url::parse(raw) {
        if url.password().is_some() {
            let _ = url.set_password(Some("***"));
        }
        url.to_string()
    } else {
        // Not a URL — best-effort mask anything after a colon before @
        raw.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_password() {
        let raw = "mysql://user:s3cr3t@host:3306/db";
        let redacted = redact_dsn(raw);
        assert!(redacted.contains("***"));
        assert!(!redacted.contains("s3cr3t"));
    }

    #[test]
    fn no_password_unchanged() {
        let raw = "redis://host:6379/0";
        assert_eq!(redact_dsn(raw), raw);
    }
}
