use crate::{Error, Result};
use sha2::{Digest, Sha256};

/// Statement-level classification produced by the SQL parser.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementKind {
    Read,
    Write,
    Destructive,
}

pub struct SafetyGuard;

impl SafetyGuard {
    /// Returns `Ok(StatementKind)` for safe operations; `Err` for blocked ones.
    /// `allow_write`: whether non-destructive writes are permitted.
    /// `confirm_token`: if present, must match the hash of the statement.
    pub fn check(
        sql: &str,
        allow_write: bool,
        confirm_token: Option<&str>,
    ) -> Result<StatementKind> {
        let kind = classify(sql);

        match kind {
            StatementKind::Read => Ok(StatementKind::Read),
            StatementKind::Write if allow_write => Ok(StatementKind::Write),
            StatementKind::Write => Err(Error::WriteNotAllowed),
            StatementKind::Destructive => {
                let token = compute_token(sql);
                match confirm_token {
                    Some(t) if t == token => Ok(StatementKind::Destructive),
                    Some(_) => Err(Error::Internal("confirm token mismatch".into())),
                    None => Err(Error::ConfirmRequired {
                        confirm_token: token,
                        impact: serde_json::json!({ "op": first_keyword(sql) }),
                    }),
                }
            }
        }
    }
}

fn classify(sql: &str) -> StatementKind {
    let upper = sql.trim().to_uppercase();
    let kw = upper.split_whitespace().next().unwrap_or("");

    match kw {
        "SELECT" | "SHOW" | "DESCRIBE" | "EXPLAIN" | "WITH" => StatementKind::Read,
        "DROP" | "TRUNCATE" => StatementKind::Destructive,
        "DELETE" | "UPDATE" => {
            // No WHERE clause → destructive.
            if !upper.contains("WHERE") {
                StatementKind::Destructive
            } else {
                StatementKind::Write
            }
        }
        "INSERT" | "UPSERT" | "REPLACE" | "MERGE" | "COPY" => StatementKind::Write,
        "ALTER" | "CREATE" | "RENAME" => StatementKind::Destructive,
        _ => StatementKind::Write,
    }
}

fn compute_token(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.trim().as_bytes());
    hex::encode(&hasher.finalize()[..8])
}

fn first_keyword(sql: &str) -> &str {
    sql.trim().split_whitespace().next().unwrap_or("UNKNOWN")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_statement_is_allowed_without_write_flag() {
        assert_eq!(
            SafetyGuard::check("select * from users", false, None).unwrap(),
            StatementKind::Read
        );
    }

    #[test]
    fn non_destructive_write_requires_write_flag() {
        assert!(matches!(
            SafetyGuard::check("update users set name = 'a' where id = 1", false, None),
            Err(Error::WriteNotAllowed)
        ));

        assert_eq!(
            SafetyGuard::check("update users set name = 'a' where id = 1", true, None).unwrap(),
            StatementKind::Write
        );
    }

    #[test]
    fn destructive_statement_requires_confirm_token() {
        let err = SafetyGuard::check("drop table users", true, None).unwrap_err();

        let token = match err {
            Error::ConfirmRequired { confirm_token, .. } => confirm_token,
            other => panic!("expected confirm required, got {other:?}"),
        };

        assert_eq!(
            SafetyGuard::check("drop table users", true, Some(&token)).unwrap(),
            StatementKind::Destructive
        );
    }
}
