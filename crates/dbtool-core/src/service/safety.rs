use crate::{Error, Result};
use sha2::{Digest, Sha256};
use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

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
        Self::check_with_target(sql, "", allow_write, confirm_token)
    }

    /// Like [`Self::check`], but binds destructive-operation confirmation to a
    /// concrete target connection label.
    pub fn check_with_target(
        sql: &str,
        target: &str,
        allow_write: bool,
        confirm_token: Option<&str>,
    ) -> Result<StatementKind> {
        let analysis = analyze(sql);
        let kind = analysis.kind.clone();

        match kind {
            StatementKind::Read => Ok(StatementKind::Read),
            StatementKind::Write if allow_write => Ok(StatementKind::Write),
            StatementKind::Write => Err(Error::WriteNotAllowed),
            StatementKind::Destructive => {
                let impact = serde_json::json!({
                    "op": analysis.operation,
                    "target": target,
                    "statements": analysis.statement_count,
                });
                let token = compute_token(&analysis.normalized_sql, target, &impact);
                match confirm_token {
                    Some(t) if t == token => Ok(StatementKind::Destructive),
                    Some(_) => Err(Error::Internal("confirm token mismatch".into())),
                    None => Err(Error::ConfirmRequired {
                        confirm_token: token,
                        impact,
                    }),
                }
            }
        }
    }

    /// Require a target-bound confirmation token for a destructive operation
    /// that is not represented by SQL (for example, dropping a MongoDB
    /// collection or an OpenSearch index).
    pub fn check_destructive_operation(
        operation: &str,
        resource: &str,
        target: &str,
        allow_write: bool,
        confirm_token: Option<&str>,
    ) -> Result<()> {
        if !allow_write {
            return Err(Error::WriteNotAllowed);
        }

        let operation = operation.trim().to_ascii_uppercase();
        let resource = resource.trim();
        if operation.is_empty() || resource.is_empty() {
            return Err(Error::Config(
                "destructive operation and resource must not be empty".into(),
            ));
        }

        let impact = serde_json::json!({
            "op": operation,
            "resource": resource,
            "target": target,
        });
        let normalized = format!("{} {}", impact["op"].as_str().unwrap_or_default(), resource);
        let token = compute_token(&normalized, target, &impact);

        match confirm_token {
            Some(candidate) if candidate == token => Ok(()),
            Some(_) => Err(Error::Internal("confirm token mismatch".into())),
            None => Err(Error::ConfirmRequired {
                confirm_token: token,
                impact,
            }),
        }
    }
}

#[derive(Debug)]
struct SafetyAnalysis {
    kind: StatementKind,
    normalized_sql: String,
    operation: String,
    statement_count: usize,
}

fn analyze(sql: &str) -> SafetyAnalysis {
    let dialect = GenericDialect {};

    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) if !statements.is_empty() => {
            let kind = statements
                .iter()
                .map(classify_statement)
                .fold(StatementKind::Read, strongest_kind);
            let normalized_sql = statements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            let operation = first_keyword(&normalized_sql).to_owned();
            SafetyAnalysis {
                kind,
                normalized_sql,
                operation,
                statement_count: statements.len(),
            }
        }
        _ => SafetyAnalysis {
            kind: classify_by_keyword(sql),
            normalized_sql: normalize_sql(sql),
            operation: first_keyword(sql).to_owned(),
            statement_count: 1,
        },
    }
}

fn classify_statement(statement: &Statement) -> StatementKind {
    match statement {
        Statement::Query(_)
        | Statement::Analyze { .. }
        | Statement::ExplainTable { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowCollation { .. }
        | Statement::Fetch { .. } => StatementKind::Read,

        Statement::Explain {
            analyze, statement, ..
        } => {
            if *analyze {
                classify_statement(statement)
            } else {
                StatementKind::Read
            }
        }

        Statement::Update { selection, .. } => {
            if selection.is_some() {
                StatementKind::Write
            } else {
                StatementKind::Destructive
            }
        }
        Statement::Delete(delete) => {
            if delete.selection.is_some() {
                StatementKind::Write
            } else {
                StatementKind::Destructive
            }
        }

        Statement::Truncate { .. }
        | Statement::Drop { .. }
        | Statement::DropFunction { .. }
        | Statement::DropProcedure { .. }
        | Statement::DropSecret { .. }
        | Statement::DropTrigger { .. }
        | Statement::AlterTable { .. }
        | Statement::AlterIndex { .. }
        | Statement::AlterView { .. }
        | Statement::AlterRole { .. }
        | Statement::CreateView { .. }
        | Statement::CreateTable(_)
        | Statement::CreateVirtualTable { .. }
        | Statement::CreateIndex(_)
        | Statement::CreateRole { .. }
        | Statement::CreateSecret { .. }
        | Statement::CreateExtension { .. }
        | Statement::CreateSchema { .. }
        | Statement::CreateDatabase { .. }
        | Statement::CreateFunction { .. }
        | Statement::CreateTrigger { .. }
        | Statement::CreateProcedure { .. }
        | Statement::CreateMacro { .. }
        | Statement::CreateStage { .. }
        | Statement::CreateSequence { .. }
        | Statement::CreateType { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. } => StatementKind::Destructive,

        _ => StatementKind::Write,
    }
}

fn strongest_kind(acc: StatementKind, next: StatementKind) -> StatementKind {
    match (acc, next) {
        (StatementKind::Destructive, _) | (_, StatementKind::Destructive) => {
            StatementKind::Destructive
        }
        (StatementKind::Write, _) | (_, StatementKind::Write) => StatementKind::Write,
        _ => StatementKind::Read,
    }
}

fn classify_by_keyword(sql: &str) -> StatementKind {
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

fn compute_token(normalized_sql: &str, target: &str, impact: &serde_json::Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(normalized_sql.as_bytes());
    hasher.update(b"\0");
    hasher.update(target.as_bytes());
    hasher.update(b"\0");
    hasher.update(impact.to_string().as_bytes());
    hex::encode(&hasher.finalize()[..8])
}

fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn first_keyword(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .unwrap_or("UNKNOWN")
        .to_uppercase()
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

    #[test]
    fn non_sql_destructive_operation_requires_target_bound_confirmation() {
        let error = SafetyGuard::check_destructive_operation(
            "drop_collection",
            "events",
            "conn:local",
            true,
            None,
        )
        .unwrap_err();
        let token = match error {
            Error::ConfirmRequired {
                confirm_token,
                impact,
            } => {
                assert_eq!(impact["op"], "DROP_COLLECTION");
                assert_eq!(impact["resource"], "events");
                assert_eq!(impact["target"], "conn:local");
                confirm_token
            }
            other => panic!("expected confirmation requirement, got {other:?}"),
        };

        SafetyGuard::check_destructive_operation(
            "drop_collection",
            "events",
            "conn:local",
            true,
            Some(&token),
        )
        .unwrap();

        assert!(matches!(
            SafetyGuard::check_destructive_operation(
                "drop_collection",
                "events",
                "conn:other",
                true,
                Some(&token),
            ),
            Err(Error::Internal(message)) if message.contains("mismatch")
        ));
    }

    #[test]
    fn non_sql_destructive_operation_still_requires_write_permission() {
        assert!(matches!(
            SafetyGuard::check_destructive_operation(
                "drop_collection",
                "events",
                "conn:local",
                false,
                None,
            ),
            Err(Error::WriteNotAllowed)
        ));
    }

    #[test]
    fn parser_ignores_comments_before_destructive_statement() {
        let err = SafetyGuard::check("/* maintenance */ drop table users", true, None).unwrap_err();

        assert!(matches!(err, Error::ConfirmRequired { .. }));
    }

    #[test]
    fn parser_detects_update_without_where_even_when_literal_mentions_where() {
        let err = SafetyGuard::check("update users set note = 'wherever'", true, None).unwrap_err();

        assert!(matches!(err, Error::ConfirmRequired { .. }));
    }

    #[test]
    fn parser_allows_delete_with_where_as_regular_write() {
        assert_eq!(
            SafetyGuard::check("delete from users where id = 1", true, None).unwrap(),
            StatementKind::Write
        );
    }

    #[test]
    fn destructive_token_is_bound_to_target() {
        let err =
            SafetyGuard::check_with_target("drop table users", "conn:a", true, None).unwrap_err();

        let token = match err {
            Error::ConfirmRequired { confirm_token, .. } => confirm_token,
            other => panic!("expected confirm required, got {other:?}"),
        };

        assert_eq!(
            SafetyGuard::check_with_target("drop table users", "conn:a", true, Some(&token))
                .unwrap(),
            StatementKind::Destructive
        );
        assert!(matches!(
            SafetyGuard::check_with_target("drop table users", "conn:b", true, Some(&token)),
            Err(Error::Internal(_))
        ));
    }
}
