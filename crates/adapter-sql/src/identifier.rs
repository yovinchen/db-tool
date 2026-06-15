use dbtool_core::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableRef {
    pub schema: Option<String>,
    pub name: String,
}

pub(crate) fn parse_table_ref(input: &str) -> Result<TableRef> {
    let mut parts = input.split('.');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();

    if parts.next().is_some() {
        return Err(invalid_identifier(input));
    }

    match second {
        Some(table) => {
            validate_identifier(first)?;
            validate_identifier(table)?;
            Ok(TableRef {
                schema: Some(first.to_owned()),
                name: table.to_owned(),
            })
        }
        None => {
            validate_identifier(first)?;
            Ok(TableRef {
                schema: None,
                name: first.to_owned(),
            })
        }
    }
}

pub(crate) fn validate_optional_schema(schema: Option<&str>) -> Result<Option<&str>> {
    if let Some(schema) = schema {
        validate_identifier(schema)?;
    }
    Ok(schema)
}

pub(crate) fn validate_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let Some(first) = chars.next() else {
        return Err(invalid_identifier(identifier));
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(invalid_identifier(identifier));
    }

    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '$') {
        return Err(invalid_identifier(identifier));
    }

    Ok(())
}

fn invalid_identifier(identifier: &str) -> Error {
    Error::Query(format!("invalid SQL identifier: {identifier}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_schema_qualified_table_refs() {
        assert_eq!(
            parse_table_ref("public.users").unwrap(),
            TableRef {
                schema: Some("public".to_owned()),
                name: "users".to_owned(),
            }
        );
    }

    #[test]
    fn rejects_injection_shaped_identifiers() {
        assert!(parse_table_ref("users;drop table users").is_err());
        assert!(parse_table_ref("public.users.extra").is_err());
        assert!(validate_optional_schema(Some("public;drop")).is_err());
    }
}
