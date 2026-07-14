use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{Document, FindOptions, ResultSet, Value},
    port::capability::SetOptions,
    service::limiter::ResultLimiter,
    Result,
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};

const TRANSFER_VERSION: u32 = 1;

#[derive(Args)]
#[command(
    about = "Export rows, keys, or documents to a dbtool JSON artifact.",
    long_about = "Export commands are read-only. They write a versioned dbtool JSON artifact that can be restored with the matching import command."
)]
pub struct ExportCmd {
    #[command(subcommand)]
    pub action: ExportAction,
}

#[derive(Subcommand)]
pub enum ExportAction {
    /// Export SQL query rows.
    Sql {
        /// SQL query to export.
        #[arg(long)]
        query: String,
        /// Output JSON artifact path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Export key/value pairs matched by a scan pattern.
    Kv {
        /// Key scan pattern.
        #[arg(long, default_value = "*")]
        pattern: String,
        /// Output JSON artifact path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Export document-store documents.
    Doc {
        /// Collection name to export.
        collection: String,
        /// JSON filter object.
        #[arg(long, default_value = "{}")]
        filter: String,
        /// Output JSON artifact path.
        #[arg(long)]
        out: PathBuf,
    },
}

#[derive(Args)]
#[command(
    about = "Import a dbtool JSON artifact into a backend.",
    long_about = "Import commands are write operations and require --allow-write before connecting. They accept only versioned dbtool export artifacts."
)]
pub struct ImportCmd {
    #[command(subcommand)]
    pub action: ImportAction,
}

#[derive(Subcommand)]
pub enum ImportAction {
    /// Import sql-rows into an existing table.
    Sql {
        /// Target table. Use a safe table or schema.table identifier.
        #[arg(long)]
        table: String,
        /// Input JSON artifact path.
        #[arg(long)]
        input: PathBuf,
    },
    /// Import kv-pairs into a key-value backend.
    Kv {
        /// Input JSON artifact path.
        #[arg(long)]
        input: PathBuf,
        /// Strip this prefix from source keys before applying --key-prefix.
        #[arg(long)]
        strip_prefix: Option<String>,
        /// Prefix to prepend to restored keys.
        #[arg(long, default_value = "")]
        key_prefix: String,
        /// Optional TTL in seconds for restored keys.
        #[arg(long)]
        ttl: Option<u64>,
    },
    /// Import documents into a document collection.
    Doc {
        /// Target collection.
        collection: String,
        /// Input JSON artifact path.
        #[arg(long)]
        input: PathBuf,
        /// Remove MongoDB-style _id fields before inserting.
        #[arg(long)]
        drop_id: bool,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum TransferArtifact {
    SqlRows {
        version: u32,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    KvPairs {
        version: u32,
        entries: Vec<KvEntry>,
    },
    Documents {
        version: u32,
        collection: Option<String>,
        documents: Vec<Document>,
    },
}

#[derive(Serialize, Deserialize)]
struct KvEntry {
    key: String,
    value: Vec<u8>,
}

pub async fn run_export(ctx: &Context, cmd: ExportCmd) -> Result<String> {
    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();
    let elapsed = || start.elapsed().as_millis() as u64;

    Ok(match cmd.action {
        ExportAction::Sql { query, out } => {
            let sql = conn.as_sql().ok_or_else(|| Error::UnsupportedCapability {
                kind: kind.clone(),
                needed: "SqlEngine",
            })?;
            let result = sql.query(&query, &[]).await?;
            let result = ResultLimiter::new(ctx.limit).apply(result);
            let count = result.rows.len();
            write_artifact(&out, sql_rows_artifact(result))?;
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "sql-rows",
                    "path": out,
                    "rows": count,
                }),
                elapsed(),
                false,
            )
        }
        ExportAction::Kv { pattern, out } => {
            let kv = conn.as_kv().ok_or_else(|| Error::UnsupportedCapability {
                kind: kind.clone(),
                needed: "KeyValueStore",
            })?;
            let keys = kv.scan(&pattern, ctx.limit).await?;
            let mut entries = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(value) = kv.get(&key).await? {
                    entries.push(KvEntry {
                        key,
                        value: value.to_vec(),
                    });
                }
            }
            let count = entries.len();
            write_artifact(
                &out,
                TransferArtifact::KvPairs {
                    version: TRANSFER_VERSION,
                    entries,
                },
            )?;
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "kv-pairs",
                    "path": out,
                    "keys": count,
                }),
                elapsed(),
                false,
            )
        }
        ExportAction::Doc {
            collection,
            filter,
            out,
        } => {
            let docs = conn
                .as_document()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "DocumentStore",
                })?;
            let filter = parse_json_value(&filter)?;
            let options = FindOptions {
                limit: Some(ctx.limit),
                ..Default::default()
            };
            let documents = docs.find(&collection, filter, options).await?;
            let count = documents.len();
            write_artifact(
                &out,
                TransferArtifact::Documents {
                    version: TRANSFER_VERSION,
                    collection: Some(collection),
                    documents,
                },
            )?;
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "documents",
                    "path": out,
                    "documents": count,
                }),
                elapsed(),
                false,
            )
        }
    })
}

pub async fn run_import(ctx: &Context, cmd: ImportCmd) -> Result<String> {
    ensure_write_allowed(ctx)?;

    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();
    let elapsed = || start.elapsed().as_millis() as u64;

    Ok(match cmd.action {
        ImportAction::Sql { table, input } => {
            let artifact = read_artifact(&input)?;
            let TransferArtifact::SqlRows {
                version,
                columns,
                rows,
            } = artifact
            else {
                return Err(Error::Serialization(
                    "expected sql-rows transfer artifact".to_owned(),
                ));
            };
            require_version(version)?;
            let sql = conn.as_sql().ok_or_else(|| Error::UnsupportedCapability {
                kind: kind.clone(),
                needed: "SqlEngine",
            })?;
            let table = validate_identifier_path(&table, "table")?;
            let columns = columns
                .iter()
                .map(|column| validate_identifier(column, "column"))
                .collect::<Result<Vec<_>>>()?;
            let statement_prefix = format!("INSERT INTO {table} ({}) VALUES ", columns.join(", "));
            let mut inserted = 0_u64;
            for row in rows {
                if row.len() != columns.len() {
                    return Err(Error::Serialization(format!(
                        "row has {} values but artifact has {} columns",
                        row.len(),
                        columns.len()
                    )));
                }
                let values = row
                    .iter()
                    .map(sql_literal)
                    .collect::<Result<Vec<_>>>()?
                    .join(", ");
                sql.execute(&format!("{statement_prefix}({values})"), &[])
                    .await?;
                inserted += 1;
            }
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "sql-rows",
                    "inserted": inserted,
                    "table": table,
                }),
                elapsed(),
                false,
            )
        }
        ImportAction::Kv {
            input,
            strip_prefix,
            key_prefix,
            ttl,
        } => {
            let artifact = read_artifact(&input)?;
            let TransferArtifact::KvPairs { version, entries } = artifact else {
                return Err(Error::Serialization(
                    "expected kv-pairs transfer artifact".to_owned(),
                ));
            };
            require_version(version)?;
            let kv = conn.as_kv().ok_or_else(|| Error::UnsupportedCapability {
                kind: kind.clone(),
                needed: "KeyValueStore",
            })?;
            let mut restored = 0_u64;
            for entry in entries {
                let key = restore_key(&entry.key, strip_prefix.as_deref(), &key_prefix)?;
                kv.set(
                    &key,
                    &entry.value,
                    SetOptions {
                        ttl_secs: ttl,
                        nx: false,
                    },
                )
                .await?;
                restored += 1;
            }
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "kv-pairs",
                    "restored": restored,
                }),
                elapsed(),
                false,
            )
        }
        ImportAction::Doc {
            collection,
            input,
            drop_id,
        } => {
            let artifact = read_artifact(&input)?;
            let TransferArtifact::Documents {
                version, documents, ..
            } = artifact
            else {
                return Err(Error::Serialization(
                    "expected documents transfer artifact".to_owned(),
                ));
            };
            require_version(version)?;
            let docs = conn
                .as_document()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "DocumentStore",
                })?;
            let mut documents = documents;
            if drop_id {
                for document in &mut documents {
                    document.remove("_id");
                }
            }
            let count = documents.len() as u64;
            if count > 0 {
                docs.insert(&collection, documents).await?;
            }
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "documents",
                    "inserted": count,
                    "collection": collection,
                }),
                elapsed(),
                false,
            )
        }
    })
}

fn sql_rows_artifact(result: ResultSet) -> TransferArtifact {
    TransferArtifact::SqlRows {
        version: TRANSFER_VERSION,
        columns: result
            .columns
            .into_iter()
            .map(|column| column.name)
            .collect(),
        rows: result.rows,
    }
}

fn parse_json_value(raw: &str) -> Result<Value> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(Value::Json)
        .map_err(|e| Error::Serialization(e.to_string()))
}

fn write_artifact(path: &Path, artifact: TransferArtifact) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|e| Error::Config(e.to_string()))?;
    }
    let bytes =
        serde_json::to_vec_pretty(&artifact).map_err(|e| Error::Serialization(e.to_string()))?;
    fs::write(path, bytes).map_err(|e| Error::Config(e.to_string()))
}

fn read_artifact(path: &Path) -> Result<TransferArtifact> {
    let bytes = fs::read(path).map_err(|e| Error::Config(e.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|e| Error::Serialization(e.to_string()))
}

fn require_version(version: u32) -> Result<()> {
    if version == TRANSFER_VERSION {
        Ok(())
    } else {
        Err(Error::Serialization(format!(
            "unsupported transfer artifact version: {version}"
        )))
    }
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    if ctx.allow_write {
        Ok(())
    } else {
        Err(Error::WriteNotAllowed)
    }
}

fn restore_key(source: &str, strip_prefix: Option<&str>, key_prefix: &str) -> Result<String> {
    let stripped = if let Some(prefix) = strip_prefix {
        source.strip_prefix(prefix).ok_or_else(|| {
            Error::Serialization(format!(
                "key '{source}' does not start with strip prefix '{prefix}'"
            ))
        })?
    } else {
        source
    };
    Ok(format!("{key_prefix}{stripped}"))
}

fn validate_identifier(value: &str, label: &str) -> Result<String> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(Error::Config(format!("invalid SQL {label}: {value}")));
    }
    Ok(value.to_owned())
}

fn validate_identifier_path(value: &str, label: &str) -> Result<String> {
    value
        .split('.')
        .map(|part| validate_identifier(part, label))
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("."))
}

fn sql_literal(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) | Value::Timestamp(value) => value.to_string(),
        Value::Float(value) => {
            if !value.is_finite() {
                return Err(Error::Serialization(
                    "non-finite floats cannot be imported into SQL".to_owned(),
                ));
            }
            value.to_string()
        }
        Value::Text(value) => quote_sql_string(value),
        Value::Bytes(value) => format!("X'{}'", bytes_to_hex(value)),
        Value::Json(value) => quote_sql_string(&value.to_string()),
        Value::Array(value) => quote_sql_string(
            &serde_json::to_string(value).map_err(|e| Error::Serialization(e.to_string()))?,
        ),
        Value::Map(value) => quote_sql_string(
            &serde_json::to_string(value).map_err(|e| Error::Serialization(e.to_string()))?,
        ),
    })
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::service::formatter::Format;

    fn test_context(allow_write: bool) -> Context {
        Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: Format::Json,
            limit: 100,
            throttle_overrides: Default::default(),
            allow_write,
            confirm: None,
        }
    }

    #[test]
    fn import_requires_write_flag() {
        assert!(matches!(
            ensure_write_allowed(&test_context(false)),
            Err(Error::WriteNotAllowed)
        ));
        assert!(ensure_write_allowed(&test_context(true)).is_ok());
    }

    #[test]
    fn restore_key_applies_strip_and_prefix() {
        assert_eq!(
            restore_key("src:1", Some("src:"), "dst:").unwrap(),
            "dst:1".to_owned()
        );
        assert!(matches!(
            restore_key("src:1", Some("missing:"), "dst:"),
            Err(Error::Serialization(message)) if message.contains("does not start")
        ));
    }

    #[test]
    fn sql_literals_escape_values_and_validate_identifiers() {
        assert_eq!(
            sql_literal(&Value::Text("O'Reilly".to_owned())).unwrap(),
            "'O''Reilly'"
        );
        assert_eq!(sql_literal(&Value::Bytes(vec![0, 255])).unwrap(), "X'00ff'");
        assert_eq!(
            validate_identifier_path("main.people", "table").unwrap(),
            "main.people"
        );
        assert!(validate_identifier_path("bad-name", "table").is_err());
    }
}
