use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{Document, FindOptions, KeyExpiry, KeyValueRestoreOutcome, ResultSet, Value},
    port::CapabilityOperation,
    service::{
        limiter::ResultLimiter,
        safety::{SafetyGuard, StatementKind},
    },
    Result,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const KV_TRANSFER_VERSION: u32 = 3;
const DOCUMENT_TRANSFER_VERSION: u32 = 3;
const SQL_TRANSFER_VERSION: u32 = 3;
const MAX_TRANSFER_ARTIFACT_BYTES: usize = 256 * 1024 * 1024;

#[derive(Args)]
#[command(
    about = "Export rows, keys, or documents to a dbtool JSON artifact.",
    long_about = "Export commands are read-only. They write a versioned dbtool JSON artifact with typed values, redacted source provenance, and explicit completeness metadata. KV and document exports observe one bounded probe item; an artifact marked incomplete cannot be imported. Files are capped at 256 MiB and published with a same-directory atomic rename."
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
    long_about = "Import commands are write operations and require --allow-write before reading the artifact, resolving the DSN, or connecting. They read at most 256 MiB, process at most the global --limit item budget, and accept only current, internally consistent, complete dbtool artifacts. SQLite, PostgreSQL, and MySQL SQL imports bind every value and commit the complete batch in one transaction, reporting atomic=true. Other SQL adapters reject that optional capability explicitly. KV and document imports do not expose a portable cross-request transaction contract and report atomic=false."
)]
pub struct ImportCmd {
    #[command(subcommand)]
    pub action: ImportAction,
}

#[derive(Subcommand)]
pub enum ImportAction {
    /// Import sql-rows into an existing table.
    #[command(
        long_about = "Import a complete sql-rows artifact into an existing table. The target adapter must advertise sql.insert_rows_atomic. Every value is sent as a bound parameter and all rows commit in one transaction; any bind, constraint, or affected-row error rolls back the complete batch."
    )]
    Sql {
        /// Target table. Use a safe table or schema.table identifier.
        #[arg(long)]
        table: String,
        /// Input JSON artifact path.
        #[arg(long)]
        input: PathBuf,
        /// Deprecated compatibility flag. Legacy SQL artifacts are rejected
        /// because their untagged values cannot be migrated without type loss.
        #[arg(long)]
        accept_legacy_unmarked: bool,
    },
    /// Import kv-pairs into a key-value backend.
    #[command(
        long_about = "Import a complete kv-pairs v3 artifact while preserving each key's exact bytes and source lifetime. Input is capped at 256 MiB and the global --limit item budget is enforced before connecting. Already-expired entries are skipped without writing. New keys use conditional create semantics so a concurrent creator is never overwritten silently. Existing keys are rejected by default; --replace-existing requires a global --confirm token bound to the target plus every transformed target key, exact value, and absolute expiry. Each key/value/expiry restore is atomic, but the complete multi-key import is not, so the response reports per_entry_atomic=true and atomic=false."
    )]
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
        /// Permit replacing keys that already exist. Existing keys additionally
        /// require the target-bound global --confirm token.
        #[arg(long)]
        replace_existing: bool,
    },
    /// Import documents into a document collection.
    #[command(
        long_about = "Import a complete documents artifact with one backend batch call. Duplicate artifact _id values are rejected offline. Existing target _id values are checked best-effort immediately before insertion; a concurrent writer can still race that check. --drop-id requests backend-generated identity instead. The generic document contract does not promise transaction-level atomicity for the batch, so the response reports atomic=false."
    )]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum TransferArtifact {
    SqlRows {
        version: u32,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        #[serde(default)]
        truncated: Option<bool>,
    },
    KvPairs {
        version: u32,
        source: ArtifactSource,
        integrity: ArtifactIntegrity,
        entries: Vec<KvEntry>,
    },
    Documents {
        version: u32,
        source: ArtifactSource,
        integrity: ArtifactIntegrity,
        collection: String,
        documents: Vec<Document>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactSource {
    connector: String,
    connection: String,
    resource: String,
    selector: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactIntegrity {
    value_codec: String,
    complete: bool,
    truncated: bool,
    source_changed: bool,
    exported_items: u64,
    selected_items: u64,
    limit: usize,
    consistency: ArtifactConsistency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ArtifactConsistency {
    /// The adapter completed one bounded traversal, but did not promise a
    /// transactionally stable snapshot while concurrent writers were active.
    BestEffort,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KvEntry {
    key: String,
    value: Value,
    expiry: KeyExpiry,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct PreparedKvEntry {
    key: String,
    value: Vec<u8>,
    expiry: KeyExpiry,
}

enum PreparedImport {
    Sql {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Kv {
        entries: Vec<PreparedKvEntry>,
        replace_existing: bool,
    },
    Doc {
        collection: String,
        documents: Vec<Document>,
    },
}

pub async fn run_export(ctx: &Context, cmd: ExportCmd) -> Result<String> {
    let probe_limit = ResultLimiter::new(ctx.limit).probe_rows()?;
    if let ExportAction::Sql { query, .. } = &cmd.action {
        ensure_readonly_export_query(query)?;
    }
    let dsn = ctx.resolve_dsn()?;
    let connection = ctx.safety_target(&dsn);
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
            let result = sql.query_bounded(&query, &[], ctx.limit).await?;
            let count = result.rows.len();
            let truncated = result.truncated;
            write_artifact(&out, sql_rows_artifact(result))?;
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "sql-rows",
                    "path": out,
                    "rows": count,
                }),
                elapsed(),
                truncated,
            )
        }
        ExportAction::Kv { pattern, out } => {
            if !conn
                .operations()
                .contains(&CapabilityOperation::KeyValueGetWithExpiry)
            {
                return Err(Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "kv.get_with_expiry",
                });
            }
            let kv = conn.as_kv().ok_or_else(|| Error::UnsupportedCapability {
                kind: kind.clone(),
                needed: "KeyValueStore",
            })?;
            let mut keys = kv.scan(&pattern, probe_limit).await?;
            ensure_unique_strings(&keys, "key returned by the source scan")?;
            let truncated = keys.len() > ctx.limit;
            keys.truncate(ctx.limit);
            let selected_items = usize_to_u64(keys.len(), "selected key count")?;
            let mut entries = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(snapshot) = kv.get_with_expiry(&key).await? {
                    entries.push(KvEntry {
                        key,
                        value: Value::Bytes(snapshot.value.to_vec()),
                        expiry: snapshot.expiry,
                    });
                }
            }
            let count = entries.len();
            let exported_items = usize_to_u64(count, "exported key count")?;
            let source_changed = exported_items != selected_items;
            let integrity = artifact_integrity(
                ctx.limit,
                exported_items,
                selected_items,
                truncated,
                source_changed,
            );
            let complete = integrity.complete;
            write_artifact(
                &out,
                TransferArtifact::KvPairs {
                    version: KV_TRANSFER_VERSION,
                    source: ArtifactSource {
                        connector: kind.clone(),
                        connection: connection.clone(),
                        resource: "key-pattern".to_owned(),
                        selector: Value::Text(pattern),
                    },
                    integrity,
                    entries,
                },
            )?;
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "kv-pairs",
                    "path": out,
                    "keys": count,
                    "complete": complete,
                    "source_changed": source_changed,
                }),
                elapsed(),
                truncated,
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
                limit: Some(probe_limit),
                ..Default::default()
            };
            let mut documents = docs.find(&collection, filter.clone(), options).await?;
            let truncated = documents.len() > ctx.limit;
            documents.truncate(ctx.limit);
            let count = documents.len();
            let exported_items = usize_to_u64(count, "exported document count")?;
            let integrity =
                artifact_integrity(ctx.limit, exported_items, exported_items, truncated, false);
            let complete = integrity.complete;
            write_artifact(
                &out,
                TransferArtifact::Documents {
                    version: DOCUMENT_TRANSFER_VERSION,
                    source: ArtifactSource {
                        connector: kind.clone(),
                        connection,
                        resource: collection.clone(),
                        selector: filter,
                    },
                    integrity,
                    collection,
                    documents,
                },
            )?;
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "documents",
                    "path": out,
                    "documents": count,
                    "complete": complete,
                }),
                elapsed(),
                truncated,
            )
        }
    })
}

pub async fn run_import(ctx: &Context, cmd: ImportCmd) -> Result<String> {
    ensure_write_allowed(ctx)?;
    let artifact = read_artifact(import_input(&cmd.action))?;
    validate_import_artifact(&cmd.action, &artifact)?;
    let prepared = prepare_import(cmd.action, artifact, ctx.limit)?;

    let dsn = ctx.resolve_dsn()?;
    let safety_target = ctx.safety_target(&dsn);
    let conn = ctx.registry.connect(&dsn).await?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();
    let elapsed = || start.elapsed().as_millis() as u64;

    Ok(match prepared {
        PreparedImport::Sql {
            table,
            columns,
            rows,
        } => {
            if !conn
                .operations()
                .contains(&CapabilityOperation::SqlInsertRowsAtomic)
            {
                return Err(Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "sql.insert_rows_atomic",
                });
            }
            let sql = conn.as_sql().ok_or_else(|| Error::UnsupportedCapability {
                kind: kind.clone(),
                needed: "SqlEngine",
            })?;
            let inserted = sql.insert_rows_atomic(&table, &columns, &rows).await?;
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "sql-rows",
                    "inserted": inserted,
                    "table": table,
                    "atomic": true,
                }),
                elapsed(),
                false,
            )
        }
        PreparedImport::Kv {
            entries,
            replace_existing,
        } => {
            if !conn
                .operations()
                .contains(&CapabilityOperation::KeyValueRestoreWithExpiry)
            {
                return Err(Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "kv.restore_with_expiry",
                });
            }
            let kv = conn.as_kv().ok_or_else(|| Error::UnsupportedCapability {
                kind: kind.clone(),
                needed: "KeyValueStore",
            })?;
            let mut existing = BTreeSet::new();
            if replace_existing {
                for entry in &entries {
                    if kv.get(&entry.key).await?.is_some() {
                        existing.insert(entry.key.clone());
                    }
                }
            }
            if !existing.is_empty() {
                let resource = serde_json::to_string(&existing)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                let confirmation_scope = kv_replace_confirmation_scope(&entries)?;
                SafetyGuard::check_destructive_operation_with_scope(
                    "import_kv_replace",
                    &resource,
                    &safety_target,
                    &confirmation_scope,
                    ctx.allow_write,
                    ctx.confirm.as_deref(),
                )?;
            }
            let mut restored = 0_u64;
            let mut expired_skipped = 0_u64;
            let mut replaced = 0_u64;
            for entry in entries {
                let was_present_at_preflight = existing.contains(&entry.key);
                let outcome = kv
                    .restore_with_expiry(
                        &entry.key,
                        &entry.value,
                        entry.expiry,
                        !was_present_at_preflight,
                    )
                    .await?;
                match outcome {
                    KeyValueRestoreOutcome::Stored => {
                        restored = restored.checked_add(1).ok_or_else(|| {
                            Error::Serialization("restored KV count overflowed u64".into())
                        })?;
                        if was_present_at_preflight {
                            replaced = replaced.checked_add(1).ok_or_else(|| {
                                Error::Serialization("replaced KV count overflowed u64".into())
                            })?;
                        }
                    }
                    KeyValueRestoreOutcome::Expired => {
                        expired_skipped = expired_skipped.checked_add(1).ok_or_else(|| {
                            Error::Serialization("expired KV count overflowed u64".into())
                        })?;
                    }
                    KeyValueRestoreOutcome::ConditionNotMet => {
                        return Err(Error::Config(format!(
                            "target key '{}' already exists or changed after preflight; no existing key was overwritten without an exact replacement confirmation, but earlier entries may already have been restored because multi-key KV import is not atomic",
                            entry.key
                        )));
                    }
                }
            }
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "kv-pairs",
                    "restored": restored,
                    "expired_skipped": expired_skipped,
                    "replaced": replaced,
                    "atomic": false,
                    "per_entry_atomic": true,
                    "expiry_preserved": true,
                }),
                elapsed(),
                false,
            )
        }
        PreparedImport::Doc {
            collection,
            documents,
        } => {
            let docs = conn
                .as_document()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "DocumentStore",
                })?;
            ensure_document_ids_are_available(docs, &collection, &documents).await?;
            let count = usize_to_u64(documents.len(), "imported document count")?;
            if count > 0 {
                docs.insert(&collection, documents).await?;
            }
            ctx.render_success(
                &kind,
                serde_json::json!({
                    "kind": "documents",
                    "inserted": count,
                    "collection": collection,
                    "atomic": false,
                }),
                elapsed(),
                false,
            )
        }
    })
}

fn sql_rows_artifact(result: ResultSet) -> TransferArtifact {
    let ResultSet {
        columns,
        rows,
        truncated,
    } = result;
    TransferArtifact::SqlRows {
        version: SQL_TRANSFER_VERSION,
        columns: columns.into_iter().map(|column| column.name).collect(),
        rows,
        truncated: Some(truncated),
    }
}

fn import_input(action: &ImportAction) -> &Path {
    match action {
        ImportAction::Sql { input, .. }
        | ImportAction::Kv { input, .. }
        | ImportAction::Doc { input, .. } => input,
    }
}

fn artifact_integrity(
    limit: usize,
    exported_items: u64,
    selected_items: u64,
    truncated: bool,
    source_changed: bool,
) -> ArtifactIntegrity {
    ArtifactIntegrity {
        value_codec: Value::WIRE_CODEC.to_owned(),
        complete: !truncated && !source_changed,
        truncated,
        source_changed,
        exported_items,
        selected_items,
        limit,
        consistency: ArtifactConsistency::BestEffort,
    }
}

fn validate_import_artifact(action: &ImportAction, artifact: &TransferArtifact) -> Result<()> {
    match (action, artifact) {
        (
            ImportAction::Sql {
                accept_legacy_unmarked,
                ..
            },
            TransferArtifact::SqlRows {
                version, truncated, ..
            },
        ) => require_complete_sql_artifact(*version, *truncated, *accept_legacy_unmarked),
        (
            ImportAction::Kv { .. },
            TransferArtifact::KvPairs {
                version,
                source,
                integrity,
                entries,
            },
        ) => {
            require_version("kv-pairs", *version, KV_TRANSFER_VERSION)?;
            validate_source("kv-pairs", source)?;
            if source.resource != "key-pattern" || !matches!(&source.selector, Value::Text(_)) {
                return Err(Error::Serialization(
                    "kv-pairs artifact source must contain a typed key-pattern selector".into(),
                ));
            }
            validate_integrity("kv-pairs", integrity, entries.len())?;
            ensure_unique_strings(
                &entries
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect::<Vec<_>>(),
                "source key in the artifact",
            )?;
            for entry in entries {
                if !matches!(entry.value, Value::Bytes(_)) {
                    return Err(Error::Serialization(format!(
                        "kv-pairs entry '{}' must use the typed Value::Bytes wire representation",
                        entry.key
                    )));
                }
            }
            require_complete_integrity("kv-pairs", integrity)
        }
        (
            ImportAction::Doc { .. },
            TransferArtifact::Documents {
                version,
                source,
                integrity,
                collection,
                documents,
            },
        ) => {
            require_version("documents", *version, DOCUMENT_TRANSFER_VERSION)?;
            validate_source("documents", source)?;
            if source.resource != *collection || !matches!(&source.selector, Value::Json(_)) {
                return Err(Error::Serialization(
                    "documents artifact source collection/filter metadata is inconsistent".into(),
                ));
            }
            validate_integrity("documents", integrity, documents.len())?;
            require_complete_integrity("documents", integrity)
        }
        (ImportAction::Sql { .. }, _) => Err(Error::Serialization(
            "expected sql-rows transfer artifact".to_owned(),
        )),
        (ImportAction::Kv { .. }, _) => Err(Error::Serialization(
            "expected kv-pairs transfer artifact".to_owned(),
        )),
        (ImportAction::Doc { .. }, _) => Err(Error::Serialization(
            "expected documents transfer artifact".to_owned(),
        )),
    }
}

fn prepare_import(
    action: ImportAction,
    artifact: TransferArtifact,
    max_items: usize,
) -> Result<PreparedImport> {
    ResultLimiter::new(max_items).probe_rows()?;
    ensure_import_item_budget(&artifact, max_items)?;

    match (action, artifact) {
        (ImportAction::Sql { table, .. }, TransferArtifact::SqlRows { columns, rows, .. }) => {
            let table = validate_identifier_path(&table, "table")?;
            let columns = columns
                .into_iter()
                .map(|column| validate_identifier(&column, "column"))
                .collect::<Result<Vec<_>>>()?;
            ensure_unique_sql_identifiers(&columns, "SQL artifact column")?;
            for (row_index, row) in rows.iter().enumerate() {
                if row.len() != columns.len() {
                    return Err(Error::Serialization(format!(
                        "SQL artifact row {} has {} values but the artifact has {} columns",
                        row_index + 1,
                        row.len(),
                        columns.len()
                    )));
                }
            }
            Ok(PreparedImport::Sql {
                table,
                columns,
                rows,
            })
        }
        (
            ImportAction::Kv {
                strip_prefix,
                key_prefix,
                replace_existing,
                ..
            },
            TransferArtifact::KvPairs { entries, .. },
        ) => Ok(PreparedImport::Kv {
            entries: prepare_kv_entries(entries, strip_prefix.as_deref(), &key_prefix)?,
            replace_existing,
        }),
        (
            ImportAction::Doc {
                collection,
                drop_id,
                ..
            },
            TransferArtifact::Documents { documents, .. },
        ) => {
            if collection.trim().is_empty() || collection.contains('\0') {
                return Err(Error::Config(
                    "document import collection must be non-empty and contain no NUL bytes".into(),
                ));
            }
            let mut documents = documents;
            if drop_id {
                for document in &mut documents {
                    document.remove("_id");
                }
            }
            ensure_unique_document_ids(&documents)?;
            Ok(PreparedImport::Doc {
                collection,
                documents,
            })
        }
        _ => Err(Error::Serialization(
            "import action does not match transfer artifact kind".into(),
        )),
    }
}

fn ensure_import_item_budget(artifact: &TransferArtifact, max_items: usize) -> Result<()> {
    let actual_items = match artifact {
        TransferArtifact::SqlRows { rows, .. } => rows.len(),
        TransferArtifact::KvPairs { entries, .. } => entries.len(),
        TransferArtifact::Documents { documents, .. } => documents.len(),
    };
    if actual_items <= max_items {
        return Ok(());
    }
    Err(Error::Config(format!(
        "transfer artifact contains {actual_items} items, exceeding the import --limit {max_items}; raise --limit deliberately or split the transfer"
    )))
}

fn validate_source(kind: &str, source: &ArtifactSource) -> Result<()> {
    if source.connector.trim().is_empty()
        || source.connection.trim().is_empty()
        || source.resource.trim().is_empty()
    {
        return Err(Error::Serialization(format!(
            "{kind} artifact source metadata contains an empty connector, connection, or resource"
        )));
    }
    Ok(())
}

fn validate_integrity(
    kind: &str,
    integrity: &ArtifactIntegrity,
    actual_items: usize,
) -> Result<()> {
    if integrity.value_codec != Value::WIRE_CODEC {
        return Err(Error::Serialization(format!(
            "unsupported {kind} value codec: {}; expected {}",
            integrity.value_codec,
            Value::WIRE_CODEC
        )));
    }
    if integrity.limit == 0 {
        return Err(Error::Serialization(format!(
            "{kind} artifact integrity limit must be greater than zero"
        )));
    }
    let actual_items = usize_to_u64(actual_items, "artifact item count")?;
    if integrity.exported_items != actual_items {
        return Err(Error::Serialization(format!(
            "{kind} artifact integrity count mismatch: metadata says {} exported item(s), payload has {actual_items}",
            integrity.exported_items
        )));
    }
    if integrity.exported_items > integrity.selected_items {
        return Err(Error::Serialization(format!(
            "{kind} artifact exported item count exceeds the selected item count"
        )));
    }
    let selected_items = usize::try_from(integrity.selected_items).map_err(|_| {
        Error::Serialization(format!(
            "{kind} artifact selected item count exceeds the platform range"
        ))
    })?;
    if selected_items > integrity.limit {
        return Err(Error::Serialization(format!(
            "{kind} artifact selected item count exceeds its declared limit"
        )));
    }
    if integrity.source_changed != (integrity.exported_items != integrity.selected_items) {
        return Err(Error::Serialization(format!(
            "{kind} artifact source_changed marker disagrees with its item counts"
        )));
    }
    if integrity.truncated && selected_items != integrity.limit {
        return Err(Error::Serialization(format!(
            "{kind} artifact truncated marker requires the declared limit to be fully consumed"
        )));
    }
    let expected_complete = !integrity.truncated && !integrity.source_changed;
    if integrity.complete != expected_complete {
        return Err(Error::Serialization(format!(
            "{kind} artifact complete marker contradicts its truncation/source-change markers"
        )));
    }
    Ok(())
}

fn require_complete_integrity(kind: &str, integrity: &ArtifactIntegrity) -> Result<()> {
    if integrity.complete {
        return Ok(());
    }
    let reason = match (integrity.truncated, integrity.source_changed) {
        (true, true) => "the export hit its limit and the source changed while items were read",
        (true, false) => "the export hit its limit",
        (false, true) => "the source changed while items were read",
        (false, false) => "its integrity markers are inconsistent",
    };
    Err(Error::Serialization(format!(
        "refusing to import incomplete {kind} artifact because {reason}; rerun export against a stable source with a sufficient --limit"
    )))
}

fn usize_to_u64(value: usize, label: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Serialization(format!("{label} exceeds the u64 range")))
}

fn ensure_unique_strings(values: &[String], label: &str) -> Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(Error::Serialization(format!(
                "duplicate {label}: '{value}'"
            )));
        }
    }
    Ok(())
}

fn ensure_unique_sql_identifiers(values: &[String], label: &str) -> Result<()> {
    let normalized = values
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect::<Vec<_>>();
    ensure_unique_strings(&normalized, label)
}

fn ensure_readonly_export_query(query: &str) -> Result<()> {
    match SafetyGuard::check(query, false, None) {
        Ok(StatementKind::Read) => Ok(()),
        _ => Err(Error::WriteNotAllowed),
    }
}

fn parse_json_value(raw: &str) -> Result<Value> {
    serde_json::from_str::<serde_json::Value>(raw)
        .map(Value::Json)
        .map_err(|e| Error::Serialization(e.to_string()))
}

fn write_artifact(path: &Path, artifact: TransferArtifact) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|e| Error::Config(e.to_string()))?;
    let bytes = serialize_artifact_bounded(&artifact, MAX_TRANSFER_ARTIFACT_BYTES)?;
    let file_name = path.file_name().ok_or_else(|| {
        Error::Config(format!(
            "artifact output path has no file name: {}",
            path.display()
        ))
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Config(format!("system clock is before the Unix epoch: {e}")))?
        .as_nanos();
    let temp_path = parent.join(format!(
        ".{}.dbtool-tmp-{}-{nonce}",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp_path)
        .map_err(|e| Error::Config(format!("create artifact temp file: {e}")))?;
    if let Err(error) = file.write_all(&bytes).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(&temp_path);
        return Err(Error::Config(format!(
            "persist artifact temp file: {error}"
        )));
    }
    drop(file);
    if let Err(error) = fs::rename(&temp_path, path) {
        let _ = fs::remove_file(&temp_path);
        return Err(Error::Config(format!(
            "publish artifact atomically: {error}"
        )));
    }
    sync_parent_directory(parent)?;
    Ok(())
}

struct BoundedVecWriter {
    bytes: Vec<u8>,
    max_bytes: usize,
}

impl Write for BoundedVecWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let next_len = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("transfer artifact size overflow"))?;
        if next_len > self.max_bytes {
            return Err(std::io::Error::other(format!(
                "transfer artifact exceeds the {}-byte safety limit",
                self.max_bytes
            )));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialize_artifact_bounded(artifact: &TransferArtifact, max_bytes: usize) -> Result<Vec<u8>> {
    let mut writer = BoundedVecWriter {
        bytes: Vec::new(),
        max_bytes,
    };
    serde_json::to_writer_pretty(&mut writer, artifact).map_err(|error| {
        Error::Serialization(format!(
            "serialize transfer artifact within {max_bytes}-byte safety limit: {error}"
        ))
    })?;
    Ok(writer.bytes)
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> Result<()> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::Config(format!("sync artifact parent directory: {error}")))
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> Result<()> {
    Ok(())
}

fn read_artifact(path: &Path) -> Result<TransferArtifact> {
    let bytes = read_file_bounded(path, MAX_TRANSFER_ARTIFACT_BYTES)?;
    let raw: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| Error::Serialization(e.to_string()))?;
    reject_legacy_value_codec_artifact(&raw)?;
    serde_json::from_value(raw).map_err(|e| Error::Serialization(e.to_string()))
}

fn read_file_bounded(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
    let max_plus_one = max_bytes
        .checked_add(1)
        .ok_or_else(|| Error::Config("artifact byte limit is too large".into()))?;
    let max_plus_one = u64::try_from(max_plus_one)
        .map_err(|_| Error::Config("artifact byte limit exceeds the u64 range".into()))?;
    let file = fs::File::open(path).map_err(|e| Error::Config(e.to_string()))?;
    let mut bytes = Vec::new();
    file.take(max_plus_one)
        .read_to_end(&mut bytes)
        .map_err(|e| Error::Config(format!("read transfer artifact: {e}")))?;
    ensure_artifact_size(bytes.len(), max_bytes)?;
    Ok(bytes)
}

fn ensure_artifact_size(actual_bytes: usize, max_bytes: usize) -> Result<()> {
    if actual_bytes <= max_bytes {
        return Ok(());
    }
    Err(Error::Config(format!(
        "transfer artifact is {actual_bytes} bytes, exceeding the {max_bytes}-byte safety limit; reduce --limit or split the transfer"
    )))
}

fn require_version(kind: &str, version: u32, expected: u32) -> Result<()> {
    if version == expected {
        Ok(())
    } else {
        Err(Error::Serialization(format!(
            "unsupported {kind} transfer artifact version: {version}; expected {expected}"
        )))
    }
}

fn reject_legacy_value_codec_artifact(raw: &serde_json::Value) -> Result<()> {
    let Some(kind) = raw.get("kind").and_then(serde_json::Value::as_str) else {
        return Ok(());
    };
    let Some(version) = raw.get("version").and_then(serde_json::Value::as_u64) else {
        return Ok(());
    };

    match kind {
        "sql-rows" if version < u64::from(SQL_TRANSFER_VERSION) => {
            return Err(legacy_value_codec_error(kind, version));
        }
        "documents" if version < 2 => {
            return Err(legacy_value_codec_error(kind, version));
        }
        "documents" if version < u64::from(DOCUMENT_TRANSFER_VERSION) => {
            return Err(legacy_transfer_integrity_error(kind, version));
        }
        "kv-pairs" if version < u64::from(KV_TRANSFER_VERSION) => {
            return Err(if version == 2 {
                legacy_kv_expiry_error(version)
            } else {
                legacy_transfer_integrity_error(kind, version)
            });
        }
        _ => {}
    }
    Ok(())
}

fn legacy_value_codec_error(kind: &str, version: u64) -> Error {
    Error::Serialization(format!(
        "refusing to import legacy {kind} v{version}: its untagged Value representation cannot distinguish bytes, timestamps, arrays, maps, and JSON; re-export from the source with a dbtool version that writes {}",
        Value::WIRE_CODEC
    ))
}

fn legacy_transfer_integrity_error(kind: &str, version: u64) -> Error {
    Error::Serialization(format!(
        "refusing to import legacy {kind} v{version}: it lacks required typed-value, source, completeness, truncation, and source-change metadata; re-export from the source"
    ))
}

fn legacy_kv_expiry_error(version: u64) -> Error {
    Error::Serialization(format!(
        "refusing to import legacy kv-pairs v{version}: it has no per-key expiry, so dbtool cannot distinguish persistent keys from expiring keys or restore their lifetime without loss; re-export from the source as kv-pairs v{KV_TRANSFER_VERSION}"
    ))
}

fn require_complete_sql_artifact(
    version: u32,
    truncated: Option<bool>,
    _accept_legacy_unmarked: bool,
) -> Result<()> {
    match version {
        SQL_TRANSFER_VERSION => {
            let truncated = truncated.ok_or_else(|| {
                Error::Serialization(
                    format!(
                        "sql-rows v{SQL_TRANSFER_VERSION} artifact is missing the required truncated integrity marker"
                    ),
                )
            })?;
            reject_truncated_sql_artifact(truncated)
        }
        1 | 2 => Err(legacy_value_codec_error("sql-rows", u64::from(version))),
        version => Err(Error::Serialization(format!(
            "unsupported sql-rows transfer artifact version: {version}"
        ))),
    }
}

fn reject_truncated_sql_artifact(truncated: bool) -> Result<()> {
    if truncated {
        Err(Error::Serialization(
            "refusing to import a truncated sql-rows artifact; rerun the export with a sufficient --limit"
                .to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

fn prepare_kv_entries(
    entries: Vec<KvEntry>,
    strip_prefix: Option<&str>,
    key_prefix: &str,
) -> Result<Vec<PreparedKvEntry>> {
    let mut prepared = Vec::with_capacity(entries.len());
    let mut target_keys = BTreeSet::new();
    for entry in entries {
        let key = restore_key(&entry.key, strip_prefix, key_prefix)?;
        if !target_keys.insert(key.clone()) {
            return Err(Error::Serialization(format!(
                "multiple artifact entries map to target key '{key}'"
            )));
        }
        let Value::Bytes(value) = entry.value else {
            return Err(Error::Serialization(format!(
                "kv-pairs entry '{}' must use the typed Value::Bytes wire representation",
                entry.key
            )));
        };
        prepared.push(PreparedKvEntry {
            key,
            value,
            expiry: entry.expiry,
        });
    }
    Ok(prepared)
}

fn kv_replace_confirmation_scope(entries: &[PreparedKvEntry]) -> Result<String> {
    SafetyGuard::confirmation_scope_digest(entries)
}

fn ensure_unique_document_ids(documents: &[Document]) -> Result<()> {
    let mut ids = BTreeSet::new();
    for document in documents {
        let Some(id) = document.get("_id") else {
            continue;
        };
        // Compare the backend-facing plain shape rather than the persistence
        // wrapper. This intentionally treats typed/scalar aliases such as
        // Value::Int(1) and Value::Json(json!(1)) as the same target identity.
        let encoded = serde_json::to_string(&canonical_document_id(id)?)
            .map_err(|e| Error::Serialization(format!("invalid document _id: {e}")))?;
        if !ids.insert(encoded) {
            return Err(Error::Serialization(
                "documents artifact contains duplicate _id values".into(),
            ));
        }
    }
    Ok(())
}

fn canonical_document_id(id: &Value) -> Result<serde_json::Value> {
    Ok(match id {
        Value::Null => serde_json::json!({"type": "null"}),
        Value::Bool(value) => serde_json::json!({"type": "bool", "value": value}),
        Value::Int(value) => canonical_number(*value as f64, Some(*value)),
        Value::Float(value) => canonical_number(*value, None),
        Value::Text(value) => serde_json::json!({"type": "string", "value": value}),
        Value::Bytes(value) => {
            serde_json::json!({"type": "binary", "value": bytes_to_hex(value)})
        }
        Value::Timestamp(value) => serde_json::json!({"type": "date", "value": value}),
        Value::Json(value) => canonical_json_document_id(value)?,
        Value::Array(values) => serde_json::json!({
            "type": "array",
            "value": values
                .iter()
                .map(canonical_document_id)
                .collect::<Result<Vec<_>>>()?
        }),
        Value::Map(values) => serde_json::json!({
            "type": "document",
            "value": values
                .iter()
                .map(|(key, value)| Ok((key.clone(), canonical_document_id(value)?)))
                .collect::<Result<BTreeMap<_, _>>>()?
        }),
    })
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

fn canonical_json_document_id(value: &serde_json::Value) -> Result<serde_json::Value> {
    Ok(match value {
        serde_json::Value::Null => serde_json::json!({"type": "null"}),
        serde_json::Value::Bool(value) => serde_json::json!({"type": "bool", "value": value}),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                canonical_number(integer as f64, Some(integer))
            } else if let Some(unsigned) = value.as_u64() {
                if let Ok(integer) = i64::try_from(unsigned) {
                    canonical_number(integer as f64, Some(integer))
                } else {
                    serde_json::json!({"type": "number", "value": value.to_string()})
                }
            } else {
                canonical_number(
                    value.as_f64().ok_or_else(|| {
                        Error::Serialization("document _id contains an invalid number".into())
                    })?,
                    None,
                )
            }
        }
        serde_json::Value::String(value) => {
            serde_json::json!({"type": "string", "value": value})
        }
        serde_json::Value::Array(values) => serde_json::json!({
            "type": "array",
            "value": values
                .iter()
                .map(canonical_json_document_id)
                .collect::<Result<Vec<_>>>()?
        }),
        serde_json::Value::Object(values) => serde_json::json!({
            "type": "document",
            "value": values
                .iter()
                .map(|(key, value)| Ok((key.clone(), canonical_json_document_id(value)?)))
                .collect::<Result<BTreeMap<_, _>>>()?
        }),
    })
}

fn canonical_number(value: f64, exact_integer: Option<i64>) -> serde_json::Value {
    if let Some(integer) = exact_integer {
        return serde_json::json!({"type": "number", "value": integer.to_string()});
    }
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64
    {
        serde_json::json!({"type": "number", "value": (value as i64).to_string()})
    } else {
        serde_json::json!({"type": "number", "value": value.to_string()})
    }
}

async fn ensure_document_ids_are_available(
    store: &dyn dbtool_core::port::capability::DocumentStore,
    collection: &str,
    documents: &[Document],
) -> Result<()> {
    for document in documents {
        let Some(id) = document.get("_id") else {
            continue;
        };
        let filter = Value::Map(BTreeMap::from([("_id".to_owned(), id.clone())]));
        let matches = store
            .find(
                collection,
                filter,
                FindOptions {
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .await?;
        if !matches.is_empty() {
            return Err(Error::Config(
                "target collection already contains an exported _id; choose an empty target or retry with --drop-id"
                    .into(),
            ));
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::service::formatter::Format;

    fn source(resource: &str, selector: Value) -> ArtifactSource {
        ArtifactSource {
            connector: "test".to_owned(),
            connection: "conn:test".to_owned(),
            resource: resource.to_owned(),
            selector,
        }
    }

    fn complete_integrity(items: usize) -> ArtifactIntegrity {
        let items = u64::try_from(items).unwrap();
        artifact_integrity(100, items, items, false, false)
    }

    fn kv_import_action() -> ImportAction {
        ImportAction::Kv {
            input: PathBuf::from("fixture.json"),
            strip_prefix: None,
            key_prefix: String::new(),
            replace_existing: false,
        }
    }

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
    fn sql_import_validates_identifiers_without_rendering_value_literals() {
        assert_eq!(
            validate_identifier_path("main.people", "table").unwrap(),
            "main.people"
        );
        assert!(validate_identifier_path("bad-name", "table").is_err());
    }

    #[test]
    fn legacy_value_codec_artifacts_fail_closed_with_reexport_guidance() {
        let sql = serde_json::json!({
            "kind": "sql-rows",
            "version": 2,
            "columns": ["id"],
            "rows": [[1]]
        });
        assert!(matches!(
            reject_legacy_value_codec_artifact(&sql),
            Err(Error::Serialization(message))
                if message.contains("untagged Value")
                    && message.contains("re-export")
                    && message.contains(Value::WIRE_CODEC)
        ));
        assert!(matches!(
            require_complete_sql_artifact(2, Some(false), true),
            Err(Error::Serialization(message)) if message.contains("untagged Value")
        ));

        let documents = serde_json::json!({
            "kind": "documents",
            "version": 1,
            "documents": [{"id": 1}]
        });
        assert!(matches!(
            reject_legacy_value_codec_artifact(&documents),
            Err(Error::Serialization(message)) if message.contains("legacy documents v1")
        ));

        let kv = serde_json::json!({
            "kind": "kv-pairs",
            "version": 1,
            "entries": [{"key": "a", "value": [0, 255]}]
        });
        assert!(matches!(
            reject_legacy_value_codec_artifact(&kv),
            Err(Error::Serialization(message))
                if message.contains("legacy kv-pairs v1")
                    && message.contains("completeness")
                    && message.contains("re-export")
        ));

        let kv_v2 = serde_json::json!({
            "kind": "kv-pairs",
            "version": 2,
            "source": {},
            "integrity": {},
            "entries": []
        });
        assert!(matches!(
            reject_legacy_value_codec_artifact(&kv_v2),
            Err(Error::Serialization(message))
                if message.contains("legacy kv-pairs v2")
                    && message.contains("per-key expiry")
                    && message.contains("persistent")
                    && message.contains("re-export")
        ));
    }

    #[test]
    fn current_sql_artifact_uses_value_wire_codec_v2_and_round_trips() {
        let expected_rows = vec![vec![
            Value::Timestamp(1_700_000_000_123),
            Value::Bytes(vec![0, 255]),
            Value::Array(vec![Value::Int(1)]),
        ]];
        let artifact = TransferArtifact::SqlRows {
            version: SQL_TRANSFER_VERSION,
            columns: vec![
                "created_at".to_owned(),
                "payload".to_owned(),
                "items".to_owned(),
            ],
            rows: expected_rows.clone(),
            truncated: Some(false),
        };

        let encoded = serde_json::to_value(&artifact).unwrap();
        assert_eq!(encoded["version"], SQL_TRANSFER_VERSION);
        assert_eq!(encoded["rows"][0][0]["$dbtool"]["codec"], Value::WIRE_CODEC);
        assert_eq!(encoded["rows"][0][0]["$dbtool"]["type"], "timestamp");
        assert_eq!(encoded["rows"][0][1]["$dbtool"]["value"], "AP8=");

        let decoded: TransferArtifact = serde_json::from_value(encoded).unwrap();
        let TransferArtifact::SqlRows { rows, .. } = decoded else {
            panic!("expected sql-rows artifact");
        };
        assert_eq!(rows, expected_rows);
    }

    #[test]
    fn current_document_artifact_versions_the_value_wire_codec() {
        let expected_documents = vec![std::collections::BTreeMap::from([
            ("name".to_owned(), Value::Text("alice".to_owned())),
            (
                "metadata".to_owned(),
                Value::Map(std::collections::BTreeMap::from([(
                    "payload".to_owned(),
                    Value::Bytes(vec![1, 2, 3]),
                )])),
            ),
        ])];
        let artifact = TransferArtifact::Documents {
            version: DOCUMENT_TRANSFER_VERSION,
            source: source("users", Value::Json(serde_json::json!({}))),
            integrity: complete_integrity(expected_documents.len()),
            collection: "users".to_owned(),
            documents: expected_documents.clone(),
        };

        let encoded = serde_json::to_value(&artifact).unwrap();
        assert_eq!(encoded["version"], DOCUMENT_TRANSFER_VERSION);
        assert_eq!(
            encoded["documents"][0]["metadata"]["$dbtool"]["type"],
            "map"
        );

        let decoded: TransferArtifact = serde_json::from_value(encoded).unwrap();
        let TransferArtifact::Documents { documents, .. } = decoded else {
            panic!("expected documents artifact");
        };
        assert_eq!(documents, expected_documents);
    }

    #[test]
    fn current_kv_artifact_uses_typed_binary_values_and_round_trips() {
        let entries = vec![KvEntry {
            key: "raw".to_owned(),
            value: Value::Bytes(vec![0, 255]),
            expiry: KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123),
        }];
        let artifact = TransferArtifact::KvPairs {
            version: KV_TRANSFER_VERSION,
            source: source("key-pattern", Value::Text("*".to_owned())),
            integrity: complete_integrity(entries.len()),
            entries: entries.clone(),
        };

        let encoded = serde_json::to_value(&artifact).unwrap();
        assert_eq!(encoded["version"], KV_TRANSFER_VERSION);
        assert_eq!(encoded["entries"][0]["value"]["$dbtool"]["type"], "bytes");
        assert_eq!(encoded["entries"][0]["value"]["$dbtool"]["value"], "AP8=");
        assert_eq!(
            encoded["entries"][0]["expiry"],
            serde_json::json!({
                "kind": "expires-at-unix-ms",
                "unix_ms": 1_710_000_000_123_i64
            })
        );
        assert_eq!(encoded["integrity"]["complete"], true);
        assert_eq!(encoded["integrity"]["consistency"], "best-effort");

        let decoded: TransferArtifact = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, artifact);
        validate_import_artifact(&kv_import_action(), &decoded).unwrap();
    }

    #[test]
    fn incomplete_kv_and_document_artifacts_are_never_importable() {
        let kv = TransferArtifact::KvPairs {
            version: KV_TRANSFER_VERSION,
            source: source("key-pattern", Value::Text("*".to_owned())),
            integrity: artifact_integrity(1, 1, 1, true, false),
            entries: vec![KvEntry {
                key: "a".to_owned(),
                value: Value::Bytes(vec![1]),
                expiry: KeyExpiry::Persistent,
            }],
        };
        assert!(matches!(
            validate_import_artifact(&kv_import_action(), &kv),
            Err(Error::Serialization(message))
                if message.contains("incomplete kv-pairs") && message.contains("hit its limit")
        ));

        let changed_kv = TransferArtifact::KvPairs {
            version: KV_TRANSFER_VERSION,
            source: source("key-pattern", Value::Text("*".to_owned())),
            integrity: artifact_integrity(2, 1, 2, false, true),
            entries: vec![KvEntry {
                key: "survived".to_owned(),
                value: Value::Bytes(vec![1]),
                expiry: KeyExpiry::Persistent,
            }],
        };
        assert!(matches!(
            validate_import_artifact(&kv_import_action(), &changed_kv),
            Err(Error::Serialization(message))
                if message.contains("incomplete kv-pairs") && message.contains("source changed")
        ));

        let documents = TransferArtifact::Documents {
            version: DOCUMENT_TRANSFER_VERSION,
            source: source("users", Value::Json(serde_json::json!({}))),
            integrity: artifact_integrity(2, 1, 2, false, true),
            collection: "users".to_owned(),
            documents: vec![Document::new()],
        };
        let action = ImportAction::Doc {
            collection: "users-copy".to_owned(),
            input: PathBuf::from("fixture.json"),
            drop_id: false,
        };
        assert!(matches!(
            validate_import_artifact(&action, &documents),
            Err(Error::Serialization(message))
                if message.contains("incomplete documents") && message.contains("source changed")
        ));
    }

    #[test]
    fn forged_integrity_counts_and_markers_fail_closed() {
        let mut integrity = complete_integrity(1);
        integrity.exported_items = 2;
        assert!(matches!(
            validate_integrity("kv-pairs", &integrity, 1),
            Err(Error::Serialization(message)) if message.contains("count mismatch")
        ));

        let mut integrity = complete_integrity(1);
        integrity.complete = false;
        assert!(matches!(
            validate_integrity("documents", &integrity, 1),
            Err(Error::Serialization(message)) if message.contains("complete marker contradicts")
        ));

        let mut integrity = complete_integrity(1);
        integrity.value_codec = "unknown".to_owned();
        assert!(matches!(
            validate_integrity("documents", &integrity, 1),
            Err(Error::Serialization(message)) if message.contains("unsupported documents value codec")
        ));
    }

    #[test]
    fn kv_import_preflight_preserves_bytes_and_rejects_duplicate_targets() {
        let prepared = prepare_kv_entries(
            vec![KvEntry {
                key: "src:a".to_owned(),
                value: Value::Bytes(vec![0, 255]),
                expiry: KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123),
            }],
            Some("src:"),
            "dst:",
        )
        .unwrap();
        assert_eq!(
            prepared,
            vec![PreparedKvEntry {
                key: "dst:a".to_owned(),
                value: vec![0, 255],
                expiry: KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123),
            }]
        );

        assert!(matches!(
            prepare_kv_entries(
                vec![
                    KvEntry {
                        key: "a".to_owned(),
                        value: Value::Bytes(vec![1]),
                        expiry: KeyExpiry::Persistent,
                    },
                    KvEntry {
                        key: "a".to_owned(),
                        value: Value::Bytes(vec![2]),
                        expiry: KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123),
                    },
                ],
                None,
                "",
            ),
            Err(Error::Serialization(message)) if message.contains("target key 'a'")
        ));
    }

    #[test]
    fn duplicate_document_ids_fail_before_backend_writes() {
        let documents = vec![
            Document::from([("_id".to_owned(), Value::Int(7))]),
            Document::from([("_id".to_owned(), Value::Json(serde_json::json!(7)))]),
        ];
        assert!(matches!(
            ensure_unique_document_ids(&documents),
            Err(Error::Serialization(message)) if message.contains("duplicate _id")
        ));

        let typed_identities = vec![
            Document::from([("_id".to_owned(), Value::Timestamp(7))]),
            Document::from([("_id".to_owned(), Value::Int(7))]),
            Document::from([("_id".to_owned(), Value::Bytes(vec![7]))]),
            Document::from([("_id".to_owned(), Value::Array(vec![Value::Int(7)]))]),
        ];
        ensure_unique_document_ids(&typed_identities).unwrap();
    }

    #[test]
    fn import_preparation_is_bounded_and_reuses_offline_results() {
        let artifact = TransferArtifact::KvPairs {
            version: KV_TRANSFER_VERSION,
            source: source("key-pattern", Value::Text("src:*".to_owned())),
            integrity: complete_integrity(1),
            entries: vec![KvEntry {
                key: "src:a".to_owned(),
                value: Value::Bytes(vec![0, 255]),
                expiry: KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123),
            }],
        };
        let action = ImportAction::Kv {
            input: PathBuf::from("fixture.json"),
            strip_prefix: Some("src:".to_owned()),
            key_prefix: "dst:".to_owned(),
            replace_existing: true,
        };

        let PreparedImport::Kv { entries, .. } =
            prepare_import(action, artifact.clone(), 1).unwrap()
        else {
            panic!("expected prepared KV import");
        };
        assert_eq!(entries[0].key, "dst:a");
        assert_eq!(entries[0].value, vec![0, 255]);
        assert_eq!(
            entries[0].expiry,
            KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123)
        );
        assert!(matches!(
            prepare_import(kv_import_action(), artifact, 0),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
    }

    #[test]
    fn artifact_publish_is_atomic_and_leaves_no_temp_file() {
        let root = std::env::temp_dir().join(format!(
            "dbtool-transfer-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = root.join("values.json");
        let artifact = TransferArtifact::KvPairs {
            version: KV_TRANSFER_VERSION,
            source: source("key-pattern", Value::Text("*".to_owned())),
            integrity: complete_integrity(0),
            entries: vec![],
        };

        write_artifact(&path, artifact.clone()).unwrap();
        assert_eq!(read_artifact(&path).unwrap(), artifact);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn artifact_reads_and_writes_enforce_byte_budget() {
        assert!(ensure_artifact_size(4, 4).is_ok());
        assert!(matches!(
            ensure_artifact_size(5, 4),
            Err(Error::Config(message)) if message.contains("5 bytes") && message.contains("4-byte")
        ));

        let path = std::env::temp_dir().join(format!(
            "dbtool-artifact-byte-budget-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&path, b"12345").unwrap();
        assert!(read_file_bounded(&path, 4).is_err());
        assert_eq!(read_file_bounded(&path, 5).unwrap(), b"12345");
        fs::remove_file(path).unwrap();

        let artifact = TransferArtifact::KvPairs {
            version: KV_TRANSFER_VERSION,
            source: source("key-pattern", Value::Text("*".to_owned())),
            integrity: complete_integrity(0),
            entries: vec![],
        };
        assert!(serialize_artifact_bounded(&artifact, 8).is_err());
        assert!(serialize_artifact_bounded(&artifact, 4096).is_ok());
    }

    #[test]
    fn kv_replace_confirmation_scope_binds_target_keys_values_and_expiry() {
        let first = vec![PreparedKvEntry {
            key: "target".into(),
            value: b"first".to_vec(),
            expiry: KeyExpiry::Persistent,
        }];
        let second = vec![PreparedKvEntry {
            key: "target".into(),
            value: b"second".to_vec(),
            expiry: KeyExpiry::Persistent,
        }];
        let different_key = vec![PreparedKvEntry {
            key: "other-target".into(),
            value: b"first".to_vec(),
            expiry: KeyExpiry::Persistent,
        }];
        let expiring = vec![PreparedKvEntry {
            key: "target".into(),
            value: b"first".to_vec(),
            expiry: KeyExpiry::ExpiresAtUnixMs(1_710_000_000_123),
        }];

        let baseline = kv_replace_confirmation_scope(&first).unwrap();
        assert_ne!(baseline, kv_replace_confirmation_scope(&second).unwrap());
        assert_ne!(
            baseline,
            kv_replace_confirmation_scope(&different_key).unwrap()
        );
        assert_ne!(baseline, kv_replace_confirmation_scope(&expiring).unwrap());
    }

    #[test]
    fn sql_artifact_integrity_marker_is_required_and_truncation_is_rejected() {
        assert!(matches!(
            require_complete_sql_artifact(SQL_TRANSFER_VERSION, None, false),
            Err(Error::Serialization(message)) if message.contains("required truncated")
        ));
        require_complete_sql_artifact(SQL_TRANSFER_VERSION, Some(false), false).unwrap();
        assert!(matches!(
            require_complete_sql_artifact(SQL_TRANSFER_VERSION, Some(true), true),
            Err(Error::Serialization(message)) if message.contains("truncated sql-rows")
        ));
    }

    #[test]
    fn export_query_contract_is_strictly_readonly() {
        assert!(ensure_readonly_export_query("select 1").is_ok());
        assert!(matches!(
            ensure_readonly_export_query("delete from users where id = 1"),
            Err(Error::WriteNotAllowed)
        ));
        assert!(matches!(
            ensure_readonly_export_query("drop table users"),
            Err(Error::WriteNotAllowed)
        ));
    }
}
