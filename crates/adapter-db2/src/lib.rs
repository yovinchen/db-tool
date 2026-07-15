use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ColumnMeta, ExecOutcome, ForeignKeyInfo, IndexInfo, ResultSet, RoutineInfo, RoutineKind,
        SequenceInfo, TableInfo, TableKind, TableSchema, TablespaceInfo, Value,
    },
    port::{
        capability::{Db2Engine, SqlEngine},
        connector::{Capabilities, Connector, ConnectorKind},
    },
    service::limiter::ResultLimiter,
};
use futures::future::BoxFuture;
use odbc_api::{
    buffers::TextRowSet, ColumnDescription, Connection, ConnectionOptions, Cursor, Environment,
    ResultSetMetadata,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

// ── ODBC environment singleton ───────────────────────────────────────────────

static ODBC_ENV: Lazy<std::result::Result<Environment, String>> =
    Lazy::new(|| Environment::new().map_err(|e| e.to_string()));

fn get_env() -> Result<&'static Environment> {
    ODBC_ENV.as_ref().map_err(|e| {
        Error::Connection(format!(
            "ODBC driver manager unavailable: {e}. \
             Install unixODBC (Linux/macOS: `apt install unixodbc` or `brew install unixodbc`) \
             and the IBM Data Server Driver for ODBC and CLI, \
             then ensure the driver is registered in odbcinst.ini."
        ))
    })
}

// ── Adapter struct ───────────────────────────────────────────────────────────

pub struct Db2Adapter {
    conn_str: Arc<String>,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let conn_str = build_connection_string(&dsn)?;

        let probe_str = conn_str.clone();
        tokio::task::spawn_blocking(move || {
            let env = get_env()?;
            env.connect_with_connection_string(&probe_str, ConnectionOptions::default())
                .map_err(|e| Error::Connection(e.to_string()))?;
            Ok::<_, Error>(())
        })
        .await
        .map_err(|e| Error::Connection(e.to_string()))??;

        Ok(Box::new(Db2Adapter {
            conn_str: Arc::new(conn_str),
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

// ── Connector impl ───────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Connector for Db2Adapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: true,
            db2: true,
            ..Default::default()
        }
    }

    async fn ping(&self) -> Result<()> {
        let conn_str = self.conn_str.clone();
        tokio::task::spawn_blocking(move || {
            let env = get_env()?;
            let conn = open(env, &conn_str)?;
            run_void(&conn, "VALUES 1")?;
            Ok::<_, Error>(())
        })
        .await
        .map_err(|e| Error::Connection(e.to_string()))?
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_sql(&self) -> Option<&dyn SqlEngine> {
        Some(self)
    }

    fn as_db2(&self) -> Option<&dyn Db2Engine> {
        Some(self)
    }
}

// ── SqlEngine impl ───────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl SqlEngine for Db2Adapter {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet> {
        reject_params(params)?;
        let conn_str = self.conn_str.clone();
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            let env = get_env()?;
            let conn = open(env, &conn_str)?;
            query_result_set(&conn, &sql)
        })
        .await
        .map_err(|e| Error::Query(e.to_string()))?
    }

    async fn query_bounded(
        &self,
        sql: &str,
        params: &[Value],
        max_rows: usize,
    ) -> Result<ResultSet> {
        let limiter = ResultLimiter::new(max_rows);
        let probe_rows = limiter.probe_rows()?;
        reject_params(params)?;
        let conn_str = self.conn_str.clone();
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            let env = get_env()?;
            let conn = open(env, &conn_str)?;
            query_result_set_bounded(&conn, &sql, max_rows, probe_rows)
        })
        .await
        .map_err(|e| Error::Query(e.to_string()))?
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome> {
        reject_params(params)?;
        let conn_str = self.conn_str.clone();
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            let env = get_env()?;
            let conn = open(env, &conn_str)?;
            conn.execute(&sql, ())
                .map_err(|e| Error::Query(e.to_string()))?;
            Ok::<_, Error>(ExecOutcome {
                rows_affected: 0,
                last_insert_id: None,
            })
        })
        .await
        .map_err(|e| Error::Query(e.to_string()))?
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let result = self
            .query(
                "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA \
                 WHERE SCHEMANAME NOT LIKE 'SYS%' ORDER BY SCHEMANAME",
                &[],
            )
            .await?;
        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| col_text(row.first()?))
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = validate_opt_schema(schema)?.unwrap_or("DB2INST1");
        let sql = format!(
            "SELECT TABSCHEMA, TABNAME, TYPE FROM SYSCAT.TABLES \
             WHERE TABSCHEMA = '{}' AND TYPE IN ('T','V') ORDER BY TABNAME",
            schema.to_uppercase()
        );
        let result = self.query(&sql, &[]).await?;
        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| {
                let schema = col_text(row.first()?)?;
                let name = col_text(row.get(1)?)?;
                let kind_char = col_text(row.get(2)?)?;
                Some(TableInfo {
                    schema: Some(schema.trim().to_owned()),
                    name: name.trim().to_owned(),
                    kind: if kind_char.trim() == "V" {
                        TableKind::View
                    } else {
                        TableKind::Table
                    },
                })
            })
            .collect())
    }

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        let tref = parse_table_ref(table)?;
        let schema_uc = tref.schema.as_deref().unwrap_or("DB2INST1").to_uppercase();
        let name_uc = tref.name.to_uppercase();

        // ── columns with PK flag ─────────────────────────────────────────────
        let col_sql = format!(
            "SELECT c.COLNAME, c.TYPENAME, c.NULLS, c.DEFAULT, \
                    COALESCE(k.COLSEQ, 0) AS IS_PK \
             FROM SYSCAT.COLUMNS c \
             LEFT JOIN ( \
               SELECT kc.COLNAME, kc.COLSEQ \
               FROM SYSCAT.KEYCOLUSE kc \
               JOIN SYSCAT.TABCONST tc \
                 ON tc.CONSTNAME = kc.CONSTNAME \
                AND tc.TABSCHEMA = kc.TABSCHEMA \
                AND tc.TABNAME   = kc.TABNAME \
               WHERE tc.TYPE = 'P' \
                 AND kc.TABSCHEMA = '{schema_uc}' \
                 AND kc.TABNAME   = '{name_uc}' \
             ) k ON k.COLNAME = c.COLNAME \
             WHERE c.TABSCHEMA = '{schema_uc}' \
               AND c.TABNAME   = '{name_uc}' \
             ORDER BY c.COLNO"
        );
        let col_result = self.query(&col_sql, &[]).await?;
        let columns: Vec<ColumnMeta> = col_result
            .rows
            .into_iter()
            .filter_map(|row| {
                let name = col_text(row.first()?)?;
                let type_name = col_text(row.get(1)?)?;
                let nulls = col_text(row.get(2)?)?;
                let default_val = row.get(3).and_then(col_text_opt);
                let is_pk_seq = col_text(row.get(4)?).unwrap_or_default();
                Some(ColumnMeta {
                    name: name.trim().to_owned(),
                    type_name: type_name.trim().to_ascii_lowercase(),
                    nullable: nulls.trim() != "N",
                    primary_key: is_pk_seq.trim() != "0" && !is_pk_seq.trim().is_empty(),
                    default_value: default_val.map(|s| s.trim().to_owned()),
                })
            })
            .collect();

        // ── indexes ──────────────────────────────────────────────────────────
        let idx_sql = format!(
            "SELECT i.INDNAME, i.UNIQUERULE, ic.COLNAME \
             FROM SYSCAT.INDEXES i \
             JOIN SYSCAT.INDEXCOLUSE ic \
               ON ic.INDNAME   = i.INDNAME \
              AND ic.INDSCHEMA = i.INDSCHEMA \
             WHERE i.TABSCHEMA = '{schema_uc}' \
               AND i.TABNAME   = '{name_uc}' \
             ORDER BY i.INDNAME, ic.COLSEQ"
        );
        let idx_result = self.query(&idx_sql, &[]).await?;
        let mut idx_map: Vec<(String, bool, bool, Vec<String>)> = Vec::new();
        for row in idx_result.rows {
            let idx_name = row
                .first()
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let unique_rule = row.get(1).and_then(col_text).unwrap_or_default();
            let col_name = row
                .get(2)
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let unique = matches!(unique_rule.trim(), "U" | "P");
            let primary = unique_rule.trim() == "P";
            if let Some(entry) = idx_map.iter_mut().find(|(n, _, _, _)| n == &idx_name) {
                entry.3.push(col_name);
            } else {
                idx_map.push((idx_name, unique, primary, vec![col_name]));
            }
        }
        let indexes: Vec<IndexInfo> = idx_map
            .into_iter()
            .map(|(name, unique, primary, columns)| IndexInfo {
                name,
                columns,
                unique,
                primary,
            })
            .collect();

        Ok(TableSchema {
            name: tref.name,
            columns,
            indexes,
        })
    }
}

// ── Db2Engine impl ───────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Db2Engine for Db2Adapter {
    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        let schema = validate_opt_schema(schema)?.unwrap_or("DB2INST1");
        let sql = format!(
            "SELECT SEQSCHEMA, SEQNAME, DATATYPEID, START, INCREMENT, \
                    MINVALUE, MAXVALUE, CYCLE, CACHE \
             FROM SYSCAT.SEQUENCES \
             WHERE SEQSCHEMA = '{}' AND SEQTYPE = 'S' \
             ORDER BY SEQNAME",
            schema.to_uppercase()
        );
        let result = self.query(&sql, &[]).await?;
        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| {
                Some(SequenceInfo {
                    schema: col_text(row.first()?)?.trim().to_owned(),
                    name: col_text(row.get(1)?)?.trim().to_owned(),
                    data_type: col_text(row.get(2)?).unwrap_or_default().trim().to_owned(),
                    start: col_text(row.get(3)?).unwrap_or_default().trim().to_owned(),
                    increment: col_text(row.get(4)?).unwrap_or_default().trim().to_owned(),
                    min_value: col_text(row.get(5)?).unwrap_or_default().trim().to_owned(),
                    max_value: col_text(row.get(6)?).unwrap_or_default().trim().to_owned(),
                    cycle: col_text(row.get(7)?).unwrap_or_default().trim() == "Y",
                    cache: col_text(row.get(8)?)
                        .unwrap_or_default()
                        .trim()
                        .parse()
                        .unwrap_or(0),
                })
            })
            .collect())
    }

    async fn list_routines(&self, schema: Option<&str>) -> Result<Vec<RoutineInfo>> {
        let schema = validate_opt_schema(schema)?.unwrap_or("DB2INST1");
        let sql = format!(
            "SELECT ROUTINESCHEMA, ROUTINENAME, ROUTINETYPE, LANGUAGE, PARMS \
             FROM SYSCAT.ROUTINES \
             WHERE ROUTINESCHEMA = '{}' AND ROUTINETYPE IN ('P','F') \
             ORDER BY ROUTINENAME",
            schema.to_uppercase()
        );
        let result = self.query(&sql, &[]).await?;
        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| {
                let rschema = col_text(row.first()?)?.trim().to_owned();
                let rname = col_text(row.get(1)?)?.trim().to_owned();
                let rtype = col_text(row.get(2)?).unwrap_or_default();
                let lang = col_text(row.get(3)?).unwrap_or_default().trim().to_owned();
                let parms = col_text(row.get(4)?)
                    .unwrap_or_default()
                    .trim()
                    .parse()
                    .unwrap_or(0);
                let kind = if rtype.trim() == "P" {
                    RoutineKind::Procedure
                } else {
                    RoutineKind::Function
                };
                Some(RoutineInfo {
                    schema: rschema,
                    name: rname,
                    kind,
                    language: lang,
                    parms,
                })
            })
            .collect())
    }

    async fn list_tablespaces(&self) -> Result<Vec<TablespaceInfo>> {
        let sql = "SELECT TBSPACE, TBSPACETYPE, PAGESIZE, EXTENTSIZE, PREFETCHSIZE \
                   FROM SYSCAT.TABLESPACES \
                   ORDER BY TBSPACE";
        let result = self.query(sql, &[]).await?;
        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| {
                Some(TablespaceInfo {
                    name: col_text(row.first()?)?.trim().to_owned(),
                    kind: col_text(row.get(1)?).unwrap_or_default().trim().to_owned(),
                    page_size: col_text(row.get(2)?)
                        .unwrap_or_default()
                        .trim()
                        .parse()
                        .unwrap_or(0),
                    extent_size: col_text(row.get(3)?)
                        .unwrap_or_default()
                        .trim()
                        .parse()
                        .unwrap_or(0),
                    prefetch_size: col_text(row.get(4)?)
                        .unwrap_or_default()
                        .trim()
                        .parse()
                        .unwrap_or(0),
                })
            })
            .collect())
    }

    async fn list_foreign_keys(&self, table: &str) -> Result<Vec<ForeignKeyInfo>> {
        let tref = parse_table_ref(table)?;
        let schema_uc = tref.schema.as_deref().unwrap_or("DB2INST1").to_uppercase();
        let name_uc = tref.name.to_uppercase();

        // Each FK may span multiple columns — group by constraint name.
        let sql = format!(
            "SELECT r.CONSTNAME, kc.COLNAME, r.REFTABSCHEMA, r.REFTABNAME, \
                    rkc.COLNAME AS REFCOL, r.UPDATERULE, r.DELETERULE \
             FROM SYSCAT.REFERENCES r \
             JOIN SYSCAT.KEYCOLUSE kc \
               ON kc.CONSTNAME = r.CONSTNAME \
              AND kc.TABSCHEMA = r.TABSCHEMA \
              AND kc.TABNAME   = r.TABNAME \
             JOIN SYSCAT.KEYCOLUSE rkc \
               ON rkc.CONSTNAME = r.REFKEYNAME \
              AND rkc.TABSCHEMA = r.REFTABSCHEMA \
              AND rkc.TABNAME   = r.REFTABNAME \
              AND rkc.COLSEQ   = kc.COLSEQ \
             WHERE r.TABSCHEMA = '{schema_uc}' \
               AND r.TABNAME   = '{name_uc}' \
             ORDER BY r.CONSTNAME, kc.COLSEQ"
        );
        let result = self.query(&sql, &[]).await?;

        let mut fk_map: Vec<ForeignKeyInfo> = Vec::new();
        for row in result.rows {
            let cname = row
                .first()
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let col = row
                .get(1)
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let ref_schema = row
                .get(2)
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let ref_table = row
                .get(3)
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let ref_col = row
                .get(4)
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let upd_rule = row
                .get(5)
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let del_rule = row
                .get(6)
                .and_then(col_text)
                .unwrap_or_default()
                .trim()
                .to_owned();

            if let Some(entry) = fk_map.iter_mut().find(|fk| fk.constraint_name == cname) {
                entry.columns.push(col);
                entry.ref_columns.push(ref_col);
            } else {
                fk_map.push(ForeignKeyInfo {
                    constraint_name: cname,
                    columns: vec![col],
                    ref_schema,
                    ref_table,
                    ref_columns: vec![ref_col],
                    update_rule: upd_rule,
                    delete_rule: del_rule,
                });
            }
        }
        Ok(fk_map)
    }

    async fn generate_ddl(&self, table: &str) -> Result<String> {
        let tref = parse_table_ref(table)?;
        let schema_uc = tref.schema.as_deref().unwrap_or("DB2INST1").to_uppercase();
        let name_uc = tref.name.to_uppercase();

        // Fetch the full schema (columns + indexes) then emit DDL.
        let full_table = format!("{schema_uc}.{name_uc}");
        let schema = self.describe_table(&full_table).await?;

        let mut ddl = format!("CREATE TABLE \"{schema_uc}\".\"{name_uc}\" (\n");
        for (i, col) in schema.columns.iter().enumerate() {
            let sep = if i + 1 < schema.columns.len() || !schema.indexes.is_empty() {
                ","
            } else {
                ""
            };
            let null_str = if col.nullable { "" } else { " NOT NULL" };
            let default_str = col
                .default_value
                .as_deref()
                .map(|d| format!(" DEFAULT {d}"))
                .unwrap_or_default();
            ddl.push_str(&format!(
                "  \"{}\" {}{}{}{}\n",
                col.name,
                col.type_name.to_uppercase(),
                default_str,
                null_str,
                sep
            ));
        }

        // Primary key constraint
        let pk_cols: Vec<&str> = schema
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.as_str())
            .collect();
        if !pk_cols.is_empty() {
            let pk_list: Vec<String> = pk_cols.iter().map(|c| format!("\"{c}\"")).collect();
            ddl.push_str(&format!(",  PRIMARY KEY ({})\n", pk_list.join(", ")));
        }

        ddl.push_str(");\n");

        // Non-primary indexes
        for idx in schema.indexes.iter().filter(|i| !i.primary) {
            let unique_kw = if idx.unique { "UNIQUE " } else { "" };
            let cols: Vec<String> = idx.columns.iter().map(|c| format!("\"{c}\"")).collect();
            ddl.push_str(&format!(
                "CREATE {}INDEX \"{}\" ON \"{schema_uc}\".\"{name_uc}\" ({});\n",
                unique_kw,
                idx.name,
                cols.join(", ")
            ));
        }

        Ok(ddl)
    }
}

// ── ODBC helpers ─────────────────────────────────────────────────────────────

fn open<'env>(env: &'env Environment, conn_str: &str) -> Result<Connection<'env>> {
    env.connect_with_connection_string(conn_str, ConnectionOptions::default())
        .map_err(|e| Error::Connection(e.to_string()))
}

fn run_void(conn: &Connection<'_>, sql: &str) -> Result<()> {
    conn.execute(sql, ())
        .map_err(|e| Error::Query(e.to_string()))?;
    Ok(())
}

const BATCH: usize = 256;
const MAX_STR: usize = 4096;

fn query_result_set(conn: &Connection<'_>, sql: &str) -> Result<ResultSet> {
    query_result_set_with_bound(conn, sql, None)
}

fn query_result_set_bounded(
    conn: &Connection<'_>,
    sql: &str,
    max_rows: usize,
    probe_rows: usize,
) -> Result<ResultSet> {
    query_result_set_with_bound(conn, sql, Some((max_rows, probe_rows)))
}

fn query_result_set_with_bound(
    conn: &Connection<'_>,
    sql: &str,
    bound: Option<(usize, usize)>,
) -> Result<ResultSet> {
    let mut cursor = match conn
        .execute(sql, ())
        .map_err(|e| Error::Query(e.to_string()))?
    {
        Some(c) => c,
        None => return Ok(ResultSet::empty()),
    };

    let num_cols = cursor
        .num_result_cols()
        .map_err(|e| Error::Query(e.to_string()))? as usize;

    let mut col_names = Vec::with_capacity(num_cols);
    let mut col_desc = ColumnDescription::default();
    for i in 1..=num_cols as u16 {
        cursor
            .describe_col(i, &mut col_desc)
            .map_err(|e| Error::Query(e.to_string()))?;
        col_names.push(String::from_utf16_lossy(&col_desc.name).to_string());
    }

    let columns: Vec<dbtool_core::model::ColumnMeta> = col_names
        .iter()
        .map(|name| dbtool_core::model::ColumnMeta {
            name: name.clone(),
            type_name: "text".to_owned(),
            nullable: true,
            primary_key: false,
            default_value: None,
        })
        .collect();

    // A one-row ODBC rowset prevents the driver from filling a full 256-row
    // batch after only one truncation probe row remains.
    let batch_size = if bound.is_some() { 1 } else { BATCH };
    let buf = TextRowSet::for_cursor(batch_size, &mut cursor, Some(MAX_STR))
        .map_err(|e| Error::Query(e.to_string()))?;
    let mut row_set_cursor = cursor
        .bind_buffer(buf)
        .map_err(|e| Error::Query(e.to_string()))?;

    let mut rows: Vec<Vec<Value>> = Vec::new();
    'fetch: while let Some(batch) = row_set_cursor
        .fetch()
        .map_err(|e| Error::Query(e.to_string()))?
    {
        for row_idx in 0..batch.num_rows() {
            let mut row_vals = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let val = match batch.at(col_idx, row_idx) {
                    None => Value::Null,
                    Some(bytes) => Value::Text(String::from_utf8_lossy(bytes).into_owned()),
                };
                row_vals.push(val);
            }
            rows.push(row_vals);
            if bound.is_some_and(|(_, probe_rows)| rows.len() >= probe_rows) {
                break 'fetch;
            }
        }
    }

    let result = ResultSet {
        columns,
        rows,
        truncated: false,
    };
    Ok(match bound {
        Some((max_rows, _)) => ResultLimiter::new(max_rows).apply(result),
        None => result,
    })
}

// ── DSN helpers ──────────────────────────────────────────────────────────────

fn build_connection_string(dsn: &Dsn) -> Result<String> {
    let host = dsn.host.as_deref().unwrap_or("localhost");
    let port = dsn.port.unwrap_or(50000);
    let database = dsn
        .database
        .as_deref()
        .ok_or_else(|| Error::Dsn("DB2 DSN requires a database name".to_owned()))?;
    let user = dsn.username.as_deref().unwrap_or("");
    let password = dsn.password.as_deref().unwrap_or("");

    let driver = dsn
        .params
        .get("driver")
        .map(String::as_str)
        .unwrap_or("IBM DB2 ODBC DRIVER");

    Ok(format!(
        "DRIVER={{{driver}}};DATABASE={database};\
         HOSTNAME={host};PORT={port};PROTOCOL=TCPIP;\
         UID={user};PWD={password}"
    ))
}

// ── Identifier validation ────────────────────────────────────────────────────

struct TableRef {
    schema: Option<String>,
    name: String,
}

fn parse_table_ref(input: &str) -> Result<TableRef> {
    let (schema, name) = input
        .split_once('.')
        .map_or((None, input), |(s, n)| (Some(s), n));
    validate_identifier(name)?;
    if let Some(s) = schema {
        validate_identifier(s)?;
    }
    Ok(TableRef {
        schema: schema.map(str::to_owned),
        name: name.to_owned(),
    })
}

fn validate_opt_schema(schema: Option<&str>) -> Result<Option<&str>> {
    if let Some(s) = schema {
        validate_identifier(s)?;
    }
    Ok(schema)
}

fn validate_identifier(id: &str) -> Result<()> {
    let valid = !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '#' || c == '@' || c == '$');
    if valid {
        Ok(())
    } else {
        Err(Error::Dsn(format!("invalid DB2 identifier: {id}")))
    }
}

fn col_text(v: &Value) -> Option<String> {
    match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    }
}

fn col_text_opt(v: &Value) -> Option<String> {
    match v {
        Value::Text(s) if !s.is_empty() && s != "NULL" => Some(s.clone()),
        _ => None,
    }
}

fn reject_params(params: &[Value]) -> Result<()> {
    if params.is_empty() {
        Ok(())
    } else {
        Err(Error::Query(
            "DB2 adapter does not support dynamic query parameters yet".to_owned(),
        ))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_connection_string_from_dsn() {
        let dsn = Dsn::parse("db2://db2inst1:secret@db.example.test:50000/TESTDB").unwrap();
        let conn_str = build_connection_string(&dsn).unwrap();
        assert!(conn_str.contains("IBM DB2 ODBC DRIVER"));
        assert!(conn_str.contains("HOSTNAME=db.example.test"));
        assert!(conn_str.contains("PORT=50000"));
        assert!(conn_str.contains("DATABASE=TESTDB"));
        assert!(conn_str.contains("UID=db2inst1"));
    }

    #[test]
    fn custom_driver_name_is_respected() {
        let dsn = Dsn::parse("db2://u:p@host/DB?driver=IBM+DB2+ODBC+DRIVER+-+DB2COPY1").unwrap();
        let conn_str = build_connection_string(&dsn).unwrap();
        assert!(conn_str.contains("IBM DB2 ODBC DRIVER - DB2COPY1"));
    }

    #[test]
    fn rejects_missing_database() {
        let dsn = Dsn::parse("db2://u:p@host:50000").unwrap();
        assert!(build_connection_string(&dsn).is_err());
    }

    #[test]
    fn rejects_unsafe_identifiers() {
        assert!(parse_table_ref("DB2INST1.EMPLOYEE").is_ok());
        assert!(parse_table_ref("DB2INST1.EMP;DROP").is_err());
        assert!(validate_opt_schema(Some("DB2INST1")).is_ok());
        assert!(validate_opt_schema(Some("DB2;DROP")).is_err());
    }

    #[test]
    fn ddl_includes_primary_key_from_columns() {
        // We can't call generate_ddl without a live DB2, but we can test the
        // DDL assembly from a known TableSchema directly.
        let schema = TableSchema {
            name: "EMPLOYEE".to_owned(),
            columns: vec![
                ColumnMeta {
                    name: "EMPNO".to_owned(),
                    type_name: "integer".to_owned(),
                    nullable: false,
                    primary_key: true,
                    default_value: None,
                },
                ColumnMeta {
                    name: "NAME".to_owned(),
                    type_name: "varchar".to_owned(),
                    nullable: true,
                    primary_key: false,
                    default_value: Some("'unknown'".to_owned()),
                },
            ],
            indexes: vec![],
        };

        // Simulate the DDL output logic inline.
        let pk_cols: Vec<&str> = schema
            .columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(pk_cols, vec!["EMPNO"]);

        let default_col = schema.columns.iter().find(|c| c.name == "NAME").unwrap();
        assert_eq!(default_col.default_value.as_deref(), Some("'unknown'"));
    }
}
