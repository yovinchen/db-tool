use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        BoundedList, ColumnMeta, ExecOutcome, ForeignKeyInfo, IndexInfo, MetadataBudget, ResultSet,
        RoutineInfo, RoutineKind, SequenceInfo, TableInfo, TableKind, TableSchema, TablespaceInfo,
        Value, DEFAULT_METADATA_BYTES,
    },
    port::{
        capability::{Db2Engine, SqlEngine},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{ListLimiter, MetadataLimiter, ResultLimiter},
};
use futures::future::BoxFuture;
use odbc_api::{
    buffers::TextRowSet, ColumnDescription, Connection, ConnectionOptions, Cursor, Environment,
    ResultSetMetadata,
};
use once_cell::sync::Lazy;
use std::sync::Arc;

const LEGACY_SCHEMA_MAX_ITEMS: usize = 100_000;
const DB2_CATALOG_TEXT_MAX_OCTETS: i64 = 4096;
// Db2 rejects a foreign key with more than 64 referencing columns. Bounding
// both selected constraint identities and this per-constraint product keeps
// the joined member query finite without splitting a valid composite key.
const DB2_MAX_FOREIGN_KEY_COLUMNS: i64 = 64;

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

#[derive(Debug)]
struct Db2ColumnDefinition {
    meta: ColumnMeta,
    primary_sequence: i32,
    generated_clause: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Db2ObjectKind {
    Table,
    View,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Db2IndexColumnOrder {
    Ascending,
    Descending,
    Include,
}

#[derive(Debug)]
struct Db2IndexColumn {
    name: String,
    order: Db2IndexColumnOrder,
}

#[derive(Debug)]
struct Db2IndexDefinition {
    schema: String,
    meta: IndexInfo,
    columns: Vec<Db2IndexColumn>,
}

struct Db2TableDefinition {
    schema: String,
    name: String,
    kind: Db2ObjectKind,
    columns: Vec<Db2ColumnDefinition>,
    indexes: Vec<Db2IndexDefinition>,
}

impl Db2TableDefinition {
    fn table_schema(&self) -> TableSchema {
        TableSchema {
            name: self.name.clone(),
            columns: self
                .columns
                .iter()
                .map(|column| column.meta.clone())
                .collect(),
            indexes: self
                .indexes
                .iter()
                .map(|index| index.meta.clone())
                .collect(),
        }
    }
}

impl Db2Adapter {
    async fn describe_table_complete(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<(Db2TableDefinition, MetadataLimiter)> {
        let table_ref = parse_table_ref(table)?;
        let schema = table_ref
            .schema
            .as_deref()
            .unwrap_or("DB2INST1")
            .to_uppercase();
        let name = table_ref.name.to_uppercase();
        let mut limiter =
            MetadataLimiter::new(budget, format!("Db2 table schema {schema}.{name}"))?;

        let kind_result = self
            .query(&db2_object_kind_sql(&schema, &name), &[])
            .await?;
        let kind = parse_db2_object_kind(kind_result.rows)?;

        let column_limit = db2_metadata_fetch_first(&limiter)?;
        let column_result = self
            .query(&db2_columns_sql(&schema, &name, column_limit), &[])
            .await?;
        if column_result.rows.is_empty() {
            return Err(Error::Query(format!(
                "Db2 table or view does not exist or exposes no columns: {schema}.{name}"
            )));
        }

        let mut columns = Vec::with_capacity(column_result.rows.len());
        for row in column_result.rows {
            let column = parse_db2_column(&row)?;
            observe_db2_column(&mut limiter, &column)?;
            columns.push(column);
        }

        let index_limit = db2_metadata_fetch_first(&limiter)?;
        let index_result = self
            .query(&db2_indexes_sql(&schema, &name, index_limit), &[])
            .await?;
        let mut indexes = Vec::new();
        for row in index_result.rows {
            accumulate_db2_index(&mut indexes, &mut limiter, &row)?;
        }

        let definition = Db2TableDefinition {
            schema,
            name,
            kind,
            columns,
            indexes,
        };
        Ok((definition, limiter))
    }
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

    fn operations(&self) -> Vec<CapabilityOperation> {
        db2_operations(self.capabilities())
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

    async fn list_schemas_bounded(&self, max_items: usize) -> Result<BoundedList<String>> {
        let (limiter, fetch_first) = db2_catalog_limit(max_items)?;
        let result = self
            .query(
                &format!(
                    "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA \
                     WHERE SCHEMANAME NOT LIKE 'SYS%' ORDER BY SCHEMANAME \
                     FETCH FIRST {fetch_first} ROWS ONLY"
                ),
                &[],
            )
            .await?;
        let schemas = result
            .rows
            .into_iter()
            .map(|row| required_catalog_text(&row, 0, "schema name"))
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(schemas))
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

    async fn list_tables_bounded(
        &self,
        schema: Option<&str>,
        max_items: usize,
    ) -> Result<BoundedList<TableInfo>> {
        let (limiter, fetch_first) = db2_catalog_limit(max_items)?;
        let schema = validate_opt_schema(schema)?.unwrap_or("DB2INST1");
        let sql = format!(
            "SELECT TABSCHEMA, TABNAME, TYPE FROM SYSCAT.TABLES \
             WHERE TABSCHEMA = '{}' AND TYPE IN ('T','V') ORDER BY TABNAME \
             FETCH FIRST {fetch_first} ROWS ONLY",
            schema.to_uppercase()
        );
        let result = self.query(&sql, &[]).await?;
        let tables = result
            .rows
            .into_iter()
            .map(|row| {
                let schema = required_catalog_text(&row, 0, "table schema")?;
                let name = required_catalog_text(&row, 1, "table name")?;
                let kind = required_catalog_text(&row, 2, "table type")?;
                Ok(TableInfo {
                    schema: Some(schema),
                    name,
                    kind: if kind == "V" {
                        TableKind::View
                    } else {
                        TableKind::Table
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(tables))
    }

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        let (definition, limiter) = self
            .describe_table_complete(
                table,
                MetadataBudget::new(LEGACY_SCHEMA_MAX_ITEMS, DEFAULT_METADATA_BYTES)?,
            )
            .await?;
        let schema = definition.table_schema();
        limiter.ensure_complete(&schema)?;
        Ok(schema)
    }

    async fn describe_table_bounded(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<TableSchema> {
        let (definition, limiter) = self.describe_table_complete(table, budget).await?;
        let schema = definition.table_schema();
        limiter.ensure_complete(&schema)?;
        Ok(schema)
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

    async fn list_sequences_bounded(
        &self,
        schema: Option<&str>,
        max_items: usize,
    ) -> Result<BoundedList<SequenceInfo>> {
        let (limiter, fetch_first) = db2_catalog_limit(max_items)?;
        let schema = validate_opt_schema(schema)?.unwrap_or("DB2INST1");
        let sql = format!(
            "SELECT SEQSCHEMA, SEQNAME, DATATYPEID, START, INCREMENT, \
                    MINVALUE, MAXVALUE, CYCLE, CACHE \
             FROM SYSCAT.SEQUENCES \
             WHERE SEQSCHEMA = '{}' AND SEQTYPE = 'S' \
             ORDER BY SEQNAME FETCH FIRST {fetch_first} ROWS ONLY",
            schema.to_uppercase()
        );
        let result = self.query(&sql, &[]).await?;
        let sequences = result
            .rows
            .into_iter()
            .map(parse_sequence_row)
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(sequences))
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

    async fn list_routines_bounded(
        &self,
        schema: Option<&str>,
        max_items: usize,
    ) -> Result<BoundedList<RoutineInfo>> {
        let (limiter, fetch_first) = db2_catalog_limit(max_items)?;
        let schema = validate_opt_schema(schema)?.unwrap_or("DB2INST1");
        let sql = format!(
            "SELECT ROUTINESCHEMA, ROUTINENAME, ROUTINETYPE, LANGUAGE, PARMS \
             FROM SYSCAT.ROUTINES \
             WHERE ROUTINESCHEMA = '{}' AND ROUTINETYPE IN ('P','F') \
             ORDER BY ROUTINENAME FETCH FIRST {fetch_first} ROWS ONLY",
            schema.to_uppercase()
        );
        let result = self.query(&sql, &[]).await?;
        let routines = result
            .rows
            .into_iter()
            .map(parse_routine_row)
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(routines))
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

    async fn list_tablespaces_bounded(
        &self,
        max_items: usize,
    ) -> Result<BoundedList<TablespaceInfo>> {
        let (limiter, fetch_first) = db2_catalog_limit(max_items)?;
        let sql = format!(
            "SELECT TBSPACE, TBSPACETYPE, PAGESIZE, EXTENTSIZE, PREFETCHSIZE \
             FROM SYSCAT.TABLESPACES \
             ORDER BY TBSPACE FETCH FIRST {fetch_first} ROWS ONLY"
        );
        let result = self.query(&sql, &[]).await?;
        let tablespaces = result
            .rows
            .into_iter()
            .map(parse_tablespace_row)
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(tablespaces))
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

    async fn list_foreign_keys_bounded(
        &self,
        table: &str,
        max_items: usize,
    ) -> Result<BoundedList<ForeignKeyInfo>> {
        let (limiter, fetch_first) = db2_catalog_limit(max_items)?;
        let member_row_envelope = db2_foreign_key_row_envelope(fetch_first)?;
        let tref = parse_table_ref(table)?;
        let schema_uc = tref.schema.as_deref().unwrap_or("DB2INST1").to_uppercase();
        let name_uc = tref.name.to_uppercase();

        let sql =
            db2_bounded_foreign_keys_sql(&schema_uc, &name_uc, fetch_first, member_row_envelope);
        let result = self.query(&sql, &[]).await?;
        let foreign_keys = group_foreign_key_rows(result.rows)?;
        Ok(limiter.finish(foreign_keys))
    }

    async fn generate_ddl(&self, table: &str) -> Result<String> {
        self.generate_ddl_bounded(
            table,
            MetadataBudget::new(LEGACY_SCHEMA_MAX_ITEMS, DEFAULT_METADATA_BYTES)?,
        )
        .await
    }

    async fn generate_ddl_bounded(&self, table: &str, budget: MetadataBudget) -> Result<String> {
        let (definition, limiter) = self.describe_table_complete(table, budget).await?;
        let ddl = format_db2_ddl(&definition)?;
        limiter.ensure_complete(&ddl)?;
        Ok(ddl)
    }
}

fn db2_columns_sql(schema: &str, table: &str, fetch_first: i64) -> String {
    format!(
        "SELECT c.COLNAME, c.TYPENAME, c.LENGTH, c.SCALE, c.NULLS, \
                CASE WHEN LENGTH(c.DEFAULT, OCTETS) <= {DB2_CATALOG_TEXT_MAX_OCTETS} \
                     THEN CAST(c.DEFAULT AS VARCHAR({DB2_CATALOG_TEXT_MAX_OCTETS} OCTETS)) \
                     ELSE CAST(NULL AS VARCHAR({DB2_CATALOG_TEXT_MAX_OCTETS} OCTETS)) END, \
                COALESCE(k.COLSEQ, 0) AS PK_SEQ, c.IDENTITY, c.GENERATED, \
                CASE WHEN LENGTH(c.TEXT, OCTETS) <= {DB2_CATALOG_TEXT_MAX_OCTETS} \
                     THEN CAST(c.TEXT AS VARCHAR({DB2_CATALOG_TEXT_MAX_OCTETS} OCTETS)) \
                     ELSE CAST(NULL AS VARCHAR({DB2_CATALOG_TEXT_MAX_OCTETS} OCTETS)) END, \
                LENGTH(c.TEXT, OCTETS), LENGTH(c.TEXT, CODEUNITS32), \
                ia.START, ia.INCREMENT, ia.MINVALUE, ia.MAXVALUE, \
                ia.CYCLE, ia.CACHE, ia.ORDER, \
                LENGTH(c.DEFAULT, OCTETS), LENGTH(c.DEFAULT, CODEUNITS32), \
                c.TYPESCHEMA, c.TYPESTRINGUNITS, c.STRINGUNITSLENGTH, c.CODEPAGE \
         FROM SYSCAT.COLUMNS c \
         LEFT JOIN ( \
           SELECT kc.COLNAME, kc.COLSEQ \
           FROM SYSCAT.KEYCOLUSE kc \
           JOIN SYSCAT.TABCONST tc \
             ON tc.CONSTNAME = kc.CONSTNAME \
            AND tc.TABSCHEMA = kc.TABSCHEMA \
            AND tc.TABNAME = kc.TABNAME \
           WHERE tc.TYPE = 'P' \
             AND kc.TABSCHEMA = '{schema}' \
             AND kc.TABNAME = '{table}' \
         ) k ON k.COLNAME = c.COLNAME \
         LEFT JOIN SYSCAT.COLIDENTATTRIBUTES ia \
           ON ia.TABSCHEMA = c.TABSCHEMA \
          AND ia.TABNAME = c.TABNAME \
          AND ia.COLNAME = c.COLNAME \
         WHERE c.TABSCHEMA = '{schema}' AND c.TABNAME = '{table}' \
         ORDER BY c.COLNO FETCH FIRST {fetch_first} ROWS ONLY"
    )
}

fn db2_object_kind_sql(schema: &str, table: &str) -> String {
    format!(
        "SELECT TYPE FROM SYSCAT.TABLES \
         WHERE TABSCHEMA = '{schema}' AND TABNAME = '{table}' \
           AND TYPE IN ('T', 'V') FETCH FIRST 2 ROWS ONLY"
    )
}

fn db2_indexes_sql(schema: &str, table: &str, fetch_first: i64) -> String {
    format!(
        "SELECT i.INDSCHEMA, i.INDNAME, i.UNIQUERULE, ic.COLNAME, \
                ic.COLORDER, COALESCE(ic.VIRTUAL, 'N') \
         FROM SYSCAT.INDEXES i \
         JOIN SYSCAT.INDEXCOLUSE ic \
           ON ic.INDNAME = i.INDNAME AND ic.INDSCHEMA = i.INDSCHEMA \
         WHERE i.TABSCHEMA = '{schema}' AND i.TABNAME = '{table}' \
         ORDER BY i.INDSCHEMA, i.INDNAME, ic.COLSEQ \
         FETCH FIRST {fetch_first} ROWS ONLY"
    )
}

fn db2_bounded_foreign_keys_sql(
    schema: &str,
    table: &str,
    constraint_probe: i64,
    member_row_envelope: i64,
) -> String {
    // Constraint identities are bounded first so a composite key is never
    // cut at the caller's N+1 boundary. Db2 permits at most 64 referencing
    // columns, so the outer product bound cannot truncate a valid constraint.
    format!(
        "WITH LIMITED_CONSTRAINTS AS ( \
           SELECT CONSTNAME, TABSCHEMA, TABNAME \
           FROM SYSCAT.REFERENCES \
           WHERE TABSCHEMA = '{schema}' AND TABNAME = '{table}' \
           ORDER BY CONSTNAME FETCH FIRST {constraint_probe} ROWS ONLY \
         ) \
         SELECT r.CONSTNAME, kc.COLNAME, r.REFTABSCHEMA, r.REFTABNAME, \
                rkc.COLNAME AS REFCOL, r.UPDATERULE, r.DELETERULE \
         FROM LIMITED_CONSTRAINTS limited \
         JOIN SYSCAT.REFERENCES r \
           ON r.CONSTNAME = limited.CONSTNAME \
          AND r.TABSCHEMA = limited.TABSCHEMA \
          AND r.TABNAME = limited.TABNAME \
         JOIN SYSCAT.KEYCOLUSE kc \
           ON kc.CONSTNAME = r.CONSTNAME \
          AND kc.TABSCHEMA = r.TABSCHEMA \
          AND kc.TABNAME = r.TABNAME \
         JOIN SYSCAT.KEYCOLUSE rkc \
           ON rkc.CONSTNAME = r.REFKEYNAME \
          AND rkc.TABSCHEMA = r.REFTABSCHEMA \
          AND rkc.TABNAME = r.REFTABNAME \
          AND rkc.COLSEQ = kc.COLSEQ \
         ORDER BY r.CONSTNAME, kc.COLSEQ \
         FETCH FIRST {member_row_envelope} ROWS ONLY"
    )
}

fn db2_foreign_key_row_envelope(constraint_probe: i64) -> Result<i64> {
    constraint_probe
        .checked_mul(DB2_MAX_FOREIGN_KEY_COLUMNS)
        .ok_or_else(|| {
            Error::Config(
                "Db2 foreign-key probe exceeds the joined member-row integer range".to_owned(),
            )
        })
}

fn db2_metadata_fetch_first(limiter: &MetadataLimiter) -> Result<i64> {
    i64::try_from(limiter.probe_items()?).map_err(|_| {
        Error::Config("Db2 metadata budget exceeds the FETCH FIRST integer range".to_owned())
    })
}

fn parse_db2_object_kind(rows: Vec<Vec<Value>>) -> Result<Db2ObjectKind> {
    match rows.as_slice() {
        [] => Err(Error::Query(
            "Db2 table or view does not exist in SYSCAT.TABLES".to_owned(),
        )),
        [row] => match required_catalog_text(row, 0, "object type")?.as_str() {
            "T" => Ok(Db2ObjectKind::Table),
            "V" => Ok(Db2ObjectKind::View),
            value => Err(Error::Serialization(format!(
                "Db2 catalog object type has unsupported value '{value}'"
            ))),
        },
        _ => Err(Error::Serialization(
            "Db2 catalog returned more than one table/view identity".to_owned(),
        )),
    }
}

fn parse_db2_column(row: &[Value]) -> Result<Db2ColumnDefinition> {
    let name = required_nonempty_catalog_text(row, 0, "column name")?;
    let raw_type = required_nonempty_catalog_text(row, 1, "column type")?;
    let length = required_catalog_i64(row, 2, "column length")?;
    let scale = required_catalog_i32(row, 3, "column scale")?;
    let type_schema = required_nonempty_catalog_text(row, 21, "column type schema")?;
    let string_units = optional_catalog_text(row, 22, "column type string units")?;
    let string_units_length = optional_catalog_i64(row, 23, "column string-units length")?;
    let codepage = required_catalog_i32(row, 24, "column code page")?;
    let type_name = format_db2_type(
        &type_schema,
        &raw_type,
        length,
        scale,
        string_units.as_deref(),
        string_units_length,
        codepage,
    )?;
    let nullable = match required_catalog_text(row, 4, "column nullable flag")?.as_str() {
        "Y" => true,
        "N" => false,
        value => {
            return Err(Error::Serialization(format!(
                "Db2 catalog column nullable flag has unknown value '{value}'"
            )))
        }
    };
    let default_value = parse_bounded_db2_catalog_text(row, 5, 19, 20, "column default")?;
    if default_value.as_deref() == Some("") {
        return Err(Error::Serialization(
            "Db2 catalog column default is empty".to_owned(),
        ));
    }
    let primary_sequence = required_catalog_i32(row, 6, "primary-key sequence")?;
    if primary_sequence < 0 {
        return Err(Error::Serialization(
            "Db2 catalog primary-key sequence is negative".to_owned(),
        ));
    }
    let generated_clause = parse_db2_generated_clause(row)?;
    if generated_clause.is_some() && default_value.is_some() {
        return Err(Error::Serialization(format!(
            "Db2 catalog column '{name}' has both DEFAULT and GENERATED metadata"
        )));
    }

    Ok(Db2ColumnDefinition {
        meta: ColumnMeta {
            name,
            type_name,
            nullable,
            primary_key: primary_sequence > 0,
            default_value,
        },
        primary_sequence,
        generated_clause,
    })
}

fn parse_db2_generated_clause(row: &[Value]) -> Result<Option<String>> {
    let identity = required_catalog_text(row, 7, "column identity flag")?;
    let generated = required_catalog_text(row, 8, "column generated flag")?;
    let expression = parse_db2_generated_expression(row)?;

    match identity.as_str() {
        "Y" => {
            if expression.is_some() {
                return Err(Error::Serialization(
                    "Db2 identity column unexpectedly has a generated expression".to_owned(),
                ));
            }
            let mode = match generated.as_str() {
                "A" => "ALWAYS",
                "D" => "BY DEFAULT",
                value => {
                    return Err(Error::Serialization(format!(
                        "Db2 identity column has invalid generated flag '{value}'"
                    )))
                }
            };
            let start = required_identity_integer(row, 12, "identity start")?;
            let increment = required_identity_integer(row, 13, "identity increment")?;
            let minimum = required_identity_integer(row, 14, "identity minimum")?;
            let maximum = required_identity_integer(row, 15, "identity maximum")?;
            let cycle = match required_identity_text(row, 16, "identity cycle flag")?.as_str() {
                "Y" => "CYCLE",
                "N" => "NO CYCLE",
                value => {
                    return Err(Error::Serialization(format!(
                        "Db2 identity cycle flag has unknown value '{value}'"
                    )))
                }
            };
            let cache_value = required_identity_text(row, 17, "identity cache")?
                .parse::<i64>()
                .map_err(|_| {
                    Error::Serialization(
                        "Db2 catalog identity cache is not a valid integer".to_owned(),
                    )
                })?;
            if cache_value < 0 {
                return Err(Error::Serialization(
                    "Db2 catalog identity cache is negative".to_owned(),
                ));
            }
            let cache = if cache_value == 0 {
                "NO CACHE".to_owned()
            } else {
                format!("CACHE {cache_value}")
            };
            let order = match required_identity_text(row, 18, "identity order flag")?.as_str() {
                "Y" => "ORDER",
                "N" => "NO ORDER",
                value => {
                    return Err(Error::Serialization(format!(
                        "Db2 identity order flag has unknown value '{value}'"
                    )))
                }
            };
            Ok(Some(format!(
                "GENERATED {mode} AS IDENTITY (START WITH {start} INCREMENT BY {increment} \
                 MINVALUE {minimum} MAXVALUE {maximum} {cycle} {cache} {order})"
            )))
        }
        "N" => {
            for (index, field) in [
                (12, "identity start"),
                (13, "identity increment"),
                (14, "identity minimum"),
                (15, "identity maximum"),
                (16, "identity cycle flag"),
                (17, "identity cache"),
                (18, "identity order flag"),
            ] {
                if optional_catalog_text(row, index, field)?.is_some() {
                    return Err(Error::Serialization(format!(
                        "Db2 non-identity column unexpectedly has {field} metadata"
                    )));
                }
            }
            match generated.as_str() {
                "" => {
                    if expression.is_some() {
                        return Err(Error::Serialization(
                            "Db2 non-generated column unexpectedly has generated expression text"
                                .to_owned(),
                        ));
                    }
                    Ok(None)
                }
                "A" => {
                    let expression = expression.ok_or_else(|| {
                        Error::Serialization(
                            "Db2 generated column is missing expression text".to_owned(),
                        )
                    })?;
                    let valid_prefix = expression
                        .get(..2)
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("AS"))
                        && expression
                            .get(2..)
                            .and_then(|suffix| suffix.chars().next())
                            .is_some_and(|character| {
                                character.is_ascii_whitespace() || character == '('
                            });
                    if !valid_prefix {
                        return Err(Error::Serialization(
                            "Db2 generated expression does not begin with AS".to_owned(),
                        ));
                    }
                    Ok(Some(format!("GENERATED ALWAYS {expression}")))
                }
                value => Err(Error::Serialization(format!(
                    "Db2 non-identity column has unsupported generated flag '{value}'"
                ))),
            }
        }
        value => Err(Error::Serialization(format!(
            "Db2 catalog column identity flag has unknown value '{value}'"
        ))),
    }
}

fn parse_db2_generated_expression(row: &[Value]) -> Result<Option<String>> {
    parse_bounded_db2_catalog_text(row, 9, 10, 11, "generated expression")
}

fn parse_bounded_db2_catalog_text(
    row: &[Value],
    text_index: usize,
    octets_index: usize,
    characters_index: usize,
    field: &str,
) -> Result<Option<String>> {
    let text = optional_catalog_raw_text(row, text_index, field)?;
    let original_octets =
        optional_catalog_i64(row, octets_index, &format!("{field} octet length"))?;
    let original_characters =
        optional_catalog_i64(row, characters_index, &format!("{field} character length"))?;

    match (text, original_octets, original_characters) {
        (None, None, None) => Ok(None),
        (text, Some(octets), Some(characters)) => {
            if octets < 0 || characters < 0 {
                return Err(Error::Serialization(format!(
                    "Db2 {field} reported a negative length"
                )));
            }
            if octets > DB2_CATALOG_TEXT_MAX_OCTETS {
                return Err(Error::Serialization(format!(
                    "Db2 {field} is {octets} octets and exceeds the safe \
                     {DB2_CATALOG_TEXT_MAX_OCTETS}-octet catalog read envelope"
                )));
            }
            let text = text.ok_or_else(|| {
                Error::Serialization(format!(
                    "Db2 {field} is missing inside its declared safe read envelope"
                ))
            })?;
            let observed_octets = i64::try_from(text.len())
                .map_err(|_| Error::Serialization(format!("Db2 {field} byte count overflowed")))?;
            let observed_characters = i64::try_from(text.chars().count()).map_err(|_| {
                Error::Serialization(format!("Db2 {field} character count overflowed"))
            })?;
            if observed_octets != octets || observed_characters != characters {
                return Err(Error::Serialization(format!(
                    "Db2 {field} was incomplete: catalog reports {octets} UTF-8 octets and \
                     {characters} characters but ODBC returned {observed_octets} octets and \
                     {observed_characters} characters"
                )));
            }
            Ok(Some(text))
        }
        _ => Err(Error::Serialization(format!(
            "Db2 {field} text and length metadata are inconsistent"
        ))),
    }
}

fn required_identity_text(row: &[Value], index: usize, field: &str) -> Result<String> {
    optional_catalog_text(row, index, field)?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Serialization(format!("Db2 catalog {field} is missing")))
}

fn required_identity_integer(row: &[Value], index: usize, field: &str) -> Result<String> {
    let value = required_identity_text(row, index, field)?;
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value.as_str());
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::Serialization(format!(
            "Db2 catalog {field} is not an integer literal"
        )));
    }
    Ok(value)
}

fn format_db2_type(
    type_schema: &str,
    type_name: &str,
    length: i64,
    scale: i32,
    string_units: Option<&str>,
    string_units_length: Option<i64>,
    codepage: i32,
) -> Result<String> {
    if length < 0 || scale < 0 {
        return Err(Error::Serialization(
            "Db2 catalog column type has negative length or scale".to_owned(),
        ));
    }
    if !type_schema.eq_ignore_ascii_case("SYSIBM") {
        return Err(Error::Serialization(format!(
            "Db2 user-defined or distinct type {type_schema}.{type_name} cannot be reconstructed \
             losslessly"
        )));
    }
    let type_name = type_name.trim().to_ascii_uppercase();
    let formatted = match type_name.as_str() {
        "DECIMAL" | "NUMERIC" | "NUMBER" => {
            reject_string_unit_metadata(&type_name, string_units, string_units_length)?;
            if !(1..=31).contains(&length) || i64::from(scale) > length {
                return Err(Error::Serialization(format!(
                    "Db2 catalog {type_name} precision/scale is invalid: {length},{scale}"
                )));
            }
            format!("{type_name}({length},{scale})")
        }
        "CHAR" | "CHARACTER" | "VARCHAR" | "CLOB" => {
            let (declared_length, units) =
                db2_character_string_length(&type_name, length, string_units, string_units_length)?;
            let bit_data = codepage == 0;
            if bit_data && units == "CODEUNITS32" {
                return Err(Error::Serialization(format!(
                    "Db2 catalog {type_name} FOR BIT DATA cannot use CODEUNITS32"
                )));
            }
            format!(
                "{type_name}({declared_length} {units}){}",
                if bit_data { " FOR BIT DATA" } else { "" }
            )
        }
        "GRAPHIC" | "VARGRAPHIC" | "DBCLOB" => {
            let (declared_length, units) =
                db2_graphic_string_length(&type_name, length, string_units, string_units_length)?;
            if codepage == 0 {
                return Err(Error::Serialization(format!(
                    "Db2 catalog {type_name} unexpectedly has code page zero"
                )));
            }
            match units {
                Some(units) => format!("{type_name}({declared_length} {units})"),
                None => format!("{type_name}({declared_length})"),
            }
        }
        "BINARY" | "VARBINARY" | "BLOB" => {
            reject_string_unit_metadata(&type_name, string_units, string_units_length)?;
            if length == 0 {
                return Err(Error::Serialization(format!(
                    "Db2 catalog {type_name} length must be positive"
                )));
            }
            format!("{type_name}({length})")
        }
        "LONG VARCHAR" => {
            reject_string_unit_length_mismatch(&type_name, string_units, string_units_length)?;
            format!(
                "LONG VARCHAR{}",
                if codepage == 0 { " FOR BIT DATA" } else { "" }
            )
        }
        "LONG VARGRAPHIC" => {
            reject_string_unit_length_mismatch(&type_name, string_units, string_units_length)?;
            if codepage == 0 {
                return Err(Error::Serialization(
                    "Db2 catalog LONG VARGRAPHIC unexpectedly has code page zero".to_owned(),
                ));
            }
            type_name
        }
        "TIMESTAMP" => {
            reject_string_unit_metadata(&type_name, string_units, string_units_length)?;
            if scale > 12 {
                return Err(Error::Serialization(format!(
                    "Db2 catalog TIMESTAMP scale is invalid: {scale}"
                )));
            }
            format!("TIMESTAMP({scale})")
        }
        "DECFLOAT" => {
            reject_string_unit_metadata(&type_name, string_units, string_units_length)?;
            match length {
                8 => "DECFLOAT(16)".to_owned(),
                16 => "DECFLOAT(34)".to_owned(),
                _ => {
                    return Err(Error::Serialization(format!(
                        "Db2 catalog DECFLOAT storage length is invalid: {length}"
                    )))
                }
            }
        }
        _ => {
            reject_string_unit_metadata(&type_name, string_units, string_units_length)?;
            type_name
        }
    };
    Ok(formatted)
}

fn db2_character_string_length(
    type_name: &str,
    storage_length: i64,
    string_units: Option<&str>,
    declared_length: Option<i64>,
) -> Result<(i64, &'static str)> {
    let (declared_length, units) = match (string_units, declared_length) {
        (Some("OCTETS"), Some(length)) => (length, "OCTETS"),
        (Some("CODEUNITS32"), Some(length)) => (length, "CODEUNITS32"),
        (None, None) => (storage_length, "OCTETS"),
        (Some(value), Some(_)) => {
            return Err(Error::Serialization(format!(
                "Db2 catalog {type_name} has unsupported string units '{value}'"
            )))
        }
        _ => {
            return Err(Error::Serialization(format!(
                "Db2 catalog {type_name} string-unit metadata is incomplete"
            )))
        }
    };
    if declared_length <= 0 {
        return Err(Error::Serialization(format!(
            "Db2 catalog {type_name} length must be positive"
        )));
    }
    let expected_storage_length = match units {
        "OCTETS" => declared_length,
        "CODEUNITS32" => declared_length.checked_mul(4).ok_or_else(|| {
            Error::Serialization(format!(
                "Db2 catalog {type_name} string-unit length overflowed"
            ))
        })?,
        _ => unreachable!("validated character string unit"),
    };
    if storage_length != expected_storage_length {
        return Err(Error::Serialization(format!(
            "Db2 catalog {type_name} storage length {storage_length} disagrees with declared \
             {declared_length} {units}"
        )));
    }
    Ok((declared_length, units))
}

fn db2_graphic_string_length(
    type_name: &str,
    storage_length: i64,
    string_units: Option<&str>,
    declared_length: Option<i64>,
) -> Result<(i64, Option<&'static str>)> {
    let (declared_length, units) = match (string_units, declared_length) {
        (Some("CODEUNITS16"), Some(length)) => (length, Some("CODEUNITS16")),
        (Some("CODEUNITS32"), Some(length)) => (length, Some("CODEUNITS32")),
        (None, None) if storage_length > 0 && storage_length % 2 == 0 => (storage_length / 2, None),
        (Some(value), Some(_)) => {
            return Err(Error::Serialization(format!(
                "Db2 catalog {type_name} has unsupported string units '{value}'"
            )))
        }
        _ => {
            return Err(Error::Serialization(format!(
                "Db2 catalog {type_name} string-unit metadata is incomplete"
            )))
        }
    };
    if declared_length <= 0 {
        return Err(Error::Serialization(format!(
            "Db2 catalog {type_name} length must be positive"
        )));
    }
    let expected_storage_length = match units {
        Some("CODEUNITS16") | None => declared_length.checked_mul(2),
        Some("CODEUNITS32") => declared_length.checked_mul(4),
        Some(_) => unreachable!("validated graphic string unit"),
    }
    .ok_or_else(|| {
        Error::Serialization(format!(
            "Db2 catalog {type_name} string-unit length overflowed"
        ))
    })?;
    if storage_length != expected_storage_length {
        return Err(Error::Serialization(format!(
            "Db2 catalog {type_name} storage length {storage_length} disagrees with its declared \
             string-unit length {declared_length}"
        )));
    }
    Ok((declared_length, units))
}

fn reject_string_unit_metadata(
    type_name: &str,
    string_units: Option<&str>,
    declared_length: Option<i64>,
) -> Result<()> {
    if string_units.is_some() || declared_length.is_some() {
        Err(Error::Serialization(format!(
            "Db2 catalog non-string type {type_name} unexpectedly has string-unit metadata"
        )))
    } else {
        Ok(())
    }
}

fn reject_string_unit_length_mismatch(
    type_name: &str,
    string_units: Option<&str>,
    declared_length: Option<i64>,
) -> Result<()> {
    match (string_units, declared_length) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => Err(Error::Serialization(format!(
            "Db2 catalog {type_name} string-unit metadata is incomplete"
        ))),
    }
}

fn observe_db2_column(limiter: &mut MetadataLimiter, column: &Db2ColumnDefinition) -> Result<()> {
    limiter.observe(&(
        "column",
        column.meta.name.as_str(),
        column.meta.type_name.as_str(),
        column.meta.nullable,
        column.meta.primary_key,
        column.primary_sequence,
        column.meta.default_value.as_deref(),
        column.generated_clause.as_deref(),
    ))
}

fn accumulate_db2_index(
    indexes: &mut Vec<Db2IndexDefinition>,
    limiter: &mut MetadataLimiter,
    row: &[Value],
) -> Result<()> {
    let schema = required_nonempty_catalog_text(row, 0, "index schema")?;
    let name = required_nonempty_catalog_text(row, 1, "index name")?;
    let unique_rule = required_catalog_text(row, 2, "index uniqueness rule")?;
    let (unique, primary) = match unique_rule.as_str() {
        "D" => (false, false),
        "U" => (true, false),
        "P" => (true, true),
        value => {
            return Err(Error::Serialization(format!(
                "Db2 catalog index uniqueness rule has unknown value '{value}'"
            )))
        }
    };
    let column = required_nonempty_catalog_text(row, 3, "index column")?;
    let order = match required_catalog_text(row, 4, "index column order")?.as_str() {
        "A" => Db2IndexColumnOrder::Ascending,
        "D" => Db2IndexColumnOrder::Descending,
        "I" => Db2IndexColumnOrder::Include,
        "R" => {
            return Err(Error::Serialization(format!(
                "Db2 random-order index column '{column}' cannot be reconstructed losslessly"
            )))
        }
        value => {
            return Err(Error::Serialization(format!(
                "Db2 catalog index column order has unknown value '{value}'"
            )))
        }
    };
    let virtual_column = required_catalog_text(row, 5, "index virtual-column flag")?;
    if virtual_column != "N" {
        return Err(Error::Serialization(format!(
            "Db2 virtual/expression index column '{column}' cannot be reconstructed losslessly"
        )));
    }

    match indexes
        .iter()
        .position(|index| index.schema == schema && index.meta.name == name)
    {
        Some(position) if position + 1 != indexes.len() => {
            return Err(Error::Serialization(format!(
                "Db2 catalog index rows are not contiguous for '{name}'"
            )))
        }
        Some(position) => {
            let index = &indexes[position];
            if index.meta.unique != unique || index.meta.primary != primary {
                return Err(Error::Serialization(format!(
                    "Db2 catalog index metadata changed within index '{name}'"
                )));
            }
        }
        None => {
            limiter.observe(&("index", schema.as_str(), name.as_str(), unique, primary))?;
            indexes.push(Db2IndexDefinition {
                schema,
                meta: IndexInfo {
                    name: name.clone(),
                    columns: Vec::new(),
                    unique,
                    primary,
                },
                columns: Vec::new(),
            });
        }
    }

    let order_code = match order {
        Db2IndexColumnOrder::Ascending => "A",
        Db2IndexColumnOrder::Descending => "D",
        Db2IndexColumnOrder::Include => "I",
    };
    limiter.observe(&("index-column", column.as_str(), order_code))?;
    let index = indexes
        .last_mut()
        .expect("an index was created or already existed");
    if order == Db2IndexColumnOrder::Include && !index.meta.unique {
        return Err(Error::Serialization(format!(
            "Db2 non-unique index '{name}' unexpectedly has an INCLUDE column"
        )));
    }
    if index
        .columns
        .iter()
        .any(|existing| existing.order == Db2IndexColumnOrder::Include)
        && order != Db2IndexColumnOrder::Include
    {
        return Err(Error::Serialization(format!(
            "Db2 catalog index key column follows an INCLUDE column in '{name}'"
        )));
    }
    index.meta.columns.push(column.clone());
    index.columns.push(Db2IndexColumn {
        name: column,
        order,
    });
    Ok(())
}

fn format_db2_ddl(definition: &Db2TableDefinition) -> Result<String> {
    if definition.kind == Db2ObjectKind::View {
        return Err(Error::UnsupportedCapability {
            kind: "db2".to_owned(),
            needed: "Db2 DDL generation for views requires the original view definition",
        });
    }
    if definition.columns.is_empty() {
        return Err(Error::Serialization(
            "Db2 DDL requires at least one column".to_owned(),
        ));
    }

    let mut elements = definition
        .columns
        .iter()
        .map(|column| {
            let mut definition = format!(
                "  {} {}",
                quote_db2_identifier(&column.meta.name),
                column.meta.type_name
            );
            if let Some(default) = column.meta.default_value.as_deref() {
                definition.push_str(" DEFAULT ");
                definition.push_str(default);
            }
            if !column.meta.nullable {
                definition.push_str(" NOT NULL");
            }
            if let Some(generated) = column.generated_clause.as_deref() {
                definition.push(' ');
                definition.push_str(generated);
            }
            definition
        })
        .collect::<Vec<_>>();

    let mut primary_columns = definition
        .columns
        .iter()
        .filter(|column| column.primary_sequence > 0)
        .map(|column| {
            (
                column.primary_sequence,
                quote_db2_identifier(&column.meta.name),
            )
        })
        .collect::<Vec<_>>();
    primary_columns.sort_by_key(|(sequence, _)| *sequence);
    for (expected, (sequence, _)) in (1_i32..).zip(&primary_columns) {
        if *sequence != expected {
            return Err(Error::Serialization(
                "Db2 primary-key column sequence is duplicated or has gaps".to_owned(),
            ));
        }
    }
    let primary_columns = primary_columns
        .into_iter()
        .map(|(_, column)| column)
        .collect::<Vec<_>>();
    let primary_indexes = definition
        .indexes
        .iter()
        .filter(|index| index.meta.primary)
        .collect::<Vec<_>>();
    if primary_indexes.len() > 1 {
        return Err(Error::Serialization(
            "Db2 catalog returned more than one primary index".to_owned(),
        ));
    }
    if let Some(primary_index) = primary_indexes.first() {
        let catalog_columns = primary_index
            .columns
            .iter()
            .map(|column| {
                if column.order != Db2IndexColumnOrder::Ascending {
                    return Err(Error::Serialization(
                        "Db2 primary-index ordering cannot be reproduced by a PRIMARY KEY clause"
                            .to_owned(),
                    ));
                }
                Ok(quote_db2_identifier(&column.name))
            })
            .collect::<Result<Vec<_>>>()?;
        if catalog_columns != primary_columns {
            return Err(Error::Serialization(
                "Db2 primary-key column metadata disagrees with the primary index".to_owned(),
            ));
        }
    }
    if !primary_columns.is_empty() {
        elements.push(format!("  PRIMARY KEY ({})", primary_columns.join(", ")));
    }

    let mut ddl = format!(
        "CREATE TABLE {}.{} (\n{}\n);\n",
        quote_db2_identifier(&definition.schema),
        quote_db2_identifier(&definition.name),
        elements.join(",\n")
    );
    for index in definition
        .indexes
        .iter()
        .filter(|index| !index.meta.primary)
    {
        if index.columns.is_empty() {
            return Err(Error::Serialization(format!(
                "Db2 index '{}' has no key columns",
                index.meta.name
            )));
        }
        let key_columns = index
            .columns
            .iter()
            .filter(|column| column.order != Db2IndexColumnOrder::Include)
            .map(|column| {
                format!(
                    "{} {}",
                    quote_db2_identifier(&column.name),
                    match column.order {
                        Db2IndexColumnOrder::Ascending => "ASC",
                        Db2IndexColumnOrder::Descending => "DESC",
                        Db2IndexColumnOrder::Include => unreachable!("filtered above"),
                    }
                )
            })
            .collect::<Vec<_>>();
        if key_columns.is_empty() {
            return Err(Error::Serialization(format!(
                "Db2 index '{}' has no key columns",
                index.meta.name
            )));
        }
        let include_columns = index
            .columns
            .iter()
            .filter(|column| column.order == Db2IndexColumnOrder::Include)
            .map(|column| quote_db2_identifier(&column.name))
            .collect::<Vec<_>>();
        ddl.push_str(&format!(
            "CREATE {}INDEX {}.{} ON {}.{} ({}){};\n",
            if index.meta.unique { "UNIQUE " } else { "" },
            quote_db2_identifier(&index.schema),
            quote_db2_identifier(&index.meta.name),
            quote_db2_identifier(&definition.schema),
            quote_db2_identifier(&definition.name),
            key_columns.join(", "),
            if include_columns.is_empty() {
                String::new()
            } else {
                format!(" INCLUDE ({})", include_columns.join(", "))
            }
        ));
    }
    Ok(ddl)
}

fn quote_db2_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
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

fn db2_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::SqlListSchemasBounded,
        CapabilityOperation::SqlListTablesBounded,
        CapabilityOperation::SqlDescribeTableBounded,
        CapabilityOperation::Db2ListSequencesBounded,
        CapabilityOperation::Db2ListRoutinesBounded,
        CapabilityOperation::Db2ListTablespacesBounded,
        CapabilityOperation::Db2ListForeignKeysBounded,
        CapabilityOperation::Db2GenerateDdlBounded,
    ]);
    operations
}

fn db2_catalog_limit(max_items: usize) -> Result<(ListLimiter, i64)> {
    let limiter = ListLimiter::new(max_items);
    let probe_items = limiter.probe_items()?;
    let fetch_first = i64::try_from(probe_items).map_err(|_| {
        Error::Config("Db2 catalog limit exceeds the FETCH FIRST integer range".to_owned())
    })?;
    Ok((limiter, fetch_first))
}

fn required_catalog_text(row: &[Value], index: usize, field: &str) -> Result<String> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.trim().to_owned()),
        _ => Err(Error::Serialization(format!(
            "Db2 catalog {field} is missing or is not text"
        ))),
    }
}

fn required_nonempty_catalog_text(row: &[Value], index: usize, field: &str) -> Result<String> {
    let value = required_catalog_text(row, index, field)?;
    if value.is_empty() {
        Err(Error::Serialization(format!(
            "Db2 catalog {field} is empty"
        )))
    } else {
        Ok(value)
    }
}

fn optional_catalog_text(row: &[Value], index: usize, field: &str) -> Result<Option<String>> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(Some(value.trim().to_owned())),
        Some(Value::Null) => Ok(None),
        _ => Err(Error::Serialization(format!(
            "Db2 catalog {field} is missing or is neither text nor null"
        ))),
    }
}

fn optional_catalog_raw_text(row: &[Value], index: usize, field: &str) -> Result<Option<String>> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(Value::Null) => Ok(None),
        _ => Err(Error::Serialization(format!(
            "Db2 catalog {field} is missing or is neither text nor null"
        ))),
    }
}

fn optional_catalog_i64(row: &[Value], index: usize, field: &str) -> Result<Option<i64>> {
    optional_catalog_text(row, index, field)?
        .map(|value| {
            value.parse().map_err(|_| {
                Error::Serialization(format!("Db2 catalog {field} is not a valid 64-bit integer"))
            })
        })
        .transpose()
}

fn required_catalog_i64(row: &[Value], index: usize, field: &str) -> Result<i64> {
    let value = required_catalog_text(row, index, field)?;
    value.parse().map_err(|_| {
        Error::Serialization(format!("Db2 catalog {field} is not a valid 64-bit integer"))
    })
}

fn required_catalog_i32(row: &[Value], index: usize, field: &str) -> Result<i32> {
    let value = required_catalog_text(row, index, field)?;
    value.parse().map_err(|_| {
        Error::Serialization(format!("Db2 catalog {field} is not a valid 32-bit integer"))
    })
}

fn parse_sequence_row(row: Vec<Value>) -> Result<SequenceInfo> {
    Ok(SequenceInfo {
        schema: required_catalog_text(&row, 0, "sequence schema")?,
        name: required_catalog_text(&row, 1, "sequence name")?,
        data_type: required_catalog_text(&row, 2, "sequence data type")?,
        start: required_catalog_text(&row, 3, "sequence start")?,
        increment: required_catalog_text(&row, 4, "sequence increment")?,
        min_value: required_catalog_text(&row, 5, "sequence minimum")?,
        max_value: required_catalog_text(&row, 6, "sequence maximum")?,
        cycle: required_catalog_text(&row, 7, "sequence cycle flag")? == "Y",
        cache: required_catalog_i64(&row, 8, "sequence cache")?,
    })
}

fn parse_routine_row(row: Vec<Value>) -> Result<RoutineInfo> {
    let routine_type = required_catalog_text(&row, 2, "routine type")?;
    let kind = match routine_type.as_str() {
        "P" => RoutineKind::Procedure,
        "F" => RoutineKind::Function,
        _ => {
            return Err(Error::Serialization(format!(
                "Db2 catalog returned unknown routine type '{routine_type}'"
            )))
        }
    };
    Ok(RoutineInfo {
        schema: required_catalog_text(&row, 0, "routine schema")?,
        name: required_catalog_text(&row, 1, "routine name")?,
        kind,
        language: required_catalog_text(&row, 3, "routine language")?,
        parms: required_catalog_i32(&row, 4, "routine parameter count")?,
    })
}

fn parse_tablespace_row(row: Vec<Value>) -> Result<TablespaceInfo> {
    Ok(TablespaceInfo {
        name: required_catalog_text(&row, 0, "tablespace name")?,
        kind: required_catalog_text(&row, 1, "tablespace type")?,
        page_size: required_catalog_i64(&row, 2, "tablespace page size")?,
        extent_size: required_catalog_i64(&row, 3, "tablespace extent size")?,
        prefetch_size: required_catalog_i64(&row, 4, "tablespace prefetch size")?,
    })
}

fn group_foreign_key_rows(rows: Vec<Vec<Value>>) -> Result<Vec<ForeignKeyInfo>> {
    let mut foreign_keys: Vec<ForeignKeyInfo> = Vec::new();
    for row in rows {
        let constraint_name = required_catalog_text(&row, 0, "foreign-key constraint name")?;
        let column = required_catalog_text(&row, 1, "foreign-key column")?;
        let ref_schema = required_catalog_text(&row, 2, "foreign-key referenced schema")?;
        let ref_table = required_catalog_text(&row, 3, "foreign-key referenced table")?;
        let ref_column = required_catalog_text(&row, 4, "foreign-key referenced column")?;
        let update_rule = required_catalog_text(&row, 5, "foreign-key update rule")?;
        let delete_rule = required_catalog_text(&row, 6, "foreign-key delete rule")?;

        if let Some(existing) = foreign_keys
            .iter_mut()
            .find(|foreign_key| foreign_key.constraint_name == constraint_name)
        {
            if existing.ref_schema != ref_schema
                || existing.ref_table != ref_table
                || existing.update_rule != update_rule
                || existing.delete_rule != delete_rule
            {
                return Err(Error::Serialization(format!(
                    "Db2 catalog returned inconsistent rows for foreign key '{constraint_name}'"
                )));
            }
            if existing.columns.len() >= DB2_MAX_FOREIGN_KEY_COLUMNS as usize {
                return Err(Error::Serialization(format!(
                    "Db2 catalog foreign key '{constraint_name}' exceeds the product maximum of \
                     {DB2_MAX_FOREIGN_KEY_COLUMNS} columns"
                )));
            }
            existing.columns.push(column);
            existing.ref_columns.push(ref_column);
        } else {
            foreign_keys.push(ForeignKeyInfo {
                constraint_name,
                columns: vec![column],
                ref_schema,
                ref_table,
                ref_columns: vec![ref_column],
                update_rule,
                delete_rule,
            });
        }
    }
    Ok(foreign_keys)
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
    fn advertises_every_bounded_db2_catalog_operation() {
        let operations = db2_operations(Capabilities {
            sql: true,
            db2: true,
            ..Default::default()
        });
        for operation in [
            CapabilityOperation::SqlListSchemasBounded,
            CapabilityOperation::SqlListTablesBounded,
            CapabilityOperation::SqlDescribeTableBounded,
            CapabilityOperation::Db2ListSequencesBounded,
            CapabilityOperation::Db2ListRoutinesBounded,
            CapabilityOperation::Db2ListTablespacesBounded,
            CapabilityOperation::Db2ListForeignKeysBounded,
            CapabilityOperation::Db2GenerateDdlBounded,
        ] {
            assert!(operations.contains(&operation), "missing {operation:?}");
        }
    }

    #[test]
    fn validates_catalog_limits_before_backend_access() {
        assert!(matches!(db2_catalog_limit(0), Err(Error::Config(_))));
        assert!(matches!(
            db2_catalog_limit(usize::MAX),
            Err(Error::Config(_))
        ));
        assert_eq!(db2_catalog_limit(2).unwrap().1, 3);

        let budget = MetadataBudget::new(3, DEFAULT_METADATA_BYTES).unwrap();
        let mut limiter = MetadataLimiter::new(budget, "test Db2 schema").unwrap();
        assert_eq!(db2_metadata_fetch_first(&limiter).unwrap(), 4);
        limiter.observe("column").unwrap();
        assert_eq!(db2_metadata_fetch_first(&limiter).unwrap(), 3);
        limiter.observe("index").unwrap();
        limiter.observe("index-column").unwrap();
        assert_eq!(db2_metadata_fetch_first(&limiter).unwrap(), 1);

        if usize::MAX > i64::MAX as usize {
            let overflow_budget =
                MetadataBudget::new(i64::MAX as usize, DEFAULT_METADATA_BYTES).unwrap();
            let overflow_limiter =
                MetadataLimiter::new(overflow_budget, "test Db2 schema").unwrap();
            assert!(matches!(
                db2_metadata_fetch_first(&overflow_limiter),
                Err(Error::Config(_))
            ));
        }

        assert_eq!(DB2_MAX_FOREIGN_KEY_COLUMNS, 64);
        assert_eq!(db2_foreign_key_row_envelope(3).unwrap(), 192);
        assert!(matches!(
            db2_foreign_key_row_envelope(i64::MAX),
            Err(Error::Config(_))
        ));
        let foreign_keys_sql = db2_bounded_foreign_keys_sql("APP", "ORDERS", 3, 192);
        assert!(foreign_keys_sql.contains("FETCH FIRST 3 ROWS ONLY"));
        assert!(foreign_keys_sql.ends_with("FETCH FIRST 192 ROWS ONLY"));
    }

    #[test]
    fn groups_composite_foreign_keys_without_splitting_constraints() {
        let row = |column: &str, ref_column: &str| {
            vec![
                Value::Text("FK_ORDER_CUSTOMER".to_owned()),
                Value::Text(column.to_owned()),
                Value::Text("APP".to_owned()),
                Value::Text("CUSTOMER".to_owned()),
                Value::Text(ref_column.to_owned()),
                Value::Text("A".to_owned()),
                Value::Text("R".to_owned()),
            ]
        };
        let grouped = group_foreign_key_rows(vec![
            row("CUSTOMER_ID", "ID"),
            row("CUSTOMER_REGION", "REGION"),
        ])
        .unwrap();

        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].columns, ["CUSTOMER_ID", "CUSTOMER_REGION"]);
        assert_eq!(grouped[0].ref_columns, ["ID", "REGION"]);

        let maximum_composite = group_foreign_key_rows(
            (0..DB2_MAX_FOREIGN_KEY_COLUMNS)
                .map(|position| row(&format!("COLUMN_{position}"), &format!("REF_{position}")))
                .collect(),
        )
        .unwrap();
        assert_eq!(maximum_composite.len(), 1);
        assert_eq!(
            maximum_composite[0].columns.len(),
            DB2_MAX_FOREIGN_KEY_COLUMNS as usize
        );
        let oversized = (0..=DB2_MAX_FOREIGN_KEY_COLUMNS)
            .map(|position| row(&format!("COLUMN_{position}"), &format!("REF_{position}")))
            .collect();
        assert!(matches!(
            group_foreign_key_rows(oversized),
            Err(Error::Serialization(message)) if message.contains("product maximum")
        ));
    }

    #[test]
    fn bounded_schema_sql_limits_each_catalog_phase_and_keeps_replay_fields() {
        let columns = db2_columns_sql("APP", "ORDERS", 4);
        let indexes = db2_indexes_sql("APP", "ORDERS", 2);

        assert!(columns.contains("c.LENGTH, c.SCALE"));
        assert!(columns.contains("c.IDENTITY, c.GENERATED"));
        assert!(columns.contains("LENGTH(c.TEXT, OCTETS)"));
        assert!(columns.contains("LENGTH(c.TEXT, CODEUNITS32)"));
        assert!(columns.contains("LENGTH(c.DEFAULT, OCTETS)"));
        assert!(columns.contains("LENGTH(c.DEFAULT, CODEUNITS32)"));
        assert!(columns.contains("c.TYPESTRINGUNITS, c.STRINGUNITSLENGTH, c.CODEPAGE"));
        assert!(columns.contains("CASE WHEN LENGTH(c.DEFAULT, OCTETS) <= 4096"));
        assert!(columns.contains("SYSCAT.COLIDENTATTRIBUTES"));
        assert!(indexes.contains("ic.COLORDER, COALESCE(ic.VIRTUAL, 'N')"));
        assert!(columns.ends_with("FETCH FIRST 4 ROWS ONLY"));
        assert!(indexes.ends_with("FETCH FIRST 2 ROWS ONLY"));
        assert_eq!(columns.matches("FETCH FIRST").count(), 1);
        assert_eq!(indexes.matches("FETCH FIRST").count(), 1);
    }

    #[test]
    fn strict_column_parser_preserves_type_and_generation_metadata() {
        let varchar = parse_db2_column(&plain_column_row(
            "NAME",
            "VARCHAR",
            64,
            0,
            "Y",
            Value::Text("'unknown'".into()),
            0,
        ))
        .unwrap();
        assert_eq!(varchar.meta.type_name, "VARCHAR(64 OCTETS)");
        assert_eq!(varchar.meta.default_value.as_deref(), Some("'unknown'"));

        let decimal = parse_db2_column(&plain_column_row(
            "TOTAL",
            "DECIMAL",
            12,
            2,
            "N",
            Value::Null,
            0,
        ))
        .unwrap();
        assert_eq!(decimal.meta.type_name, "DECIMAL(12,2)");

        let identity = parse_db2_column(&identity_column_row()).unwrap();
        assert_eq!(identity.meta.type_name, "INTEGER");
        assert_eq!(
            identity.generated_clause.as_deref(),
            Some(
                "GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 \
                 MAXVALUE 2147483647 NO CYCLE CACHE 20 NO ORDER)"
            )
        );

        let mut malformed = plain_column_row("BROKEN", "VARCHAR", 64, 0, "M", Value::Null, 0);
        assert!(parse_db2_column(&malformed).is_err());
        malformed[4] = Value::Text("Y".into());
        malformed[2] = Value::Text("0".into());
        assert!(parse_db2_column(&malformed).is_err());

        let mut missing_expression =
            plain_column_row("CALCULATED", "DECIMAL", 12, 2, "Y", Value::Null, 0);
        missing_expression[8] = Value::Text("A".into());
        assert!(parse_db2_column(&missing_expression).is_err());
    }

    #[test]
    fn generated_expression_parser_accepts_n_and_rejects_n_plus_one_octets() {
        let exact_expression = format!("AS({})", "X".repeat(4092));
        assert_eq!(exact_expression.len(), DB2_CATALOG_TEXT_MAX_OCTETS as usize);
        let mut exact_row = plain_column_row("CALCULATED", "INTEGER", 4, 0, "Y", Value::Null, 0);
        exact_row[8] = Value::Text("A".into());
        exact_row[9] = Value::Text(exact_expression.clone());
        exact_row[10] = Value::Text(DB2_CATALOG_TEXT_MAX_OCTETS.to_string());
        exact_row[11] = Value::Text(DB2_CATALOG_TEXT_MAX_OCTETS.to_string());
        assert!(parse_db2_column(&exact_row).is_ok());

        let mut probe_row = exact_row;
        probe_row[10] = Value::Text((DB2_CATALOG_TEXT_MAX_OCTETS + 1).to_string());
        probe_row[11] = Value::Text((DB2_CATALOG_TEXT_MAX_OCTETS + 1).to_string());
        let error = parse_db2_column(&probe_row).unwrap_err();
        assert!(matches!(
            error,
            Error::Serialization(message) if message.contains("exceeds the safe")
        ));
    }

    #[test]
    fn default_parser_accepts_n_and_rejects_n_plus_one_octets_without_truncation() {
        let exact_default = "X".repeat(DB2_CATALOG_TEXT_MAX_OCTETS as usize);
        let exact_row = plain_column_row(
            "PAYLOAD",
            "VARCHAR",
            DB2_CATALOG_TEXT_MAX_OCTETS,
            0,
            "Y",
            Value::Text(exact_default.clone()),
            0,
        );
        assert_eq!(
            parse_db2_column(&exact_row)
                .unwrap()
                .meta
                .default_value
                .as_deref(),
            Some(exact_default.as_str())
        );

        let mut oversized = plain_column_row(
            "PAYLOAD",
            "CLOB",
            DB2_CATALOG_TEXT_MAX_OCTETS + 1,
            0,
            "Y",
            Value::Text("X".repeat(DB2_CATALOG_TEXT_MAX_OCTETS as usize + 1)),
            0,
        );
        oversized[5] = Value::Null;
        let error = parse_db2_column(&oversized).unwrap_err();
        assert!(matches!(
            error,
            Error::Serialization(message) if message.contains("column default")
                && message.contains("exceeds the safe")
        ));

        let mut mismatched = plain_column_row(
            "LABEL",
            "VARCHAR",
            16,
            0,
            "Y",
            Value::Text("'雪'".into()),
            0,
        );
        mismatched[19] = Value::Text("4".into());
        assert!(matches!(
            parse_db2_column(&mismatched),
            Err(Error::Serialization(message)) if message.contains("was incomplete")
        ));
    }

    #[test]
    fn strict_type_parser_reconstructs_units_and_bit_data_and_rejects_distinct_types() {
        let mut unicode = plain_column_row("LABEL", "VARCHAR", 40, 0, "Y", Value::Null, 0);
        unicode[22] = Value::Text("CODEUNITS32".into());
        unicode[23] = Value::Text("10".into());
        assert_eq!(
            parse_db2_column(&unicode).unwrap().meta.type_name,
            "VARCHAR(10 CODEUNITS32)"
        );

        let mut bit_data = plain_column_row("TOKEN", "VARCHAR", 16, 0, "N", Value::Null, 0);
        bit_data[24] = Value::Text("0".into());
        assert_eq!(
            parse_db2_column(&bit_data).unwrap().meta.type_name,
            "VARCHAR(16 OCTETS) FOR BIT DATA"
        );

        let mut graphic = plain_column_row("GLYPH", "GRAPHIC", 8, 0, "Y", Value::Null, 0);
        graphic[24] = Value::Text("1200".into());
        assert_eq!(
            parse_db2_column(&graphic).unwrap().meta.type_name,
            "GRAPHIC(4)"
        );

        let mut distinct = plain_column_row("MONEY", "MONEY_T", 0, 0, "Y", Value::Null, 0);
        distinct[21] = Value::Text("APP".into());
        assert!(matches!(
            parse_db2_column(&distinct),
            Err(Error::Serialization(message)) if message.contains("cannot be reconstructed")
        ));
    }

    #[test]
    fn metadata_budget_counts_columns_index_identities_and_composite_memberships() {
        let column = parse_db2_column(&plain_column_row(
            "ID",
            "INTEGER",
            4,
            0,
            "N",
            Value::Null,
            1,
        ))
        .unwrap();
        let budget = MetadataBudget::new(4, DEFAULT_METADATA_BYTES).unwrap();
        let mut limiter = MetadataLimiter::new(budget, "test Db2 schema").unwrap();
        observe_db2_column(&mut limiter, &column).unwrap();

        let mut indexes = Vec::new();
        accumulate_db2_index(
            &mut indexes,
            &mut limiter,
            &index_row("PK_ORDERS", "P", "TENANT"),
        )
        .unwrap();
        accumulate_db2_index(
            &mut indexes,
            &mut limiter,
            &index_row("PK_ORDERS", "P", "ID"),
        )
        .unwrap();
        assert_eq!(limiter.observed_items(), 4);
        assert_eq!(indexes[0].meta.columns, ["TENANT", "ID"]);
        assert_eq!(db2_metadata_fetch_first(&limiter).unwrap(), 1);

        assert!(matches!(
            accumulate_db2_index(
                &mut indexes,
                &mut limiter,
                &index_row("PK_ORDERS", "P", "REVISION"),
            ),
            Err(Error::MetadataBudgetExceeded { unit: "items", .. })
        ));

        let mut byte_limiter =
            MetadataLimiter::new(MetadataBudget::new(4, 1).unwrap(), "test Db2 schema").unwrap();
        assert!(matches!(
            observe_db2_column(&mut byte_limiter, &column),
            Err(Error::MetadataBudgetExceeded { unit: "bytes", .. })
        ));
    }

    #[test]
    fn strict_index_parser_rejects_unknown_or_inconsistent_rows() {
        let mut limiter = MetadataLimiter::new(
            MetadataBudget::new(10, DEFAULT_METADATA_BYTES).unwrap(),
            "test Db2 schema",
        )
        .unwrap();
        let mut indexes = Vec::new();
        assert!(
            accumulate_db2_index(&mut indexes, &mut limiter, &index_row("IX", "X", "A")).is_err()
        );

        accumulate_db2_index(&mut indexes, &mut limiter, &index_row("IX", "D", "A")).unwrap();
        assert!(
            accumulate_db2_index(&mut indexes, &mut limiter, &index_row("IX", "U", "B")).is_err()
        );
        assert!(
            accumulate_db2_index(&mut indexes, &mut limiter, &[Value::Text("IX".into())]).is_err()
        );

        let mut random_limiter = MetadataLimiter::new(
            MetadataBudget::new(10, DEFAULT_METADATA_BYTES).unwrap(),
            "test Db2 schema",
        )
        .unwrap();
        let mut random_indexes = Vec::new();
        assert!(matches!(
            accumulate_db2_index(
                &mut random_indexes,
                &mut random_limiter,
                &index_row_with_order("IX_RANDOM", "D", "A", "R"),
            ),
            Err(Error::Serialization(message)) if message.contains("random-order")
        ));

        let mut virtual_row = index_row("IX_EXPR", "D", "A");
        virtual_row[5] = Value::Text("Y".into());
        assert!(matches!(
            accumulate_db2_index(&mut random_indexes, &mut random_limiter, &virtual_row),
            Err(Error::Serialization(message)) if message.contains("virtual/expression")
        ));
    }

    #[test]
    fn ddl_formatter_preserves_descending_keys_and_include_columns() {
        let definition = Db2TableDefinition {
            schema: "APP".into(),
            name: "ORDERS".into(),
            kind: Db2ObjectKind::Table,
            columns: vec![
                parse_db2_column(&plain_column_row(
                    "NAME",
                    "VARCHAR",
                    64,
                    0,
                    "Y",
                    Value::Null,
                    0,
                ))
                .unwrap(),
                parse_db2_column(&plain_column_row(
                    "STATUS",
                    "VARCHAR",
                    16,
                    0,
                    "Y",
                    Value::Null,
                    0,
                ))
                .unwrap(),
            ],
            indexes: vec![index_definition(
                "UX_ORDERS_NAME",
                true,
                false,
                &[
                    ("NAME", Db2IndexColumnOrder::Descending),
                    ("STATUS", Db2IndexColumnOrder::Include),
                ],
            )],
        };

        let ddl = format_db2_ddl(&definition).unwrap();
        assert!(ddl.contains(
            "CREATE UNIQUE INDEX \"APP\".\"UX_ORDERS_NAME\" ON \"APP\".\"ORDERS\" \
             (\"NAME\" DESC) INCLUDE (\"STATUS\");"
        ));
    }

    #[test]
    fn ddl_formatter_rejects_views_instead_of_recasting_them_as_tables() {
        let definition = Db2TableDefinition {
            schema: "APP".into(),
            name: "ORDER_VIEW".into(),
            kind: Db2ObjectKind::View,
            columns: vec![parse_db2_column(&plain_column_row(
                "ID",
                "INTEGER",
                4,
                0,
                "Y",
                Value::Null,
                0,
            ))
            .unwrap()],
            indexes: Vec::new(),
        };
        assert!(matches!(
            format_db2_ddl(&definition),
            Err(Error::UnsupportedCapability { .. })
        ));
    }

    #[test]
    fn object_kind_probe_requires_exactly_one_table_or_view_identity() {
        assert_eq!(
            parse_db2_object_kind(vec![vec![Value::Text("T".into())]]).unwrap(),
            Db2ObjectKind::Table
        );
        assert_eq!(
            parse_db2_object_kind(vec![vec![Value::Text("V".into())]]).unwrap(),
            Db2ObjectKind::View
        );
        assert!(parse_db2_object_kind(Vec::new()).is_err());
        assert!(parse_db2_object_kind(vec![
            vec![Value::Text("T".into())],
            vec![Value::Text("V".into())],
        ])
        .is_err());
        assert!(db2_object_kind_sql("APP", "ORDERS").ends_with("FETCH FIRST 2 ROWS ONLY"));
    }

    #[test]
    fn ddl_formatter_is_replayable_without_double_or_trailing_commas() {
        let mut generated_row =
            plain_column_row("CALCULATED", "DECIMAL", 12, 2, "Y", Value::Null, 0);
        generated_row[8] = Value::Text("A".into());
        generated_row[9] = Value::Text("AS (\"TOTAL\" * 2)".into());
        generated_row[10] = Value::Text("16".into());
        generated_row[11] = Value::Text("16".into());

        let definition = Db2TableDefinition {
            schema: "APP".into(),
            name: "ORDERS".into(),
            kind: Db2ObjectKind::Table,
            columns: vec![
                parse_db2_column(&identity_column_row()).unwrap(),
                parse_db2_column(&plain_column_row(
                    "NAME",
                    "VARCHAR",
                    64,
                    0,
                    "Y",
                    Value::Text("'unknown'".into()),
                    0,
                ))
                .unwrap(),
                parse_db2_column(&plain_column_row(
                    "TOTAL",
                    "DECIMAL",
                    12,
                    2,
                    "N",
                    Value::Null,
                    0,
                ))
                .unwrap(),
                parse_db2_column(&generated_row).unwrap(),
            ],
            indexes: vec![
                index_definition(
                    "PK_ORDERS",
                    true,
                    true,
                    &[("ID", Db2IndexColumnOrder::Ascending)],
                ),
                index_definition(
                    "IX_ORDERS_NAME",
                    true,
                    false,
                    &[("NAME", Db2IndexColumnOrder::Ascending)],
                ),
            ],
        };

        let ddl = format_db2_ddl(&definition).unwrap();
        assert_eq!(
            ddl,
            concat!(
                "CREATE TABLE \"APP\".\"ORDERS\" (\n",
                "  \"ID\" INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY ",
                "(START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 ",
                "NO CYCLE CACHE 20 NO ORDER),\n",
                "  \"NAME\" VARCHAR(64 OCTETS) DEFAULT 'unknown',\n",
                "  \"TOTAL\" DECIMAL(12,2) NOT NULL,\n",
                "  \"CALCULATED\" DECIMAL(12,2) GENERATED ALWAYS AS ",
                "(\"TOTAL\" * 2),\n",
                "  PRIMARY KEY (\"ID\")\n",
                ");\n",
                "CREATE UNIQUE INDEX \"APP\".\"IX_ORDERS_NAME\" ON ",
                "\"APP\".\"ORDERS\" (\"NAME\" ASC);\n",
            )
        );
        assert!(!ddl.contains(",,"));
        assert!(!ddl.contains(",\n);"));
        assert!(!ddl.contains("\n,  PRIMARY KEY"));
    }

    #[test]
    fn ddl_formatter_preserves_primary_key_sequence_independent_of_column_order() {
        let definition = Db2TableDefinition {
            schema: "APP".into(),
            name: "COMPOSITE".into(),
            kind: Db2ObjectKind::Table,
            columns: vec![
                parse_db2_column(&plain_column_row(
                    "FIRST_IN_TABLE",
                    "INTEGER",
                    4,
                    0,
                    "N",
                    Value::Null,
                    2,
                ))
                .unwrap(),
                parse_db2_column(&plain_column_row(
                    "SECOND_IN_TABLE",
                    "INTEGER",
                    4,
                    0,
                    "N",
                    Value::Null,
                    1,
                ))
                .unwrap(),
            ],
            indexes: vec![index_definition(
                "PK_COMPOSITE",
                true,
                true,
                &[
                    ("SECOND_IN_TABLE", Db2IndexColumnOrder::Ascending),
                    ("FIRST_IN_TABLE", Db2IndexColumnOrder::Ascending),
                ],
            )],
        };

        let ddl = format_db2_ddl(&definition).unwrap();
        assert!(ddl.contains("PRIMARY KEY (\"SECOND_IN_TABLE\", \"FIRST_IN_TABLE\")"));
    }

    fn plain_column_row(
        name: &str,
        type_name: &str,
        length: i64,
        scale: i32,
        nullable: &str,
        default_value: Value,
        primary_sequence: i32,
    ) -> Vec<Value> {
        let (default_octets, default_characters) = match &default_value {
            Value::Text(value) => (
                Value::Text(value.len().to_string()),
                Value::Text(value.chars().count().to_string()),
            ),
            Value::Null => (Value::Null, Value::Null),
            _ => panic!("test default must be text or null"),
        };
        let character_type = matches!(type_name, "CHAR" | "CHARACTER" | "VARCHAR" | "CLOB");
        vec![
            Value::Text(name.into()),
            Value::Text(type_name.into()),
            Value::Text(length.to_string()),
            Value::Text(scale.to_string()),
            Value::Text(nullable.into()),
            default_value,
            Value::Text(primary_sequence.to_string()),
            Value::Text("N".into()),
            Value::Text(String::new()),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            default_octets,
            default_characters,
            Value::Text("SYSIBM".into()),
            if character_type {
                Value::Text("OCTETS".into())
            } else {
                Value::Null
            },
            if character_type {
                Value::Text(length.to_string())
            } else {
                Value::Null
            },
            Value::Text(if character_type { "1208" } else { "0" }.into()),
        ]
    }

    fn identity_column_row() -> Vec<Value> {
        let mut row = plain_column_row("ID", "INTEGER", 4, 0, "N", Value::Null, 1);
        row[7] = Value::Text("Y".into());
        row[8] = Value::Text("A".into());
        row[12] = Value::Text("1".into());
        row[13] = Value::Text("1".into());
        row[14] = Value::Text("1".into());
        row[15] = Value::Text("2147483647".into());
        row[16] = Value::Text("N".into());
        row[17] = Value::Text("20".into());
        row[18] = Value::Text("N".into());
        row
    }

    fn index_row(name: &str, unique_rule: &str, column: &str) -> Vec<Value> {
        index_row_with_order(name, unique_rule, column, "A")
    }

    fn index_row_with_order(
        name: &str,
        unique_rule: &str,
        column: &str,
        order: &str,
    ) -> Vec<Value> {
        vec![
            Value::Text("APP".into()),
            Value::Text(name.into()),
            Value::Text(unique_rule.into()),
            Value::Text(column.into()),
            Value::Text(order.into()),
            Value::Text("N".into()),
        ]
    }

    fn index_definition(
        name: &str,
        unique: bool,
        primary: bool,
        columns: &[(&str, Db2IndexColumnOrder)],
    ) -> Db2IndexDefinition {
        Db2IndexDefinition {
            schema: "APP".into(),
            meta: IndexInfo {
                name: name.into(),
                columns: columns.iter().map(|(name, _)| (*name).into()).collect(),
                unique,
                primary,
            },
            columns: columns
                .iter()
                .map(|(name, order)| Db2IndexColumn {
                    name: (*name).into(),
                    order: *order,
                })
                .collect(),
        }
    }
}
