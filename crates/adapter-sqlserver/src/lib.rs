use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        BoundedList, ColumnMeta, ExecOutcome, IndexInfo, InputBudget, MetadataBudget, ReadBudget,
        ResultSet, SqlExecuteInput, TableInfo, TableKind, TableSchema, Value,
        DEFAULT_METADATA_BYTES, MAX_INPUT_BYTES,
    },
    port::{
        capability::SqlEngine,
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{InputLimiter, ListLimiter, MetadataLimiter, ReadLimiter, ResultLimiter},
};
use futures::{future::BoxFuture, TryStreamExt};
use tiberius::{AuthMethod, Client, Column, ColumnData, Config, EncryptionLevel, QueryItem, Row};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

type SqlServerClient = Client<Compat<TcpStream>>;
const LEGACY_SCHEMA_MAX_ITEMS: usize = 100_000;
const SQLSERVER_MAX_STATEMENT_BYTES: usize = MAX_INPUT_BYTES;
const SQLSERVER_MAX_PARAMETERS: usize = 2_100;

pub struct SqlServerAdapter {
    client: Mutex<Option<SqlServerClient>>,
    dsn: Dsn,
    kind: ConnectorKind,
}

impl SqlServerAdapter {
    async fn query_backend(&self, sql: &str, params: &[Value]) -> Result<ResultSet> {
        reject_dynamic_params(params)?;
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| Error::Connection("SQL Server connection is closed".to_owned()))?;

        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| Error::Query(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        rows_to_result_set(rows)
    }

    async fn describe_table_complete(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;
        let schema = table_ref.schema.as_deref().unwrap_or("dbo");
        let mut limiter = MetadataLimiter::new(budget, "SQL Server table schema")?;

        let column_top = metadata_top(&limiter)?;
        let col_result = self
            .query_backend(
                &sqlserver_columns_sql(schema, &table_ref.name, column_top),
                &[],
            )
            .await?;
        if col_result.rows.is_empty() {
            return Err(Error::Query(format!(
                "SQL Server table or view does not exist or exposes no columns: {schema}.{}",
                table_ref.name
            )));
        }
        let mut columns = Vec::with_capacity(col_result.rows.len());
        for row in col_result.rows {
            let column = parse_sqlserver_column(&row)?;
            limiter.observe(&column)?;
            columns.push(column);
        }

        let index_top = metadata_top(&limiter)?;
        let idx_result = self
            .query_backend(
                &sqlserver_indexes_sql(schema, &table_ref.name, index_top),
                &[],
            )
            .await?;
        let mut indexes = Vec::new();
        for row in idx_result.rows {
            accumulate_sqlserver_index(&mut indexes, &mut limiter, &row)?;
        }

        let table_schema = TableSchema {
            name: table_ref.name,
            columns,
            indexes,
        };
        limiter.ensure_complete(&table_schema)?;
        Ok(table_schema)
    }
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let client = connect_client(&dsn).await?;
        let kind = ConnectorKind(dsn.scheme.clone());

        Ok(Box::new(SqlServerAdapter {
            client: Mutex::new(Some(client)),
            dsn,
            kind,
        }) as Box<dyn Connector>)
    })
}

async fn connect_client(dsn: &Dsn) -> Result<SqlServerClient> {
    let config = config_from_dsn(dsn)?;
    let addr = config.get_addr();
    let tcp = TcpStream::connect(&addr)
        .await
        .map_err(|e| Error::Connection(format!("{addr}: {e}")))?;
    tcp.set_nodelay(true)
        .map_err(|e| Error::Connection(e.to_string()))?;

    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| Error::Connection(e.to_string()))
}

#[async_trait::async_trait]
impl Connector for SqlServerAdapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: true,
            ..Default::default()
        }
    }

    fn operations(&self) -> Vec<CapabilityOperation> {
        sqlserver_operations(self.capabilities())
    }

    async fn ping(&self) -> Result<()> {
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| Error::Connection("SQL Server connection is closed".to_owned()))?;

        client
            .query("SELECT 1", &[])
            .await
            .map_err(|e| Error::Connection(e.to_string()))?
            .into_first_result()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<()> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.take() {
            client
                .close()
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
        }
        Ok(())
    }

    fn as_sql(&self) -> Option<&dyn SqlEngine> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl SqlEngine for SqlServerAdapter {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet> {
        self.query_backend(sql, params).await
    }

    async fn query_bounded(
        &self,
        sql: &str,
        params: &[Value],
        max_rows: usize,
    ) -> Result<ResultSet> {
        let limiter = ResultLimiter::new(max_rows);
        let probe_rows = limiter.probe_rows()?;
        reject_dynamic_params(params)?;
        // A Tiberius QueryStream is flushed before the shared client can run a
        // later query. Use a disposable connection so truncation closes the
        // unread response rather than deferring an unbounded drain.
        let mut client = connect_client(&self.dsn).await?;
        let mut stream = client
            .query(sql, &[])
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        let mut columns = Vec::new();
        let mut rows = Vec::new();
        while rows.len() < probe_rows {
            let Some(item) = stream
                .try_next()
                .await
                .map_err(|e| Error::Query(e.to_string()))?
            else {
                break;
            };

            match item {
                QueryItem::Metadata(metadata) if metadata.result_index() == 0 => {
                    columns = metadata.columns().iter().map(column_meta).collect();
                }
                QueryItem::Metadata(_) => break,
                QueryItem::Row(row) if row.result_index() == 0 => rows.push(row_values(row)),
                QueryItem::Row(_) => break,
            }
        }
        drop(stream);
        client
            .close()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        Ok(limiter.apply(ResultSet {
            columns,
            rows,
            truncated: false,
        }))
    }

    async fn query_budgeted(
        &self,
        sql: &str,
        params: &[Value],
        budget: ReadBudget,
    ) -> Result<ResultSet> {
        let mut limiter = ReadLimiter::new(budget, "SQL Server query result")?;
        let probe_rows = limiter.probe_items()?;
        reject_dynamic_params(params)?;

        // Tiberius exposes rows as a token stream but has no configurable
        // decoded-row byte ceiling. A disposable connection ensures that we
        // observe at most N+1 rows and drop unread response state immediately.
        // One oversized row or metadata token is necessarily decoded by the
        // driver before the recursive core byte accounting can reject it.
        let mut client = connect_client(&self.dsn).await?;
        let mut stream = client
            .query(sql, &[])
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        let mut columns = Vec::new();
        let mut header_observed = false;
        let mut rows = Vec::with_capacity(budget.max_items.min(256));
        while limiter.observed_items() < probe_rows {
            let Some(item) = stream
                .try_next()
                .await
                .map_err(|e| Error::Query(e.to_string()))?
            else {
                break;
            };

            match item {
                QueryItem::Metadata(metadata) if metadata.result_index() == 0 => {
                    if header_observed {
                        return Err(Error::Serialization(
                            "SQL Server query emitted duplicate first-result metadata".to_owned(),
                        ));
                    }
                    columns = metadata.columns().iter().map(column_meta).collect();
                    limiter.observe_header(&columns)?;
                    header_observed = true;
                }
                QueryItem::Metadata(_) => break,
                QueryItem::Row(row) if row.result_index() == 0 => {
                    if !header_observed {
                        columns = row.columns().iter().map(column_meta).collect();
                        limiter.observe_header(&columns)?;
                        header_observed = true;
                    }
                    limiter.retain_item(row_values(row), &mut rows)?;
                }
                QueryItem::Row(_) => break,
            }
        }
        if !header_observed {
            limiter.observe_header(&columns)?;
        }
        drop(stream);
        client
            .close()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        finish_budgeted_result(limiter, columns, rows)
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome> {
        self.execute_budgeted(sql, params, InputBudget::default())
            .await
    }

    async fn execute_budgeted(
        &self,
        sql: &str,
        params: &[Value],
        budget: InputBudget,
    ) -> Result<ExecOutcome> {
        preflight_sqlserver_execute(sql, params, budget)?;
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| Error::Connection("SQL Server connection is closed".to_owned()))?;

        let result = client
            .execute(sql, &[])
            .await
            .map_err(|error| {
                Error::OutcomeIndeterminate(format!(
                    "SQL Server execute may have reached the backend: {error}; inspect database state before retrying"
                ))
            })?;

        Ok(ExecOutcome {
            rows_affected: result.total(),
            last_insert_id: None,
        })
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let result = self
            .query_backend("SELECT name FROM sys.schemas ORDER BY name", &[])
            .await?;

        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| match row.first() {
                Some(Value::Text(value)) => Some(value.clone()),
                _ => None,
            })
            .collect())
    }

    async fn list_schemas_bounded(&self, max_items: usize) -> Result<BoundedList<String>> {
        let (limiter, top) = sqlserver_catalog_limit(max_items)?;
        let result = self
            .query_backend(
                &format!("SELECT TOP ({top}) name FROM sys.schemas ORDER BY name"),
                &[],
            )
            .await?;
        let schemas = result
            .rows
            .into_iter()
            .map(|row| match row.first() {
                Some(Value::Text(value)) => Ok(value.clone()),
                _ => Err(Error::Serialization(
                    "SQL Server catalog schema name is not text".to_owned(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(schemas))
    }

    async fn list_schemas_budgeted(&self, budget: ReadBudget) -> Result<BoundedList<String>> {
        let (mut limiter, top) = sqlserver_budgeted_catalog_limit(budget)?;
        let result = self
            .query_backend(
                &format!("SELECT TOP ({top}) name FROM sys.schemas ORDER BY name"),
                &[],
            )
            .await?;
        let mut schemas = Vec::with_capacity(result.rows.len().min(budget.max_items));
        for row in result.rows {
            let schema = match row.first() {
                Some(Value::Text(value)) => value.clone(),
                _ => {
                    return Err(Error::Serialization(
                        "SQL Server catalog schema name is not text".to_owned(),
                    ));
                }
            };
            limiter.retain_item(schema, &mut schemas)?;
        }
        limiter.finish(schemas)
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = validate_optional_schema(schema)?.unwrap_or("dbo");
        let sql = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{}' \
             ORDER BY TABLE_NAME",
            schema
        );
        let result = self.query_backend(&sql, &[]).await?;

        Ok(result
            .rows
            .into_iter()
            .filter_map(|row| {
                let schema = value_text(row.first()?)?;
                let name = value_text(row.get(1)?)?;
                let table_type = value_text(row.get(2)?)?;
                Some(TableInfo {
                    schema: Some(schema.to_owned()),
                    name: name.to_owned(),
                    kind: if table_type.contains("VIEW") {
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
        let (limiter, top) = sqlserver_catalog_limit(max_items)?;
        let schema = validate_optional_schema(schema)?.unwrap_or("dbo");
        let sql = format!(
            "SELECT TOP ({top}) TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{schema}' \
             ORDER BY TABLE_NAME"
        );
        let result = self.query_backend(&sql, &[]).await?;
        let tables = result
            .rows
            .into_iter()
            .map(|row| {
                let schema = row.first().and_then(value_text).ok_or_else(|| {
                    Error::Serialization("SQL Server catalog table schema is not text".to_owned())
                })?;
                let name = row.get(1).and_then(value_text).ok_or_else(|| {
                    Error::Serialization("SQL Server catalog table name is not text".to_owned())
                })?;
                let table_type = row.get(2).and_then(value_text).ok_or_else(|| {
                    Error::Serialization("SQL Server catalog table type is not text".to_owned())
                })?;
                Ok(TableInfo {
                    schema: Some(schema.to_owned()),
                    name: name.to_owned(),
                    kind: if table_type.contains("VIEW") {
                        TableKind::View
                    } else {
                        TableKind::Table
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(tables))
    }

    async fn list_tables_budgeted(
        &self,
        schema: Option<&str>,
        budget: ReadBudget,
    ) -> Result<BoundedList<TableInfo>> {
        let (mut limiter, top) = sqlserver_budgeted_catalog_limit(budget)?;
        let schema = validate_optional_schema(schema)?.unwrap_or("dbo");
        let sql = format!(
            "SELECT TOP ({top}) TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{schema}' \
             ORDER BY TABLE_NAME"
        );
        let result = self.query_backend(&sql, &[]).await?;
        let mut tables = Vec::with_capacity(result.rows.len().min(budget.max_items));
        for row in result.rows {
            let schema = row.first().and_then(value_text).ok_or_else(|| {
                Error::Serialization("SQL Server catalog table schema is not text".to_owned())
            })?;
            let name = row.get(1).and_then(value_text).ok_or_else(|| {
                Error::Serialization("SQL Server catalog table name is not text".to_owned())
            })?;
            let table_type = row.get(2).and_then(value_text).ok_or_else(|| {
                Error::Serialization("SQL Server catalog table type is not text".to_owned())
            })?;
            limiter.retain_item(
                TableInfo {
                    schema: Some(schema.to_owned()),
                    name: name.to_owned(),
                    kind: if table_type.contains("VIEW") {
                        TableKind::View
                    } else {
                        TableKind::Table
                    },
                },
                &mut tables,
            )?;
        }
        limiter.finish(tables)
    }

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        self.describe_table_complete(
            table,
            MetadataBudget::new(LEGACY_SCHEMA_MAX_ITEMS, DEFAULT_METADATA_BYTES)?,
        )
        .await
    }

    async fn describe_table_bounded(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<TableSchema> {
        self.describe_table_complete(table, budget).await
    }
}

fn config_from_dsn(dsn: &Dsn) -> Result<Config> {
    let mut config = Config::new();
    config.host(dsn.host.as_deref().unwrap_or("localhost"));
    config.port(dsn.port.unwrap_or(1433));
    config.database(dsn.database.as_deref().unwrap_or("master"));
    config.application_name("dbtool");

    if let Some(user) = &dsn.username {
        let password = dsn.password.as_deref().unwrap_or_default();
        config.authentication(AuthMethod::sql_server(user, password));
    }

    if let Some(encrypt) = dsn_param(dsn, &["encrypt", "encryption"]) {
        match encrypt.to_ascii_lowercase().as_str() {
            "false" | "no" | "off" => config.encryption(EncryptionLevel::Off),
            "danger_plaintext" | "not_supported" | "notsupported" => {
                config.encryption(EncryptionLevel::NotSupported)
            }
            "optional" | "on" => config.encryption(EncryptionLevel::On),
            "true" | "yes" | "required" => config.encryption(EncryptionLevel::Required),
            other => {
                return Err(Error::Dsn(format!(
                    "unsupported SQL Server encrypt value: {other}"
                )))
            }
        }
    }

    if dsn_bool(
        dsn,
        &["trust-server-certificate", "trust_server_certificate"],
    ) {
        config.trust_cert();
    } else if let Some(ca) = dsn_param(
        dsn,
        &[
            "trust-server-certificate-ca",
            "trust_server_certificate_ca",
            "tls-ca",
            "ssl-ca",
        ],
    ) {
        config.trust_cert_ca(ca);
    }

    Ok(config)
}

fn rows_to_result_set(rows: Vec<Row>) -> Result<ResultSet> {
    let Some(first) = rows.first() else {
        return Ok(ResultSet::empty());
    };

    let columns = first.columns().iter().map(column_meta).collect();
    let rows = rows.into_iter().map(row_values).collect();

    Ok(ResultSet {
        columns,
        rows,
        truncated: false,
    })
}

fn finish_budgeted_result(
    limiter: ReadLimiter,
    columns: Vec<ColumnMeta>,
    rows: Vec<Vec<Value>>,
) -> Result<ResultSet> {
    limiter.finish_with(rows, |rows, truncated| ResultSet {
        columns,
        rows,
        truncated,
    })
}

fn column_meta(column: &Column) -> ColumnMeta {
    ColumnMeta {
        name: column.name().to_owned(),
        type_name: format!("{:?}", column.column_type()).to_ascii_lowercase(),
        nullable: true,
        primary_key: false,
        default_value: None,
    }
}

fn row_values(row: Row) -> Vec<Value> {
    row.into_iter().map(column_data_value).collect()
}

fn column_data_value(value: ColumnData<'static>) -> Value {
    match value {
        ColumnData::U8(value) => value.map_or(Value::Null, |v| Value::Int(i64::from(v))),
        ColumnData::I16(value) => value.map_or(Value::Null, |v| Value::Int(i64::from(v))),
        ColumnData::I32(value) => value.map_or(Value::Null, |v| Value::Int(i64::from(v))),
        ColumnData::I64(value) => value.map_or(Value::Null, Value::Int),
        ColumnData::F32(value) => value.map_or(Value::Null, |v| Value::Float(f64::from(v))),
        ColumnData::F64(value) => value.map_or(Value::Null, Value::Float),
        ColumnData::Bit(value) => value.map_or(Value::Null, Value::Bool),
        ColumnData::String(value) => value.map_or(Value::Null, |v| Value::Text(v.into_owned())),
        ColumnData::Guid(value) => value.map_or(Value::Null, |v| Value::Text(v.to_string())),
        ColumnData::Binary(value) => value.map_or(Value::Null, |v| Value::Bytes(v.into_owned())),
        ColumnData::Numeric(value) => value.map_or(Value::Null, |v| Value::Text(format!("{v:?}"))),
        ColumnData::Xml(value) => value.map_or(Value::Null, |v| Value::Text(format!("{v:?}"))),
        ColumnData::DateTime(value) => value.map_or(Value::Null, |v| Value::Text(format!("{v:?}"))),
        ColumnData::SmallDateTime(value) => {
            value.map_or(Value::Null, |v| Value::Text(format!("{v:?}")))
        }
        ColumnData::Time(value) => value.map_or(Value::Null, |v| Value::Text(format!("{v:?}"))),
        ColumnData::Date(value) => value.map_or(Value::Null, |v| Value::Text(format!("{v:?}"))),
        ColumnData::DateTime2(value) => {
            value.map_or(Value::Null, |v| Value::Text(format!("{v:?}")))
        }
        ColumnData::DateTimeOffset(value) => {
            value.map_or(Value::Null, |v| Value::Text(format!("{v:?}")))
        }
    }
}

fn sqlserver_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::SqlQueryBudgeted,
        CapabilityOperation::SqlExecuteBudgeted,
        CapabilityOperation::SqlListSchemasBounded,
        CapabilityOperation::SqlListSchemasBudgeted,
        CapabilityOperation::SqlListTablesBounded,
        CapabilityOperation::SqlListTablesBudgeted,
        CapabilityOperation::SqlDescribeTableBounded,
    ]);
    operations
}

fn metadata_top(limiter: &MetadataLimiter) -> Result<i64> {
    i64::try_from(limiter.probe_items()?).map_err(|_| {
        Error::Config("SQL Server metadata budget exceeds the TOP integer range".to_owned())
    })
}

fn sqlserver_columns_sql(schema: &str, name: &str, top: i64) -> String {
    format!(
        "SELECT TOP ({top}) c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.COLUMN_DEFAULT, \
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk \
         FROM INFORMATION_SCHEMA.COLUMNS c \
         LEFT JOIN ( \
             SELECT kcu.COLUMN_NAME \
             FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
             JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu \
                 ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
                 AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA \
                 AND tc.TABLE_NAME = kcu.TABLE_NAME \
             WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
               AND tc.TABLE_SCHEMA = '{schema}' AND tc.TABLE_NAME = '{name}' \
         ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME \
         WHERE c.TABLE_SCHEMA = '{schema}' AND c.TABLE_NAME = '{name}' \
         ORDER BY c.ORDINAL_POSITION"
    )
}

fn sqlserver_indexes_sql(schema: &str, name: &str, top: i64) -> String {
    format!(
        "SELECT TOP ({top}) i.name, i.is_unique, i.is_primary_key, c.name \
         FROM sys.indexes i \
         JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id \
         JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
         JOIN sys.tables t ON i.object_id = t.object_id \
         JOIN sys.schemas s ON t.schema_id = s.schema_id \
         WHERE t.name = '{name}' AND s.name = '{schema}' AND i.type > 0 \
         ORDER BY i.name, ic.key_ordinal"
    )
}

fn parse_sqlserver_column(row: &[Value]) -> Result<ColumnMeta> {
    let name = row
        .first()
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("SQL Server column name is not text".to_owned()))?;
    let data_type = row
        .get(1)
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("SQL Server column type is not text".to_owned()))?;
    let nullable = match row.get(2).and_then(value_text) {
        Some("YES") => true,
        Some("NO") => false,
        _ => {
            return Err(Error::Serialization(
                "SQL Server column nullable flag is invalid".to_owned(),
            ))
        }
    };
    let default_value = match row.get(3) {
        Some(Value::Text(value)) => Some(value.clone()),
        Some(Value::Null) => None,
        _ => {
            return Err(Error::Serialization(
                "SQL Server column default is neither text nor null".to_owned(),
            ))
        }
    };
    let primary_key = match row.get(4) {
        Some(Value::Int(0)) => false,
        Some(Value::Int(1)) => true,
        _ => {
            return Err(Error::Serialization(
                "SQL Server primary-key flag is invalid".to_owned(),
            ))
        }
    };
    Ok(ColumnMeta {
        name: name.to_owned(),
        type_name: data_type.to_owned(),
        nullable,
        primary_key,
        default_value,
    })
}

fn accumulate_sqlserver_index(
    indexes: &mut Vec<IndexInfo>,
    limiter: &mut MetadataLimiter,
    row: &[Value],
) -> Result<()> {
    let name = row
        .first()
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("SQL Server index name is not text".to_owned()))?;
    let unique = match row.get(1) {
        Some(Value::Bool(value)) => *value,
        _ => {
            return Err(Error::Serialization(
                "SQL Server index uniqueness flag is not boolean".to_owned(),
            ))
        }
    };
    let primary = match row.get(2) {
        Some(Value::Bool(value)) => *value,
        _ => {
            return Err(Error::Serialization(
                "SQL Server index primary flag is not boolean".to_owned(),
            ))
        }
    };
    let column = row
        .get(3)
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("SQL Server index column is not text".to_owned()))?;

    let is_new = match indexes.last() {
        Some(index) => index.name != name,
        None => true,
    };
    if is_new {
        limiter.observe(&("index", name, unique, primary))?;
        indexes.push(IndexInfo {
            name: name.to_owned(),
            columns: Vec::new(),
            unique,
            primary,
        });
    } else if indexes
        .last()
        .is_some_and(|index| index.unique != unique || index.primary != primary)
    {
        return Err(Error::Serialization(format!(
            "SQL Server index metadata changed within index {name}"
        )));
    }
    limiter.observe(&("index-column", column))?;
    indexes
        .last_mut()
        .expect("an index was created or already existed")
        .columns
        .push(column.to_owned());
    Ok(())
}

fn sqlserver_catalog_limit(max_items: usize) -> Result<(ListLimiter, i64)> {
    let limiter = ListLimiter::new(max_items);
    let probe_items = limiter.probe_items()?;
    let top = i64::try_from(probe_items).map_err(|_| {
        Error::Config("SQL Server catalog limit exceeds the TOP integer range".to_owned())
    })?;
    Ok((limiter, top))
}

fn sqlserver_budgeted_catalog_limit(budget: ReadBudget) -> Result<(ReadLimiter, i64)> {
    let limiter = ReadLimiter::new(budget, "SQL Server catalog response")?;
    let top = i64::try_from(limiter.probe_items()?).map_err(|_| {
        Error::Config("SQL Server catalog item budget exceeds the TOP integer range".to_owned())
    })?;
    Ok((limiter, top))
}

#[derive(Debug)]
struct TableRef {
    schema: Option<String>,
    name: String,
}

fn parse_table_ref(input: &str) -> Result<TableRef> {
    let (schema, name) = input
        .split_once('.')
        .map_or((None, input), |(schema, name)| (Some(schema), name));
    validate_identifier(name)?;
    if let Some(schema) = schema {
        validate_identifier(schema)?;
    }

    Ok(TableRef {
        schema: schema.map(str::to_owned),
        name: name.to_owned(),
    })
}

fn validate_optional_schema(schema: Option<&str>) -> Result<Option<&str>> {
    if let Some(schema) = schema {
        validate_identifier(schema)?;
    }
    Ok(schema)
}

fn validate_identifier(identifier: &str) -> Result<()> {
    let valid = !identifier.is_empty()
        && identifier
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_');

    if valid {
        Ok(())
    } else {
        Err(Error::Dsn(format!(
            "invalid SQL Server identifier: {identifier}"
        )))
    }
}

fn value_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

fn reject_dynamic_params(params: &[Value]) -> Result<()> {
    if params.is_empty() {
        Ok(())
    } else {
        Err(Error::Query(
            "SQL Server adapter does not support dynamic parameters yet".to_owned(),
        ))
    }
}

fn preflight_sqlserver_execute(sql: &str, params: &[Value], budget: InputBudget) -> Result<()> {
    let request = SqlExecuteInput { sql, params };
    let limiter = InputLimiter::new(budget, "SQL Server execute input")?;
    if params.is_empty() {
        limiter.validate_request(&request)?;
    } else {
        limiter.validate_items_with_request(params, &request)?;
    }
    if sql.as_bytes().contains(&0) {
        return Err(Error::Query(
            "SQL Server statement contains a NUL byte".to_owned(),
        ));
    }
    if sql.len() > SQLSERVER_MAX_STATEMENT_BYTES {
        return Err(Error::Query(format!(
            "SQL Server statement exceeds the fixed {SQLSERVER_MAX_STATEMENT_BYTES}-byte ceiling"
        )));
    }
    if params.len() > SQLSERVER_MAX_PARAMETERS {
        return Err(Error::Query(format!(
            "SQL Server request exceeds the fixed {SQLSERVER_MAX_PARAMETERS}-parameter ceiling"
        )));
    }
    reject_dynamic_params(params)
}

fn dsn_param<'a>(dsn: &'a Dsn, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| dsn.params.get(*name).map(String::as_str))
}

fn dsn_bool(dsn: &Dsn, names: &[&str]) -> bool {
    dsn_param(dsn, names)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_catalog_operations_and_top_limit_are_explicit() {
        let operations = sqlserver_operations(Capabilities {
            sql: true,
            ..Default::default()
        });
        assert!(operations.contains(&CapabilityOperation::SqlListSchemasBounded));
        assert!(operations.contains(&CapabilityOperation::SqlListSchemasBudgeted));
        assert!(operations.contains(&CapabilityOperation::SqlListTablesBounded));
        assert!(operations.contains(&CapabilityOperation::SqlListTablesBudgeted));
        assert!(operations.contains(&CapabilityOperation::SqlDescribeTableBounded));
        assert!(operations.contains(&CapabilityOperation::SqlQueryBudgeted));
        assert!(operations.contains(&CapabilityOperation::SqlExecuteBudgeted));
        assert!(matches!(sqlserver_catalog_limit(0), Err(Error::Config(_))));
        assert!(matches!(
            sqlserver_catalog_limit(usize::MAX),
            Err(Error::Config(_))
        ));
        assert_eq!(sqlserver_catalog_limit(2).unwrap().1, 3);
        assert!(matches!(
            sqlserver_budgeted_catalog_limit(ReadBudget {
                max_items: 0,
                max_bytes: 1,
            }),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            sqlserver_budgeted_catalog_limit(ReadBudget {
                max_items: usize::MAX,
                max_bytes: 1,
            }),
            Err(Error::Config(_))
        ));
        assert_eq!(
            sqlserver_budgeted_catalog_limit(ReadBudget::new(2, 1024).unwrap())
                .unwrap()
                .1,
            3
        );
    }

    #[test]
    fn execute_budget_preflight_is_exact_and_counts_unsupported_params_before_access() {
        let scalar_bytes = (1..=1024)
            .find(|bytes| {
                preflight_sqlserver_execute(
                    "delete from jobs",
                    &[],
                    InputBudget::new(1, *bytes, *bytes).unwrap(),
                )
                .is_ok()
            })
            .expect("small SQL Server scalar request must fit");
        preflight_sqlserver_execute(
            "delete from jobs",
            &[],
            InputBudget::new(1, scalar_bytes, scalar_bytes).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            preflight_sqlserver_execute(
                "delete from jobs",
                &[],
                InputBudget::new(1, scalar_bytes, scalar_bytes - 1).unwrap(),
            ),
            Err(Error::InputBudgetExceeded { .. })
        ));

        let params = [Value::Int(1), Value::Int(2)];
        assert!(matches!(
            preflight_sqlserver_execute(
                "update jobs set id = @P1",
                &params,
                InputBudget {
                    max_items: 1,
                    ..InputBudget::default()
                },
            ),
            Err(Error::InputBudgetExceeded { unit: "items", .. })
        ));
        assert!(matches!(
            preflight_sqlserver_execute(
                "update jobs set id = @P1",
                &params,
                InputBudget::default(),
            ),
            Err(Error::Query(_))
        ));
        assert!(matches!(
            preflight_sqlserver_execute("x\0y", &[], InputBudget::default()),
            Err(Error::Query(_))
        ));
    }

    #[test]
    fn budgeted_catalog_retains_n_probes_n_plus_one_and_enforces_exact_bytes() {
        let finish = |max_bytes: usize| -> Result<BoundedList<String>> {
            let (mut limiter, top) =
                sqlserver_budgeted_catalog_limit(ReadBudget::new(2, max_bytes)?)?;
            assert_eq!(top, 3);
            let mut retained = Vec::new();
            for item in ["alpha", "beta", "gamma"] {
                limiter.retain_item(item.to_owned(), &mut retained)?;
            }
            limiter.finish(retained)
        };
        let exact_bytes = (1..=4096)
            .find(|max_bytes| finish(*max_bytes).is_ok())
            .expect("the SQL Server catalog fixture must fit");
        let exact = finish(exact_bytes).unwrap();
        assert_eq!(exact.items, ["alpha", "beta"]);
        assert!(exact.truncated);
        assert!(matches!(
            finish(exact_bytes - 1),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == exact_bytes - 1
        ));

        let (mut limiter, _) =
            sqlserver_budgeted_catalog_limit(ReadBudget::new(2, 4096).unwrap()).unwrap();
        let mut retained = Vec::new();
        for item in ["alpha", "beta"] {
            limiter.retain_item(item.to_owned(), &mut retained).unwrap();
        }
        let complete = limiter.finish(retained).unwrap();
        assert_eq!(complete.items, ["alpha", "beta"]);
        assert!(!complete.truncated);
    }

    #[test]
    fn bounded_schema_sql_pushes_remaining_probe_to_top() {
        let columns = sqlserver_columns_sql("dbo", "users", 4);
        let indexes = sqlserver_indexes_sql("dbo", "users", 2);
        assert!(columns.starts_with("SELECT TOP (4)"));
        assert!(indexes.starts_with("SELECT TOP (2)"));
        assert!(columns.contains("ORDER BY c.ORDINAL_POSITION"));
        assert!(indexes.contains("ORDER BY i.name, ic.key_ordinal"));
    }

    #[test]
    fn bounded_schema_parsing_is_strict_and_counts_nested_index_items() {
        let column = parse_sqlserver_column(&[
            Value::Text("id".into()),
            Value::Text("int".into()),
            Value::Text("NO".into()),
            Value::Null,
            Value::Int(1),
        ])
        .unwrap();
        assert!(column.primary_key);

        let budget = MetadataBudget::new(2, DEFAULT_METADATA_BYTES).unwrap();
        let mut limiter = MetadataLimiter::new(budget, "test schema").unwrap();
        let mut indexes = Vec::new();
        accumulate_sqlserver_index(
            &mut indexes,
            &mut limiter,
            &[
                Value::Text("ix_users".into()),
                Value::Bool(false),
                Value::Bool(false),
                Value::Text("name".into()),
            ],
        )
        .unwrap();
        assert_eq!(limiter.observed_items(), 2);
        assert_eq!(indexes[0].columns, ["name"]);
        assert!(matches!(
            accumulate_sqlserver_index(
                &mut indexes,
                &mut limiter,
                &[
                    Value::Text("ix_users".into()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Text("email".into()),
                ],
            ),
            Err(Error::MetadataBudgetExceeded { unit: "items", .. })
        ));

        assert!(parse_sqlserver_column(&[Value::Text("id".into())]).is_err());
    }

    #[test]
    fn builds_config_from_url_parts() {
        let dsn = Dsn::parse(
            "sqlserver://sa:Password_123@db.example.test:11433/app?trust-server-certificate=true",
        )
        .unwrap();

        let config = config_from_dsn(&dsn).unwrap();

        assert_eq!(config.get_addr(), "db.example.test:11433");
    }

    #[test]
    fn rejects_unsafe_identifiers() {
        assert!(parse_table_ref("dbo.users").is_ok());
        assert!(parse_table_ref("dbo.users;drop").is_err());
        assert!(validate_optional_schema(Some("dbo")).is_ok());
        assert!(validate_optional_schema(Some("dbo;drop")).is_err());
    }

    #[test]
    fn maps_common_column_data_to_core_values() {
        assert_eq!(column_data_value(ColumnData::I32(Some(42))), Value::Int(42));
        assert_eq!(
            column_data_value(ColumnData::F64(Some(3.5))),
            Value::Float(3.5)
        );
        assert_eq!(
            column_data_value(ColumnData::Bit(Some(true))),
            Value::Bool(true)
        );
        assert_eq!(
            column_data_value(ColumnData::String(Some("hello".into()))),
            Value::Text("hello".to_owned())
        );
        assert_eq!(column_data_value(ColumnData::I32(None)), Value::Null);
    }

    #[test]
    fn budgeted_result_retains_n_probes_n_plus_one_and_rejects_large_units() {
        let columns = vec![ColumnMeta {
            name: "payload".to_owned(),
            type_name: "nvarchar".to_owned(),
            nullable: true,
            primary_key: false,
            default_value: None,
        }];
        let mut exact =
            ReadLimiter::new(ReadBudget::new(2, 4096).unwrap(), "SQL Server query result").unwrap();
        exact.observe_header(&columns).unwrap();
        let mut exact_rows = Vec::new();
        for value in ["one", "two"] {
            exact
                .retain_item(vec![Value::Text(value.to_owned())], &mut exact_rows)
                .unwrap();
        }
        let exact = finish_budgeted_result(exact, columns.clone(), exact_rows).unwrap();
        assert_eq!(exact.rows.len(), 2);
        assert!(!exact.truncated);

        let mut limiter =
            ReadLimiter::new(ReadBudget::new(2, 4096).unwrap(), "SQL Server query result").unwrap();
        assert_eq!(limiter.probe_items().unwrap(), 3);
        limiter.observe_header(&columns).unwrap();
        let mut rows = Vec::new();
        for value in ["one", "two", "probe"] {
            limiter
                .retain_item(vec![Value::Text(value.to_owned())], &mut rows)
                .unwrap();
        }
        let result = finish_budgeted_result(limiter, columns.clone(), rows).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert!(result.truncated);

        let mut header_probe =
            ReadLimiter::new(ReadBudget::new(1, 4096).unwrap(), "header probe").unwrap();
        header_probe.observe_header(&columns).unwrap();
        let header_bytes = header_probe.observed_bytes();
        let mut limiter = ReadLimiter::new(
            ReadBudget::new(1, header_bytes + 16).unwrap(),
            "SQL Server query result",
        )
        .unwrap();
        limiter.observe_header(&columns).unwrap();
        let mut rows = Vec::new();
        let oversized = column_data_value(ColumnData::String(Some("x".repeat(1024).into())));
        let error = limiter.retain_item(vec![oversized], &mut rows).unwrap_err();
        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
        assert!(rows.is_empty());
        assert_eq!(limiter.observed_items(), 0);
    }
}
