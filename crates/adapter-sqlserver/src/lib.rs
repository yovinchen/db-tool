use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ColumnMeta, ExecOutcome, IndexInfo, ResultSet, TableInfo, TableKind, TableSchema, Value,
    },
    port::{
        capability::SqlEngine,
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use tiberius::{AuthMethod, Client, Column, ColumnData, Config, EncryptionLevel, Row};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

type SqlServerClient = Client<Compat<TcpStream>>;

pub struct SqlServerAdapter {
    client: Mutex<Option<SqlServerClient>>,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let config = config_from_dsn(&dsn)?;
        let addr = config.get_addr();
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| Error::Connection(format!("{addr}: {e}")))?;
        tcp.set_nodelay(true)
            .map_err(|e| Error::Connection(e.to_string()))?;

        let client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        Ok(Box::new(SqlServerAdapter {
            client: Mutex::new(Some(client)),
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
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

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome> {
        reject_dynamic_params(params)?;
        let mut guard = self.client.lock().await;
        let client = guard
            .as_mut()
            .ok_or_else(|| Error::Connection("SQL Server connection is closed".to_owned()))?;

        let result = client
            .execute(sql, &[])
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        Ok(ExecOutcome {
            rows_affected: result.total(),
            last_insert_id: None,
        })
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let result = self
            .query("SELECT name FROM sys.schemas ORDER BY name", &[])
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

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = validate_optional_schema(schema)?.unwrap_or("dbo");
        let sql = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = '{}' \
             ORDER BY TABLE_NAME",
            schema
        );
        let result = self.query(&sql, &[]).await?;

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

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;
        let schema = table_ref.schema.as_deref().unwrap_or("dbo");

        let col_sql = format!(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.COLUMN_DEFAULT, \
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
             ORDER BY c.ORDINAL_POSITION",
            schema = schema,
            name = table_ref.name,
        );
        let col_result = self.query(&col_sql, &[]).await?;

        let columns = col_result
            .rows
            .into_iter()
            .filter_map(|row| {
                let name = value_text(row.first()?)?.to_owned();
                let data_type = value_text(row.get(1)?)?.to_owned();
                let nullable = value_text(row.get(2)?)? == "YES";
                let default_value = match row.get(3)? {
                    Value::Text(s) => Some(s.clone()),
                    _ => None,
                };
                let primary_key = matches!(row.get(4)?, Value::Int(1));
                Some(ColumnMeta {
                    name,
                    type_name: data_type,
                    nullable,
                    primary_key,
                    default_value,
                })
            })
            .collect();

        let idx_sql = format!(
            "SELECT i.name, i.is_unique, i.is_primary_key, c.name \
             FROM sys.indexes i \
             JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id \
             JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
             JOIN sys.tables t ON i.object_id = t.object_id \
             JOIN sys.schemas s ON t.schema_id = s.schema_id \
             WHERE t.name = '{name}' AND s.name = '{schema}' AND i.type > 0 \
             ORDER BY i.name, ic.key_ordinal",
            schema = schema,
            name = table_ref.name,
        );
        let idx_result = self.query(&idx_sql, &[]).await?;

        let mut indexes: Vec<IndexInfo> = Vec::new();
        for row in idx_result.rows {
            let idx_name = match row.first() {
                Some(Value::Text(s)) => s.clone(),
                _ => continue,
            };
            let unique = matches!(row.get(1), Some(Value::Bool(true)));
            let primary = matches!(row.get(2), Some(Value::Bool(true)));
            let col = match row.get(3) {
                Some(Value::Text(s)) => s.clone(),
                _ => continue,
            };
            match indexes.last_mut() {
                Some(idx) if idx.name == idx_name => idx.columns.push(col),
                _ => indexes.push(IndexInfo {
                    name: idx_name,
                    columns: vec![col],
                    unique,
                    primary,
                }),
            }
        }

        Ok(TableSchema {
            name: table_ref.name,
            columns,
            indexes,
        })
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
}
