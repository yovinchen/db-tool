use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{ColumnMeta, ExecOutcome, ResultSet, TableInfo, TableKind, TableSchema, Value},
    port::{
        capability::SqlEngine,
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use sqlx::mysql::{MySqlArguments, MySqlRow};
use sqlx::query::Query;
use sqlx::{types::Json, Column, MySql, MySqlPool, Row};

use crate::{
    group_index_rows,
    identifier::{parse_table_ref, validate_optional_schema},
    structured_json, timestamp_utc,
    value::{column_type_name, mysql_value},
};

pub struct MySqlAdapter {
    pool: MySqlPool,
    kind: ConnectorKind,
}

fn bind_mysql_params<'q>(
    sql: &'q str,
    params: &[Value],
) -> Result<Query<'q, MySql, MySqlArguments>> {
    let mut query = sqlx::query::<MySql>(sql);
    for (index, param) in params.iter().enumerate() {
        query = match param {
            Value::Null => query.bind(Option::<String>::None),
            Value::Bool(value) => query.bind(*value),
            Value::Int(value) => query.bind(*value),
            Value::Timestamp(value) => {
                query.bind(timestamp_utc(*value, index + 1, "MySQL")?.naive_utc())
            }
            Value::Float(value) => query.bind(*value),
            Value::Text(value) => query.bind(value.clone()),
            Value::Bytes(value) => query.bind(value.clone()),
            Value::Json(_) | Value::Array(_) | Value::Map(_) => {
                query.bind(Json(structured_json(param)?))
            }
        };
    }
    Ok(query)
}

pub fn mysql_factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let driver_url = dsn.raw_with_scheme("mysql")?;
        let pool = MySqlPool::connect(&driver_url)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(Box::new(MySqlAdapter {
            pool,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for MySqlAdapter {
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
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Error::Connection(e.to_string()))
    }

    async fn close(self: Box<Self>) -> Result<()> {
        self.pool.close().await;
        Ok(())
    }

    fn as_sql(&self) -> Option<&dyn SqlEngine> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl SqlEngine for MySqlAdapter {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet> {
        let rows = bind_mysql_params(sql, params)?
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        if rows.is_empty() {
            return Ok(ResultSet::empty());
        }

        let columns: Vec<ColumnMeta> = rows[0]
            .columns()
            .iter()
            .map(|c| ColumnMeta {
                name: c.name().to_owned(),
                type_name: column_type_name(c),
                nullable: true,
                primary_key: false,
                default_value: None,
            })
            .collect();

        let result_rows: Vec<Vec<Value>> = rows
            .iter()
            .map(|row| (0..columns.len()).map(|i| mysql_value(row, i)).collect())
            .collect();

        Ok(ResultSet {
            columns,
            rows: result_rows,
            truncated: false,
        })
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome> {
        let result = bind_mysql_params(sql, params)?
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(ExecOutcome {
            rows_affected: result.rows_affected(),
            last_insert_id: Some(result.last_insert_id()),
        })
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SHOW DATABASES")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        rows.iter().map(|r| mysql_text(r, 0)).collect()
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = validate_optional_schema(schema)?;
        let rows = if let Some(schema) = schema {
            sqlx::query(
                "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            )
            .bind(schema)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()",
            )
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| Error::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|r| {
                let name = mysql_text(r, 0)?;
                let table_type = mysql_text(r, 1)?;
                Ok(TableInfo {
                    schema: schema.map(str::to_owned),
                    name,
                    kind: if table_type.contains("VIEW") {
                        TableKind::View
                    } else {
                        TableKind::Table
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?)
    }

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;
        let schema = table_ref.schema.as_deref();

        let col_rows = if let Some(s) = schema {
            sqlx::query(
                "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT \
                 FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            )
            .bind(s)
            .bind(&table_ref.name)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT \
                 FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            )
            .bind(&table_ref.name)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| Error::Query(e.to_string()))?;

        let columns = col_rows
            .iter()
            .map(|r| {
                Ok(ColumnMeta {
                    name: mysql_text(r, 0)?,
                    type_name: mysql_text(r, 1)?,
                    nullable: mysql_text(r, 2)? == "YES",
                    primary_key: mysql_text(r, 3)? == "PRI",
                    default_value: r.try_get::<Option<String>, _>(4).ok().flatten(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let idx_rows = if let Some(s) = schema {
            sqlx::query(
                "SELECT INDEX_NAME, CAST(NON_UNIQUE AS CHAR), COLUMN_NAME \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            )
            .bind(s)
            .bind(&table_ref.name)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT INDEX_NAME, CAST(NON_UNIQUE AS CHAR), COLUMN_NAME \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            )
            .bind(&table_ref.name)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| Error::Query(e.to_string()))?;

        let index_rows = idx_rows
            .iter()
            .map(|r| {
                let name = mysql_text(r, 0)?;
                let non_unique = mysql_text(r, 1)?
                    .parse::<u64>()
                    .map_err(|e| Error::Query(format!("invalid NON_UNIQUE metadata value: {e}")))?;
                let col = mysql_text(r, 2)?;
                let unique = non_unique == 0;
                let primary = name == "PRIMARY";
                Ok((name, unique, primary, col))
            })
            .collect::<Result<Vec<_>>>()?;
        let indexes = group_index_rows(index_rows.into_iter());

        Ok(TableSchema {
            name: table_ref.name,
            columns,
            indexes,
        })
    }
}

fn mysql_text(row: &MySqlRow, index: usize) -> Result<String> {
    if let Ok(value) = row.try_get::<String, _>(index) {
        return Ok(value);
    }

    let bytes = row
        .try_get::<Vec<u8>, _>(index)
        .map_err(|e| Error::Query(e.to_string()))?;
    String::from_utf8(bytes).map_err(|e| Error::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_parameter_builder_accepts_all_portable_values() {
        let params = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(1),
            Value::Float(2.5),
            Value::Text("text".into()),
            Value::Bytes(vec![0, 255]),
            Value::Timestamp(1_700_000_000_123),
            Value::Json(serde_json::json!({"source": "test"})),
        ];
        assert!(bind_mysql_params("select ?, ?, ?, ?, ?, ?, ?, ?", &params).is_ok());

        assert!(bind_mysql_params("select ?", &[Value::Timestamp(i64::MAX)]).is_err());
    }
}
