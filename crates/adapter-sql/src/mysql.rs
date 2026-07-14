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
use sqlx::mysql::MySqlRow;
use sqlx::{Column, MySqlPool, Row};

use crate::{
    group_index_rows,
    identifier::{parse_table_ref, validate_optional_schema},
    value::{column_type_name, mysql_value},
};

pub struct MySqlAdapter {
    pool: MySqlPool,
    kind: ConnectorKind,
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
    async fn query(&self, sql: &str, _params: &[Value]) -> Result<ResultSet> {
        let rows = sqlx::query(sql)
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

    async fn execute(&self, sql: &str, _params: &[Value]) -> Result<ExecOutcome> {
        let result = sqlx::query(sql)
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
                "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME \
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
                "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME \
                 FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
                 ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            )
            .bind(&table_ref.name)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| Error::Query(e.to_string()))?;

        let indexes = group_index_rows(idx_rows.iter().filter_map(|r| {
            let name = mysql_text(r, 0).ok()?;
            let non_unique: i64 = r.try_get(1).ok()?;
            let col = mysql_text(r, 2).ok()?;
            let unique = non_unique == 0;
            let primary = name == "PRIMARY";
            Some((name, unique, primary, col))
        }));

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
