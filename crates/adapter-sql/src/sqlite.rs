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
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row, SqlitePool};

use crate::{
    identifier::{parse_table_ref, validate_optional_schema},
    value::{column_type_name, sqlite_value},
};

pub struct SqliteAdapter {
    pool: SqlitePool,
}

pub fn sqlite_factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        // SQLite DSN: sqlite:///absolute/path.db  or  sqlite::memory:
        let path = dsn.raw.trim_start_matches("sqlite://");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(path)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(Box::new(SqliteAdapter { pool }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for SqliteAdapter {
    fn kind(&self) -> ConnectorKind {
        ConnectorKind("sqlite".into())
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
impl SqlEngine for SqliteAdapter {
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

        let result_rows = rows
            .iter()
            .map(|row| (0..columns.len()).map(|i| sqlite_value(row, i)).collect())
            .collect();

        Ok(ResultSet {
            columns,
            rows: result_rows,
            truncated: false,
        })
    }

    async fn execute(&self, sql: &str, _params: &[Value]) -> Result<ExecOutcome> {
        let r = sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(ExecOutcome {
            rows_affected: r.rows_affected(),
            last_insert_id: Some(r.last_insert_rowid() as u64),
        })
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        Ok(vec!["main".to_owned()])
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = validate_optional_schema(schema)?;
        let catalog = match schema {
            None | Some("main") => "sqlite_master",
            Some("temp") => "sqlite_temp_master",
            Some(other) => {
                return Err(Error::Query(format!(
                    "unsupported SQLite schema for table listing: {other}"
                )))
            }
        };
        let rows = sqlx::query(&format!(
            "SELECT name, type FROM {catalog} WHERE type IN ('table','view') ORDER BY name"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|r| TableInfo {
                schema: None,
                name: r.get(0),
                kind: if r.get::<String, _>(1) == "view" {
                    TableKind::View
                } else {
                    TableKind::Table
                },
            })
            .collect())
    }

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;

        let col_rows =
            sqlx::query("SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?)")
                .bind(&table_ref.name)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;

        let columns: Vec<ColumnMeta> = col_rows
            .iter()
            .map(|r| ColumnMeta {
                name: r.get(0),
                type_name: r.get(1),
                nullable: r.get::<i32, _>(2) == 0,
                default_value: r.get::<Option<String>, _>(3),
                primary_key: r.get::<i32, _>(4) > 0,
            })
            .collect();

        let idx_list = sqlx::query("SELECT name, \"unique\" FROM pragma_index_list(?)")
            .bind(&table_ref.name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        let mut indexes: Vec<IndexInfo> = Vec::new();
        for idx_row in &idx_list {
            let idx_name: String = idx_row.get(0);
            let is_unique: bool = idx_row.get(1);
            let col_rows = sqlx::query("SELECT name FROM pragma_index_info(?)")
                .bind(&idx_name)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            let cols: Vec<String> = col_rows
                .iter()
                .filter_map(|r| r.try_get::<Option<String>, _>(0).ok().flatten())
                .collect();
            indexes.push(IndexInfo {
                primary: false,
                name: idx_name,
                columns: cols,
                unique: is_unique,
            });
        }

        let pk_cols: Vec<String> = columns
            .iter()
            .filter(|c| c.primary_key)
            .map(|c| c.name.clone())
            .collect();
        if !pk_cols.is_empty() {
            indexes.insert(
                0,
                IndexInfo {
                    name: format!("{}_pkey", table_ref.name),
                    columns: pk_cols,
                    unique: true,
                    primary: true,
                },
            );
        }

        Ok(TableSchema {
            name: table_ref.name,
            columns,
            indexes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::model::{TableKind, Value};

    async fn memory_sqlite() -> Box<dyn Connector> {
        sqlite_factory(Dsn::parse("sqlite::memory:").unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn sqlite_smoke_round_trips_typed_values_and_schema() {
        let connector = memory_sqlite().await;
        let sql = connector.as_sql().unwrap();

        sql.execute(
            "create table users (
                id integer primary key,
                active boolean not null,
                score real,
                name text,
                payload blob
            )",
            &[],
        )
        .await
        .unwrap();
        sql.execute(
            "insert into users (id, active, score, name, payload)
             values (1, true, 42.5, 'alice', x'CAFE')",
            &[],
        )
        .await
        .unwrap();

        let rows = sql
            .query(
                "select id, active, score, name, payload from users where id = 1",
                &[],
            )
            .await
            .unwrap();

        assert_eq!(rows.row_count(), 1);
        assert_eq!(rows.rows[0][0], Value::Int(1));
        assert_eq!(rows.rows[0][1], Value::Bool(true));
        assert_eq!(rows.rows[0][2], Value::Float(42.5));
        assert_eq!(rows.rows[0][3], Value::Text("alice".to_owned()));
        assert_eq!(rows.rows[0][4], Value::Bytes(vec![0xCA, 0xFE]));

        let tables = sql.list_tables(None).await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
        assert!(matches!(tables[0].kind, TableKind::Table));

        let schema = sql.describe_table("users").await.unwrap();
        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns.len(), 5);
        assert_eq!(schema.columns[0].name, "id");
        assert!(
            schema.columns[0].primary_key,
            "id should be detected as primary key"
        );
        assert!(
            !schema.columns[1].primary_key,
            "active should not be a primary key"
        );

        let pk_index = schema.indexes.iter().find(|i| i.primary);
        assert!(
            pk_index.is_some(),
            "describe_table should return a primary-key index"
        );
        assert_eq!(pk_index.unwrap().columns, vec!["id"]);
        assert!(pk_index.unwrap().unique);
    }

    #[tokio::test]
    async fn sqlite_rejects_unsafe_table_identifier_before_querying() {
        let connector = memory_sqlite().await;
        let sql = connector.as_sql().unwrap();

        let err = sql
            .describe_table("users;drop table users")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Query(_)));
    }
}
