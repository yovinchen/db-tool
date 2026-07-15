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
    service::limiter::ResultLimiter,
};
use futures::{future::BoxFuture, TryStreamExt};
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqlitePoolOptions};
use sqlx::{types::Json, Column, Row, Sqlite, SqlitePool};

use crate::{
    identifier::{parse_table_ref, validate_optional_schema},
    structured_json, timestamp_utc,
    value::{column_type_name, sqlite_value},
};

pub struct SqliteAdapter {
    pool: SqlitePool,
}

fn bind_sqlite_params<'q>(
    sql: &'q str,
    params: &[Value],
) -> Result<Query<'q, Sqlite, SqliteArguments<'q>>> {
    let mut query = sqlx::query::<Sqlite>(sql);
    for (index, param) in params.iter().enumerate() {
        query = match param {
            Value::Null => query.bind(Option::<String>::None),
            Value::Bool(value) => query.bind(*value),
            Value::Int(value) => query.bind(*value),
            Value::Timestamp(value) => query.bind(timestamp_utc(*value, index + 1, "SQLite")?),
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
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet> {
        let rows = bind_sqlite_params(sql, params)?
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
            .map(|row| {
                (0..columns.len())
                    .map(|index| sqlite_value(row, index))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ResultSet {
            columns,
            rows: result_rows,
            truncated: false,
        })
    }

    async fn query_bounded(
        &self,
        sql: &str,
        params: &[Value],
        max_rows: usize,
    ) -> Result<ResultSet> {
        let limiter = ResultLimiter::new(max_rows);
        let probe_rows = limiter.probe_rows()?;
        let mut stream = bind_sqlite_params(sql, params)?.fetch(&self.pool);
        let mut columns = Vec::new();
        let mut result_rows = Vec::new();

        while result_rows.len() < probe_rows {
            let Some(row) = stream
                .try_next()
                .await
                .map_err(|e| Error::Query(e.to_string()))?
            else {
                break;
            };

            if columns.is_empty() {
                columns = row
                    .columns()
                    .iter()
                    .map(|column| ColumnMeta {
                        name: column.name().to_owned(),
                        type_name: column_type_name(column),
                        nullable: true,
                        primary_key: false,
                        default_value: None,
                    })
                    .collect();
            }
            result_rows.push(
                (0..columns.len())
                    .map(|index| sqlite_value(&row, index))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        drop(stream);

        Ok(limiter.apply(ResultSet {
            columns,
            rows: result_rows,
            truncated: false,
        }))
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome> {
        let r = bind_sqlite_params(sql, params)?
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

    #[tokio::test]
    async fn sqlite_binds_all_supported_parameters_without_interpolation() {
        let connector = memory_sqlite().await;
        let sql = connector.as_sql().unwrap();

        sql.execute(
            "create table bound_values (
                id integer primary key,
                note text not null,
                score real not null,
                enabled boolean not null,
                payload blob not null,
                optional text
            )",
            &[],
        )
        .await
        .unwrap();

        let injection = "O'Reilly'); drop table bound_values; --";
        let outcome = sql
            .execute(
                "insert into bound_values (id, note, score, enabled, payload, optional)
                 values (?, ?, ?, ?, ?, ?)",
                &[
                    Value::Int(7),
                    Value::Text(injection.into()),
                    Value::Float(12.75),
                    Value::Bool(true),
                    Value::Bytes(vec![0, 0x7f, 0xff]),
                    Value::Null,
                ],
            )
            .await
            .unwrap();
        assert_eq!(outcome.rows_affected, 1);

        let rows = sql
            .query(
                "select id, note, score, enabled, payload, optional
                 from bound_values where id = ? and note = ?",
                &[Value::Int(7), Value::Text(injection.into())],
            )
            .await
            .unwrap();

        assert_eq!(rows.row_count(), 1);
        assert_eq!(rows.rows[0][0], Value::Int(7));
        assert_eq!(rows.rows[0][1], Value::Text(injection.into()));
        assert_eq!(rows.rows[0][2], Value::Float(12.75));
        assert_eq!(rows.rows[0][3], Value::Bool(true));
        assert_eq!(rows.rows[0][4], Value::Bytes(vec![0, 0x7f, 0xff]));
        assert_eq!(rows.rows[0][5], Value::Null);

        let table_survived = sql
            .query("select count(*) as total from bound_values", &[])
            .await
            .unwrap();
        assert_eq!(table_survived.rows[0][0], Value::Int(1));
    }

    #[tokio::test]
    async fn sqlite_binds_timestamp_and_json_parameters() {
        let connector = memory_sqlite().await;
        let sql = connector.as_sql().unwrap();

        let rows = sql
            .query(
                "select CAST(strftime('%s', ?) AS INTEGER) * 1000 as timestamp_ms,
                        json_extract(?, '$.id') as json_id",
                &[
                    Value::Timestamp(1_700_000_000_123),
                    Value::Json(serde_json::json!({"id": 7})),
                ],
            )
            .await
            .unwrap();

        assert_eq!(rows.rows[0][0], Value::Int(1_700_000_000_000));
        assert_eq!(rows.rows[0][1], Value::Int(7));
    }

    #[tokio::test]
    async fn sqlite_bounded_query_streams_one_probe_row_and_preserves_params() {
        let connector = memory_sqlite().await;
        let sql = connector.as_sql().unwrap();

        let limited = sql
            .query_bounded(
                "with recursive numbers(value) as (
                    select 1
                    union all
                    select value + 1 from numbers where value < 10000
                 )
                 select value from numbers",
                &[],
                3,
            )
            .await
            .unwrap();
        assert_eq!(
            limited.rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );
        assert!(limited.truncated);

        let exact = sql
            .query_bounded(
                "select ? as value union all select ? union all select ?",
                &[Value::Int(7), Value::Int(8), Value::Int(9)],
                3,
            )
            .await
            .unwrap();
        assert_eq!(
            exact.rows,
            vec![
                vec![Value::Int(7)],
                vec![Value::Int(8)],
                vec![Value::Int(9)]
            ]
        );
        assert!(!exact.truncated);

        let empty = sql
            .query_bounded("select 1 as value where false", &[], 3)
            .await
            .unwrap();
        assert!(empty.rows.is_empty());
        assert!(!empty.truncated);
    }

    #[tokio::test]
    async fn sqlite_bounded_query_rejects_invalid_limits_before_sql() {
        let connector = memory_sqlite().await;
        let sql = connector.as_sql().unwrap();

        assert!(matches!(
            sql.query_bounded("not valid sql", &[], 0).await,
            Err(Error::Config(_))
        ));
        assert!(matches!(
            sql.query_bounded("not valid sql", &[], usize::MAX).await,
            Err(Error::Config(_))
        ));
    }
}
