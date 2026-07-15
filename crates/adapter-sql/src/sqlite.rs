use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ColumnMeta, ExecOutcome, IndexInfo, ResultSet, TableInfo, TableKind, TableSchema, Value,
    },
    port::{
        capability::SqlEngine,
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::ResultLimiter,
};
use futures::{future::BoxFuture, TryStreamExt};
use sqlx::query::Query;
use sqlx::sqlite::{SqliteArguments, SqlitePoolOptions};
use sqlx::{types::Json, Column, Row, Sqlite, SqlitePool};

use crate::{
    identifier::{parse_table_ref, validate_optional_schema},
    quoted_identifier, quoted_table, structured_json, timestamp_utc, validate_atomic_insert,
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

    fn operations(&self) -> Vec<CapabilityOperation> {
        let mut operations = CapabilityOperation::SQL.to_vec();
        operations.push(CapabilityOperation::SqlInsertRowsAtomic);
        operations
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

    async fn insert_rows_atomic(
        &self,
        table: &str,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> Result<u64> {
        let table = validate_atomic_insert(table, columns, rows)?;
        if rows.is_empty() {
            return Ok(0);
        }

        let statement = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quoted_table(&table, '"'),
            columns
                .iter()
                .map(|column| quoted_identifier(column, '"'))
                .collect::<Vec<_>>()
                .join(", "),
            vec!["?"; columns.len()].join(", ")
        );
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| Error::Query(error.to_string()))?;

        for row in rows {
            let query = match bind_sqlite_params(&statement, row) {
                Ok(query) => query,
                Err(error) => {
                    return match transaction.rollback().await {
                        Ok(()) => Err(error),
                        Err(rollback) => Err(Error::Query(format!(
                            "{error}; SQLite transaction rollback also failed: {rollback}"
                        ))),
                    };
                }
            };
            let result = match query.execute(&mut *transaction).await {
                Ok(result) => result,
                Err(error) => {
                    return match transaction.rollback().await {
                        Ok(()) => Err(Error::Query(error.to_string())),
                        Err(rollback) => Err(Error::Query(format!(
                            "{error}; SQLite transaction rollback also failed: {rollback}"
                        ))),
                    };
                }
            };
            if result.rows_affected() != 1 {
                let error = Error::Query(format!(
                    "atomic SQLite insert expected one affected row, got {}",
                    result.rows_affected()
                ));
                return match transaction.rollback().await {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(Error::Query(format!(
                        "{error}; SQLite transaction rollback also failed: {rollback}"
                    ))),
                };
            }
        }

        transaction
            .commit()
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        u64::try_from(rows.len())
            .map_err(|_| Error::Internal("SQLite inserted row count exceeded u64".into()))
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("PRAGMA database_list")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        rows.iter()
            .map(|row| {
                row.try_get::<String, _>(1)
                    .map_err(|error| Error::Query(error.to_string()))
            })
            .collect()
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = validate_optional_schema(schema)?.unwrap_or("main");
        let catalog = sqlite_catalog(schema);
        let rows = sqlx::query(&format!(
            "SELECT name, type FROM {catalog} \
             WHERE type IN ('table','view') ORDER BY name"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|r| TableInfo {
                schema: Some(schema.to_owned()),
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
        let schema = table_ref.schema.as_deref().unwrap_or("main");
        let catalog = sqlite_catalog(schema);

        let relation = sqlx::query(&format!(
            "SELECT name, type FROM {catalog} \
             WHERE name = ? COLLATE NOCASE AND type IN ('table','view')"
        ))
        .bind(&table_ref.name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;
        let Some(relation) = relation else {
            return Err(Error::Query(format!(
                "SQLite table or view does not exist: {schema}.{}",
                table_ref.name
            )));
        };
        let relation_name = relation
            .try_get::<String, _>(0)
            .map_err(|error| Error::Query(error.to_string()))?;

        let col_rows = sqlx::query(
            "SELECT name, type, \"notnull\", dflt_value, pk \
             FROM pragma_table_xinfo(?, ?) ORDER BY cid",
        )
        .bind(&relation_name)
        .bind(schema)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;

        let raw_columns = col_rows
            .iter()
            .map(|r| {
                Ok((
                    r.try_get::<String, _>(0)
                        .map_err(|error| Error::Query(error.to_string()))?,
                    r.try_get::<String, _>(1)
                        .map_err(|error| Error::Query(error.to_string()))?,
                    r.try_get::<i32, _>(2)
                        .map_err(|error| Error::Query(error.to_string()))?
                        != 0,
                    r.try_get::<Option<String>, _>(3)
                        .map_err(|error| Error::Query(error.to_string()))?,
                    r.try_get::<i32, _>(4)
                        .map_err(|error| Error::Query(error.to_string()))?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let idx_list = sqlx::query(
            "SELECT name, \"unique\", origin \
             FROM pragma_index_list(?, ?) ORDER BY seq",
        )
        .bind(&relation_name)
        .bind(schema)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;

        let mut indexes: Vec<IndexInfo> = Vec::new();
        for idx_row in &idx_list {
            let idx_name = idx_row
                .try_get::<String, _>(0)
                .map_err(|error| Error::Query(error.to_string()))?;
            let is_unique = idx_row
                .try_get::<i32, _>(1)
                .map_err(|error| Error::Query(error.to_string()))?
                != 0;
            let origin = idx_row
                .try_get::<String, _>(2)
                .map_err(|error| Error::Query(error.to_string()))?;
            let col_rows =
                sqlx::query("SELECT seqno, name FROM pragma_index_info(?, ?) ORDER BY seqno")
                    .bind(&idx_name)
                    .bind(schema)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| Error::Query(e.to_string()))?;
            let cols = col_rows
                .iter()
                .map(|row| {
                    let sequence = row
                        .try_get::<i32, _>(0)
                        .map_err(|error| Error::Query(error.to_string()))?;
                    Ok(row
                        .try_get::<Option<String>, _>(1)
                        .map_err(|error| Error::Query(error.to_string()))?
                        .unwrap_or_else(|| format!("<expression:{sequence}>")))
                })
                .collect::<Result<Vec<_>>>()?;
            indexes.push(IndexInfo {
                primary: origin == "pk",
                name: idx_name,
                columns: cols,
                unique: is_unique,
            });
        }

        let mut pk_cols = raw_columns
            .iter()
            .filter(|(_, _, _, _, position)| *position > 0)
            .map(|(name, _, _, _, position)| (*position, name.clone()))
            .collect::<Vec<_>>();
        pk_cols.sort_by_key(|(position, _)| *position);
        let pk_cols = pk_cols
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>();
        let has_primary_index = indexes.iter().any(|index| index.primary);
        let rowid_primary_key = !pk_cols.is_empty() && !has_primary_index;
        if rowid_primary_key {
            indexes.insert(
                0,
                IndexInfo {
                    name: format!("{relation_name}_pkey"),
                    columns: pk_cols,
                    unique: true,
                    primary: true,
                },
            );
        }

        let columns = raw_columns
            .into_iter()
            .map(
                |(name, type_name, declared_not_null, default_value, pk_position)| ColumnMeta {
                    name,
                    type_name,
                    nullable: !(declared_not_null || (rowid_primary_key && pk_position > 0)),
                    default_value,
                    primary_key: pk_position > 0,
                },
            )
            .collect();

        Ok(TableSchema {
            name: relation_name,
            columns,
            indexes,
        })
    }
}

fn sqlite_catalog(schema: &str) -> String {
    format!("\"{schema}\".sqlite_schema")
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
        assert_eq!(tables[0].schema.as_deref(), Some("main"));
        assert_eq!(tables[0].qualified_name(), "main.users");
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
    async fn sqlite_qualified_metadata_uses_the_requested_attached_schema() {
        let connector = memory_sqlite().await;
        let sql = connector.as_sql().unwrap();

        sql.execute("attach database ':memory:' as aux", &[])
            .await
            .unwrap();
        sql.execute("create table main.users (main_id integer)", &[])
            .await
            .unwrap();
        sql.execute(
            "create table aux.users (aux_id integer primary key, note text not null)",
            &[],
        )
        .await
        .unwrap();
        sql.execute(
            "create view aux.user_notes as select aux_id, note from users",
            &[],
        )
        .await
        .unwrap();

        let schemas = sql.list_schemas().await.unwrap();
        assert!(schemas.iter().any(|schema| schema == "main"));
        assert!(schemas.iter().any(|schema| schema == "aux"));

        let aux_tables = sql.list_tables(Some("aux")).await.unwrap();
        assert_eq!(aux_tables.len(), 2);
        assert!(aux_tables
            .iter()
            .all(|table| table.schema.as_deref() == Some("aux")));
        assert!(aux_tables.iter().any(|table| {
            table.qualified_name() == "aux.users" && table.kind == TableKind::Table
        }));
        assert!(aux_tables.iter().any(|table| {
            table.qualified_name() == "aux.user_notes" && table.kind == TableKind::View
        }));

        let main = sql.describe_table("main.users").await.unwrap();
        assert_eq!(main.columns[0].name, "main_id");

        let aux = sql.describe_table("aux.users").await.unwrap();
        assert_eq!(aux.columns[0].name, "aux_id");
        assert_eq!(aux.columns[1].name, "note");

        assert!(matches!(
            sql.list_tables(Some("missing")).await,
            Err(Error::Query(_))
        ));
        assert!(matches!(
            sql.describe_table("missing.users").await,
            Err(Error::Query(_))
        ));
    }

    #[tokio::test]
    async fn sqlite_schema_reports_exact_defaults_nullability_and_primary_indexes() {
        let connector = memory_sqlite().await;
        let sql = connector.as_sql().unwrap();

        sql.execute(
            "create table inventory (
                id integer primary key,
                code text not null default 'new',
                base integer,
                generated integer generated always as (base + 1)
            )",
            &[],
        )
        .await
        .unwrap();
        sql.execute(
            "create unique index inventory_code_lower on inventory(lower(code))",
            &[],
        )
        .await
        .unwrap();

        let inventory = sql.describe_table("main.inventory").await.unwrap();
        assert_eq!(
            inventory.columns.len(),
            4,
            "generated columns must be visible"
        );
        assert_eq!(inventory.columns[0].type_name, "INTEGER");
        assert!(inventory.columns[0].primary_key);
        assert!(!inventory.columns[0].nullable);
        assert_eq!(inventory.columns[1].type_name, "TEXT");
        assert!(!inventory.columns[1].nullable);
        assert_eq!(inventory.columns[1].default_value.as_deref(), Some("'new'"));
        assert_eq!(
            inventory
                .indexes
                .iter()
                .filter(|index| index.primary)
                .count(),
            1,
            "rowid primary keys should have one synthetic portable index"
        );
        assert!(inventory.indexes.iter().any(|index| {
            index.name == "inventory_code_lower"
                && index.unique
                && index.columns == ["<expression:0>"]
        }));

        sql.execute(
            "create table composite (
                tenant text,
                id integer,
                primary key (tenant, id)
            )",
            &[],
        )
        .await
        .unwrap();
        let composite = sql.describe_table("composite").await.unwrap();
        let primary = composite
            .indexes
            .iter()
            .filter(|index| index.primary)
            .collect::<Vec<_>>();
        assert_eq!(
            primary.len(),
            1,
            "SQLite autoindexes must not be duplicated"
        );
        assert_eq!(primary[0].columns, ["tenant", "id"]);
        assert!(
            composite.columns[0].nullable,
            "ordinary SQLite rowid tables permit NULL in non-INTEGER primary keys"
        );

        sql.execute("create table MixedCase (id integer primary key)", &[])
            .await
            .unwrap();
        let mixed = sql.describe_table("mixedcase").await.unwrap();
        assert_eq!(mixed.name, "MixedCase");
        assert_eq!(mixed.columns[0].name, "id");
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
    async fn sqlite_atomic_insert_binds_injection_text_and_rolls_back_every_row() {
        let connector = memory_sqlite().await;
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlInsertRowsAtomic));
        let sql = connector.as_sql().unwrap();

        sql.execute(
            "create table atomic_rows (id integer primary key, note text not null)",
            &[],
        )
        .await
        .unwrap();
        let injection = "O'Reilly'); drop table atomic_rows; --";
        let error = sql
            .insert_rows_atomic(
                "atomic_rows",
                &["id".into(), "note".into()],
                &[
                    vec![Value::Int(1), Value::Text(injection.into())],
                    vec![Value::Int(1), Value::Text("duplicate".into())],
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Query(_)));

        let empty = sql
            .query("select count(*) as total from atomic_rows", &[])
            .await
            .unwrap();
        assert_eq!(empty.rows[0][0], Value::Int(0));

        assert_eq!(
            sql.insert_rows_atomic(
                "atomic_rows",
                &["id".into(), "note".into()],
                &[vec![Value::Int(2), Value::Text(injection.into())]],
            )
            .await
            .unwrap(),
            1
        );
        let preserved = sql
            .query("select note from atomic_rows where id = 2", &[])
            .await
            .unwrap();
        assert_eq!(preserved.rows[0][0], Value::Text(injection.into()));

        for (table, columns, rows) in [
            ("bad-table", vec!["id".into()], vec![vec![Value::Int(3)]]),
            ("atomic_rows", Vec::new(), vec![Vec::new()]),
            (
                "atomic_rows",
                vec!["id".into(), "ID".into()],
                vec![vec![Value::Int(3), Value::Int(4)]],
            ),
            (
                "atomic_rows",
                vec!["id".into(), "note".into()],
                vec![vec![Value::Int(3)]],
            ),
        ] {
            assert!(sql
                .insert_rows_atomic(table, &columns, &rows)
                .await
                .is_err());
        }
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
