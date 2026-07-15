use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{ColumnMeta, ExecOutcome, ResultSet, TableInfo, TableKind, TableSchema, Value},
    port::{
        capability::SqlEngine,
        connector::{Capabilities, Connector, ConnectorKind},
    },
    service::limiter::ResultLimiter,
};
use futures::{future::BoxFuture, TryStreamExt};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgArguments, PgTypeInfo};
use sqlx::query::Query;
use sqlx::{types::Json, Column, Encode, PgPool, Postgres, Row, Type};

use crate::{
    group_index_rows,
    identifier::{parse_table_ref, validate_optional_schema},
    structured_json, timestamp_utc,
    value::{column_type_name, postgres_value},
};

pub struct PostgresAdapter {
    pool: PgPool,
    kind: ConnectorKind,
}

/// PostgreSQL OID 705 (`unknown`) lets the server infer a NULL parameter's
/// concrete type from its SQL context instead of incorrectly forcing TEXT.
struct PgNull;

impl Type<Postgres> for PgNull {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("unknown")
    }
}

impl<'q> Encode<'q, Postgres> for PgNull {
    fn encode_by_ref(
        &self,
        _buffer: &mut PgArgumentBuffer,
    ) -> std::result::Result<IsNull, BoxDynError> {
        Ok(IsNull::Yes)
    }
}

fn bind_postgres_params<'q>(
    sql: &'q str,
    params: &[Value],
) -> Result<Query<'q, Postgres, PgArguments>> {
    let mut query = sqlx::query::<Postgres>(sql);
    for (index, param) in params.iter().enumerate() {
        query = match param {
            Value::Null => query.bind(PgNull),
            Value::Bool(value) => query.bind(*value),
            Value::Int(value) => query.bind(*value),
            Value::Timestamp(value) => query.bind(timestamp_utc(*value, index + 1, "PostgreSQL")?),
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

pub fn postgres_factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let driver_url = dsn.raw_with_scheme("postgres")?;
        let pool = PgPool::connect(&driver_url)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(Box::new(PostgresAdapter {
            pool,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for PostgresAdapter {
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
impl SqlEngine for PostgresAdapter {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet> {
        let rows = bind_postgres_params(sql, params)?
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
                    .map(|index| postgres_value(row, index))
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
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        let mut stream = bind_postgres_params(sql, params)?.fetch(&mut *connection);
        let mut columns = Vec::new();
        let mut result_rows = Vec::new();

        while result_rows.len() < probe_rows {
            let row = match stream.try_next().await {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(error) => {
                    drop(stream);
                    connection.close_on_drop();
                    return Err(Error::Query(error.to_string()));
                }
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
            let decoded = (0..columns.len())
                .map(|index| postgres_value(&row, index))
                .collect::<Result<Vec<_>>>();
            match decoded {
                Ok(decoded) => result_rows.push(decoded),
                Err(error) => {
                    drop(stream);
                    connection.close_on_drop();
                    return Err(error);
                }
            }
        }
        let retire_connection = result_rows.len() == probe_rows;
        drop(stream);
        if retire_connection {
            // PostgreSQL may still be sending the remainder of the result.
            // Retire this socket instead of returning it to the pool, where a
            // later checkout would have to drain an unbounded response first.
            connection
                .close()
                .await
                .map_err(|e| Error::Connection(e.to_string()))?;
        }

        Ok(limiter.apply(ResultSet {
            columns,
            rows: result_rows,
            truncated: false,
        }))
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome> {
        let result = bind_postgres_params(sql, params)?
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(ExecOutcome {
            rows_affected: result.rows_affected(),
            last_insert_id: None,
        })
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let rows =
            sqlx::query("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
        Ok(rows.iter().map(|r| r.get::<String, _>(0)).collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let s = validate_optional_schema(schema)?.unwrap_or("public");
        let rows = sqlx::query(
            "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = $1",
        )
        .bind(s)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|r| TableInfo {
                schema: Some(s.to_owned()),
                name: r.get(0),
                kind: if r.get::<String, _>(1).contains("VIEW") {
                    TableKind::View
                } else {
                    TableKind::Table
                },
            })
            .collect())
    }

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;
        let schema = table_ref.schema.as_deref().unwrap_or("public");

        let col_rows = sqlx::query(
            "SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, \
                    (kcu.column_name IS NOT NULL) AS is_pk \
             FROM information_schema.columns c \
             LEFT JOIN ( \
                 SELECT kcu.column_name \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage kcu \
                     ON tc.constraint_name = kcu.constraint_name \
                     AND tc.table_schema = kcu.table_schema \
                     AND tc.table_name = kcu.table_name \
                 WHERE tc.constraint_type = 'PRIMARY KEY' \
                   AND tc.table_schema = $1 AND tc.table_name = $2 \
             ) kcu ON c.column_name = kcu.column_name \
             WHERE c.table_schema = $1 AND c.table_name = $2 \
             ORDER BY c.ordinal_position",
        )
        .bind(schema)
        .bind(&table_ref.name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;

        let columns = col_rows
            .iter()
            .map(|r| ColumnMeta {
                name: r.get(0),
                type_name: r.get(1),
                nullable: r.get::<String, _>(2) == "YES",
                primary_key: r.get::<bool, _>(4),
                default_value: r.get::<Option<String>, _>(3),
            })
            .collect();

        let idx_rows = sqlx::query(
            "SELECT i.relname, ix.indisunique, ix.indisprimary, a.attname \
             FROM pg_class t \
             JOIN pg_index ix ON t.oid = ix.indrelid \
             JOIN pg_class i ON i.oid = ix.indexrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) \
             WHERE t.relname = $1 AND n.nspname = $2 \
             ORDER BY i.relname, array_position(ix.indkey, a.attnum)",
        )
        .bind(&table_ref.name)
        .bind(schema)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;

        let indexes = group_index_rows(idx_rows.iter().map(|r| {
            let name: String = r.get(0);
            let unique: bool = r.get(1);
            let primary: bool = r.get(2);
            let col: String = r.get(3);
            (name, unique, primary, col)
        }));

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

    #[test]
    fn postgres_parameter_builder_accepts_all_portable_values() {
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
        assert!(bind_postgres_params("select $1, $2, $3, $4, $5, $6, $7, $8", &params).is_ok());

        assert!(bind_postgres_params("select $1", &[Value::Timestamp(i64::MAX)]).is_err());
    }
}
