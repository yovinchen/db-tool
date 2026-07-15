use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        BoundedList, ColumnMeta, ExecOutcome, MetadataBudget, ResultSet, TableInfo, TableKind,
        TableSchema, Value,
    },
    port::{
        capability::SqlEngine,
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{MetadataLimiter, ResultLimiter},
};
use futures::{future::BoxFuture, TryStreamExt};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgArguments, PgTypeInfo};
use sqlx::query::Query;
use sqlx::{types::Json, Column, Encode, PgPool, Postgres, Row, Type};

use crate::{
    bounded_catalog_limit, bounded_metadata_limit, group_index_rows,
    identifier::{parse_table_ref, validate_optional_schema},
    quoted_identifier, quoted_table, structured_json, timestamp_utc, validate_atomic_insert,
    value::{column_type_name, postgres_value},
};

pub struct PostgresAdapter {
    pool: PgPool,
    kind: ConnectorKind,
}

fn postgres_operations() -> Vec<CapabilityOperation> {
    let mut operations = CapabilityOperation::SQL.to_vec();
    operations.extend([
        CapabilityOperation::SqlInsertRowsAtomic,
        CapabilityOperation::SqlListSchemasBounded,
        CapabilityOperation::SqlListTablesBounded,
        CapabilityOperation::SqlDescribeTableBounded,
    ]);
    operations
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

    fn operations(&self) -> Vec<CapabilityOperation> {
        postgres_operations()
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

        let placeholders = (1..=columns.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            quoted_table(&table, '"'),
            columns
                .iter()
                .map(|column| quoted_identifier(column, '"'))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| Error::Query(error.to_string()))?;

        for row in rows {
            let query = match bind_postgres_params(&statement, row) {
                Ok(query) => query,
                Err(error) => {
                    return match transaction.rollback().await {
                        Ok(()) => Err(error),
                        Err(rollback) => Err(Error::Query(format!(
                            "{error}; PostgreSQL transaction rollback also failed: {rollback}"
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
                            "{error}; PostgreSQL transaction rollback also failed: {rollback}"
                        ))),
                    };
                }
            };
            if result.rows_affected() != 1 {
                let error = Error::Query(format!(
                    "atomic PostgreSQL insert expected one affected row, got {}",
                    result.rows_affected()
                ));
                return match transaction.rollback().await {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(Error::Query(format!(
                        "{error}; PostgreSQL transaction rollback also failed: {rollback}"
                    ))),
                };
            }
        }

        transaction
            .commit()
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        u64::try_from(rows.len())
            .map_err(|_| Error::Internal("PostgreSQL inserted row count exceeded u64".into()))
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let rows =
            sqlx::query("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
        Ok(rows.iter().map(|r| r.get::<String, _>(0)).collect())
    }

    async fn list_schemas_bounded(&self, max_items: usize) -> Result<BoundedList<String>> {
        let (limiter, sql_limit) = bounded_catalog_limit(max_items, "PostgreSQL")?;
        let rows = sqlx::query(
            "SELECT schema_name FROM information_schema.schemata \
             ORDER BY schema_name LIMIT $1",
        )
        .bind(sql_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| Error::Query(error.to_string()))?;
        let schemas = rows
            .iter()
            .map(|row| {
                row.try_get::<String, _>(0)
                    .map_err(|error| Error::Query(error.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(schemas))
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let s = validate_optional_schema(schema)?.unwrap_or("public");
        if self.kind.0 == "redshift" {
            return redshift_list_tables(&self.pool, s).await;
        }
        let rows = sqlx::query(
            "SELECT n.nspname, c.relname, c.relkind::text \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'v', 'm', 'f') \
             ORDER BY c.relname",
        )
        .bind(s)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;
        rows.iter()
            .map(|r| {
                let schema = r
                    .try_get::<String, _>(0)
                    .map_err(|error| Error::Query(error.to_string()))?;
                let name = r
                    .try_get::<String, _>(1)
                    .map_err(|error| Error::Query(error.to_string()))?;
                let relkind = r
                    .try_get::<String, _>(2)
                    .map_err(|error| Error::Query(error.to_string()))?;
                Ok(TableInfo {
                    schema: Some(schema),
                    name,
                    kind: postgres_table_kind(&relkind)?,
                })
            })
            .collect()
    }

    async fn list_tables_bounded(
        &self,
        schema: Option<&str>,
        max_items: usize,
    ) -> Result<BoundedList<TableInfo>> {
        let (limiter, sql_limit) = bounded_catalog_limit(max_items, "PostgreSQL")?;
        let schema = validate_optional_schema(schema)?.unwrap_or("public");
        let tables = if self.kind.0 == "redshift" {
            redshift_list_tables_bounded(&self.pool, schema, sql_limit).await?
        } else {
            let rows = sqlx::query(
                "SELECT n.nspname, c.relname, c.relkind::text \
                 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'v', 'm', 'f') \
                 ORDER BY c.relname LIMIT $2",
            )
            .bind(schema)
            .bind(sql_limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
            rows.iter()
                .map(|row| {
                    let schema = row
                        .try_get::<String, _>(0)
                        .map_err(|error| Error::Query(error.to_string()))?;
                    let name = row
                        .try_get::<String, _>(1)
                        .map_err(|error| Error::Query(error.to_string()))?;
                    let relkind = row
                        .try_get::<String, _>(2)
                        .map_err(|error| Error::Query(error.to_string()))?;
                    Ok(TableInfo {
                        schema: Some(schema),
                        name,
                        kind: postgres_table_kind(&relkind)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };
        Ok(limiter.finish(tables))
    }

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;
        let schema = table_ref.schema.as_deref().unwrap_or("public");
        if self.kind.0 == "redshift" {
            return redshift_describe_table(&self.pool, schema, &table_ref.name).await;
        }

        let col_rows = sqlx::query(
            "SELECT a.attname, \
                    pg_catalog.format_type(a.atttypid, a.atttypmod), \
                    a.attnotnull, \
                    CASE WHEN a.attgenerated = '' \
                         THEN pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) \
                         ELSE NULL \
                    END, \
                    EXISTS ( \
                        SELECT 1 FROM pg_catalog.pg_constraint con \
                        WHERE con.conrelid = c.oid AND con.contype = 'p' \
                          AND a.attnum = ANY(con.conkey) \
                    ) AS is_pk \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid \
             LEFT JOIN pg_catalog.pg_attrdef ad \
                    ON ad.adrelid = c.oid AND ad.adnum = a.attnum \
             WHERE n.nspname = $1 AND c.relname = $2 \
               AND c.relkind IN ('r', 'p', 'v', 'm', 'f') \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(schema)
        .bind(&table_ref.name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;

        let columns = col_rows
            .iter()
            .map(|r| {
                Ok(ColumnMeta {
                    name: r
                        .try_get(0)
                        .map_err(|error| Error::Query(error.to_string()))?,
                    type_name: r
                        .try_get(1)
                        .map_err(|error| Error::Query(error.to_string()))?,
                    nullable: !r
                        .try_get::<bool, _>(2)
                        .map_err(|error| Error::Query(error.to_string()))?,
                    default_value: r
                        .try_get(3)
                        .map_err(|error| Error::Query(error.to_string()))?,
                    primary_key: r
                        .try_get(4)
                        .map_err(|error| Error::Query(error.to_string()))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let idx_rows = sqlx::query(
            "SELECT i.relname, ix.indisunique, ix.indisprimary, a.attname \
             FROM pg_class t \
             JOIN pg_index ix ON t.oid = ix.indrelid \
             JOIN pg_class i ON i.oid = ix.indexrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS indexed_key(attnum, ord) \
                  ON indexed_key.ord <= ix.indnkeyatts \
             JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = indexed_key.attnum \
             WHERE t.relname = $1 AND n.nspname = $2 \
             ORDER BY i.relname, indexed_key.ord",
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

    async fn describe_table_bounded(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;
        let schema_name = table_ref.schema.as_deref().unwrap_or("public");
        if self.kind.0 == "redshift" {
            return redshift_describe_table_bounded(
                &self.pool,
                schema_name,
                &table_ref.name,
                budget,
            )
            .await;
        }

        let mut limiter = MetadataLimiter::new(budget, format!("PostgreSQL table schema {table}"))?;
        let column_limit = bounded_metadata_limit(&limiter, "PostgreSQL")?;
        let column_rows = sqlx::query(postgres_bounded_column_query())
            .bind(schema_name)
            .bind(&table_ref.name)
            .bind(column_limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| Error::Query(error.to_string()))?;

        let mut columns = Vec::with_capacity(column_rows.len());
        for row in column_rows {
            let column = ColumnMeta {
                name: row
                    .try_get(0)
                    .map_err(|error| Error::Query(error.to_string()))?,
                type_name: row
                    .try_get(1)
                    .map_err(|error| Error::Query(error.to_string()))?,
                nullable: !row
                    .try_get::<bool, _>(2)
                    .map_err(|error| Error::Query(error.to_string()))?,
                default_value: row
                    .try_get(3)
                    .map_err(|error| Error::Query(error.to_string()))?,
                primary_key: row
                    .try_get(4)
                    .map_err(|error| Error::Query(error.to_string()))?,
            };
            limiter.observe(&column)?;
            columns.push(column);
        }
        if columns.is_empty() {
            return Err(Error::Query(format!(
                "PostgreSQL table or view does not exist or has no readable columns: {table}"
            )));
        }

        let index_limit = bounded_metadata_limit(&limiter, "PostgreSQL")?;
        let index_rows = sqlx::query(postgres_bounded_index_query())
            .bind(&table_ref.name)
            .bind(schema_name)
            .bind(index_limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| Error::Query(error.to_string()))?;

        let mut flat_indexes = Vec::with_capacity(index_rows.len());
        let mut previous_index = None::<String>;
        for row in index_rows {
            let name = row
                .try_get::<String, _>(0)
                .map_err(|error| Error::Query(error.to_string()))?;
            let unique = row
                .try_get::<bool, _>(1)
                .map_err(|error| Error::Query(error.to_string()))?;
            let primary = row
                .try_get::<bool, _>(2)
                .map_err(|error| Error::Query(error.to_string()))?;
            let column = row
                .try_get::<String, _>(3)
                .map_err(|error| Error::Query(error.to_string()))?;
            if previous_index.as_deref() != Some(name.as_str()) {
                limiter.observe(&(name.as_str(), unique, primary))?;
                previous_index = Some(name.clone());
            }
            limiter.observe(&column)?;
            flat_indexes.push((name, unique, primary, column));
        }
        let indexes = group_index_rows(flat_indexes.into_iter());
        let schema = TableSchema {
            name: table_ref.name,
            columns,
            indexes,
        };
        limiter.ensure_complete(&schema)?;
        Ok(schema)
    }
}

fn postgres_bounded_column_query() -> &'static str {
    "SELECT a.attname, \
            pg_catalog.format_type(a.atttypid, a.atttypmod), \
            a.attnotnull, \
            CASE WHEN a.attgenerated = '' \
                 THEN pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) \
                 ELSE NULL \
            END, \
            EXISTS ( \
                SELECT 1 FROM pg_catalog.pg_constraint con \
                WHERE con.conrelid = c.oid AND con.contype = 'p' \
                  AND a.attnum = ANY(con.conkey) \
            ) AS is_pk \
     FROM pg_catalog.pg_class c \
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
     JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid \
     LEFT JOIN pg_catalog.pg_attrdef ad \
            ON ad.adrelid = c.oid AND ad.adnum = a.attnum \
     WHERE n.nspname = $1 AND c.relname = $2 \
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f') \
       AND a.attnum > 0 AND NOT a.attisdropped \
     ORDER BY a.attnum LIMIT $3"
}

fn postgres_bounded_index_query() -> &'static str {
    "SELECT i.relname, ix.indisunique, ix.indisprimary, \
            COALESCE(a.attname, pg_catalog.pg_get_indexdef(ix.indexrelid, indexed_key.ord::integer, true)) \
     FROM pg_class t \
     JOIN pg_index ix ON t.oid = ix.indrelid \
     JOIN pg_class i ON i.oid = ix.indexrelid \
     JOIN pg_namespace n ON n.oid = t.relnamespace \
     JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS indexed_key(attnum, ord) \
          ON indexed_key.ord <= ix.indnkeyatts \
     LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = indexed_key.attnum \
     WHERE t.relname = $1 AND n.nspname = $2 \
     ORDER BY i.relname, indexed_key.ord LIMIT $3"
}

async fn redshift_list_tables(pool: &PgPool, schema: &str) -> Result<Vec<TableInfo>> {
    let rows = sqlx::query(
        "SELECT table_schema, table_name, table_type \
         FROM information_schema.tables \
         WHERE table_schema = $1 ORDER BY table_name",
    )
    .bind(schema)
    .fetch_all(pool)
    .await
    .map_err(|error| Error::Query(error.to_string()))?;

    rows.iter()
        .map(|row| {
            let schema = row
                .try_get::<String, _>(0)
                .map_err(|error| Error::Query(error.to_string()))?;
            let name = row
                .try_get::<String, _>(1)
                .map_err(|error| Error::Query(error.to_string()))?;
            let table_type = row
                .try_get::<String, _>(2)
                .map_err(|error| Error::Query(error.to_string()))?;
            Ok(TableInfo {
                schema: Some(schema),
                name,
                kind: if table_type.contains("VIEW") {
                    TableKind::View
                } else {
                    TableKind::Table
                },
            })
        })
        .collect()
}

async fn redshift_list_tables_bounded(
    pool: &PgPool,
    schema: &str,
    sql_limit: i64,
) -> Result<Vec<TableInfo>> {
    let rows = sqlx::query(
        "SELECT table_schema, table_name, table_type \
         FROM information_schema.tables \
         WHERE table_schema = $1 ORDER BY table_name LIMIT $2",
    )
    .bind(schema)
    .bind(sql_limit)
    .fetch_all(pool)
    .await
    .map_err(|error| Error::Query(error.to_string()))?;

    rows.iter()
        .map(|row| {
            let schema = row
                .try_get::<String, _>(0)
                .map_err(|error| Error::Query(error.to_string()))?;
            let name = row
                .try_get::<String, _>(1)
                .map_err(|error| Error::Query(error.to_string()))?;
            let table_type = row
                .try_get::<String, _>(2)
                .map_err(|error| Error::Query(error.to_string()))?;
            Ok(TableInfo {
                schema: Some(schema),
                name,
                kind: if table_type.contains("VIEW") {
                    TableKind::View
                } else {
                    TableKind::Table
                },
            })
        })
        .collect()
}

async fn redshift_describe_table(pool: &PgPool, schema: &str, table: &str) -> Result<TableSchema> {
    let rows = sqlx::query(
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
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(|error| Error::Query(error.to_string()))?;

    let columns = rows
        .iter()
        .map(|row| {
            Ok(ColumnMeta {
                name: row
                    .try_get(0)
                    .map_err(|error| Error::Query(error.to_string()))?,
                type_name: row
                    .try_get(1)
                    .map_err(|error| Error::Query(error.to_string()))?,
                nullable: row
                    .try_get::<String, _>(2)
                    .map_err(|error| Error::Query(error.to_string()))?
                    == "YES",
                default_value: row
                    .try_get(3)
                    .map_err(|error| Error::Query(error.to_string()))?,
                primary_key: row
                    .try_get(4)
                    .map_err(|error| Error::Query(error.to_string()))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TableSchema {
        name: table.to_owned(),
        columns,
        // Amazon Redshift does not implement PostgreSQL secondary indexes.
        indexes: vec![],
    })
}

async fn redshift_describe_table_bounded(
    pool: &PgPool,
    schema: &str,
    table: &str,
    budget: MetadataBudget,
) -> Result<TableSchema> {
    let mut limiter =
        MetadataLimiter::new(budget, format!("Redshift table schema {schema}.{table}"))?;
    let column_limit = bounded_metadata_limit(&limiter, "Redshift")?;
    let rows = sqlx::query(redshift_bounded_column_query())
        .bind(schema)
        .bind(table)
        .bind(column_limit)
        .fetch_all(pool)
        .await
        .map_err(|error| Error::Query(error.to_string()))?;

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let column = ColumnMeta {
            name: row
                .try_get(0)
                .map_err(|error| Error::Query(error.to_string()))?,
            type_name: row
                .try_get(1)
                .map_err(|error| Error::Query(error.to_string()))?,
            nullable: row
                .try_get::<String, _>(2)
                .map_err(|error| Error::Query(error.to_string()))?
                == "YES",
            default_value: row
                .try_get(3)
                .map_err(|error| Error::Query(error.to_string()))?,
            primary_key: row
                .try_get(4)
                .map_err(|error| Error::Query(error.to_string()))?,
        };
        limiter.observe(&column)?;
        columns.push(column);
    }
    if columns.is_empty() {
        return Err(Error::Query(format!(
            "Redshift table or view does not exist or has no readable columns: {schema}.{table}"
        )));
    }

    let result = TableSchema {
        name: table.to_owned(),
        columns,
        // Amazon Redshift does not implement PostgreSQL secondary indexes.
        indexes: vec![],
    };
    limiter.ensure_complete(&result)?;
    Ok(result)
}

fn redshift_bounded_column_query() -> &'static str {
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
     ORDER BY c.ordinal_position LIMIT $3"
}

fn postgres_table_kind(relkind: &str) -> Result<TableKind> {
    match relkind {
        "v" => Ok(TableKind::View),
        "m" => Ok(TableKind::MaterializedView),
        "r" | "p" | "f" => Ok(TableKind::Table),
        other => Err(Error::Query(format!(
            "unsupported PostgreSQL relation kind in table metadata: {other}"
        ))),
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

    #[test]
    fn postgres_relation_kinds_preserve_materialized_views() {
        assert_eq!(postgres_table_kind("r").unwrap(), TableKind::Table);
        assert_eq!(postgres_table_kind("p").unwrap(), TableKind::Table);
        assert_eq!(postgres_table_kind("f").unwrap(), TableKind::Table);
        assert_eq!(postgres_table_kind("v").unwrap(), TableKind::View);
        assert_eq!(
            postgres_table_kind("m").unwrap(),
            TableKind::MaterializedView
        );
        assert!(postgres_table_kind("S").is_err());
    }

    #[test]
    fn postgres_family_bounded_schema_queries_keep_protocol_limits() {
        for query in [
            postgres_bounded_column_query(),
            postgres_bounded_index_query(),
            redshift_bounded_column_query(),
        ] {
            assert!(query.ends_with("LIMIT $3"));
            assert_eq!(query.matches("LIMIT $3").count(), 1);
        }
        assert!(postgres_bounded_index_query().contains("pg_get_indexdef"));
        assert!(postgres_bounded_index_query().contains("LEFT JOIN pg_attribute"));
    }

    #[test]
    fn postgres_family_advertises_bounded_table_description() {
        assert!(postgres_operations().contains(&CapabilityOperation::SqlDescribeTableBounded));
    }

    #[tokio::test]
    async fn postgres_live_metadata_distinguishes_primary_include_generated_and_matview() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_POSTGRES_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table = format!("dbtool_meta_{suffix}");
        let view = format!("dbtool_meta_mv_{suffix}");
        let connector = postgres_factory(Dsn::parse(&raw_dsn).unwrap())
            .await
            .unwrap();
        let sql = connector.as_sql().unwrap();

        sql.execute(
            &format!(
                "CREATE TABLE {table} (\
                    id bigint, base integer, payload text, \
                    generated integer GENERATED ALWAYS AS (base + 1) STORED, \
                    PRIMARY KEY (id) INCLUDE (payload))"
            ),
            &[],
        )
        .await
        .unwrap();
        sql.execute(
            &format!("CREATE MATERIALIZED VIEW {view} AS SELECT id, generated FROM {table}"),
            &[],
        )
        .await
        .unwrap();

        let metadata = async {
            Ok::<_, Error>((
                sql.describe_table(&table).await?,
                sql.describe_table(&view).await?,
                sql.list_tables(Some("public")).await?,
            ))
        }
        .await;

        let view_cleanup = sql
            .execute(&format!("DROP MATERIALIZED VIEW {view}"), &[])
            .await;
        let table_cleanup = sql.execute(&format!("DROP TABLE {table}"), &[]).await;
        view_cleanup.unwrap();
        table_cleanup.unwrap();

        let (table_schema, view_schema, tables) = metadata.unwrap();

        let id = table_schema
            .columns
            .iter()
            .find(|column| column.name == "id")
            .unwrap();
        let payload = table_schema
            .columns
            .iter()
            .find(|column| column.name == "payload")
            .unwrap();
        let generated = table_schema
            .columns
            .iter()
            .find(|column| column.name == "generated")
            .unwrap();
        assert!(id.primary_key);
        assert!(!payload.primary_key);
        assert_eq!(generated.default_value, None);
        let primary = table_schema
            .indexes
            .iter()
            .find(|index| index.primary)
            .unwrap();
        assert_eq!(primary.columns, ["id"]);
        assert_eq!(view_schema.columns.len(), 2);
        assert!(tables
            .iter()
            .any(|item| { item.name == view && item.kind == TableKind::MaterializedView }));
    }

    #[tokio::test]
    async fn postgres_live_bounded_catalog_is_schema_scoped_and_exact() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_POSTGRES_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let schema = format!("dbtool_bound_{suffix}");
        let connector = postgres_factory(Dsn::parse(&raw_dsn).unwrap())
            .await
            .unwrap();
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlListSchemasBounded));
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlListTablesBounded));
        let sql = connector.as_sql().unwrap();
        sql.execute(&format!("CREATE SCHEMA {schema}"), &[])
            .await
            .unwrap();

        let exercise = async {
            for table in ["alpha", "beta", "gamma"] {
                sql.execute(&format!("CREATE TABLE {schema}.{table} (id integer)"), &[])
                    .await?;
            }

            let limited = sql.list_tables_bounded(Some(&schema), 2).await?;
            assert_eq!(
                limited
                    .items
                    .iter()
                    .map(|table| table.qualified_name())
                    .collect::<Vec<_>>(),
                [format!("{schema}.alpha"), format!("{schema}.beta")]
            );
            assert!(limited.truncated);

            sql.execute(&format!("DROP TABLE {schema}.gamma"), &[])
                .await?;
            let exact = sql.list_tables_bounded(Some(&schema), 2).await?;
            assert_eq!(exact.items.len(), 2);
            assert!(exact
                .items
                .iter()
                .all(|table| table.schema.as_deref() == Some(schema.as_str())));
            assert!(!exact.truncated);
            Ok::<_, Error>(())
        }
        .await;

        sql.execute(&format!("DROP SCHEMA {schema} CASCADE"), &[])
            .await
            .unwrap();
        exercise.unwrap();
    }

    #[tokio::test]
    async fn postgres_live_atomic_insert_rolls_back_and_preserves_typed_values() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_POSTGRES_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table = format!("dbtool_atomic_{suffix}");
        let connector = postgres_factory(Dsn::parse(&raw_dsn).unwrap())
            .await
            .unwrap();
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlInsertRowsAtomic));
        let sql = connector.as_sql().unwrap();
        sql.execute(
            &format!(
                "CREATE TABLE {table} (\
                    id bigint PRIMARY KEY, note text NOT NULL, payload bytea NOT NULL, \
                    happened_at timestamptz NOT NULL, metadata jsonb NOT NULL)"
            ),
            &[],
        )
        .await
        .unwrap();

        let columns = vec![
            "id".into(),
            "note".into(),
            "payload".into(),
            "happened_at".into(),
            "metadata".into(),
        ];
        let timestamp = 1_700_000_000_123;
        let injection = "O'Reilly'); drop table dbtool_atomic; --";
        let exercise = async {
            let error = sql
                .insert_rows_atomic(
                    &table,
                    &columns,
                    &[
                        vec![
                            Value::Int(1),
                            Value::Text(injection.into()),
                            Value::Bytes(vec![0, 127, 255]),
                            Value::Timestamp(timestamp),
                            Value::Json(serde_json::json!({"attempt": 1})),
                        ],
                        vec![
                            Value::Int(1),
                            Value::Text("duplicate".into()),
                            Value::Bytes(vec![1]),
                            Value::Timestamp(timestamp),
                            Value::Json(serde_json::json!({"attempt": 2})),
                        ],
                    ],
                )
                .await
                .unwrap_err();
            assert!(matches!(error, Error::Query(_)));
            let empty = sql
                .query(&format!("SELECT count(*) AS total FROM {table}"), &[])
                .await?;
            assert_eq!(empty.rows[0][0], Value::Int(0));

            assert_eq!(
                sql.insert_rows_atomic(
                    &table,
                    &columns,
                    &[vec![
                        Value::Int(2),
                        Value::Text(injection.into()),
                        Value::Bytes(vec![0, 127, 255]),
                        Value::Timestamp(timestamp),
                        Value::Json(serde_json::json!({"kept": true})),
                    ]],
                )
                .await?,
                1
            );
            let row = sql
                .query(
                    &format!(
                        "SELECT note, payload, happened_at, metadata FROM {table} WHERE id = 2"
                    ),
                    &[],
                )
                .await?;
            assert_eq!(row.rows[0][0], Value::Text(injection.into()));
            assert_eq!(row.rows[0][1], Value::Bytes(vec![0, 127, 255]));
            assert_eq!(row.rows[0][2], Value::Timestamp(timestamp));
            assert_eq!(
                row.rows[0][3],
                Value::Json(serde_json::json!({"kept": true}))
            );
            Ok::<(), Error>(())
        }
        .await;

        let cleanup = sql.execute(&format!("DROP TABLE {table}"), &[]).await;
        cleanup.unwrap();
        exercise.unwrap();
    }
}
