use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        BoundedList, ColumnMeta, ExecOutcome, InputBudget, MetadataBudget, ReadBudget, ResultSet,
        TableInfo, TableKind, TableSchema, Value,
    },
    port::{
        capability::SqlEngine,
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{MetadataLimiter, ResultLimiter},
};
use futures::{future::BoxFuture, TryStreamExt};
use sqlx::mysql::{MySqlArguments, MySqlRow};
use sqlx::query::Query;
use sqlx::{types::Json, Column, MySql, MySqlPool, Row};

use crate::{
    bounded_catalog_limit, bounded_metadata_limit, budgeted_catalog_limit, group_index_rows,
    identifier::{parse_table_ref, validate_optional_schema, TableRef},
    mutation_outcome_indeterminate, preflight_sql_atomic_insert, preflight_sql_execute,
    quoted_identifier, quoted_table, structured_json, timestamp_utc, validate_sql_statement,
    value::{column_type_name, mysql_value},
    SqlMutationLimits, SqlReadEnvelope, PORTABLE_SQL_STATEMENT_BYTES,
};

pub struct MySqlAdapter {
    pool: MySqlPool,
    kind: ConnectorKind,
}

const MYSQL_MUTATION_LIMITS: SqlMutationLimits = SqlMutationLimits {
    backend: "MySQL",
    max_statement_bytes: PORTABLE_SQL_STATEMENT_BYTES,
    max_parameters: 65_535,
    max_identifier_bytes: 64,
};

fn mysql_operations() -> Vec<CapabilityOperation> {
    let mut operations = CapabilityOperation::SQL.to_vec();
    operations.extend([
        CapabilityOperation::SqlQueryBudgeted,
        CapabilityOperation::SqlExecuteBudgeted,
        CapabilityOperation::SqlInsertRowsAtomic,
        CapabilityOperation::SqlInsertRowsAtomicBudgeted,
        CapabilityOperation::SqlListSchemasBounded,
        CapabilityOperation::SqlListSchemasBudgeted,
        CapabilityOperation::SqlListTablesBounded,
        CapabilityOperation::SqlListTablesBudgeted,
        CapabilityOperation::SqlDescribeTableBounded,
    ]);
    operations
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

async fn ensure_transactional_mysql_table(pool: &MySqlPool, table: &TableRef) -> Result<()> {
    let row = if let Some(schema) = &table.schema {
        sqlx::query(
            "SELECT t.ENGINE, e.TRANSACTIONS \
             FROM information_schema.TABLES t \
             LEFT JOIN information_schema.ENGINES e ON e.ENGINE = t.ENGINE \
             WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ? AND t.TABLE_TYPE = 'BASE TABLE'",
        )
        .bind(schema)
        .bind(&table.name)
        .fetch_optional(pool)
        .await
    } else {
        sqlx::query(
            "SELECT t.ENGINE, e.TRANSACTIONS \
             FROM information_schema.TABLES t \
             LEFT JOIN information_schema.ENGINES e ON e.ENGINE = t.ENGINE \
             WHERE t.TABLE_SCHEMA = DATABASE() AND t.TABLE_NAME = ? \
               AND t.TABLE_TYPE = 'BASE TABLE'",
        )
        .bind(&table.name)
        .fetch_optional(pool)
        .await
    }
    .map_err(|error| Error::Query(error.to_string()))?
    .ok_or_else(|| {
        Error::Query(format!(
            "MySQL atomic insert target is not an existing base table: {}",
            quoted_table(table, '`')
        ))
    })?;

    let engine = mysql_optional_text(&row, 0)?
        .ok_or_else(|| Error::Query("MySQL target table did not report a storage engine".into()))?;
    let transactions = mysql_optional_text(&row, 1)?;
    if !transactions
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("YES"))
    {
        return Err(Error::Query(format!(
            "MySQL storage engine {engine} does not guarantee transactions; atomic insert refused before writing"
        )));
    }
    Ok(())
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

    fn operations(&self) -> Vec<CapabilityOperation> {
        mysql_operations()
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
            .map(|row| {
                (0..columns.len())
                    .map(|index| mysql_value(row, index))
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
        let mut stream = bind_mysql_params(sql, params)?.fetch(&mut *connection);
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
                .map(|index| mysql_value(&row, index))
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
            // MySQL pool checkout pings wait for unread protocol frames. Close
            // a truncated result socket so a later query never drains the
            // discarded tail before it can start.
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

    async fn query_budgeted(
        &self,
        sql: &str,
        params: &[Value],
        budget: ReadBudget,
    ) -> Result<ResultSet> {
        // Validate before acquiring a socket or binding parameters so invalid
        // caller envelopes cannot trigger backend work.
        let mut envelope = SqlReadEnvelope::new(budget, &self.kind.0)?;
        let probe_rows = envelope.probe_rows()?;
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| Error::Connection(error.to_string()))?;

        // SQLx 0.8 does not expose a configurable inbound MySQL packet or
        // decoded-row ceiling. The driver can therefore assemble one protocol
        // value before dbtool charges it. Recursive charging still happens
        // before retention, and any early stop retires the socket so unread
        // frames can never be drained by a later pool checkout.
        let mut stream = bind_mysql_params(sql, params)?.fetch(&mut *connection);
        while envelope.observed_rows() < probe_rows {
            let row = match stream.try_next().await {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(error) => {
                    drop(stream);
                    connection.close_on_drop();
                    return Err(Error::Query(error.to_string()));
                }
            };

            if envelope.observed_rows() == 0 {
                let columns = row
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
                if let Err(error) = envelope.observe_columns(columns) {
                    drop(stream);
                    connection.close_on_drop();
                    return Err(error);
                }
            }

            let decoded = (0..envelope.column_count())
                .map(|index| mysql_value(&row, index))
                .collect::<Result<Vec<_>>>();
            let decoded = match decoded {
                Ok(decoded) => decoded,
                Err(error) => {
                    drop(stream);
                    connection.close_on_drop();
                    return Err(error);
                }
            };
            if let Err(error) = envelope.observe_row(decoded) {
                drop(stream);
                connection.close_on_drop();
                return Err(error);
            }
        }

        let retire_connection = envelope.observed_rows() == probe_rows;
        drop(stream);
        if retire_connection {
            connection
                .close()
                .await
                .map_err(|error| Error::Connection(error.to_string()))?;
        }
        envelope.finish()
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
        preflight_sql_execute(sql, params, budget, MYSQL_MUTATION_LIMITS)?;
        let result = bind_mysql_params(sql, params)?
            .execute(&self.pool)
            .await
            .map_err(|error| mutation_outcome_indeterminate("MySQL", "execute", error))?;
        Ok(ExecOutcome {
            rows_affected: result.rows_affected(),
            last_insert_id: Some(result.last_insert_id()),
        })
    }

    async fn insert_rows_atomic(
        &self,
        table: &str,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> Result<u64> {
        self.insert_rows_atomic_budgeted(table, columns, rows, InputBudget::default())
            .await
    }

    async fn insert_rows_atomic_budgeted(
        &self,
        table: &str,
        columns: &[String],
        rows: &[Vec<Value>],
        budget: InputBudget,
    ) -> Result<u64> {
        let table =
            preflight_sql_atomic_insert(table, columns, rows, budget, MYSQL_MUTATION_LIMITS)?;
        if rows.is_empty() {
            ensure_transactional_mysql_table(&self.pool, &table).await?;
            return Ok(0);
        }

        let statement = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quoted_table(&table, '`'),
            columns
                .iter()
                .map(|column| quoted_identifier(column, '`'))
                .collect::<Vec<_>>()
                .join(", "),
            vec!["?"; columns.len()].join(", ")
        );
        validate_sql_statement(&statement, MYSQL_MUTATION_LIMITS)?;
        let queries = rows
            .iter()
            .map(|row| bind_mysql_params(&statement, row))
            .collect::<Result<Vec<_>>>()?;
        ensure_transactional_mysql_table(&self.pool, &table).await?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| Error::Query(error.to_string()))?;

        for query in queries {
            let result = match query.execute(&mut *transaction).await {
                Ok(result) => result,
                Err(error) => {
                    return match transaction.rollback().await {
                        Ok(()) => Err(Error::Query(error.to_string())),
                        Err(rollback) => Err(mutation_outcome_indeterminate(
                            "MySQL",
                            "atomic insert rollback",
                            format!("write failed: {error}; rollback failed: {rollback}"),
                        )),
                    };
                }
            };
            if result.rows_affected() != 1 {
                let error = Error::Query(format!(
                    "atomic MySQL insert expected one affected row, got {}",
                    result.rows_affected()
                ));
                return match transaction.rollback().await {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(mutation_outcome_indeterminate(
                        "MySQL",
                        "atomic insert rollback",
                        format!("{error}; rollback failed: {rollback}"),
                    )),
                };
            }
        }

        transaction.commit().await.map_err(|error| {
            mutation_outcome_indeterminate("MySQL", "atomic insert commit", error)
        })?;
        u64::try_from(rows.len())
            .map_err(|_| Error::Internal("MySQL inserted row count exceeded u64".into()))
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SHOW DATABASES")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        rows.iter().map(|r| mysql_text(r, 0)).collect()
    }

    async fn list_schemas_bounded(&self, max_items: usize) -> Result<BoundedList<String>> {
        let (limiter, sql_limit) = bounded_catalog_limit(max_items, "MySQL")?;
        let rows = sqlx::query(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA \
             ORDER BY SCHEMA_NAME LIMIT ?",
        )
        .bind(sql_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| Error::Query(error.to_string()))?;
        let schemas = rows
            .iter()
            .map(|row| mysql_text(row, 0))
            .collect::<Result<Vec<_>>>()?;
        Ok(limiter.finish(schemas))
    }

    async fn list_schemas_budgeted(&self, budget: ReadBudget) -> Result<BoundedList<String>> {
        let (mut limiter, sql_limit) = budgeted_catalog_limit(budget, "MySQL")?;
        let rows = sqlx::query(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA \
             ORDER BY SCHEMA_NAME LIMIT ?",
        )
        .bind(sql_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| Error::Query(error.to_string()))?;
        let mut schemas = Vec::with_capacity(rows.len().min(budget.max_items));
        for row in rows {
            limiter.retain_item(mysql_text(&row, 0)?, &mut schemas)?;
        }
        limiter.finish(schemas)
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = validate_optional_schema(schema)?;
        let rows = if let Some(schema) = schema {
            sqlx::query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                 FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
            )
            .bind(schema)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                 FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME",
            )
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| Error::Query(e.to_string()))?;

        Ok(rows
            .iter()
            .map(|r| {
                let effective_schema = mysql_text(r, 0)?;
                let name = mysql_text(r, 1)?;
                let table_type = mysql_text(r, 2)?;
                Ok(TableInfo {
                    schema: Some(effective_schema),
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

    async fn list_tables_bounded(
        &self,
        schema: Option<&str>,
        max_items: usize,
    ) -> Result<BoundedList<TableInfo>> {
        let (limiter, sql_limit) = bounded_catalog_limit(max_items, "MySQL")?;
        let schema = validate_optional_schema(schema)?;
        let rows = if let Some(schema) = schema {
            sqlx::query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                 FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME LIMIT ?",
            )
            .bind(schema)
            .bind(sql_limit)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                 FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME LIMIT ?",
            )
            .bind(sql_limit)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|error| Error::Query(error.to_string()))?;

        let tables = rows
            .iter()
            .map(|row| {
                let effective_schema = mysql_text(row, 0)?;
                let name = mysql_text(row, 1)?;
                let table_type = mysql_text(row, 2)?;
                Ok(TableInfo {
                    schema: Some(effective_schema),
                    name,
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
        let (mut limiter, sql_limit) = budgeted_catalog_limit(budget, "MySQL")?;
        let schema = validate_optional_schema(schema)?;
        let rows = if let Some(schema) = schema {
            sqlx::query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                 FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME LIMIT ?",
            )
            .bind(schema)
            .bind(sql_limit)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
                 FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME LIMIT ?",
            )
            .bind(sql_limit)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|error| Error::Query(error.to_string()))?;

        let mut tables = Vec::with_capacity(rows.len().min(budget.max_items));
        for row in rows {
            let effective_schema = mysql_text(&row, 0)?;
            let name = mysql_text(&row, 1)?;
            let table_type = mysql_text(&row, 2)?;
            limiter.retain_item(
                TableInfo {
                    schema: Some(effective_schema),
                    name,
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
                    default_value: mysql_optional_text(r, 4)?,
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

    async fn describe_table_bounded(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;
        let schema = table_ref.schema.as_deref();
        let mut limiter = MetadataLimiter::new(budget, format!("MySQL table schema {table}"))?;

        let column_limit = bounded_metadata_limit(&limiter, "MySQL")?;
        let column_rows = if let Some(schema) = schema {
            sqlx::query(mysql_bounded_column_query(true))
                .bind(schema)
                .bind(&table_ref.name)
                .bind(column_limit)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query(mysql_bounded_column_query(false))
                .bind(&table_ref.name)
                .bind(column_limit)
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|error| Error::Query(error.to_string()))?;

        let mut columns = Vec::with_capacity(column_rows.len());
        for row in column_rows {
            let column = ColumnMeta {
                name: mysql_text(&row, 0)?,
                type_name: mysql_text(&row, 1)?,
                nullable: mysql_text(&row, 2)? == "YES",
                primary_key: mysql_text(&row, 3)? == "PRI",
                default_value: mysql_optional_text(&row, 4)?,
            };
            limiter.observe(&column)?;
            columns.push(column);
        }
        if columns.is_empty() {
            return Err(Error::Query(format!(
                "MySQL table or view does not exist or has no readable columns: {table}"
            )));
        }

        let index_limit = bounded_metadata_limit(&limiter, "MySQL")?;
        let index_rows = if let Some(schema) = schema {
            sqlx::query(mysql_bounded_index_query(true))
                .bind(schema)
                .bind(&table_ref.name)
                .bind(index_limit)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query(mysql_bounded_index_query(false))
                .bind(&table_ref.name)
                .bind(index_limit)
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|error| Error::Query(error.to_string()))?;

        let mut flat_indexes = Vec::with_capacity(index_rows.len());
        let mut previous_index = None::<String>;
        for row in index_rows {
            let name = mysql_text(&row, 0)?;
            let non_unique = mysql_text(&row, 1)?.parse::<u64>().map_err(|error| {
                Error::Query(format!("invalid NON_UNIQUE metadata value: {error}"))
            })?;
            let column = mysql_text(&row, 2)?;
            let unique = non_unique == 0;
            let primary = name == "PRIMARY";
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

fn mysql_bounded_column_query(qualified: bool) -> &'static str {
    if qualified {
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT \
         FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION LIMIT ?"
    } else {
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT \
         FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION LIMIT ?"
    }
}

fn mysql_bounded_index_query(qualified: bool) -> &'static str {
    if qualified {
        "SELECT INDEX_NAME, CAST(NON_UNIQUE AS CHAR), COLUMN_NAME \
         FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
         ORDER BY INDEX_NAME, SEQ_IN_INDEX LIMIT ?"
    } else {
        "SELECT INDEX_NAME, CAST(NON_UNIQUE AS CHAR), COLUMN_NAME \
         FROM information_schema.STATISTICS \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? \
         ORDER BY INDEX_NAME, SEQ_IN_INDEX LIMIT ?"
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

fn mysql_optional_text(row: &MySqlRow, index: usize) -> Result<Option<String>> {
    if let Ok(value) = row.try_get::<Option<String>, _>(index) {
        return Ok(value);
    }

    let bytes = row
        .try_get::<Option<Vec<u8>>, _>(index)
        .map_err(|error| Error::Query(error.to_string()))?;
    bytes
        .map(String::from_utf8)
        .transpose()
        .map_err(|error| Error::Serialization(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE_MAX_ITEMS: usize = 100_000;

    async fn execute_fixture(
        sql: &dyn SqlEngine,
        statement: &str,
        params: &[Value],
    ) -> Result<ExecOutcome> {
        sql.execute_budgeted(statement, params, InputBudget::default())
            .await
    }

    async fn query_fixture(
        sql: &dyn SqlEngine,
        statement: &str,
        params: &[Value],
    ) -> Result<ResultSet> {
        let result = sql
            .query_budgeted(
                statement,
                params,
                ReadBudget::new(FIXTURE_MAX_ITEMS, dbtool_core::model::DEFAULT_READ_BYTES)?,
            )
            .await?;
        if result.truncated {
            return Err(Error::Query(
                "MySQL test fixture exceeded its exact read budget".to_owned(),
            ));
        }
        Ok(result)
    }

    async fn list_schemas_fixture(sql: &dyn SqlEngine) -> Result<Vec<String>> {
        let result = sql
            .list_schemas_budgeted(ReadBudget::new(
                FIXTURE_MAX_ITEMS,
                dbtool_core::model::DEFAULT_READ_BYTES,
            )?)
            .await?;
        if result.truncated {
            return Err(Error::Query(
                "MySQL schema fixture exceeded its exact read budget".to_owned(),
            ));
        }
        Ok(result.items)
    }

    async fn list_tables_fixture(
        sql: &dyn SqlEngine,
        schema: Option<&str>,
    ) -> Result<Vec<TableInfo>> {
        let result = sql
            .list_tables_budgeted(
                schema,
                ReadBudget::new(FIXTURE_MAX_ITEMS, dbtool_core::model::DEFAULT_READ_BYTES)?,
            )
            .await?;
        if result.truncated {
            return Err(Error::Query(
                "MySQL table fixture exceeded its exact read budget".to_owned(),
            ));
        }
        Ok(result.items)
    }

    async fn describe_fixture(sql: &dyn SqlEngine, table: &str) -> Result<TableSchema> {
        sql.describe_table_bounded(
            table,
            MetadataBudget::new(
                FIXTURE_MAX_ITEMS,
                dbtool_core::model::DEFAULT_METADATA_BYTES,
            )?,
        )
        .await
    }

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

    #[test]
    fn mysql_bounded_schema_queries_keep_every_catalog_phase_server_limited() {
        for qualified in [false, true] {
            let columns = mysql_bounded_column_query(qualified);
            let indexes = mysql_bounded_index_query(qualified);
            assert!(columns.ends_with("ORDER BY ORDINAL_POSITION LIMIT ?"));
            assert!(indexes.ends_with("ORDER BY INDEX_NAME, SEQ_IN_INDEX LIMIT ?"));
            assert_eq!(columns.matches("LIMIT ?").count(), 1);
            assert_eq!(indexes.matches("LIMIT ?").count(), 1);
        }
    }

    #[test]
    fn mysql_family_advertises_budgeted_reads_and_bounded_table_description() {
        assert!(mysql_operations().contains(&CapabilityOperation::SqlQueryBudgeted));
        assert!(mysql_operations().contains(&CapabilityOperation::SqlExecuteBudgeted));
        assert!(mysql_operations().contains(&CapabilityOperation::SqlInsertRowsAtomicBudgeted));
        assert!(mysql_operations().contains(&CapabilityOperation::SqlListSchemasBudgeted));
        assert!(mysql_operations().contains(&CapabilityOperation::SqlListTablesBudgeted));
        assert!(mysql_operations().contains(&CapabilityOperation::SqlDescribeTableBounded));
    }

    #[tokio::test]
    async fn mysql_live_budgeted_query_distinguishes_n_from_n_plus_one() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_MYSQL_DSN") else {
            return;
        };
        let connector = mysql_factory(Dsn::parse(&raw_dsn).unwrap()).await.unwrap();
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlQueryBudgeted));
        let sql = connector.as_sql().unwrap();

        let limited = sql
            .query_budgeted(
                "SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4",
                &[],
                ReadBudget::new(3, dbtool_core::model::DEFAULT_READ_BYTES).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(limited.rows.len(), 3);
        assert!(limited.truncated);

        let exact = sql
            .query_budgeted(
                "SELECT ? AS value UNION ALL SELECT ? UNION ALL SELECT ?",
                &[Value::Int(7), Value::Int(8), Value::Int(9)],
                ReadBudget::new(3, dbtool_core::model::DEFAULT_READ_BYTES).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(exact.rows.len(), 3);
        assert!(!exact.truncated);
    }

    #[tokio::test]
    async fn mysql_live_metadata_exposes_effective_schema_and_reusable_identity() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_MYSQL_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table = format!("dbtool_meta_{suffix}");
        let connector = mysql_factory(Dsn::parse(&raw_dsn).unwrap()).await.unwrap();
        let sql = connector.as_sql().unwrap();

        execute_fixture(
            sql,
            &format!(
                "CREATE TABLE {table} (\
                    id bigint PRIMARY KEY, \
                    code varchar(32) NOT NULL DEFAULT 'new')"
            ),
            &[],
        )
        .await
        .unwrap();

        let metadata = async {
            let default_tables = list_tables_fixture(sql, None).await?;
            let item = default_tables
                .iter()
                .find(|item| item.name == table)
                .ok_or_else(|| Error::Query("created MySQL table was not listed".into()))?;
            let schema = item
                .schema
                .clone()
                .ok_or_else(|| Error::Query("MySQL table omitted its effective schema".into()))?;
            let explicit_tables = list_tables_fixture(sql, Some(&schema)).await?;
            let described = describe_fixture(sql, &item.qualified_name()).await?;
            Ok::<_, Error>((schema, explicit_tables, described))
        }
        .await;

        execute_fixture(sql, &format!("DROP TABLE {table}"), &[])
            .await
            .unwrap();

        let (schema, explicit_tables, described) = metadata.unwrap();
        assert!(!schema.is_empty());
        assert!(explicit_tables
            .iter()
            .any(|item| item.name == table && item.schema.as_deref() == Some(schema.as_str())));
        assert!(described.columns[0].primary_key);
        assert_eq!(described.columns[1].default_value.as_deref(), Some("new"));
    }

    #[tokio::test]
    #[allow(deprecated)] // Verifies the retained 1.x item-only catalog contract.
    async fn mysql_live_bounded_catalog_distinguishes_n_from_n_plus_one() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_MYSQL_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let tables = [
            format!("dbtool_bound_{suffix}_alpha"),
            format!("dbtool_bound_{suffix}_beta"),
            format!("dbtool_bound_{suffix}_gamma"),
        ];
        let connector = mysql_factory(Dsn::parse(&raw_dsn).unwrap()).await.unwrap();
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlListSchemasBounded));
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlListTablesBounded));
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlListSchemasBudgeted));
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlListTablesBudgeted));
        let sql = connector.as_sql().unwrap();
        let existing_count = list_tables_fixture(sql, None).await.unwrap().len();
        for table in &tables {
            execute_fixture(sql, &format!("CREATE TABLE {table} (id integer)"), &[])
                .await
                .unwrap();
        }

        let exercise = async {
            let total = existing_count + tables.len();
            let exact = sql.list_tables_bounded(None, total).await?;
            assert_eq!(exact.items.len(), total);
            assert!(!exact.truncated);

            let limited = sql.list_tables_bounded(None, total - 1).await?;
            assert_eq!(limited.items.len(), total - 1);
            assert!(limited.truncated);
            assert!(limited.items.iter().all(|table| table
                .schema
                .as_deref()
                .is_some_and(|schema| !schema.is_empty())));

            let all_tables = list_tables_fixture(sql, None).await?;
            let probe = all_tables
                .get(total - 1)
                .cloned()
                .ok_or_else(|| Error::Query("MySQL table probe item was not listed".into()))?;
            let table_bytes = serde_json::to_vec(&limited).unwrap().len()
                + serde_json::to_vec(&probe).unwrap().len();
            let budgeted = sql
                .list_tables_budgeted(None, ReadBudget::new(total - 1, table_bytes)?)
                .await?;
            assert_eq!(
                serde_json::to_value(&budgeted).unwrap(),
                serde_json::to_value(&limited).unwrap()
            );
            assert!(matches!(
                sql.list_tables_budgeted(
                    None,
                    ReadBudget::new(total - 1, table_bytes - 1)?
                )
                .await,
                Err(Error::ReadBudgetExceeded {
                    unit: "bytes",
                    limit,
                    ..
                }) if limit == table_bytes - 1
            ));

            let complete = BoundedList::complete(all_tables);
            let complete_bytes = serde_json::to_vec(&complete).unwrap().len();
            let budgeted_complete = sql
                .list_tables_budgeted(None, ReadBudget::new(total, complete_bytes)?)
                .await?;
            assert_eq!(
                serde_json::to_value(&budgeted_complete).unwrap(),
                serde_json::to_value(&complete).unwrap()
            );

            let all_schemas = list_schemas_fixture(sql).await?;
            let exact_schemas = sql.list_schemas_bounded(all_schemas.len()).await?;
            assert_eq!(exact_schemas.items, all_schemas);
            assert!(!exact_schemas.truncated);

            let schema_limit = all_schemas
                .len()
                .checked_sub(1)
                .filter(|limit| *limit > 0)
                .ok_or_else(|| {
                    Error::Query("MySQL did not expose enough schemas for an N+1 probe".into())
                })?;
            let limited_schemas = sql.list_schemas_bounded(schema_limit).await?;
            assert_eq!(limited_schemas.items.len(), schema_limit);
            assert!(limited_schemas.truncated);
            let schema_probe = all_schemas
                .get(schema_limit)
                .ok_or_else(|| Error::Query("MySQL schema probe item was not listed".into()))?;
            let limited_schema_bytes = serde_json::to_vec(&limited_schemas).unwrap().len()
                + serde_json::to_vec(schema_probe).unwrap().len();
            let budgeted_limited_schemas = sql
                .list_schemas_budgeted(ReadBudget::new(schema_limit, limited_schema_bytes)?)
                .await?;
            assert_eq!(budgeted_limited_schemas, limited_schemas);
            assert!(matches!(
                sql.list_schemas_budgeted(ReadBudget::new(
                    schema_limit,
                    limited_schema_bytes - 1,
                )?)
                .await,
                Err(Error::ReadBudgetExceeded {
                    unit: "bytes",
                    limit,
                    ..
                }) if limit == limited_schema_bytes - 1
            ));

            let schema_bytes = serde_json::to_vec(&exact_schemas).unwrap().len();
            let budgeted_schemas = sql
                .list_schemas_budgeted(ReadBudget::new(all_schemas.len(), schema_bytes)?)
                .await?;
            assert_eq!(budgeted_schemas, exact_schemas);
            assert!(matches!(
                sql.list_schemas_budgeted(ReadBudget::new(
                    all_schemas.len(),
                    schema_bytes - 1,
                )?)
                .await,
                Err(Error::ReadBudgetExceeded {
                    unit: "bytes",
                    limit,
                    ..
                }) if limit == schema_bytes - 1
            ));
            Ok::<_, Error>(())
        }
        .await;

        for table in &tables {
            execute_fixture(sql, &format!("DROP TABLE {table}"), &[])
                .await
                .unwrap();
        }
        exercise.unwrap();
    }

    #[tokio::test]
    #[allow(deprecated)] // Verifies the retained 1.x atomic-insert contract.
    async fn mysql_live_atomic_insert_rolls_back_typed_rows_and_rejects_myisam() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_MYSQL_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table = format!("dbtool_atomic_{suffix}");
        let myisam = format!("dbtool_atomic_myisam_{suffix}");
        let connector = mysql_factory(Dsn::parse(&raw_dsn).unwrap()).await.unwrap();
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::SqlInsertRowsAtomic));
        let sql = connector.as_sql().unwrap();
        execute_fixture(
            sql,
            &format!(
                "CREATE TABLE {table} (\
                    id bigint PRIMARY KEY, note text NOT NULL, payload blob NOT NULL, \
                    happened_at datetime(3) NOT NULL, metadata json NOT NULL) ENGINE=InnoDB"
            ),
            &[],
        )
        .await
        .unwrap();
        execute_fixture(
            sql,
            &format!("CREATE TABLE {myisam} (id bigint PRIMARY KEY) ENGINE=MyISAM"),
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
            let empty =
                query_fixture(sql, &format!("SELECT count(*) AS total FROM {table}"), &[]).await?;
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
            let row = query_fixture(
                sql,
                &format!("SELECT note, payload, happened_at, metadata FROM {table} WHERE id = 2"),
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

            let error = sql
                .insert_rows_atomic(&myisam, &["id".into()], &[vec![Value::Int(1)]])
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                Error::Query(message)
                    if message.contains("does not guarantee transactions")
            ));
            assert!(matches!(
                sql.insert_rows_atomic(&myisam, &["id".into()], &[])
                    .await,
                Err(Error::Query(message))
                    if message.contains("does not guarantee transactions")
            ));
            let myisam_empty =
                query_fixture(sql, &format!("SELECT count(*) AS total FROM {myisam}"), &[]).await?;
            assert_eq!(myisam_empty.rows[0][0], Value::Int(0));
            Ok::<(), Error>(())
        }
        .await;

        let cleanup_myisam = execute_fixture(sql, &format!("DROP TABLE {myisam}"), &[]).await;
        let cleanup_table = execute_fixture(sql, &format!("DROP TABLE {table}"), &[]).await;
        cleanup_myisam.unwrap();
        cleanup_table.unwrap();
        exercise.unwrap();
    }

    #[tokio::test]
    async fn mysql_live_budgeted_mutation_crud_rejects_before_write_and_cleans_up() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_MYSQL_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table = format!("dbtool_exact_{suffix}");
        let rejected_table = format!("dbtool_rejected_{suffix}");
        let connector = mysql_factory(Dsn::parse(&raw_dsn).unwrap()).await.unwrap();
        for operation in [
            CapabilityOperation::SqlExecuteBudgeted,
            CapabilityOperation::SqlInsertRowsAtomicBudgeted,
        ] {
            assert!(connector.operations().contains(&operation));
        }
        let sql = connector.as_sql().unwrap();

        assert!(matches!(
            sql.execute_budgeted(
                &format!("CREATE TABLE {rejected_table} (id bigint) ENGINE=InnoDB"),
                &[],
                InputBudget::new(1, 1, 1).unwrap(),
            )
            .await,
            Err(Error::InputBudgetExceeded { .. })
        ));
        let absent = query_fixture(
            sql,
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = database() AND table_name = ?",
            &[Value::Text(rejected_table)],
        )
        .await
        .unwrap();
        assert_eq!(absent.rows[0][0], Value::Int(0));

        sql.execute_budgeted(
            &format!(
                "CREATE TABLE {table} (id bigint PRIMARY KEY, note varchar(1024) NOT NULL) ENGINE=InnoDB"
            ),
            &[],
            InputBudget::default(),
        )
        .await
        .unwrap();
        let exercise = async {
            sql.execute_budgeted(
                &format!("INSERT INTO {table} (id, note) VALUES (?, ?)"),
                &[Value::Int(1), Value::Text("created".into())],
                InputBudget::default(),
            )
            .await?;

            let late_rows = vec![
                vec![Value::Int(2), Value::Text("short".into())],
                vec![Value::Int(3), Value::Text("late".repeat(128))],
            ];
            let short_row_bytes = serde_json::to_vec(&late_rows[0]).unwrap().len();
            assert!(matches!(
                sql.insert_rows_atomic_budgeted(
                    &table,
                    &["id".into(), "note".into()],
                    &late_rows,
                    InputBudget::new(
                        2,
                        short_row_bytes,
                        dbtool_core::model::DEFAULT_INPUT_BATCH_BYTES,
                    )?,
                )
                .await,
                Err(Error::InputBudgetExceeded { .. })
            ));
            let unchanged =
                query_fixture(sql, &format!("SELECT count(*) FROM {table}"), &[]).await?;
            assert_eq!(unchanged.rows[0][0], Value::Int(1));

            assert_eq!(
                sql.insert_rows_atomic_budgeted(
                    &table,
                    &["id".into(), "note".into()],
                    &[
                        vec![Value::Int(2), Value::Text("atomic-two".into())],
                        vec![Value::Int(3), Value::Text("atomic-three".into())],
                    ],
                    InputBudget::default(),
                )
                .await?,
                2
            );
            sql.execute_budgeted(
                &format!("UPDATE {table} SET note = ? WHERE id = ?"),
                &[Value::Text("updated".into()), Value::Int(1)],
                InputBudget::default(),
            )
            .await?;
            sql.execute_budgeted(
                &format!("DELETE FROM {table} WHERE id = ?"),
                &[Value::Int(3)],
                InputBudget::default(),
            )
            .await?;
            let readback = query_fixture(
                sql,
                &format!("SELECT id, note FROM {table} ORDER BY id"),
                &[],
            )
            .await?;
            assert_eq!(readback.rows.len(), 2);
            assert_eq!(readback.rows[0][1], Value::Text("updated".into()));
            Ok::<_, Error>(())
        }
        .await;

        sql.execute_budgeted(&format!("DROP TABLE {table}"), &[], InputBudget::default())
            .await
            .unwrap();
        exercise.unwrap();
    }
}
