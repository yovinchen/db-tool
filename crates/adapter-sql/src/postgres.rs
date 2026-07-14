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
use sqlx::{Column, PgPool, Row};

use crate::{
    group_index_rows,
    identifier::{parse_table_ref, validate_optional_schema},
    value::{column_type_name, postgres_value},
};

pub struct PostgresAdapter {
    pool: PgPool,
    kind: ConnectorKind,
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
            .map(|row| (0..columns.len()).map(|i| postgres_value(row, i)).collect())
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
             JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkeys) \
             WHERE t.relname = $1 AND n.nspname = $2 \
             ORDER BY i.relname, array_position(ix.indkeys, a.attnum)",
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
