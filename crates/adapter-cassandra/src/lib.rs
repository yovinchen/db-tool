use std::{
    collections::BTreeMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        BoundedList, ColumnMeta, ExecOutcome, IndexInfo, InputBudget, MetadataBudget, ReadBudget,
        ResultSet, TableInfo, TableKind, TableSchema, Value, DEFAULT_METADATA_BYTES,
        MAX_INPUT_BYTES,
    },
    port::{
        capability::{CqlEngine, SqlEngine},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{InputLimiter, ListLimiter, MetadataLimiter, ReadLimiter, ResultLimiter},
};
use futures::{future::BoxFuture, StreamExt};
use scylla::{
    client::{
        execution_profile::ExecutionProfile, session::Session, session_builder::SessionBuilder,
    },
    errors::TranslationError,
    frame::response::result::{CollectionType, ColumnType, NativeType},
    policies::address_translator::{AddressTranslator, UntranslatedPeer},
    response::query_result::QueryRowsResult,
    statement::unprepared::Statement,
    value::{CqlValue, Row},
};

pub struct CassandraAdapter {
    session: Session,
    kind: ConnectorKind,
    keyspace: Option<String>,
}

const LEGACY_SCHEMA_MAX_ITEMS: usize = 100_000;
const CASSANDRA_MAX_CQL_BYTES: usize = MAX_INPUT_BYTES;

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let contact_point = contact_point_from_dsn(&dsn)?;
        let mut builder = SessionBuilder::new()
            .known_node(contact_point)
            .connection_timeout(duration_param(&dsn, "connect-timeout-ms", 5_000)?);

        if let Some(timeout) = optional_duration_param(&dsn, "request-timeout-ms")? {
            let profile = ExecutionProfile::builder()
                .request_timeout(Some(timeout))
                .build();
            builder = builder.default_execution_profile_handle(profile.into_handle());
        }

        if let Some(user) = &dsn.username {
            builder = builder.user(user.clone(), dsn.password.clone().unwrap_or_default());
        }

        if use_contact_point_translation(&dsn)? {
            builder = builder.address_translator(Arc::new(ContactPointTranslator {
                target: contact_socket_addr_from_dsn(&dsn)?,
            }));
        }

        if let Some(keyspace) = &dsn.database {
            builder = builder.use_keyspace(keyspace.clone(), false);
        }

        let session = builder
            .build()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        Ok(Box::new(CassandraAdapter {
            session,
            kind: ConnectorKind(dsn.scheme),
            keyspace: dsn.database,
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for CassandraAdapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            sql: true,
            cql: true,
            ..Default::default()
        }
    }

    fn operations(&self) -> Vec<CapabilityOperation> {
        cassandra_operations(self.capabilities())
    }

    async fn ping(&self) -> Result<()> {
        self.session
            .query_unpaged("SELECT release_version FROM system.local", &[])
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_sql(&self) -> Option<&dyn SqlEngine> {
        Some(self)
    }

    fn as_cql(&self) -> Option<&dyn CqlEngine> {
        Some(self)
    }
}

impl CassandraAdapter {
    async fn send_cql_mutation(&self, cql: &str) -> Result<ExecOutcome> {
        self.session
            .query_unpaged(cql, &[])
            .await
            .map_err(|error| {
                Error::OutcomeIndeterminate(format!(
                    "Cassandra CQL mutation may have reached the backend: {error}; inspect database state before retrying"
                ))
            })?;

        Ok(ExecOutcome {
            rows_affected: 0,
            last_insert_id: None,
        })
    }

    async fn query_catalog_bounded<T, F>(
        &self,
        cql: &str,
        max_items: usize,
        mut convert: F,
    ) -> Result<BoundedList<T>>
    where
        F: FnMut(Vec<Value>) -> Result<Option<T>>,
    {
        let (limiter, probe_items, page_size) = bounded_catalog_plan(max_items)?;
        let statement = Statement::new(cql).with_page_size(page_size);
        let pager = self
            .session
            .query_iter(statement, &[])
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        let mut rows = pager
            .rows_stream::<Row>()
            .map_err(|error| Error::Query(error.to_string()))?;

        // The caller's logical budget can be much larger than the actual
        // catalog. Do not reserve that entire amount up front: each server
        // page and the initial allocation stay capped while the loop still
        // stops exactly at the N+1 probe item.
        let mut items = Vec::with_capacity(probe_items.min(256));
        while items.len() < probe_items {
            let Some(row) = rows.next().await else {
                break;
            };
            let row = row.map_err(|error| Error::Query(error.to_string()))?;
            if let Some(item) = convert(cql_row_values(row))? {
                items.push(item);
            }
        }
        drop(rows);
        Ok(limiter.finish(items))
    }

    async fn query_catalog_budgeted<T, F, C>(
        &self,
        cql: &str,
        budget: ReadBudget,
        mut convert: F,
        complete: C,
    ) -> Result<BoundedList<T>>
    where
        F: FnMut(Vec<Value>, &mut ReadLimiter, &mut Vec<T>) -> Result<()>,
        C: FnOnce(ReadLimiter, Vec<T>) -> Result<BoundedList<T>>,
    {
        let (mut limiter, probe_items, page_size) = budgeted_catalog_plan(budget)?;
        let statement = Statement::new(cql).with_page_size(page_size);
        let pager = self
            .session
            .query_iter(statement, &[])
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        let mut rows = pager
            .rows_stream::<Row>()
            .map_err(|error| Error::Query(error.to_string()))?;

        let mut items = Vec::with_capacity(budget.max_items.min(256));
        while limiter.observed_items() < probe_items {
            let Some(row) = rows.next().await else {
                break;
            };
            let row = row.map_err(|error| Error::Query(error.to_string()))?;
            convert(cql_row_values(row), &mut limiter, &mut items)?;
        }
        drop(rows);
        complete(limiter, items)
    }

    async fn query_metadata_rows(
        &self,
        cql: &str,
        limiter: &MetadataLimiter,
    ) -> Result<Vec<Vec<Value>>> {
        let probe_items = limiter.probe_items()?;
        let page_size = metadata_page_size(probe_items)?;
        let statement = Statement::new(cql).with_page_size(page_size);
        let pager = self
            .session
            .query_iter(statement, &[])
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        let mut stream = pager
            .rows_stream::<Row>()
            .map_err(|error| Error::Query(error.to_string()))?;
        let mut rows = Vec::with_capacity(probe_items.min(256));
        while rows.len() < probe_items {
            let Some(row) = stream.next().await else {
                break;
            };
            rows.push(cql_row_values(
                row.map_err(|error| Error::Query(error.to_string()))?,
            ));
        }
        drop(stream);
        Ok(rows)
    }

    async fn describe_table_complete(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<TableSchema> {
        let table_ref = parse_table_ref(table)?;
        let keyspace = table_ref
            .schema
            .as_deref()
            .or(self.keyspace.as_deref())
            .ok_or_else(|| {
                Error::Dsn(
                    "Cassandra table schema requires keyspace.table or a DSN keyspace".to_owned(),
                )
            })?;
        let keyspace = validate_identifier(keyspace, "keyspace")?;
        let mut limiter = MetadataLimiter::new(budget, "Cassandra table schema")?;
        let column_rows = self
            .query_metadata_rows(
                &format!(
                    "SELECT column_name, type, kind, position FROM system_schema.columns \
                     WHERE keyspace_name = '{}' AND table_name = '{}'",
                    keyspace, table_ref.name
                ),
                &limiter,
            )
            .await?;
        if column_rows.is_empty() {
            return Err(Error::Query(format!(
                "Cassandra table does not exist: {keyspace}.{}",
                table_ref.name
            )));
        }

        let mut columns = Vec::with_capacity(column_rows.len());
        for row in column_rows {
            let column = parse_described_column(&row)?;
            limiter.observe(&column.meta)?;
            columns.push(column);
        }
        columns.sort_by(|left, right| {
            left.kind_rank
                .cmp(&right.kind_rank)
                .then_with(|| left.position.cmp(&right.position))
                .then_with(|| left.meta.name.cmp(&right.meta.name))
        });

        let mut indexes = Vec::new();
        if let Some(primary) = primary_index(&columns) {
            limiter.observe(&("index", &primary.name, primary.unique, primary.primary))?;
            for column in &primary.columns {
                limiter.observe(&("index-column", column))?;
            }
            indexes.push(primary);
        }

        let index_rows = self
            .query_metadata_rows(
                &format!(
                    "SELECT index_name, options FROM system_schema.indexes \
                     WHERE keyspace_name = '{}' AND table_name = '{}'",
                    keyspace, table_ref.name
                ),
                &limiter,
            )
            .await?;
        for row in index_rows {
            let index = parse_cassandra_index(&row)?;
            limiter.observe(&("index", &index.name, index.unique, index.primary))?;
            for column in &index.columns {
                limiter.observe(&("index-column", column))?;
            }
            indexes.push(index);
        }

        let schema = TableSchema {
            name: table_ref.name,
            columns: columns.into_iter().map(|column| column.meta).collect(),
            indexes,
        };
        limiter.ensure_complete(&schema)?;
        Ok(schema)
    }
}

fn cassandra_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::SqlQueryBudgeted,
        CapabilityOperation::CqlQueryBudgeted,
        CapabilityOperation::CqlExecuteBudgeted,
        CapabilityOperation::SqlListSchemasBounded,
        CapabilityOperation::SqlListSchemasBudgeted,
        CapabilityOperation::SqlListTablesBounded,
        CapabilityOperation::SqlListTablesBudgeted,
        CapabilityOperation::CqlListKeyspacesBounded,
        CapabilityOperation::CqlListKeyspacesBudgeted,
        CapabilityOperation::CqlListTablesBounded,
        CapabilityOperation::CqlListTablesBudgeted,
        CapabilityOperation::SqlDescribeTableBounded,
        CapabilityOperation::CqlDescribeTableBounded,
    ]);
    operations
}

fn metadata_page_size(probe_items: usize) -> Result<i32> {
    i32::try_from(probe_items.min(256))
        .map_err(|_| Error::Internal("bounded CQL metadata page size overflow".to_owned()))
}

fn bounded_catalog_plan(max_items: usize) -> Result<(ListLimiter, usize, i32)> {
    let limiter = ListLimiter::new(max_items);
    let probe_items = limiter.probe_items()?;
    let page_size = i32::try_from(probe_items.min(256))
        .map_err(|_| Error::Internal("bounded CQL catalog page size overflow".to_owned()))?;
    Ok((limiter, probe_items, page_size))
}

fn budgeted_catalog_plan(budget: ReadBudget) -> Result<(ReadLimiter, usize, i32)> {
    let limiter = ReadLimiter::new(budget, "Cassandra catalog response")?;
    let probe_items = limiter.probe_items()?;
    let page_size = i32::try_from(probe_items.min(256))
        .map_err(|_| Error::Internal("budgeted CQL catalog page size overflow".to_owned()))?;
    Ok((limiter, probe_items, page_size))
}

#[async_trait::async_trait]
impl SqlEngine for CassandraAdapter {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<ResultSet> {
        reject_dynamic_params(params)?;
        let rows = self
            .session
            .query_unpaged(sql, &[])
            .await
            .map_err(|e| Error::Query(e.to_string()))?
            .into_rows_result()
            .map_err(|e| Error::Query(e.to_string()))?;
        rows_to_result_set(rows)
    }

    async fn query_bounded(
        &self,
        sql: &str,
        params: &[Value],
        max_rows: usize,
    ) -> Result<ResultSet> {
        let limiter = ResultLimiter::new(max_rows);
        let probe_rows = limiter.probe_rows()?;
        reject_dynamic_params(params)?;

        // Keep every driver's raw page bounded as well as the collected core
        // rows. A smaller page remains accurate because the stream stops after
        // the single truncation probe row is observed.
        let page_size = i32::try_from(probe_rows.min(256))
            .map_err(|_| Error::Internal("bounded CQL page size overflow".to_owned()))?;
        let statement = Statement::new(sql).with_page_size(page_size);
        let pager = self
            .session
            .query_iter(statement, &[])
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let mut rows_stream = pager
            .rows_stream::<Row>()
            .map_err(|e| Error::Query(e.to_string()))?;

        let columns = {
            let specs = rows_stream.column_specs();
            specs
                .iter()
                .map(|spec| ColumnMeta {
                    name: spec.name().to_owned(),
                    type_name: cql_type_name(spec.typ()),
                    nullable: true,
                    primary_key: false,
                    default_value: None,
                })
                .collect()
        };

        let mut output_rows = Vec::new();
        while output_rows.len() < probe_rows {
            let Some(row) = rows_stream.next().await else {
                break;
            };
            let row = row.map_err(|e| Error::Query(e.to_string()))?;
            output_rows.push(cql_row_values(row));
        }
        drop(rows_stream);

        Ok(limiter.apply(ResultSet {
            columns,
            rows: output_rows,
            truncated: false,
        }))
    }

    async fn query_budgeted(
        &self,
        sql: &str,
        params: &[Value],
        budget: ReadBudget,
    ) -> Result<ResultSet> {
        let mut limiter = ReadLimiter::new(budget, "Cassandra query result")?;
        let probe_rows = limiter.probe_items()?;
        reject_dynamic_params(params)?;

        // A page size of one is the smallest protocol boundary Scylla exposes.
        // It prevents a page of several rows from being decoded ahead of the
        // caller envelope. The driver must still decode one complete frame and
        // convert one complete recursive row before its serialized core size is
        // known; an oversized single row therefore remains the residual bound.
        let statement = Statement::new(sql).with_page_size(budgeted_query_page_size());
        let pager = self
            .session
            .query_iter(statement, &[])
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let mut rows_stream = pager
            .rows_stream::<Row>()
            .map_err(|e| Error::Query(e.to_string()))?;

        let columns = {
            let specs = rows_stream.column_specs();
            specs
                .iter()
                .map(|spec| ColumnMeta {
                    name: spec.name().to_owned(),
                    type_name: cql_type_name(spec.typ()),
                    nullable: true,
                    primary_key: false,
                    default_value: None,
                })
                .collect::<Vec<_>>()
        };
        limiter.observe_header(&columns)?;

        let mut output_rows = Vec::with_capacity(budget.max_items.min(256));
        while limiter.observed_items() < probe_rows {
            let Some(row) = rows_stream.next().await else {
                break;
            };
            let row = row.map_err(|e| Error::Query(e.to_string()))?;
            limiter.retain_item(cql_row_values(row), &mut output_rows)?;
        }
        drop(rows_stream);

        finish_budgeted_result(limiter, columns, output_rows)
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecOutcome> {
        preflight_cassandra_sql_execute(sql, params, InputBudget::default())?;
        self.send_cql_mutation(sql).await
    }

    async fn list_schemas(&self) -> Result<Vec<String>> {
        let result = self
            .query("SELECT keyspace_name FROM system_schema.keyspaces", &[])
            .await?;
        let mut schemas: Vec<_> = result
            .rows
            .into_iter()
            .filter_map(|row| match row.first() {
                Some(Value::Text(value)) => Some(value.clone()),
                _ => None,
            })
            .collect();
        schemas.sort();
        Ok(schemas)
    }

    async fn list_schemas_bounded(&self, max_items: usize) -> Result<BoundedList<String>> {
        self.query_catalog_bounded(
            "SELECT keyspace_name FROM system_schema.keyspaces",
            max_items,
            |row| {
                row.first()
                    .and_then(value_text)
                    .map(str::to_owned)
                    .map(Some)
                    .ok_or_else(|| {
                        Error::Serialization(
                            "Cassandra catalog keyspace_name is not text".to_owned(),
                        )
                    })
            },
        )
        .await
    }

    async fn list_schemas_budgeted(&self, budget: ReadBudget) -> Result<BoundedList<String>> {
        self.query_catalog_budgeted(
            "SELECT keyspace_name FROM system_schema.keyspaces",
            budget,
            |row, limiter, keyspaces| {
                let keyspace = row
                    .first()
                    .and_then(value_text)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        Error::Serialization(
                            "Cassandra catalog keyspace_name is not text".to_owned(),
                        )
                    })?;
                limiter.retain_item(keyspace, keyspaces)
            },
            |limiter, keyspaces| limiter.finish(keyspaces),
        )
        .await
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let result = if let Some(keyspace) = optional_keyspace(schema)? {
            self.query(
                &format!(
                    "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = '{}'",
                    keyspace
                ),
                &[],
            )
            .await?
        } else if let Some(keyspace) = &self.keyspace {
            self.query(
                &format!(
                    "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = '{}'",
                    validate_identifier(keyspace, "keyspace")?
                ),
                &[],
            )
            .await?
        } else {
            self.query(
                "SELECT keyspace_name, table_name FROM system_schema.tables",
                &[],
            )
            .await?
        };

        let mut tables = Vec::new();
        for row in result.rows {
            let Some(keyspace) = row.first().and_then(value_text) else {
                continue;
            };
            if schema.is_none() && self.keyspace.is_none() && is_system_keyspace(keyspace) {
                continue;
            }
            let Some(name) = row.get(1).and_then(value_text) else {
                continue;
            };
            tables.push(TableInfo {
                schema: Some(keyspace.to_owned()),
                name: name.to_owned(),
                kind: TableKind::Table,
            });
        }
        tables.sort_by(|left, right| {
            left.schema
                .cmp(&right.schema)
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(tables)
    }

    async fn list_tables_bounded(
        &self,
        schema: Option<&str>,
        max_items: usize,
    ) -> Result<BoundedList<TableInfo>> {
        let selected_keyspace = optional_keyspace(schema)?
            .map(str::to_owned)
            .or_else(|| self.keyspace.clone());
        let sql = match selected_keyspace.as_deref() {
            Some(keyspace) => format!(
                "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = '{}'",
                validate_identifier(keyspace, "keyspace")?
            ),
            None => "SELECT keyspace_name, table_name FROM system_schema.tables".to_owned(),
        };

        self.query_catalog_bounded(&sql, max_items, |row| {
            let Some(keyspace) = row.first().and_then(value_text) else {
                return Err(Error::Serialization(
                    "Cassandra catalog keyspace_name is not text".to_owned(),
                ));
            };
            if selected_keyspace.is_none() && is_system_keyspace(keyspace) {
                return Ok(None);
            }
            let Some(name) = row.get(1).and_then(value_text) else {
                return Err(Error::Serialization(
                    "Cassandra catalog table_name is not text".to_owned(),
                ));
            };
            Ok(Some(TableInfo {
                schema: Some(keyspace.to_owned()),
                name: name.to_owned(),
                kind: TableKind::Table,
            }))
        })
        .await
    }

    async fn list_tables_budgeted(
        &self,
        schema: Option<&str>,
        budget: ReadBudget,
    ) -> Result<BoundedList<TableInfo>> {
        let selected_keyspace = optional_keyspace(schema)?
            .map(str::to_owned)
            .or_else(|| self.keyspace.clone());
        let cql = match selected_keyspace.as_deref() {
            Some(keyspace) => format!(
                "SELECT keyspace_name, table_name FROM system_schema.tables WHERE keyspace_name = '{}'",
                validate_identifier(keyspace, "keyspace")?
            ),
            None => "SELECT keyspace_name, table_name FROM system_schema.tables".to_owned(),
        };

        self.query_catalog_budgeted(
            &cql,
            budget,
            |row, limiter, tables| {
                let Some(keyspace) = row.first().and_then(value_text) else {
                    return Err(Error::Serialization(
                        "Cassandra catalog keyspace_name is not text".to_owned(),
                    ));
                };
                if selected_keyspace.is_none() && is_system_keyspace(keyspace) {
                    return Ok(());
                }
                let Some(name) = row.get(1).and_then(value_text) else {
                    return Err(Error::Serialization(
                        "Cassandra catalog table_name is not text".to_owned(),
                    ));
                };
                limiter.retain_item(
                    TableInfo {
                        schema: Some(keyspace.to_owned()),
                        name: name.to_owned(),
                        kind: TableKind::Table,
                    },
                    tables,
                )
            },
            |limiter, tables| limiter.finish(tables),
        )
        .await
    }

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
        self.describe_table_complete(
            table,
            MetadataBudget::new(LEGACY_SCHEMA_MAX_ITEMS, DEFAULT_METADATA_BYTES)?,
        )
        .await
    }

    async fn describe_table_bounded(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<TableSchema> {
        self.describe_table_complete(table, budget).await
    }
}

#[async_trait::async_trait]
impl CqlEngine for CassandraAdapter {
    async fn query_cql(&self, cql: &str) -> Result<ResultSet> {
        <Self as SqlEngine>::query(self, cql, &[]).await
    }

    async fn query_cql_bounded(&self, cql: &str, max_rows: usize) -> Result<ResultSet> {
        <Self as SqlEngine>::query_bounded(self, cql, &[], max_rows).await
    }

    async fn query_cql_budgeted(&self, cql: &str, budget: ReadBudget) -> Result<ResultSet> {
        <Self as SqlEngine>::query_budgeted(self, cql, &[], budget).await
    }

    async fn execute_cql(&self, cql: &str) -> Result<ExecOutcome> {
        self.execute_cql_budgeted(cql, InputBudget::default()).await
    }

    async fn execute_cql_budgeted(&self, cql: &str, budget: InputBudget) -> Result<ExecOutcome> {
        preflight_cql_execute(cql, budget)?;
        self.send_cql_mutation(cql).await
    }

    async fn list_keyspaces(&self) -> Result<Vec<String>> {
        <Self as SqlEngine>::list_schemas(self).await
    }

    async fn list_keyspaces_bounded(&self, max_items: usize) -> Result<BoundedList<String>> {
        <Self as SqlEngine>::list_schemas_bounded(self, max_items).await
    }

    async fn list_keyspaces_budgeted(&self, budget: ReadBudget) -> Result<BoundedList<String>> {
        <Self as SqlEngine>::list_schemas_budgeted(self, budget).await
    }

    async fn list_cql_tables(&self, keyspace: Option<&str>) -> Result<Vec<TableInfo>> {
        <Self as SqlEngine>::list_tables(self, keyspace).await
    }

    async fn list_cql_tables_bounded(
        &self,
        keyspace: Option<&str>,
        max_items: usize,
    ) -> Result<BoundedList<TableInfo>> {
        <Self as SqlEngine>::list_tables_bounded(self, keyspace, max_items).await
    }

    async fn list_cql_tables_budgeted(
        &self,
        keyspace: Option<&str>,
        budget: ReadBudget,
    ) -> Result<BoundedList<TableInfo>> {
        <Self as SqlEngine>::list_tables_budgeted(self, keyspace, budget).await
    }

    async fn describe_cql_table(&self, table: &str) -> Result<TableSchema> {
        <Self as SqlEngine>::describe_table(self, table).await
    }

    async fn describe_cql_table_bounded(
        &self,
        table: &str,
        budget: MetadataBudget,
    ) -> Result<TableSchema> {
        <Self as SqlEngine>::describe_table_bounded(self, table, budget).await
    }
}

struct DescribedColumn {
    meta: ColumnMeta,
    kind_rank: u8,
    position: i64,
}

fn primary_index(columns: &[DescribedColumn]) -> Option<IndexInfo> {
    let primary_columns = columns
        .iter()
        .filter(|column| column.meta.primary_key)
        .map(|column| column.meta.name.clone())
        .collect::<Vec<_>>();

    (!primary_columns.is_empty()).then(|| IndexInfo {
        name: "PRIMARY".to_owned(),
        columns: primary_columns,
        unique: true,
        primary: true,
    })
}

fn cassandra_index_column(options: &Value) -> Result<String> {
    let Value::Map(options) = options else {
        return Err(Error::Serialization(
            "Cassandra index options are not a map".to_owned(),
        ));
    };
    let target = options
        .get("target")
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("Cassandra index target is not text".to_owned()))?
        .trim();
    if target.is_empty() {
        return Err(Error::Serialization(
            "Cassandra index target is empty".to_owned(),
        ));
    }

    if let Some(open) = target.find('(') {
        let function = &target[..open];
        if target.ends_with(')')
            && matches!(
                function.to_ascii_lowercase().as_str(),
                "keys" | "values" | "entries" | "full"
            )
        {
            return Ok(target[open + 1..target.len() - 1]
                .trim()
                .trim_matches('"')
                .to_owned());
        }
    }

    let column = target.trim_matches('"').to_owned();
    if column.is_empty() {
        Err(Error::Serialization(
            "Cassandra index target column is empty".to_owned(),
        ))
    } else {
        Ok(column)
    }
}

fn parse_described_column(row: &[Value]) -> Result<DescribedColumn> {
    let name = row
        .first()
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("Cassandra column name is not text".to_owned()))?;
    let type_name = row
        .get(1)
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("Cassandra column type is not text".to_owned()))?;
    let kind = row
        .get(2)
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("Cassandra column kind is not text".to_owned()))?;
    if !matches!(kind, "partition_key" | "clustering" | "regular" | "static") {
        return Err(Error::Serialization(format!(
            "unsupported Cassandra column kind: {kind}"
        )));
    }
    let position = row.get(3).and_then(Value::as_i64).ok_or_else(|| {
        Error::Serialization("Cassandra column position is not an integer".to_owned())
    })?;
    Ok(DescribedColumn {
        meta: ColumnMeta {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            nullable: !matches!(kind, "partition_key" | "clustering"),
            primary_key: matches!(kind, "partition_key" | "clustering"),
            default_value: None,
        },
        kind_rank: cql_column_kind_rank(kind),
        position,
    })
}

fn parse_cassandra_index(row: &[Value]) -> Result<IndexInfo> {
    let name = row
        .first()
        .and_then(value_text)
        .ok_or_else(|| Error::Serialization("Cassandra index name is not text".to_owned()))?;
    let options = row
        .get(1)
        .ok_or_else(|| Error::Serialization("Cassandra index options are missing".to_owned()))?;
    Ok(IndexInfo {
        name: name.to_owned(),
        columns: vec![cassandra_index_column(options)?],
        unique: false,
        primary: false,
    })
}

struct TableRef {
    schema: Option<String>,
    name: String,
}

struct ContactPointTranslator {
    target: SocketAddr,
}

#[async_trait::async_trait]
impl AddressTranslator for ContactPointTranslator {
    async fn translate_address(
        &self,
        _untranslated_peer: &UntranslatedPeer,
    ) -> std::result::Result<SocketAddr, TranslationError> {
        Ok(self.target)
    }
}

fn contact_point_from_dsn(dsn: &Dsn) -> Result<String> {
    let host = dsn.host.as_deref().unwrap_or("127.0.0.1");
    if host.is_empty() {
        return Err(Error::Dsn("Cassandra host must not be empty".to_owned()));
    }
    Ok(format!("{}:{}", host, dsn.port.unwrap_or(9042)))
}

fn contact_socket_addr_from_dsn(dsn: &Dsn) -> Result<SocketAddr> {
    let contact_point = contact_point_from_dsn(dsn)?;
    contact_point
        .to_socket_addrs()
        .map_err(|e| {
            Error::Dsn(format!(
                "invalid Cassandra contact point {contact_point}: {e}"
            ))
        })?
        .next()
        .ok_or_else(|| Error::Dsn(format!("invalid Cassandra contact point: {contact_point}")))
}

fn use_contact_point_translation(dsn: &Dsn) -> Result<bool> {
    match dsn
        .params
        .get("address-translator")
        .or_else(|| dsn.params.get("address_translator"))
        .or_else(|| dsn.params.get("translate-addresses"))
        .or_else(|| dsn.params.get("translate_addresses"))
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        None | Some("default" | "none" | "off" | "false") => Ok(false),
        Some("contact-point" | "contact_point" | "contactpoint" | "true" | "on") => Ok(true),
        Some(other) => Err(Error::Dsn(format!(
            "unsupported Cassandra address-translator value: {other}"
        ))),
    }
}

fn optional_duration_param(dsn: &Dsn, key: &str) -> Result<Option<Duration>> {
    dsn.params
        .get(key)
        .map(|value| {
            value
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|e| Error::Dsn(format!("invalid Cassandra {key}: {e}")))
        })
        .transpose()
}

fn duration_param(dsn: &Dsn, key: &str, default_ms: u64) -> Result<Duration> {
    Ok(optional_duration_param(dsn, key)?.unwrap_or_else(|| Duration::from_millis(default_ms)))
}

fn rows_to_result_set(rows: QueryRowsResult) -> Result<ResultSet> {
    let columns = rows
        .column_specs()
        .iter()
        .map(|spec| ColumnMeta {
            name: spec.name().to_owned(),
            type_name: cql_type_name(spec.typ()),
            nullable: true,
            primary_key: false,
            default_value: None,
        })
        .collect();

    let mut output_rows = Vec::with_capacity(rows.rows_num());
    for row in rows
        .rows::<Row>()
        .map_err(|e| Error::Query(e.to_string()))?
    {
        let row = row.map_err(|e| Error::Query(e.to_string()))?;
        output_rows.push(cql_row_values(row));
    }

    Ok(ResultSet {
        columns,
        rows: output_rows,
        truncated: false,
    })
}

fn finish_budgeted_result(
    limiter: ReadLimiter,
    columns: Vec<ColumnMeta>,
    rows: Vec<Vec<Value>>,
) -> Result<ResultSet> {
    limiter.finish_with(rows, |rows, truncated| ResultSet {
        columns,
        rows,
        truncated,
    })
}

fn budgeted_query_page_size() -> i32 {
    1
}

fn cql_row_values(row: Row) -> Vec<Value> {
    row.columns
        .into_iter()
        .map(|value| value.map_or(Value::Null, cql_value_to_value))
        .collect()
}

fn cql_type_name(typ: &ColumnType<'_>) -> String {
    match typ {
        ColumnType::Native(native) => native_type_name(native).to_owned(),
        ColumnType::Collection { frozen, typ } => {
            let inner = match typ {
                CollectionType::List(value) => format!("list<{}>", cql_type_name(value)),
                CollectionType::Map(key, value) => {
                    format!("map<{}, {}>", cql_type_name(key), cql_type_name(value))
                }
                CollectionType::Set(value) => format!("set<{}>", cql_type_name(value)),
                other => format!("{other:?}"),
            };
            if *frozen {
                format!("frozen<{inner}>")
            } else {
                inner
            }
        }
        ColumnType::Vector { typ, dimensions } => {
            format!("vector<{}, {}>", cql_type_name(typ), dimensions)
        }
        ColumnType::UserDefinedType { frozen, definition } => {
            let name = if definition.keyspace.is_empty() {
                definition.name.to_string()
            } else {
                format!("{}.{}", definition.keyspace, definition.name)
            };
            if *frozen {
                format!("frozen<{name}>")
            } else {
                name
            }
        }
        ColumnType::Tuple(types) => format!(
            "tuple<{}>",
            types
                .iter()
                .map(cql_type_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => format!("{other:?}"),
    }
}

fn native_type_name(native: &NativeType) -> &'static str {
    match native {
        NativeType::Ascii => "ascii",
        NativeType::Boolean => "boolean",
        NativeType::Blob => "blob",
        NativeType::Counter => "counter",
        NativeType::Date => "date",
        NativeType::Decimal => "decimal",
        NativeType::Double => "double",
        NativeType::Duration => "duration",
        NativeType::Float => "float",
        NativeType::Int => "int",
        NativeType::BigInt => "bigint",
        NativeType::Text => "text",
        NativeType::Timestamp => "timestamp",
        NativeType::Inet => "inet",
        NativeType::SmallInt => "smallint",
        NativeType::TinyInt => "tinyint",
        NativeType::Time => "time",
        NativeType::Timeuuid => "timeuuid",
        NativeType::Uuid => "uuid",
        NativeType::Varint => "varint",
        _ => "unknown",
    }
}

fn cql_value_to_value(value: CqlValue) -> Value {
    match value {
        CqlValue::Ascii(value) | CqlValue::Text(value) => Value::Text(value),
        CqlValue::Boolean(value) => Value::Bool(value),
        CqlValue::Blob(value) => Value::Bytes(value),
        CqlValue::Counter(value) => Value::Int(value.0),
        CqlValue::Double(value) => Value::Float(value),
        CqlValue::Float(value) => Value::Float(f64::from(value)),
        CqlValue::Int(value) => Value::Int(i64::from(value)),
        CqlValue::BigInt(value) => Value::Int(value),
        CqlValue::SmallInt(value) => Value::Int(i64::from(value)),
        CqlValue::TinyInt(value) => Value::Int(i64::from(value)),
        CqlValue::Timestamp(value) => Value::Timestamp(value.0),
        CqlValue::Inet(value) => Value::Text(value.to_string()),
        CqlValue::Timeuuid(value) => Value::Text(value.to_string()),
        CqlValue::Uuid(value) => Value::Text(value.to_string()),
        CqlValue::List(values) | CqlValue::Set(values) | CqlValue::Vector(values) => {
            Value::Array(values.into_iter().map(cql_value_to_value).collect())
        }
        CqlValue::Tuple(values) => Value::Array(
            values
                .into_iter()
                .map(|value| value.map_or(Value::Null, cql_value_to_value))
                .collect(),
        ),
        CqlValue::Map(entries) => {
            let mut map = BTreeMap::new();
            for (key, value) in entries {
                map.insert(cql_map_key(key), cql_value_to_value(value));
            }
            Value::Map(map)
        }
        CqlValue::UserDefinedType { fields, .. } => {
            let mut map = BTreeMap::new();
            for (key, value) in fields {
                map.insert(key, value.map_or(Value::Null, cql_value_to_value));
            }
            Value::Map(map)
        }
        other => Value::Text(other.to_string()),
    }
}

fn cql_map_key(value: CqlValue) -> String {
    match cql_value_to_value(value) {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Text(value) => value,
        Value::Bytes(value) => format!("{value:?}"),
        Value::Timestamp(value) => value.to_string(),
        Value::Json(value) => value.to_string(),
        Value::Array(value) => format!("{value:?}"),
        Value::Map(value) => format!("{value:?}"),
    }
}

fn optional_keyspace(schema: Option<&str>) -> Result<Option<&str>> {
    schema
        .map(|schema| validate_identifier(schema, "keyspace"))
        .transpose()
}

fn parse_table_ref(value: &str) -> Result<TableRef> {
    let parts: Vec<_> = value.split('.').collect();
    match parts.as_slice() {
        [name] => Ok(TableRef {
            schema: None,
            name: validate_identifier(name, "table")?.to_owned(),
        }),
        [schema, name] => Ok(TableRef {
            schema: Some(validate_identifier(schema, "keyspace")?.to_owned()),
            name: validate_identifier(name, "table")?.to_owned(),
        }),
        _ => Err(Error::Query(format!(
            "Cassandra table reference must be table or keyspace.table: {value}"
        ))),
    }
}

fn validate_identifier<'a>(value: &'a str, label: &str) -> Result<&'a str> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(Error::Query(format!("invalid Cassandra {label}: {value}")));
    }
    Ok(value)
}

fn reject_dynamic_params(params: &[Value]) -> Result<()> {
    if params.is_empty() {
        Ok(())
    } else {
        Err(Error::Query(
            "Cassandra dynamic parameters are not supported by this adapter".to_owned(),
        ))
    }
}

fn validate_cql_statement(cql: &str) -> Result<()> {
    if cql.as_bytes().contains(&0) {
        return Err(Error::Query(
            "Cassandra CQL statement contains a NUL byte".to_owned(),
        ));
    }
    if cql.len() > CASSANDRA_MAX_CQL_BYTES {
        return Err(Error::Query(format!(
            "Cassandra CQL statement exceeds the fixed {CASSANDRA_MAX_CQL_BYTES}-byte ceiling"
        )));
    }
    Ok(())
}

fn preflight_cql_execute(cql: &str, budget: InputBudget) -> Result<()> {
    let request = ("cql", cql);
    InputLimiter::new(budget, "Cassandra CQL execute input")?.validate_request(&request)?;
    validate_cql_statement(cql)
}

fn preflight_cassandra_sql_execute(sql: &str, params: &[Value], budget: InputBudget) -> Result<()> {
    let request = ("cql", sql, "params", params);
    let limiter = InputLimiter::new(budget, "Cassandra SQL-compatible execute input")?;
    if params.is_empty() {
        limiter.validate_request(&request)?;
    } else {
        limiter.validate_items_with_request(params, &request)?;
    }
    validate_cql_statement(sql)?;
    reject_dynamic_params(params)
}

fn value_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value),
        _ => None,
    }
}

fn cql_column_kind_rank(kind: &str) -> u8 {
    match kind {
        "partition_key" => 0,
        "clustering" => 1,
        "static" => 2,
        _ => 3,
    }
}

fn is_system_keyspace(keyspace: &str) -> bool {
    matches!(
        keyspace,
        "system"
            | "system_auth"
            | "system_distributed"
            | "system_schema"
            | "system_traces"
            | "system_views"
            | "system_virtual_schema"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_catalog_contract_is_explicit_and_validated_before_access() {
        let operations = cassandra_operations(Capabilities {
            sql: true,
            cql: true,
            ..Default::default()
        });
        for operation in [
            CapabilityOperation::SqlQueryBudgeted,
            CapabilityOperation::CqlQueryBudgeted,
            CapabilityOperation::CqlExecuteBudgeted,
            CapabilityOperation::SqlListSchemasBounded,
            CapabilityOperation::SqlListSchemasBudgeted,
            CapabilityOperation::SqlListTablesBounded,
            CapabilityOperation::SqlListTablesBudgeted,
            CapabilityOperation::CqlListKeyspacesBounded,
            CapabilityOperation::CqlListKeyspacesBudgeted,
            CapabilityOperation::CqlListTablesBounded,
            CapabilityOperation::CqlListTablesBudgeted,
            CapabilityOperation::SqlDescribeTableBounded,
            CapabilityOperation::CqlDescribeTableBounded,
        ] {
            assert!(operations.contains(&operation));
        }

        assert!(matches!(bounded_catalog_plan(0), Err(Error::Config(_))));
        assert!(matches!(
            bounded_catalog_plan(usize::MAX),
            Err(Error::Config(_))
        ));
        let (_, probe_items, page_size) = bounded_catalog_plan(2).unwrap();
        assert_eq!((probe_items, page_size), (3, 3));
        let (_, probe_items, page_size) = bounded_catalog_plan(1_000).unwrap();
        assert_eq!((probe_items, page_size), (1_001, 256));
        assert!(matches!(
            budgeted_catalog_plan(ReadBudget {
                max_items: 0,
                max_bytes: 1,
            }),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            budgeted_catalog_plan(ReadBudget {
                max_items: usize::MAX,
                max_bytes: 1,
            }),
            Err(Error::Config(_))
        ));
        let (_, probe_items, page_size) =
            budgeted_catalog_plan(ReadBudget::new(2, 1024).unwrap()).unwrap();
        assert_eq!((probe_items, page_size), (3, 3));
        let statement = Statement::new("SELECT now() FROM system.local")
            .with_page_size(budgeted_query_page_size());
        assert_eq!(statement.get_page_size(), 1);
    }

    #[test]
    fn cql_execute_budget_preflight_is_exact_and_sql_alias_counts_late_params() {
        let cql = "delete from ks.jobs where id = 1";
        let scalar_bytes = (1..=1024)
            .find(|bytes| {
                preflight_cql_execute(cql, InputBudget::new(1, *bytes, *bytes).unwrap()).is_ok()
            })
            .expect("small CQL scalar request must fit");
        preflight_cql_execute(
            cql,
            InputBudget::new(1, scalar_bytes, scalar_bytes).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            preflight_cql_execute(
                cql,
                InputBudget::new(1, scalar_bytes, scalar_bytes - 1).unwrap(),
            ),
            Err(Error::InputBudgetExceeded { .. })
        ));

        let params = [Value::Int(1), Value::Text("late".into())];
        assert!(matches!(
            preflight_cassandra_sql_execute(
                "update ks.jobs set note = ? where id = ?",
                &params,
                InputBudget {
                    max_items: 1,
                    ..InputBudget::default()
                },
            ),
            Err(Error::InputBudgetExceeded { unit: "items", .. })
        ));
        assert!(matches!(
            preflight_cassandra_sql_execute(
                "update ks.jobs set note = ? where id = ?",
                &params,
                InputBudget::default(),
            ),
            Err(Error::Query(_))
        ));
        assert!(matches!(
            preflight_cql_execute("x\0y", InputBudget::default()),
            Err(Error::Query(_))
        ));
    }

    #[tokio::test]
    async fn cassandra_live_budgeted_cql_rejects_before_write_and_cleans_keyspace() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_CASSANDRA_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let keyspace = format!("dbtool_it_input_{suffix}");
        let rejected = format!("{keyspace}_rejected");
        let table = format!("{keyspace}.items");
        let connector = factory(Dsn::parse(&raw_dsn).unwrap()).await.unwrap();
        assert!(connector
            .operations()
            .contains(&CapabilityOperation::CqlExecuteBudgeted));
        let cql = connector.as_cql().unwrap();

        let exercise = async {
            let error = cql
                .execute_cql_budgeted(
                    &format!(
                        "CREATE KEYSPACE {rejected} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
                    ),
                    InputBudget::new(1, 1, 1)?,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), "INPUT_BUDGET_EXCEEDED");
            assert!(!cql.list_keyspaces().await?.contains(&rejected));

            cql.execute_cql_budgeted(
                &format!(
                    "CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
                ),
                InputBudget::default(),
            )
            .await?;
            cql.execute_cql_budgeted(
                &format!("CREATE TABLE {table} (id int PRIMARY KEY, note text)"),
                InputBudget::default(),
            )
            .await?;
            cql.execute_cql_budgeted(
                &format!("INSERT INTO {table} (id, note) VALUES (1, 'created')"),
                InputBudget::default(),
            )
            .await?;
            cql.execute_cql_budgeted(
                &format!("UPDATE {table} SET note = 'updated' WHERE id = 1"),
                InputBudget::default(),
            )
            .await?;
            let readback = cql
                .query_cql(&format!("SELECT id, note FROM {table} WHERE id = 1"))
                .await?;
            assert_eq!(readback.rows.len(), 1);
            assert_eq!(readback.rows[0][0], Value::Int(1));
            assert_eq!(readback.rows[0][1], Value::Text("updated".into()));

            cql.execute_cql_budgeted(
                &format!("DELETE FROM {table} WHERE id = 1"),
                InputBudget::default(),
            )
            .await?;
            assert!(cql
                .query_cql(&format!("SELECT id FROM {table} WHERE id = 1"))
                .await?
                .rows
                .is_empty());
            cql.execute_cql_budgeted(
                &format!("DROP TABLE {table}"),
                InputBudget::default(),
            )
            .await?;
            Ok::<(), Error>(())
        }
        .await;

        cql.execute_cql_budgeted(
            &format!("DROP KEYSPACE IF EXISTS {keyspace}"),
            InputBudget::default(),
        )
        .await
        .unwrap();
        assert!(!cql.list_keyspaces().await.unwrap().contains(&keyspace));
        exercise.unwrap();
        connector.close().await.unwrap();
    }

    #[test]
    fn budgeted_catalog_retains_n_probes_n_plus_one_and_enforces_exact_bytes() {
        let expected = BoundedList {
            items: vec!["alpha".to_owned(), "beta".to_owned()],
            truncated: true,
        };

        let finish = |max_bytes: usize| -> Result<BoundedList<String>> {
            let (mut limiter, probe_items, _) =
                budgeted_catalog_plan(ReadBudget::new(2, max_bytes)?)?;
            assert_eq!(probe_items, 3);
            let mut retained = Vec::new();
            for item in ["alpha", "beta", "gamma"] {
                limiter.retain_item(item.to_owned(), &mut retained)?;
            }
            limiter.finish(retained)
        };

        let exact_bytes = (1..=4096)
            .find(|max_bytes| finish(*max_bytes).is_ok())
            .expect("the small Cassandra catalog fixture must fit");
        assert_eq!(finish(exact_bytes).unwrap(), expected);
        assert!(matches!(
            finish(exact_bytes - 1),
            Err(Error::ReadBudgetExceeded {
                unit: "bytes",
                limit,
                ..
            }) if limit == exact_bytes - 1
        ));

        let complete = BoundedList::complete(vec!["alpha".to_owned(), "beta".to_owned()]);
        let complete_finish = |max_bytes: usize| -> Result<BoundedList<String>> {
            let (mut limiter, _, _) = budgeted_catalog_plan(ReadBudget::new(2, max_bytes)?)?;
            let mut retained = Vec::new();
            for item in ["alpha", "beta"] {
                limiter.retain_item(item.to_owned(), &mut retained)?;
            }
            limiter.finish(retained)
        };
        let complete_bytes = (1..=4096)
            .find(|max_bytes| complete_finish(*max_bytes).is_ok())
            .expect("the complete Cassandra catalog fixture must fit");
        assert_eq!(complete_finish(complete_bytes).unwrap(), complete);
    }

    #[test]
    fn contact_point_defaults_port_and_host() {
        let dsn = Dsn::parse("cassandra://example.test/app").unwrap();

        assert_eq!(contact_point_from_dsn(&dsn).unwrap(), "example.test:9042");
    }

    #[test]
    fn contact_point_uses_explicit_port() {
        let dsn = Dsn::parse("scylla://db.example.test:19042").unwrap();

        assert_eq!(
            contact_point_from_dsn(&dsn).unwrap(),
            "db.example.test:19042"
        );
    }

    #[test]
    fn contact_point_translation_is_explicit() {
        let dsn = Dsn::parse("cassandra://127.0.0.1:19042").unwrap();
        assert!(!use_contact_point_translation(&dsn).unwrap());

        let dsn =
            Dsn::parse("cassandra://127.0.0.1:19042?address-translator=contact-point").unwrap();
        assert!(use_contact_point_translation(&dsn).unwrap());
        assert_eq!(
            contact_socket_addr_from_dsn(&dsn).unwrap().to_string(),
            "127.0.0.1:19042"
        );
    }

    #[test]
    fn parses_table_refs_with_optional_keyspace() {
        let table = parse_table_ref("ks.users").unwrap();

        assert_eq!(table.schema.as_deref(), Some("ks"));
        assert_eq!(table.name, "users");

        let table = parse_table_ref("users").unwrap();
        assert_eq!(table.schema, None);
        assert_eq!(table.name, "users");
    }

    #[test]
    fn rejects_unsafe_identifiers() {
        assert!(parse_table_ref("ks.users;drop").is_err());
        assert!(optional_keyspace(Some("system-schema")).is_err());
    }

    #[test]
    fn maps_cql_values_to_core_values() {
        assert_eq!(cql_value_to_value(CqlValue::Int(42)), Value::Int(42));
        assert_eq!(
            cql_value_to_value(CqlValue::Text("alice".to_owned())),
            Value::Text("alice".to_owned())
        );
        assert_eq!(
            cql_value_to_value(CqlValue::List(vec![CqlValue::Boolean(true)])),
            Value::Array(vec![Value::Bool(true)])
        );
    }

    #[test]
    fn budgeted_result_retains_n_probes_n_plus_one_and_rejects_large_units() {
        let columns = vec![ColumnMeta {
            name: "payload".to_owned(),
            type_name: "text".to_owned(),
            nullable: true,
            primary_key: false,
            default_value: None,
        }];
        let mut exact =
            ReadLimiter::new(ReadBudget::new(2, 4096).unwrap(), "Cassandra query result").unwrap();
        exact.observe_header(&columns).unwrap();
        let mut exact_rows = Vec::new();
        for value in ["one", "two"] {
            exact
                .retain_item(vec![Value::Text(value.to_owned())], &mut exact_rows)
                .unwrap();
        }
        let exact = finish_budgeted_result(exact, columns.clone(), exact_rows).unwrap();
        assert_eq!(exact.rows.len(), 2);
        assert!(!exact.truncated);

        let mut limiter =
            ReadLimiter::new(ReadBudget::new(2, 4096).unwrap(), "Cassandra query result").unwrap();
        assert_eq!(limiter.probe_items().unwrap(), 3);
        limiter.observe_header(&columns).unwrap();
        let mut rows = Vec::new();
        for value in ["one", "two", "probe"] {
            limiter
                .retain_item(vec![Value::Text(value.to_owned())], &mut rows)
                .unwrap();
        }
        let result = finish_budgeted_result(limiter, columns.clone(), rows).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert!(result.truncated);

        let mut header_probe =
            ReadLimiter::new(ReadBudget::new(1, 4096).unwrap(), "header probe").unwrap();
        header_probe.observe_header(&columns).unwrap();
        let header_bytes = header_probe.observed_bytes();
        let mut limiter = ReadLimiter::new(
            ReadBudget::new(1, header_bytes + 16).unwrap(),
            "Cassandra query result",
        )
        .unwrap();
        limiter.observe_header(&columns).unwrap();
        let mut rows = Vec::new();
        let oversized = cql_value_to_value(CqlValue::List(vec![CqlValue::Text("x".repeat(1024))]));
        let error = limiter.retain_item(vec![oversized], &mut rows).unwrap_err();
        assert_eq!(error.code(), "READ_BUDGET_EXCEEDED");
        assert!(rows.is_empty());
        assert_eq!(limiter.observed_items(), 0);
    }

    #[test]
    fn synthesizes_primary_index_in_partition_and_clustering_order() {
        let columns = vec![
            DescribedColumn {
                meta: ColumnMeta {
                    name: "tenant_id".to_owned(),
                    type_name: "uuid".to_owned(),
                    nullable: false,
                    primary_key: true,
                    default_value: None,
                },
                kind_rank: 0,
                position: 0,
            },
            DescribedColumn {
                meta: ColumnMeta {
                    name: "created_at".to_owned(),
                    type_name: "timestamp".to_owned(),
                    nullable: false,
                    primary_key: true,
                    default_value: None,
                },
                kind_rank: 1,
                position: 0,
            },
            DescribedColumn {
                meta: ColumnMeta {
                    name: "payload".to_owned(),
                    type_name: "text".to_owned(),
                    nullable: true,
                    primary_key: false,
                    default_value: None,
                },
                kind_rank: 3,
                position: -1,
            },
        ];

        let index = primary_index(&columns).expect("primary index");
        assert_eq!(index.name, "PRIMARY");
        assert_eq!(index.columns, vec!["tenant_id", "created_at"]);
        assert!(index.unique);
        assert!(index.primary);
    }

    #[test]
    fn reads_secondary_index_target_from_cassandra_options() {
        let options = Value::Map(BTreeMap::from([(
            "target".to_owned(),
            Value::Text("values(\"tags\")".to_owned()),
        )]));

        assert_eq!(cassandra_index_column(&options).unwrap(), "tags");
        assert_eq!(
            cassandra_index_column(&Value::Map(BTreeMap::from([(
                "target".to_owned(),
                Value::Text("email".to_owned()),
            )])))
            .unwrap(),
            "email"
        );
        assert!(cassandra_index_column(&Value::Map(BTreeMap::new())).is_err());
    }

    #[test]
    fn bounded_schema_parsers_fail_closed_on_missing_catalog_fields() {
        let column = parse_described_column(&[
            Value::Text("tenant_id".into()),
            Value::Text("uuid".into()),
            Value::Text("partition_key".into()),
            Value::Int(0),
        ])
        .unwrap();
        assert!(column.meta.primary_key);
        assert!(!column.meta.nullable);
        assert!(parse_described_column(&[Value::Text("tenant_id".into())]).is_err());
        assert!(parse_described_column(&[
            Value::Text("id".into()),
            Value::Text("uuid".into()),
            Value::Text("mystery".into()),
            Value::Int(0),
        ])
        .is_err());

        let index = parse_cassandra_index(&[
            Value::Text("events_tags_idx".into()),
            Value::Map(BTreeMap::from([(
                "target".into(),
                Value::Text("values(\"tags\")".into()),
            )])),
        ])
        .unwrap();
        assert_eq!(index.columns, ["tags"]);
        assert!(parse_cassandra_index(&[Value::Text("broken".into())]).is_err());
    }
}
