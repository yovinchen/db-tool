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
        BoundedList, ColumnMeta, ExecOutcome, IndexInfo, MetadataBudget, ReadBudget, ResultSet,
        TableInfo, TableKind, TableSchema, Value, DEFAULT_METADATA_BYTES,
    },
    port::{
        capability::{CqlEngine, SqlEngine},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{ListLimiter, MetadataLimiter, ReadLimiter, ResultLimiter},
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
        CapabilityOperation::SqlListSchemasBounded,
        CapabilityOperation::SqlListTablesBounded,
        CapabilityOperation::CqlListKeyspacesBounded,
        CapabilityOperation::CqlListTablesBounded,
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
        reject_dynamic_params(params)?;
        self.session
            .query_unpaged(sql, &[])
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        Ok(ExecOutcome {
            rows_affected: 0,
            last_insert_id: None,
        })
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
        <Self as SqlEngine>::execute(self, cql, &[]).await
    }

    async fn list_keyspaces(&self) -> Result<Vec<String>> {
        <Self as SqlEngine>::list_schemas(self).await
    }

    async fn list_keyspaces_bounded(&self, max_items: usize) -> Result<BoundedList<String>> {
        <Self as SqlEngine>::list_schemas_bounded(self, max_items).await
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
            CapabilityOperation::SqlListSchemasBounded,
            CapabilityOperation::SqlListTablesBounded,
            CapabilityOperation::CqlListKeyspacesBounded,
            CapabilityOperation::CqlListTablesBounded,
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
        let statement = Statement::new("SELECT now() FROM system.local")
            .with_page_size(budgeted_query_page_size());
        assert_eq!(statement.get_page_size(), 1);
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
