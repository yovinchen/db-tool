use std::{
    collections::BTreeMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

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
use scylla::{
    client::{
        execution_profile::ExecutionProfile, session::Session, session_builder::SessionBuilder,
    },
    errors::TranslationError,
    frame::response::result::{CollectionType, ColumnType, NativeType},
    policies::address_translator::{AddressTranslator, UntranslatedPeer},
    response::query_result::QueryRowsResult,
    value::{CqlValue, Row},
};

pub struct CassandraAdapter {
    session: Session,
    kind: ConnectorKind,
    keyspace: Option<String>,
}

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
            ..Default::default()
        }
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

    async fn describe_table(&self, table: &str) -> Result<TableSchema> {
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
        let result = self
            .query(
                &format!(
                    "SELECT column_name, type, kind, position FROM system_schema.columns \
                     WHERE keyspace_name = '{}' AND table_name = '{}'",
                    keyspace, table_ref.name
                ),
                &[],
            )
            .await?;

        let mut columns = result
            .rows
            .into_iter()
            .filter_map(|row| {
                let name = row.first().and_then(value_text)?.to_owned();
                let type_name = row.get(1).and_then(value_text)?.to_owned();
                let kind = row.get(2).and_then(value_text).unwrap_or("regular");
                let position = row.get(3).and_then(Value::as_i64).unwrap_or(i64::MAX);
                Some(DescribedColumn {
                    meta: ColumnMeta {
                        name,
                        type_name,
                        nullable: !matches!(kind, "partition_key" | "clustering"),
                    },
                    kind_rank: cql_column_kind_rank(kind),
                    position,
                })
            })
            .collect::<Vec<_>>();

        columns.sort_by(|left, right| {
            left.kind_rank
                .cmp(&right.kind_rank)
                .then_with(|| left.position.cmp(&right.position))
                .then_with(|| left.meta.name.cmp(&right.meta.name))
        });

        Ok(TableSchema {
            name: table_ref.name,
            columns: columns.into_iter().map(|column| column.meta).collect(),
            indexes: vec![],
        })
    }
}

struct DescribedColumn {
    meta: ColumnMeta,
    kind_rank: u8,
    position: i64,
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
        })
        .collect();

    let mut output_rows = Vec::with_capacity(rows.rows_num());
    for row in rows
        .rows::<Row>()
        .map_err(|e| Error::Query(e.to_string()))?
    {
        let row = row.map_err(|e| Error::Query(e.to_string()))?;
        output_rows.push(
            row.columns
                .into_iter()
                .map(|value| value.map_or(Value::Null, cql_value_to_value))
                .collect(),
        );
    }

    Ok(ResultSet {
        columns,
        rows: output_rows,
        truncated: false,
    })
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
}
