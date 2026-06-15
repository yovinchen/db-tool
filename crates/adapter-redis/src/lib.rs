use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::Value,
    port::{
        capability::{KeyValueStore, SetOptions},
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use std::collections::BTreeMap;

pub struct RedisAdapter {
    conn: tokio::sync::Mutex<MultiplexedConnection>,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let client =
            Client::open(dsn.raw.as_str()).map_err(|e| Error::Connection(e.to_string()))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(Box::new(RedisAdapter {
            conn: tokio::sync::Mutex::new(conn),
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for RedisAdapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            key_value: true,
            ..Default::default()
        }
    }

    async fn ping(&self) -> Result<()> {
        let mut c = self.conn.lock().await;
        redis::cmd("PING")
            .query_async::<()>(&mut *c)
            .await
            .map_err(|e| Error::Connection(e.to_string()))
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn as_kv(&self) -> Option<&dyn KeyValueStore> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl KeyValueStore for RedisAdapter {
    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>> {
        let mut c = self.conn.lock().await;
        let val: Option<Vec<u8>> = c.get(key).await.map_err(|e| Error::Query(e.to_string()))?;
        Ok(val.map(bytes::Bytes::from))
    }

    async fn set(&self, key: &str, value: &[u8], options: SetOptions) -> Result<()> {
        let mut c = self.conn.lock().await;
        if let Some(ttl) = options.ttl_secs {
            c.set_ex::<_, _, ()>(key, value, ttl).await
        } else {
            c.set::<_, _, ()>(key, value).await
        }
        .map_err(|e| Error::Query(e.to_string()))
    }

    async fn delete(&self, keys: &[String]) -> Result<u64> {
        let mut c = self.conn.lock().await;
        c.del::<_, u64>(keys)
            .await
            .map_err(|e| Error::Query(e.to_string()))
    }

    async fn scan(&self, pattern: &str, limit: usize) -> Result<Vec<String>> {
        let mut c = self.conn.lock().await;
        let mut keys: Vec<String> = Vec::new();
        let mut iter: redis::AsyncIter<String> = c
            .scan_match(pattern)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        while let Some(k) = iter.next_item().await {
            keys.push(k);
            if keys.len() >= limit {
                break;
            }
        }
        Ok(keys)
    }

    async fn raw_command(&self, args: &[String]) -> Result<Value> {
        validate_raw_command(args)?;
        let mut cmd = redis::cmd(args[0].as_str());
        for arg in &args[1..] {
            cmd.arg(arg.as_str());
        }
        let mut c = self.conn.lock().await;
        let val: redis::Value = cmd
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(redis_value_to_core(val))
    }
}

fn validate_raw_command(args: &[String]) -> Result<()> {
    let command = args
        .first()
        .ok_or_else(|| Error::Config("raw command requires at least one argument".into()))?
        .to_ascii_uppercase();

    match command.as_str() {
        "FLUSHALL" | "FLUSHDB" | "SHUTDOWN" | "CONFIG" | "MODULE" | "SCRIPT" | "EVAL"
        | "EVALSHA" => Err(Error::WriteNotAllowed),
        _ => Ok(()),
    }
}

fn redis_value_to_core(value: redis::Value) -> Value {
    match value {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(value) => Value::Int(value),
        redis::Value::BulkString(bytes) => bytes_to_value(bytes),
        redis::Value::Array(values) | redis::Value::Set(values) => {
            Value::Array(values.into_iter().map(redis_value_to_core).collect())
        }
        redis::Value::SimpleString(value) => Value::Text(value),
        redis::Value::Okay => Value::Text("OK".to_owned()),
        redis::Value::Map(values) => redis_pairs_to_map(values),
        redis::Value::Attribute { data, attributes } => {
            let mut map = BTreeMap::new();
            map.insert("data".to_owned(), redis_value_to_core(*data));
            map.insert("attributes".to_owned(), redis_pairs_to_map(attributes));
            Value::Map(map)
        }
        redis::Value::Double(value) => Value::Float(value),
        redis::Value::Boolean(value) => Value::Bool(value),
        redis::Value::VerbatimString { text, .. } => Value::Text(text),
        redis::Value::BigNumber(value) => Value::Text(value.to_string()),
        redis::Value::Push { kind, data } => {
            let mut map = BTreeMap::new();
            map.insert("kind".to_owned(), Value::Text(format!("{kind:?}")));
            map.insert(
                "data".to_owned(),
                Value::Array(data.into_iter().map(redis_value_to_core).collect()),
            );
            Value::Map(map)
        }
        redis::Value::ServerError(error) => Value::Text(format!("{error:?}")),
    }
}

fn redis_pairs_to_map(values: Vec<(redis::Value, redis::Value)>) -> Value {
    let map = values
        .into_iter()
        .map(|(key, value)| (redis_key_to_string(key), redis_value_to_core(value)))
        .collect();
    Value::Map(map)
}

fn redis_key_to_string(value: redis::Value) -> String {
    match redis_value_to_core(value) {
        Value::Text(value) => value,
        Value::Int(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        other => serde_json::to_string(&other).unwrap_or_else(|_| "<non-string-key>".to_owned()),
    }
}

fn bytes_to_value(bytes: Vec<u8>) -> Value {
    String::from_utf8(bytes)
        .map(Value::Text)
        .unwrap_or_else(|err| Value::Bytes(err.into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_command_validation_blocks_global_destructive_commands() {
        assert!(matches!(
            validate_raw_command(&["FLUSHALL".to_owned()]),
            Err(Error::WriteNotAllowed)
        ));
        assert!(validate_raw_command(&["XLEN".to_owned(), "stream".to_owned()]).is_ok());
    }

    #[test]
    fn redis_values_convert_to_typed_core_values() {
        let value = redis_value_to_core(redis::Value::Array(vec![
            redis::Value::Int(42),
            redis::Value::BulkString(b"hello".to_vec()),
            redis::Value::Boolean(true),
        ]));

        assert_eq!(
            value,
            Value::Array(vec![
                Value::Int(42),
                Value::Text("hello".to_owned()),
                Value::Bool(true),
            ])
        );
    }
}
