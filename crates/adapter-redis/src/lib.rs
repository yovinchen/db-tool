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
            producer: true,
            consumer: true,
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
        let mut cmd = redis::cmd(args[0].as_str());
        for arg in &args[1..] {
            cmd.arg(arg.as_str());
        }
        let mut c = self.conn.lock().await;
        let val: redis::Value = cmd
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(Value::Text(format!("{val:?}")))
    }
}
