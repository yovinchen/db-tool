use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ConsumeOptions, LagInfo, Message, MessagePlacement, PartitionWatermark, ProduceOutcome,
        TopicDetail, TopicInfo, Value,
    },
    port::{
        capability::{AdminInspect, KeyValueStore, MessageConsumer, MessageProducer, SetOptions},
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::{future::BoxFuture, StreamExt};
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamId, StreamInfoStreamReply, StreamReadReply},
    AsyncCommands, Client,
};
use std::collections::{BTreeMap, HashMap};
use tokio::time::{timeout, Instant};

pub struct RedisAdapter {
    client: Client,
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
            client,
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
            admin: true,
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

    fn as_producer(&self) -> Option<&dyn MessageProducer> {
        Some(self)
    }

    fn as_consumer(&self) -> Option<&dyn MessageConsumer> {
        Some(self)
    }

    fn as_admin(&self) -> Option<&dyn AdminInspect> {
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

#[async_trait::async_trait]
impl MessageProducer for RedisAdapter {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        match parse_message_target(target)? {
            RedisMessageTarget::Stream(stream) => self.produce_stream(stream, messages).await,
            RedisMessageTarget::PubSub(channel) => self.publish_pubsub(channel, messages).await,
        }
    }
}

#[async_trait::async_trait]
impl MessageConsumer for RedisAdapter {
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        match parse_message_target(source)? {
            RedisMessageTarget::Stream(stream) => self.consume_stream(stream, options).await,
            RedisMessageTarget::PubSub(channel) => self.consume_pubsub(channel, options).await,
        }
    }
}

#[async_trait::async_trait]
impl AdminInspect for RedisAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let mut cursor = 0_u64;
        let mut topics = Vec::new();
        let mut c = self.conn.lock().await;

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("TYPE")
                .arg("stream")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;

            topics.extend(keys.into_iter().map(|name| TopicInfo {
                name,
                partitions: 1,
                replicas: 1,
            }));

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    async fn topic_detail(&self, name: &str) -> Result<TopicDetail> {
        match parse_message_target(name)? {
            RedisMessageTarget::Stream(stream) => self.stream_detail(stream).await,
            RedisMessageTarget::PubSub(channel) => self.pubsub_detail(channel).await,
        }
    }

    async fn consumer_lag(&self, group: &str) -> Result<Vec<LagInfo>> {
        Ok(vec![LagInfo {
            topic: String::new(),
            partition: 0,
            group: group.to_owned(),
            committed: 0,
            latest: 0,
            lag: 0,
        }])
    }
}

impl RedisAdapter {
    async fn produce_stream(&self, stream: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        let mut placements = Vec::with_capacity(messages.len());
        let mut c = self.conn.lock().await;

        for message in messages {
            let mut cmd = redis::cmd("XADD");
            cmd.arg(stream)
                .arg("*")
                .arg("payload")
                .arg(&message.payload[..]);
            if let Some(key) = message.key {
                cmd.arg("key").arg(&key[..]);
            }
            for (key, value) in message.headers {
                cmd.arg(format!("h:{key}")).arg(value);
            }

            let id: String = cmd
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            placements.push(MessagePlacement {
                partition: 0,
                offset: redis_stream_offset(&id),
            });
        }

        Ok(ProduceOutcome {
            produced: placements.len() as u64,
            placements,
        })
    }

    async fn publish_pubsub(
        &self,
        channel: &str,
        messages: Vec<Message>,
    ) -> Result<ProduceOutcome> {
        let mut c = self.conn.lock().await;
        let mut produced = 0_u64;

        for message in messages {
            c.publish::<_, _, i64>(channel, &message.payload[..])
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            produced += 1;
        }

        Ok(ProduceOutcome {
            produced,
            placements: vec![],
        })
    }

    async fn consume_stream(&self, stream: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        if options.max == 0 {
            return Ok(vec![]);
        }

        let offset = options
            .offset
            .map(|offset| format!("{offset}-0"))
            .unwrap_or_else(|| "0-0".to_owned());
        let block_ms = duration_millis_usize(options.timeout);
        let mut c = self.conn.lock().await;
        let reply: StreamReadReply = redis::cmd("XREAD")
            .arg("COUNT")
            .arg(options.max)
            .arg("BLOCK")
            .arg(block_ms)
            .arg("STREAMS")
            .arg(stream)
            .arg(offset)
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        Ok(reply
            .keys
            .into_iter()
            .flat_map(|key| key.ids.into_iter())
            .take(options.max)
            .map(stream_id_to_message)
            .collect())
    }

    async fn consume_pubsub(&self, channel: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        if options.max == 0 {
            return Ok(vec![]);
        }

        let mut pubsub = self
            .client
            .get_async_pubsub()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        pubsub
            .subscribe(channel)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        let deadline = Instant::now() + options.timeout;
        let mut stream = pubsub.on_message();
        let mut messages = Vec::new();

        while messages.len() < options.max {
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            match timeout(deadline - now, stream.next()).await {
                Ok(Some(message)) => {
                    let payload = message
                        .get_payload::<Vec<u8>>()
                        .map_err(|e| Error::Query(e.to_string()))?;
                    let channel = message
                        .get_channel::<String>()
                        .map_err(|e| Error::Query(e.to_string()))?;
                    messages.push(Message {
                        key: None,
                        payload: payload.into(),
                        headers: HashMap::from([("redis_channel".to_owned(), channel)]),
                        partition: None,
                        offset: None,
                        timestamp: None,
                    });
                }
                Ok(None) | Err(_) => break,
            }
        }

        Ok(messages)
    }

    async fn stream_detail(&self, stream: &str) -> Result<TopicDetail> {
        let mut c = self.conn.lock().await;
        let info: StreamInfoStreamReply = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(stream)
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        let mut config = HashMap::new();
        config.insert("kind".to_owned(), "stream".to_owned());
        config.insert("length".to_owned(), info.length.to_string());
        config.insert("groups".to_owned(), info.groups.to_string());
        config.insert(
            "last_generated_id".to_owned(),
            info.last_generated_id.clone(),
        );

        Ok(TopicDetail {
            info: TopicInfo {
                name: stream.to_owned(),
                partitions: 1,
                replicas: 1,
            },
            config,
            watermarks: vec![PartitionWatermark {
                partition: 0,
                low: redis_stream_offset(&info.first_entry.id),
                high: redis_stream_offset(&info.last_generated_id),
            }],
        })
    }

    async fn pubsub_detail(&self, channel: &str) -> Result<TopicDetail> {
        let mut c = self.conn.lock().await;
        let counts: Vec<(String, i64)> = redis::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg(channel)
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let subscribers = counts
            .into_iter()
            .find(|(name, _)| name == channel)
            .map(|(_, count)| count)
            .unwrap_or_default();

        Ok(TopicDetail {
            info: TopicInfo {
                name: channel.to_owned(),
                partitions: 1,
                replicas: 1,
            },
            config: HashMap::from([
                ("kind".to_owned(), "pubsub".to_owned()),
                ("subscribers".to_owned(), subscribers.to_string()),
            ]),
            watermarks: vec![],
        })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedisMessageTarget<'a> {
    Stream(&'a str),
    PubSub(&'a str),
}

fn parse_message_target(target: &str) -> Result<RedisMessageTarget<'_>> {
    let parsed = if let Some(stream) = target.strip_prefix("stream:") {
        RedisMessageTarget::Stream(stream)
    } else if let Some(channel) = target.strip_prefix("pubsub:") {
        RedisMessageTarget::PubSub(channel)
    } else {
        RedisMessageTarget::Stream(target)
    };

    match parsed {
        RedisMessageTarget::Stream(stream) => validate_redis_name("stream", stream)?,
        RedisMessageTarget::PubSub(channel) => validate_redis_name("pubsub channel", channel)?,
    }

    Ok(parsed)
}

fn validate_redis_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty() || name.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(Error::Query(format!("invalid Redis {kind} name: {name:?}")));
    }
    Ok(())
}

fn stream_id_to_message(entry: StreamId) -> Message {
    let payload = entry.get::<Vec<u8>>("payload").unwrap_or_default();
    let key = entry.get::<Vec<u8>>("key").map(bytes::Bytes::from);
    let mut headers = HashMap::from([("redis_stream_id".to_owned(), entry.id.clone())]);

    for (field, value) in entry.map {
        if let Some(header) = field.strip_prefix("h:") {
            headers.insert(header.to_owned(), redis_field_to_string(&value));
        }
    }

    Message {
        key,
        payload: payload.into(),
        headers,
        partition: Some(0),
        offset: Some(redis_stream_offset(&entry.id)),
        timestamp: redis_stream_timestamp(&entry.id),
    }
}

fn redis_field_to_string(value: &redis::Value) -> String {
    match value {
        redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        redis::Value::SimpleString(value) => value.clone(),
        redis::Value::Int(value) => value.to_string(),
        other => format!("{other:?}"),
    }
}

fn redis_stream_offset(id: &str) -> i64 {
    id.split_once('-')
        .map(|(millis, _)| millis)
        .unwrap_or(id)
        .parse()
        .unwrap_or_default()
}

fn redis_stream_timestamp(id: &str) -> Option<i64> {
    Some(redis_stream_offset(id)).filter(|value| *value > 0)
}

fn duration_millis_usize(duration: std::time::Duration) -> usize {
    duration.as_millis().clamp(1, usize::MAX as u128) as usize
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

    #[test]
    fn redis_message_targets_are_explicit_or_stream_by_default() {
        assert_eq!(
            parse_message_target("events").unwrap(),
            RedisMessageTarget::Stream("events")
        );
        assert_eq!(
            parse_message_target("stream:events").unwrap(),
            RedisMessageTarget::Stream("events")
        );
        assert_eq!(
            parse_message_target("pubsub:events").unwrap(),
            RedisMessageTarget::PubSub("events")
        );
        assert!(parse_message_target("stream:").is_err());
    }

    #[test]
    fn redis_stream_ids_map_to_offsets_and_timestamps() {
        assert_eq!(redis_stream_offset("1710000000000-3"), 1_710_000_000_000);
        assert_eq!(
            redis_stream_timestamp("1710000000000-3"),
            Some(1_710_000_000_000)
        );
        assert_eq!(redis_stream_offset("bad-id"), 0);
        assert_eq!(redis_stream_timestamp("bad-id"), None);
    }
}
