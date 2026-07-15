use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ConsumeOptions, DeleteResourceOptions, DeleteResourceOutcome, LagInfo, Message,
        MessagePlacement, MessageResource, MessageResourceKind, PartitionWatermark, ProduceOutcome,
        TopicDetail, TopicInfo, Value,
    },
    port::{
        capability::{
            AdminInspect, AdminMutate, KeyValueStore, MessageConsumer, MessageProducer, SetOptions,
        },
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
};
use futures::{future::BoxFuture, StreamExt};
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamId, StreamInfoGroupsReply, StreamInfoStreamReply, StreamReadReply},
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
        let driver_url = dsn.raw_with_scheme("redis")?;
        let client =
            Client::open(driver_url.as_str()).map_err(|e| Error::Connection(e.to_string()))?;
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

    fn operations(&self) -> Vec<CapabilityOperation> {
        redis_operations(self.capabilities())
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

    fn as_admin_mutate(&self) -> Option<&dyn AdminMutate> {
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
        let nx = options.nx;
        let response: Option<String> = redis_set_command(key, value, options)
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

        if nx && response.is_none() {
            return Err(Error::Query(
                "SET NX condition not met because the key already exists".to_owned(),
            ));
        }

        Ok(())
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

fn redis_set_command(key: &str, value: &[u8], options: SetOptions) -> redis::Cmd {
    let mut command = redis::cmd("SET");
    command.arg(key).arg(value);
    if let Some(ttl) = options.ttl_secs {
        command.arg("EX").arg(ttl);
    }
    if options.nx {
        command.arg("NX");
    }
    command
}

#[async_trait::async_trait]
impl MessageProducer for RedisAdapter {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        match parse_message_target(target)? {
            RedisMessageTarget::Stream(stream) => {
                for message in &messages {
                    validate_stream_produce_message(message)?;
                }
                self.produce_stream(stream, messages).await
            }
            RedisMessageTarget::PubSub(channel) => {
                for message in &messages {
                    validate_pubsub_produce_message(message)?;
                }
                self.publish_pubsub(channel, messages).await
            }
        }
    }
}

#[async_trait::async_trait]
impl MessageConsumer for RedisAdapter {
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        match parse_message_target(source)? {
            RedisMessageTarget::Stream(stream) => {
                validate_stream_consume_options(&options)?;
                self.consume_stream(stream, options).await
            }
            RedisMessageTarget::PubSub(channel) => {
                validate_pubsub_consume_options(&options)?;
                self.consume_pubsub(channel, options).await
            }
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
        let streams = self.list_topics().await?;
        let mut results = Vec::new();

        for stream in streams {
            let mut c = self.conn.lock().await;
            let groups: StreamInfoGroupsReply = redis::cmd("XINFO")
                .arg("GROUPS")
                .arg(&stream.name)
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            let latest: u64 = redis::cmd("XLEN")
                .arg(&stream.name)
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            drop(c);

            for g in groups.groups {
                if g.name != group {
                    continue;
                }
                let (latest, committed, lag) = redis_lag_dimensions(latest, g.lag)?;
                results.push(LagInfo {
                    topic: stream.name.clone(),
                    partition: 0,
                    group: group.to_owned(),
                    committed,
                    latest,
                    lag,
                });
            }
        }

        Ok(results)
    }
}

#[async_trait::async_trait]
impl AdminMutate for RedisAdapter {
    async fn delete_resource(
        &self,
        resource: MessageResource,
        options: DeleteResourceOptions,
    ) -> Result<DeleteResourceOutcome> {
        validate_redis_delete_request(&resource, options)?;

        // TYPE, XLEN, and DEL must be one atomic server-side operation. A
        // separate preflight would allow another client to replace the stream
        // with a different key type between TYPE and DEL.
        let script = r#"
local resource_type = redis.call('TYPE', KEYS[1]).ok
if resource_type == 'none' then
  return {0, 0}
end
if resource_type ~= 'stream' then
  return {-1, 0}
end
local messages = redis.call('XLEN', KEYS[1])
local deleted = redis.call('DEL', KEYS[1])
return {deleted, messages}
"#;
        let mut c = self.conn.lock().await;
        let (delete_status, messages_before): (i64, u64) = redis::cmd("EVAL")
            .arg(script)
            .arg(1)
            .arg(&resource.name)
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        match delete_status {
            1 => {}
            0 => {
                return Err(Error::Query(format!(
                    "Redis Stream {:?} does not exist",
                    resource.name
                )))
            }
            -1 => {
                return Err(Error::Query(format!(
                    "Redis resource {:?} is not a stream",
                    resource.name
                )))
            }
            status => {
                return Err(Error::Query(format!(
                    "Redis returned unexpected stream deletion status {status} for {:?}",
                    resource.name
                )))
            }
        }
        let resource_type_after: String = redis::cmd("TYPE")
            .arg(&resource.name)
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        if resource_type_after != "none" {
            return Err(Error::Query(format!(
                "Redis deleted stream {:?}, but a resource now exists at that key",
                resource.name
            )));
        }

        Ok(DeleteResourceOutcome {
            resource,
            acknowledged: true,
            verified_absent: true,
            messages_before: Some(messages_before),
            consumers_before: None,
        })
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
        let block_ms = duration_millis_usize(options.timeout)?;
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

        let deadline = checked_deadline(options.timeout)?;
        let mut pubsub = self
            .client
            .get_async_pubsub()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        pubsub
            .subscribe(channel)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;

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

fn redis_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::MessageAdminListTopics,
        CapabilityOperation::MessageAdminTopicDetail,
        CapabilityOperation::MessageAdminConsumerLag,
        CapabilityOperation::MessageAdminDelete,
    ]);
    operations
}

fn validate_redis_delete_request(
    resource: &MessageResource,
    options: DeleteResourceOptions,
) -> Result<()> {
    if resource.kind != MessageResourceKind::RedisStream {
        return Err(Error::Config(format!(
            "Redis can delete only redis-stream resources, not {}",
            resource.kind.as_str()
        )));
    }
    if options.if_empty || options.if_unused {
        return Err(Error::Config(
            "Redis Stream deletion does not support AMQP if-empty/if-unused options".into(),
        ));
    }
    validate_redis_name("stream", &resource.name)
}

fn redis_lag_dimensions(latest: u64, lag: Option<usize>) -> Result<(i64, i64, i64)> {
    let lag = lag.ok_or_else(|| {
        Error::Query(
            "Redis server did not report consumer-group lag; pending is not a lag substitute"
                .into(),
        )
    })?;
    let lag = u64::try_from(lag)
        .map_err(|_| Error::Serialization("Redis consumer-group lag exceeds u64".into()))?;
    let committed = latest.checked_sub(lag).ok_or_else(|| {
        Error::Serialization(format!(
            "Redis consumer-group lag {lag} exceeds stream length {latest}"
        ))
    })?;

    Ok((
        i64::try_from(latest)
            .map_err(|_| Error::Serialization("Redis stream length exceeds i64".into()))?,
        i64::try_from(committed)
            .map_err(|_| Error::Serialization("Redis committed count exceeds i64".into()))?,
        i64::try_from(lag)
            .map_err(|_| Error::Serialization("Redis consumer-group lag exceeds i64".into()))?,
    ))
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

fn validate_stream_produce_message(message: &Message) -> Result<()> {
    validate_stream_partition(message.partition, "producer")?;
    if message.offset.is_some() {
        return Err(Error::Config(
            "Redis Stream producer does not support producer offsets".into(),
        ));
    }
    if message.timestamp.is_some() {
        return Err(Error::Config(
            "Redis Stream producer does not support caller-supplied timestamps".into(),
        ));
    }
    Ok(())
}

fn validate_pubsub_produce_message(message: &Message) -> Result<()> {
    if message.key.is_some() {
        return Err(Error::Config(
            "Redis PubSub producer supports payload only; message keys are not supported".into(),
        ));
    }
    if !message.headers.is_empty() {
        return Err(Error::Config(
            "Redis PubSub producer supports payload only; headers are not supported".into(),
        ));
    }
    if message.partition.is_some() {
        return Err(Error::Config(
            "Redis PubSub producer supports payload only; partitions are not supported".into(),
        ));
    }
    if message.offset.is_some() {
        return Err(Error::Config(
            "Redis PubSub producer supports payload only; producer offsets are not supported"
                .into(),
        ));
    }
    if message.timestamp.is_some() {
        return Err(Error::Config(
            "Redis PubSub producer supports payload only; timestamps are not supported".into(),
        ));
    }
    Ok(())
}

fn validate_stream_consume_options(options: &ConsumeOptions) -> Result<()> {
    validate_stream_partition(options.partition, "consumer")?;
    if options.offset.is_some_and(|offset| offset < 0) {
        return Err(Error::Config(
            "Redis Stream consumer offset must be greater than or equal to zero".into(),
        ));
    }
    Ok(())
}

fn validate_pubsub_consume_options(options: &ConsumeOptions) -> Result<()> {
    if options.partition.is_some() {
        return Err(Error::Config(
            "Redis PubSub consumer does not support partitions".into(),
        ));
    }
    if options.offset.is_some() {
        return Err(Error::Config(
            "Redis PubSub consumer does not support offsets".into(),
        ));
    }
    Ok(())
}

fn validate_stream_partition(partition: Option<i32>, operation: &str) -> Result<()> {
    if partition.is_some_and(|partition| partition != 0) {
        return Err(Error::Config(format!(
            "Redis Stream {operation} supports only partition 0"
        )));
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

fn duration_millis_usize(duration: std::time::Duration) -> Result<usize> {
    if duration.is_zero() {
        return Err(Error::Config(
            "Redis message timeout must be greater than zero".into(),
        ));
    }

    let sub_millisecond = duration
        .subsec_nanos()
        .checked_rem(1_000_000)
        .is_some_and(|remainder| remainder != 0);
    let milliseconds = duration
        .as_millis()
        .checked_add(u128::from(sub_millisecond))
        .ok_or_else(|| Error::Config("Redis message timeout is too large".into()))?;
    usize::try_from(milliseconds)
        .map_err(|_| Error::Config("Redis message timeout is too large for this platform".into()))
}

fn checked_deadline(timeout: std::time::Duration) -> Result<Instant> {
    Instant::now().checked_add(timeout).ok_or_else(|| {
        Error::Config("Redis PubSub consume timeout is too large for this platform".into())
    })
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

    fn message() -> Message {
        Message {
            key: None,
            payload: bytes::Bytes::from_static(b"payload"),
            headers: HashMap::new(),
            partition: None,
            offset: None,
            timestamp: None,
        }
    }

    #[test]
    fn raw_command_validation_blocks_global_destructive_commands() {
        assert!(matches!(
            validate_raw_command(&["FLUSHALL".to_owned()]),
            Err(Error::WriteNotAllowed)
        ));
        assert!(validate_raw_command(&["XLEN".to_owned(), "stream".to_owned()]).is_ok());
    }

    #[test]
    fn set_command_combines_expiry_and_nx_atomically() {
        let command = redis_set_command(
            "user:1",
            b"alice",
            SetOptions {
                ttl_secs: Some(30),
                nx: true,
            },
        );
        let packed = String::from_utf8(command.get_packed_command()).unwrap();

        assert_eq!(
            packed,
            "*6\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$5\r\nalice\r\n$2\r\nEX\r\n$2\r\n30\r\n$2\r\nNX\r\n"
        );
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

    #[test]
    fn redis_stream_preserves_supported_metadata_and_rejects_the_rest() {
        let mut supported = message();
        supported.key = Some(bytes::Bytes::from_static(b"key"));
        supported
            .headers
            .insert("trace".to_owned(), "abc".to_owned());
        supported.partition = Some(0);
        assert!(validate_stream_produce_message(&supported).is_ok());

        let mut unsupported = message();
        unsupported.partition = Some(1);
        assert!(matches!(
            validate_stream_produce_message(&unsupported),
            Err(Error::Config(message)) if message.contains("partition 0")
        ));

        let mut unsupported = message();
        unsupported.offset = Some(1);
        assert!(matches!(
            validate_stream_produce_message(&unsupported),
            Err(Error::Config(message)) if message.contains("producer offsets")
        ));

        let mut unsupported = message();
        unsupported.timestamp = Some(1_710_000_000_123);
        assert!(matches!(
            validate_stream_produce_message(&unsupported),
            Err(Error::Config(message)) if message.contains("timestamps")
        ));
    }

    #[test]
    fn redis_pubsub_rejects_every_non_payload_field_and_position() {
        let mut unsupported = message();
        unsupported.key = Some(bytes::Bytes::from_static(b"key"));
        assert!(matches!(
            validate_pubsub_produce_message(&unsupported),
            Err(Error::Config(message)) if message.contains("message keys")
        ));

        let mut unsupported = message();
        unsupported
            .headers
            .insert("trace".to_owned(), "abc".to_owned());
        assert!(matches!(
            validate_pubsub_produce_message(&unsupported),
            Err(Error::Config(message)) if message.contains("headers")
        ));

        for field in ["partitions", "producer offsets", "timestamps"] {
            let mut unsupported = message();
            match field {
                "partitions" => unsupported.partition = Some(0),
                "producer offsets" => unsupported.offset = Some(0),
                "timestamps" => unsupported.timestamp = Some(0),
                _ => unreachable!(),
            }
            assert!(matches!(
                validate_pubsub_produce_message(&unsupported),
                Err(Error::Config(message)) if message.contains(field)
            ));
        }

        assert!(matches!(
            validate_pubsub_consume_options(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: Some(0),
                offset: None,
            }),
            Err(Error::Config(message)) if message.contains("partitions")
        ));
        assert!(matches!(
            validate_pubsub_consume_options(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: None,
                offset: Some(0),
            }),
            Err(Error::Config(message)) if message.contains("offsets")
        ));
    }

    #[test]
    fn redis_stream_consumer_accepts_only_partition_zero() {
        let options = |partition| ConsumeOptions {
            max: 1,
            timeout: std::time::Duration::from_secs(1),
            partition,
            offset: Some(42),
        };

        assert!(validate_stream_consume_options(&options(None)).is_ok());
        assert!(validate_stream_consume_options(&options(Some(0))).is_ok());
        assert!(matches!(
            validate_stream_consume_options(&options(Some(1))),
            Err(Error::Config(message)) if message.contains("partition 0")
        ));

        let mut negative_offset = options(Some(0));
        negative_offset.offset = Some(-1);
        assert!(matches!(
            validate_stream_consume_options(&negative_offset),
            Err(Error::Config(message)) if message.contains("greater than or equal to zero")
        ));
    }

    #[test]
    fn redis_timeout_conversion_rounds_up_and_rejects_overflow() {
        assert!(matches!(
            duration_millis_usize(std::time::Duration::ZERO),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
        assert_eq!(
            duration_millis_usize(std::time::Duration::from_micros(1)).unwrap(),
            1
        );
        assert_eq!(
            duration_millis_usize(std::time::Duration::from_millis(1500)).unwrap(),
            1500
        );
        assert!(duration_millis_usize(std::time::Duration::MAX).is_err());
    }

    #[test]
    fn redis_declares_complete_real_admin_profile() {
        let operations = redis_operations(Capabilities {
            key_value: true,
            producer: true,
            consumer: true,
            admin: true,
            ..Default::default()
        });

        for operation in [
            CapabilityOperation::MessageAdminListTopics,
            CapabilityOperation::MessageAdminTopicDetail,
            CapabilityOperation::MessageAdminConsumerLag,
            CapabilityOperation::MessageAdminDelete,
        ] {
            assert!(operations.contains(&operation));
        }
        assert!(operations.contains(&CapabilityOperation::KeyValueGet));
        assert!(operations.contains(&CapabilityOperation::MessageProduce));
        assert!(operations.contains(&CapabilityOperation::MessageConsume));
    }

    #[test]
    fn redis_delete_accepts_only_streams_without_amqp_options() {
        let stream = MessageResource {
            kind: MessageResourceKind::RedisStream,
            name: "events".to_owned(),
        };
        assert!(validate_redis_delete_request(&stream, DeleteResourceOptions::default()).is_ok());
        assert!(matches!(
            validate_redis_delete_request(
                &stream,
                DeleteResourceOptions {
                    if_empty: true,
                    if_unused: false,
                }
            ),
            Err(Error::Config(message)) if message.contains("AMQP")
        ));

        let queue = MessageResource {
            kind: MessageResourceKind::AmqpQueue,
            name: "events".to_owned(),
        };
        assert!(matches!(
            validate_redis_delete_request(&queue, DeleteResourceOptions::default()),
            Err(Error::Config(message)) if message.contains("redis-stream")
        ));
    }

    #[test]
    fn redis_lag_uses_server_lag_and_count_dimensions_only() {
        assert_eq!(redis_lag_dimensions(10, Some(3)).unwrap(), (10, 7, 3));
        assert!(matches!(
            redis_lag_dimensions(10, None),
            Err(Error::Query(message)) if message.contains("pending is not a lag substitute")
        ));
        assert!(matches!(
            redis_lag_dimensions(2, Some(3)),
            Err(Error::Serialization(message)) if message.contains("exceeds stream length")
        ));
    }
}
