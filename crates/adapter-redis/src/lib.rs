use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        AckMode, BoundedList, ConsumeCursor, ConsumeOptions, ConsumerIdentity,
        DeleteResourceOptions, DeleteResourceOutcome, KeyExpiry, KeyValueRestoreOutcome,
        KeyValueSnapshot, LagInfo, Message, MessageCursor, MessagePlacement, MessageResource,
        MessageResourceKind, MetadataBudget, PartitionWatermark, ProduceOutcome, TopicDetail,
        TopicInfo, Value,
    },
    port::{
        capability::{
            AdminInspect, AdminMutate, KeyValueStore, MessageConsumer, MessageProducer, SetOptions,
        },
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::{ListLimiter, MetadataLimiter},
};
use futures::{future::BoxFuture, StreamExt};
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamInfoGroupsReply, StreamInfoStreamReply},
    AsyncCommands, Client,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use tokio::time::{timeout, Instant};

const REDIS_SCAN_COUNT: usize = 10;
const REDIS_STREAM_SCAN_COUNT: usize = 100;
const REDIS_STREAM_SCAN_PAGE_MAX: usize = 4096;
const REDIS_STREAM_SCAN_KEY_BYTES_MAX: usize = 896 * 1024;
const REDIS_STREAM_SCAN_PAGE_SCRIPT: &str = r#"
local page = redis.call('SCAN', ARGV[1], 'TYPE', 'stream', 'COUNT', ARGV[2])
local keys = page[2]
local max_items = tonumber(ARGV[3])
local max_key_bytes = tonumber(ARGV[4])
if #keys > max_items then
  return redis.error_reply('dbtool stream catalog page exceeds item budget')
end
local key_bytes = 0
for index = 1, #keys do
  key_bytes = key_bytes + string.len(keys[index])
  if key_bytes > max_key_bytes then
    return redis.error_reply('dbtool stream catalog page exceeds byte budget')
  end
end
return {page[1], keys}
"#;
// XINFO STREAM includes the complete first and last entry payloads. Keep
// those payloads inside Redis and return only the scalar fields needed by the
// portable TopicDetail so one large Stream entry cannot become an unbounded
// RESP response.
const REDIS_STREAM_DETAIL_SCRIPT: &str = r#"
local resource_type = redis.call('TYPE', KEYS[1]).ok
if resource_type == 'none' then
  return redis.error_reply('dbtool stream does not exist')
end
if resource_type ~= 'stream' then
  return redis.error_reply('dbtool resource is not a stream')
end

local info = redis.call('XINFO', 'STREAM', KEYS[1])
local function field(name)
  for index = 1, #info, 2 do
    if info[index] == name then
      return info[index + 1]
    end
  end
  return nil
end

local length = field('length')
local groups = field('groups')
local last_generated_id = field('last-generated-id')
local first_entry = field('first-entry')
if length == nil or groups == nil or last_generated_id == nil then
  return redis.error_reply('dbtool incomplete XINFO STREAM response')
end
local first_id = '0-0'
if type(first_entry) == 'table' and first_entry[1] ~= nil then
  first_id = first_entry[1]
end
return {length, groups, last_generated_id, first_id}
"#;

// XINFO GROUPS has no server-side name filter. Run it in Lua, inspect at most
// the caller-owned N+1 work allowance, and return one fixed-shape match rather
// than every consumer group's metadata over RESP.
const REDIS_GROUP_LAG_SCRIPT: &str = r#"
local resource_type = redis.call('TYPE', KEYS[1]).ok
if resource_type ~= 'stream' then
  return redis.error_reply('dbtool resource is not a stream')
end
local max_groups = tonumber(ARGV[2])
if max_groups == nil or max_groups < 1 then
  return redis.error_reply('dbtool invalid consumer group scan budget')
end

local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
local inspected = 0
for _, info in ipairs(groups) do
  inspected = inspected + 1
  -- max_groups is the caller's remaining allowance plus one probe item.
  -- Stop on that probe so server-side inspection is bounded by N+1 rather
  -- than examining one additional group before reporting overflow.
  if inspected >= max_groups then
    return {-1, inspected, '', false, 0, false}
  end
  local values = {}
  for index = 1, #info, 2 do
    values[info[index]] = info[index + 1]
  end
  if values['name'] == ARGV[1] then
    return {
      1,
      inspected,
      values['last-delivered-id'] or '',
      values['entries-read'] or false,
      values['pending'] or 0,
      values['lag'] or false
    }
  end
end
return {0, inspected, '', false, 0, false}
"#;
const REDIS_LUA_SAFE_INTEGER_MAX_MS: i64 = 9_007_199_254_740_991;

// TIME intentionally precedes PTTL. Computing the absolute deadline from an
// earlier server timestamp and a later remaining TTL can only shorten the
// observed lifetime; it can never extend the source key's lifetime.
const GET_WITH_EXPIRY_SCRIPT: &str = r#"
local value = redis.call('GET', KEYS[1])
local server_time = redis.call('TIME')
local pttl = redis.call('PTTL', KEYS[1])
if value == false or pttl == -2 then
  return {0, '', server_time[1], server_time[2], pttl}
end
return {1, value, server_time[1], server_time[2], pttl}
"#;

// Status values: 1 = stored, 0 = NX condition not met, 2 = expired,
// -1 = invalid server time, -2 = invalid adapter input.
const RESTORE_WITH_EXPIRY_SCRIPT: &str = r#"
local server_time = redis.call('TIME')
local seconds = tonumber(server_time[1])
local micros = tonumber(server_time[2])
local max_safe_ms = 9007199254740991
if seconds == nil or micros == nil or seconds < 0 or micros < 0 or micros >= 1000000 then
  return -1
end
local now_ms = seconds * 1000 + math.floor(micros / 1000)
if now_ms > max_safe_ms then
  return -1
end

local mode = ARGV[2]
local nx = ARGV[4]
if nx ~= '0' and nx ~= '1' then
  return -2
end

if mode == 'persistent' then
  local stored
  if nx == '1' then
    stored = redis.call('SET', KEYS[1], ARGV[1], 'NX')
  else
    stored = redis.call('SET', KEYS[1], ARGV[1])
  end
  if stored == false then
    return 0
  end
  return 1
end

if mode ~= 'expires_at_unix_ms' then
  return -2
end
local deadline_text = ARGV[3]
if string.sub(deadline_text, 1, 1) == '-' then
  return 2
end
local deadline = tonumber(deadline_text)
if deadline == nil or deadline < 0 or deadline > max_safe_ms or deadline ~= math.floor(deadline) then
  return -2
end
if deadline <= now_ms then
  return 2
end

local remaining = string.format('%.0f', deadline - now_ms)
local stored
if nx == '1' then
  stored = redis.call('SET', KEYS[1], ARGV[1], 'PX', remaining, 'NX')
else
  stored = redis.call('SET', KEYS[1], ARGV[1], 'PX', remaining)
end
if stored == false then
  return 0
end
return 1
"#;

pub struct RedisAdapter {
    client: Client,
    conn: tokio::sync::Mutex<MultiplexedConnection>,
    kind: ConnectorKind,
    consumer_lag_supported: bool,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let driver_url = dsn.raw_with_scheme("redis")?;
        let client =
            Client::open(driver_url.as_str()).map_err(|e| Error::Connection(e.to_string()))?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        let kind = ConnectorKind(dsn.scheme);
        let consumer_lag_supported = detect_consumer_lag_support(&kind, &mut conn).await;
        Ok(Box::new(RedisAdapter {
            client,
            conn: tokio::sync::Mutex::new(conn),
            kind,
            consumer_lag_supported,
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
        redis_operations(self.capabilities(), self.consumer_lag_supported)
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

    async fn get_with_expiry(&self, key: &str) -> Result<Option<KeyValueSnapshot>> {
        let mut c = self.conn.lock().await;
        let response: (i64, Vec<u8>, i64, i64, i64) = redis::cmd("EVAL")
            .arg(GET_WITH_EXPIRY_SCRIPT)
            .arg(1)
            .arg(key)
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        decode_get_with_expiry_response(response)
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

    async fn restore_with_expiry(
        &self,
        key: &str,
        value: &[u8],
        expiry: KeyExpiry,
        nx: bool,
    ) -> Result<KeyValueRestoreOutcome> {
        validate_restore_expiry(expiry)?;
        let (mode, deadline) = match expiry {
            KeyExpiry::Persistent => ("persistent", String::new()),
            KeyExpiry::ExpiresAtUnixMs(deadline) => ("expires_at_unix_ms", deadline.to_string()),
        };
        let mut c = self.conn.lock().await;
        let status: i64 = redis::cmd("EVAL")
            .arg(RESTORE_WITH_EXPIRY_SCRIPT)
            .arg(1)
            .arg(key)
            .arg(value)
            .arg(mode)
            .arg(deadline)
            .arg(if nx { "1" } else { "0" })
            .query_async(&mut *c)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        decode_restore_with_expiry_status(status)
    }

    async fn delete(&self, keys: &[String]) -> Result<u64> {
        let mut c = self.conn.lock().await;
        c.del::<_, u64>(keys)
            .await
            .map_err(|e| Error::Query(e.to_string()))
    }

    async fn scan(&self, pattern: &str, limit: usize) -> Result<Vec<String>> {
        let count = redis_scan_count(limit)?;
        let mut c = self.conn.lock().await;
        let mut cursor = 0_u64;
        let mut collector = ScanCollector::new(limit)?;

        loop {
            let (next_cursor, page): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(count)
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;

            match collector.push_page(next_cursor, page)? {
                ScanProgress::Complete => return Ok(collector.into_keys()),
                ScanProgress::Continue(next_cursor) => cursor = next_cursor,
            }
        }
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
        validate_raw_response_budget(&val)?;
        redis_value_to_core(val)
    }
}

fn decode_get_with_expiry_response(
    (status, value, seconds, micros, pttl): (i64, Vec<u8>, i64, i64, i64),
) -> Result<Option<KeyValueSnapshot>> {
    let server_time_ms = checked_redis_time_ms(seconds, micros)?;
    match status {
        0 if pttl == -2 && value.is_empty() => Ok(None),
        0 => Err(Error::Serialization(format!(
            "Redis lifetime script returned inconsistent missing-key state (pttl={pttl}, value_bytes={})",
            value.len()
        ))),
        1 => {
            let expiry = match pttl {
                -1 => KeyExpiry::Persistent,
                pttl if pttl >= 0 => {
                    let deadline = server_time_ms.checked_add(pttl).ok_or_else(|| {
                        Error::Serialization("Redis expiry deadline overflowed i64".into())
                    })?;
                    if deadline > REDIS_LUA_SAFE_INTEGER_MAX_MS {
                        return Err(Error::Serialization(format!(
                            "Redis expiry deadline {deadline} exceeds the exact Lua integer range"
                        )));
                    }
                    KeyExpiry::ExpiresAtUnixMs(deadline)
                }
                other => {
                    return Err(Error::Serialization(format!(
                        "Redis PTTL returned unsupported negative value {other}"
                    )))
                }
            };
            Ok(Some(KeyValueSnapshot {
                value: bytes::Bytes::from(value),
                expiry,
            }))
        }
        other => Err(Error::Serialization(format!(
            "Redis lifetime script returned unexpected status {other}"
        ))),
    }
}

fn checked_redis_time_ms(seconds: i64, micros: i64) -> Result<i64> {
    if seconds < 0 || !(0..1_000_000).contains(&micros) {
        return Err(Error::Serialization(format!(
            "Redis TIME returned invalid seconds/microseconds pair ({seconds}, {micros})"
        )));
    }
    seconds
        .checked_mul(1_000)
        .and_then(|millis| millis.checked_add(micros / 1_000))
        .ok_or_else(|| Error::Serialization("Redis TIME milliseconds overflowed i64".into()))
}

fn validate_restore_expiry(expiry: KeyExpiry) -> Result<()> {
    if let KeyExpiry::ExpiresAtUnixMs(deadline) = expiry {
        if deadline > REDIS_LUA_SAFE_INTEGER_MAX_MS {
            return Err(Error::Config(format!(
                "Redis absolute expiry {deadline} exceeds the exact Lua integer range"
            )));
        }
    }
    Ok(())
}

fn decode_restore_with_expiry_status(status: i64) -> Result<KeyValueRestoreOutcome> {
    match status {
        1 => Ok(KeyValueRestoreOutcome::Stored),
        0 => Ok(KeyValueRestoreOutcome::ConditionNotMet),
        2 => Ok(KeyValueRestoreOutcome::Expired),
        -1 => Err(Error::Serialization(
            "Redis TIME could not be represented safely by the lifetime script".into(),
        )),
        -2 => Err(Error::Internal(
            "Redis lifetime script rejected adapter-generated arguments".into(),
        )),
        other => Err(Error::Serialization(format!(
            "Redis lifetime restore script returned unexpected status {other}"
        ))),
    }
}

fn redis_scan_count(limit: usize) -> Result<u64> {
    if limit == 0 {
        return Err(Error::Config(
            "Redis SCAN limit must be greater than zero".to_owned(),
        ));
    }

    u64::try_from(limit.min(REDIS_SCAN_COUNT))
        .map_err(|_| Error::Config("Redis SCAN page size exceeds the u64 range".to_owned()))
}

#[derive(Debug, PartialEq, Eq)]
enum ScanProgress {
    Complete,
    Continue(u64),
}

struct ScanCollector {
    limit: usize,
    keys: Vec<String>,
    seen_keys: HashSet<String>,
    seen_cursors: HashSet<u64>,
}

impl ScanCollector {
    fn new(limit: usize) -> Result<Self> {
        redis_scan_count(limit)?;
        Ok(Self {
            limit,
            keys: Vec::new(),
            seen_keys: HashSet::new(),
            seen_cursors: HashSet::new(),
        })
    }

    fn push_page(&mut self, next_cursor: u64, raw_keys: Vec<Vec<u8>>) -> Result<ScanProgress> {
        let page = raw_keys
            .into_iter()
            .map(|key| {
                String::from_utf8(key).map_err(|_| {
                    Error::Serialization(
                        "Redis SCAN returned a non-UTF-8 key; the portable key API requires UTF-8"
                            .to_owned(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;

        for key in page {
            if self.seen_keys.insert(key.clone()) {
                self.keys.push(key);
                if self.keys.len() == self.limit {
                    return Ok(ScanProgress::Complete);
                }
            }
        }

        if next_cursor == 0 {
            return Ok(ScanProgress::Complete);
        }
        if !self.seen_cursors.insert(next_cursor) {
            return Err(Error::Query(format!(
                "Redis SCAN cursor {next_cursor} repeated before reaching cursor 0"
            )));
        }

        Ok(ScanProgress::Continue(next_cursor))
    }

    fn into_keys(self) -> Vec<String> {
        self.keys
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
                validate_stream_consume_options(&self.kind, &options)?;
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
        self.scan_stream_topics(None).await
    }

    async fn list_topics_bounded(&self, max_items: usize) -> Result<BoundedList<TopicInfo>> {
        let limiter = ListLimiter::new(max_items);
        let probe_items = limiter.probe_items()?;
        let topics = self.scan_stream_topics(Some(probe_items)).await?;
        Ok(limiter.finish(topics))
    }

    async fn topic_detail(&self, name: &str) -> Result<TopicDetail> {
        match parse_message_target(name)? {
            RedisMessageTarget::Stream(stream) => self.stream_detail(stream).await,
            RedisMessageTarget::PubSub(channel) => self.pubsub_detail(channel).await,
        }
    }

    async fn topic_detail_bounded(
        &self,
        name: &str,
        budget: MetadataBudget,
    ) -> Result<TopicDetail> {
        let detail = match parse_message_target(name)? {
            RedisMessageTarget::Stream(stream) => self.stream_detail_bounded(stream).await?,
            RedisMessageTarget::PubSub(channel) => self.pubsub_detail(channel).await?,
        };
        enforce_redis_topic_detail_budget(detail, budget)
    }

    async fn consumer_lag(&self, group: &str) -> Result<Vec<LagInfo>> {
        if !self.consumer_lag_supported {
            return Err(Error::UnsupportedCapability {
                kind: self.kind.0.clone(),
                needed: CapabilityOperation::MessageAdminConsumerLag.as_str(),
            });
        }
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
            drop(c);

            for g in groups.groups {
                if g.name != group {
                    continue;
                }
                let (latest, committed, lag) =
                    redis_lag_dimensions(&g.last_delivered_id, g.entries_read, g.pending, g.lag)?;
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

    async fn consumer_lag_bounded(
        &self,
        group: &str,
        budget: MetadataBudget,
    ) -> Result<Vec<LagInfo>> {
        if !self.consumer_lag_supported {
            return Err(Error::UnsupportedCapability {
                kind: self.kind.0.clone(),
                needed: CapabilityOperation::MessageAdminConsumerLagBounded.as_str(),
            });
        }
        validate_redis_name("consumer group", group)?;
        let budget = budget.validate()?;
        let mut response_limiter = MetadataLimiter::new(budget, "Redis consumer lag")?;
        let mut inspected_items = 0_usize;
        let streams = self
            .scan_stream_topics(Some(metadata_work_probe(inspected_items, budget)?))
            .await?;
        let mut results = Vec::new();

        for stream in streams {
            observe_metadata_work(&mut inspected_items, budget, "Redis consumer lag scan")?;
            let group_probe = metadata_work_probe(inspected_items, budget)?;
            let mut c = self.conn.lock().await;
            let (status, inspected_groups, last_delivered_id, entries_read, pending, server_lag): (
                i64,
                usize,
                String,
                Option<usize>,
                usize,
                Option<usize>,
            ) = redis::cmd("EVAL")
                .arg(REDIS_GROUP_LAG_SCRIPT)
                .arg(1)
                .arg(&stream.name)
                .arg(group)
                .arg(group_probe)
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            drop(c);

            for _ in 0..inspected_groups {
                observe_metadata_work(&mut inspected_items, budget, "Redis consumer lag scan")?;
            }
            match status {
                0 => {}
                1 => {
                    let (latest, committed, lag) = redis_lag_dimensions(
                        &last_delivered_id,
                        entries_read,
                        pending,
                        server_lag,
                    )?;
                    let item = LagInfo {
                        topic: stream.name,
                        partition: 0,
                        group: group.to_owned(),
                        committed,
                        latest,
                        lag,
                    };
                    response_limiter.observe(&item)?;
                    results.push(item);
                }
                -1 => {
                    return Err(Error::Serialization(
                        "Redis consumer group scan exceeded its probe without tripping the metadata work budget"
                            .into(),
                    ))
                }
                other => {
                    return Err(Error::Serialization(format!(
                        "Redis consumer group scan returned unexpected status {other}"
                    )))
                }
            }
        }

        results.sort_by(|left, right| left.topic.cmp(&right.topic));
        response_limiter.ensure_complete(&results)?;
        Ok(results)
    }
}

impl RedisAdapter {
    async fn scan_stream_topics(&self, probe_items: Option<usize>) -> Result<Vec<TopicInfo>> {
        let mut cursor = 0_u64;
        let mut topics = Vec::new();
        let mut names = HashSet::new();
        let mut cursors = HashSet::new();
        let mut c = self.conn.lock().await;

        loop {
            let requested_count = probe_items
                .map(|limit| limit.saturating_sub(topics.len()))
                .unwrap_or(REDIS_STREAM_SCAN_COUNT)
                .clamp(1, REDIS_STREAM_SCAN_COUNT);
            // Wrap one SCAN page in a read-only Lua call so the server returns
            // either a capped response or a short error. A plain SCAN command
            // would let a COUNT-hint overshoot allocate an arbitrary RESP
            // frame before the client could inspect it.
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("EVAL")
                .arg(REDIS_STREAM_SCAN_PAGE_SCRIPT)
                .arg(0)
                .arg(cursor)
                .arg(requested_count)
                .arg(REDIS_STREAM_SCAN_PAGE_MAX)
                .arg(REDIS_STREAM_SCAN_KEY_BYTES_MAX)
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;

            // Retain a defensive client-side shape check even though the Lua
            // wrapper already enforces the response limit before transport.
            if keys.len() > REDIS_STREAM_SCAN_PAGE_MAX {
                return Err(Error::Serialization(format!(
                    "Redis SCAN returned {} stream keys in one page, exceeding the accepted page budget {REDIS_STREAM_SCAN_PAGE_MAX}",
                    keys.len()
                )));
            }

            for name in keys {
                if names.insert(name.clone()) {
                    topics.push(TopicInfo {
                        name,
                        partitions: 1,
                        replicas: 1,
                    });
                    if probe_items.is_some_and(|limit| topics.len() >= limit) {
                        break;
                    }
                }
            }

            if next_cursor == 0 || probe_items.is_some_and(|limit| topics.len() >= limit) {
                break;
            }
            if !cursors.insert(next_cursor) {
                return Err(Error::Serialization(
                    "Redis SCAN repeated a non-zero cursor before completing the stream catalog"
                        .into(),
                ));
            }
            cursor = next_cursor;
        }

        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    async fn stream_detail_bounded(&self, stream: &str) -> Result<TopicDetail> {
        let mut c = self.conn.lock().await;
        let (length, groups, last_generated_id, first_id): (usize, usize, String, String) =
            redis::cmd("EVAL")
                .arg(REDIS_STREAM_DETAIL_SCRIPT)
                .arg(1)
                .arg(stream)
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;

        Ok(TopicDetail {
            info: TopicInfo {
                name: stream.to_owned(),
                partitions: 1,
                replicas: 1,
            },
            config: HashMap::from([
                ("kind".to_owned(), "stream".to_owned()),
                ("length".to_owned(), length.to_string()),
                ("groups".to_owned(), groups.to_string()),
                ("last_generated_id".to_owned(), last_generated_id.clone()),
            ]),
            watermarks: vec![PartitionWatermark {
                partition: 0,
                low: redis_stream_offset(&first_id)?,
                high: redis_stream_offset(&last_generated_id)?,
            }],
        })
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
                offset: redis_stream_offset(&id)?,
                cursor: Some(MessageCursor::RedisStream {
                    stream: stream.to_owned(),
                    id,
                }),
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
        match options.identity.clone() {
            ConsumerIdentity::Stateless => self.consume_stream_stateless(stream, options).await,
            ConsumerIdentity::Group {
                group,
                member: Some(member),
            } => {
                self.consume_stream_group(stream, &group, &member, options)
                    .await
            }
            ConsumerIdentity::Group { member: None, .. } => Err(Error::Config(
                "Redis Stream consumer groups require an explicit consumer member".into(),
            )),
            ConsumerIdentity::Durable { .. } => Err(Error::UnsupportedCapability {
                kind: self.kind.0.clone(),
                needed: CapabilityOperation::MessageConsumeDurable.as_str(),
            }),
        }
    }

    async fn consume_stream_stateless(
        &self,
        stream: &str,
        options: ConsumeOptions,
    ) -> Result<Vec<Message>> {
        let offset = redis_stream_start(&self.kind, &options)?;
        let block_ms = duration_millis_usize(options.timeout)?;
        let mut c = self.conn.lock().await;
        let reply: redis::Value = redis::cmd("XREAD")
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

        let reply = parse_stream_read_reply(reply, stream)?;
        let mut entries = Vec::new();
        extend_unique_stream_entries(&mut entries, &mut HashSet::new(), reply, options.max)?;
        entries
            .into_iter()
            .map(|entry| stream_id_to_message(stream, entry))
            .collect()
    }

    async fn consume_stream_group(
        &self,
        stream: &str,
        group: &str,
        member: &str,
        options: ConsumeOptions,
    ) -> Result<Vec<Message>> {
        // The adapter deadline bounds lock wait plus Redis BLOCK time. CLI
        // FlowControl owns the full-request deadline (including conversion and
        // XACK); embedded callers that need the same envelope must wrap the
        // returned future with their own request deadline.
        let deadline = checked_deadline(options.timeout)?;
        let mut c = self.conn.lock().await;
        if Instant::now() >= deadline {
            return Ok(vec![]);
        }
        // Do not reserve from an untrusted embedded max value before Redis has
        // actually returned any bounded data.
        let mut entries = Vec::new();
        let mut seen_ids = HashSet::new();

        // Redis retains unacknowledged deliveries in the PEL. Read this
        // member's own pending entries first so `--ack none` deterministically
        // replays them on the next bounded invocation instead of continuously
        // claiming new work.
        let pending = xreadgroup(&mut c, stream, group, member, options.max, "0", None).await?;
        extend_unique_stream_entries(&mut entries, &mut seen_ids, pending, options.max)?;

        if entries.len() < options.max {
            let now = Instant::now();
            if now < deadline {
                let remaining = options.max - entries.len();
                let block_ms = duration_millis_usize(deadline - now)?;
                let fresh = xreadgroup(
                    &mut c,
                    stream,
                    group,
                    member,
                    remaining,
                    ">",
                    Some(block_ms),
                )
                .await?;
                extend_unique_stream_entries(&mut entries, &mut seen_ids, fresh, options.max)?;
            }
        }

        // Convert every entry before advancing broker-owned state. Missing or
        // unrepresentable payload/header data leaves the complete batch in the
        // PEL for explicit recovery.
        let messages = entries
            .iter()
            .cloned()
            .map(|entry| stream_id_to_message(stream, entry))
            .collect::<Result<Vec<_>>>()?;

        if options.ack == AckMode::OnSuccess && !entries.is_empty() {
            let mut command = redis::cmd("XACK");
            command.arg(stream).arg(group);
            for entry in &entries {
                command.arg(&entry.id);
            }
            let acknowledged: usize = command
                .query_async(&mut *c)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            if acknowledged != entries.len() {
                return Err(Error::Query(format!(
                    "Redis acknowledged {acknowledged} of {} Stream entries for group {group:?}",
                    entries.len()
                )));
            }
        }

        Ok(messages)
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
                        cursor: None,
                        metadata: None,
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
                low: redis_stream_offset(&info.first_entry.id)?,
                high: redis_stream_offset(&info.last_generated_id)?,
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

const RAW_ADAPTER_ITEM_LIMIT: usize = 10_000;
const RAW_MAX_ARGUMENT_BYTES: usize = 1024 * 1024;
const RAW_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

fn validate_raw_command(args: &[String]) -> Result<()> {
    validate_raw_request_size(args)?;
    let command = normalized_raw_command(args)?;
    if is_forbidden_raw_command(&command) {
        return Err(Error::Config(format!(
            "Redis raw command {command} is forbidden by the adapter safety policy"
        )));
    }

    match command.as_str() {
        "PING" => expect_raw_arity(args, &[1, 2], &command),
        "ECHO" => expect_raw_arity(args, &[2], &command),
        "GET" | "TTL" | "PTTL" | "TYPE" | "STRLEN" | "HLEN" | "LLEN" | "SCARD" | "ZCARD"
        | "XLEN" => expect_raw_arity(args, &[2], &command),
        "DBSIZE" | "TIME" | "LASTSAVE" => expect_raw_arity(args, &[1], &command),
        "HGET" | "HEXISTS" | "SISMEMBER" | "ZSCORE" => expect_raw_arity(args, &[3], &command),
        "LINDEX" => {
            expect_raw_arity(args, &[3], &command)?;
            parse_raw_i64(args, 2, &command)?;
            Ok(())
        }
        "MGET" | "EXISTS" => {
            expect_raw_min_arity(args, 2, &command)?;
            check_raw_item_budget(&command, args.len() - 1)
        }
        "HMGET" => {
            expect_raw_min_arity(args, 3, &command)?;
            check_raw_item_budget(&command, args.len() - 2)
        }
        "LRANGE" => {
            expect_raw_arity(args, &[4], &command)?;
            check_raw_index_range(args, 2, 3, &command)
        }
        "ZRANGE" | "ZREVRANGE" => {
            expect_raw_arity(args, &[4, 5], &command)?;
            if args.len() == 5 && !args[4].eq_ignore_ascii_case("WITHSCORES") {
                return Err(Error::Config(format!(
                    "Redis raw {command} permits only the optional WITHSCORES modifier"
                )));
            }
            check_raw_index_range(args, 2, 3, &command)
        }
        "SRANDMEMBER" => {
            expect_raw_arity(args, &[2, 3], &command)?;
            if args.len() == 3 {
                let count = parse_raw_i64(args, 2, &command)?;
                let count = count.checked_abs().ok_or_else(|| {
                    Error::Config(format!(
                        "Redis raw {command} count is outside the i64 range"
                    ))
                })?;
                check_raw_item_budget(&command, count as usize)?;
            }
            Ok(())
        }
        "XRANGE" | "XREVRANGE" => {
            expect_raw_arity(args, &[6], &command)?;
            if !args[4].eq_ignore_ascii_case("COUNT") {
                return Err(Error::Config(format!(
                    "Redis raw {command} requires an explicit COUNT"
                )));
            }
            let count = parse_raw_positive_usize(args, 5, &command)?;
            check_raw_item_budget(&command, count)
        }

        "SET" => expect_raw_min_arity(args, 3, &command),
        "DEL" | "UNLINK" => {
            expect_raw_min_arity(args, 2, &command)?;
            check_raw_item_budget(&command, args.len() - 1)
        }
        "INCR" | "DECR" | "PERSIST" | "GETDEL" => expect_raw_arity(args, &[2], &command),
        "APPEND" | "INCRBY" | "INCRBYFLOAT" | "DECRBY" => expect_raw_arity(args, &[3], &command),
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
            expect_raw_arity(args, &[3, 4], &command)?;
            if args.len() == 4
                && !matches!(
                    args[3].to_ascii_uppercase().as_str(),
                    "NX" | "XX" | "GT" | "LT"
                )
            {
                return Err(Error::Config(format!(
                    "Redis raw {command} accepts only NX, XX, GT, or LT as its condition"
                )));
            }
            Ok(())
        }
        "RENAME" | "RENAMENX" => expect_raw_arity(args, &[3], &command),
        "HSET" => {
            expect_raw_min_arity(args, 4, &command)?;
            if !(args.len() - 2).is_multiple_of(2) {
                return Err(Error::Config(format!(
                    "Redis raw {command} requires field/value pairs"
                )));
            }
            check_raw_item_budget(&command, (args.len() - 2) / 2)
        }
        "HDEL" | "SADD" | "SREM" | "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "ZREM" | "XDEL" => {
            expect_raw_min_arity(args, 3, &command)?;
            check_raw_item_budget(&command, args.len() - 2)
        }
        "HINCRBY" | "HINCRBYFLOAT" | "LSET" | "ZINCRBY" => expect_raw_arity(args, &[4], &command),
        "LTRIM" => {
            expect_raw_arity(args, &[4], &command)?;
            check_raw_index_range(args, 2, 3, &command)
        }
        "LPOP" | "RPOP" | "SPOP" => {
            expect_raw_arity(args, &[2, 3], &command)?;
            if args.len() == 3 {
                let count = parse_raw_positive_usize(args, 2, &command)?;
                check_raw_item_budget(&command, count)?;
            }
            Ok(())
        }
        "ZADD" => validate_raw_simple_zadd(args, &command),
        _ => Err(Error::Config(format!(
            "Redis raw command {command} is not in the adapter allowlist"
        ))),
    }
}

fn normalized_raw_command(args: &[String]) -> Result<String> {
    let raw = args
        .first()
        .ok_or_else(|| Error::Config("raw command requires at least one argument".into()))?;
    if raw.is_empty()
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(Error::Config(
            "Redis raw command name must contain only ASCII letters, digits, or underscore".into(),
        ));
    }
    Ok(raw.to_ascii_uppercase())
}

fn validate_raw_request_size(args: &[String]) -> Result<()> {
    let mut total = 0_usize;
    for (index, arg) in args.iter().enumerate() {
        if arg.len() > RAW_MAX_ARGUMENT_BYTES {
            return Err(Error::Config(format!(
                "Redis raw argument {index} exceeds the 1 MiB adapter budget"
            )));
        }
        total = total
            .checked_add(arg.len())
            .ok_or_else(|| Error::Config("Redis raw request byte budget overflow".into()))?;
    }
    if total > RAW_MAX_REQUEST_BYTES {
        return Err(Error::Config(
            "Redis raw request exceeds the 8 MiB adapter budget".into(),
        ));
    }
    Ok(())
}

fn validate_raw_response_budget(value: &redis::Value) -> Result<()> {
    let mut budget = RawResponseBudget::default();
    budget.visit(value)
}

#[derive(Default)]
struct RawResponseBudget {
    items: usize,
    bytes: usize,
}

impl RawResponseBudget {
    fn visit(&mut self, value: &redis::Value) -> Result<()> {
        self.items = self
            .items
            .checked_add(1)
            .ok_or_else(|| Error::Serialization("RESP item budget overflow".into()))?;
        if self.items > RAW_ADAPTER_ITEM_LIMIT {
            return Err(Error::Serialization(format!(
                "RESP response exceeds the adapter item budget {RAW_ADAPTER_ITEM_LIMIT}"
            )));
        }

        match value {
            redis::Value::BulkString(bytes) => self.add_bytes(bytes.len())?,
            redis::Value::SimpleString(text) | redis::Value::VerbatimString { text, .. } => {
                self.add_bytes(text.len())?
            }
            redis::Value::Array(values)
            | redis::Value::Set(values)
            | redis::Value::Push { data: values, .. } => {
                for value in values {
                    self.visit(value)?;
                }
            }
            redis::Value::Map(values) => {
                for (key, value) in values {
                    self.visit(key)?;
                    self.visit(value)?;
                }
            }
            redis::Value::Attribute { data, attributes } => {
                self.visit(data)?;
                for (key, value) in attributes {
                    self.visit(key)?;
                    self.visit(value)?;
                }
            }
            redis::Value::BigNumber(value) => self.add_bytes(value.to_string().len())?,
            redis::Value::ServerError(error) => {
                self.add_bytes(format!("{error:?}").len())?;
            }
            redis::Value::Nil
            | redis::Value::Int(_)
            | redis::Value::Okay
            | redis::Value::Double(_)
            | redis::Value::Boolean(_) => {}
        }
        Ok(())
    }

    fn add_bytes(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| Error::Serialization("RESP byte budget overflow".into()))?;
        if self.bytes > RAW_MAX_REQUEST_BYTES {
            return Err(Error::Serialization(
                "RESP response exceeds the 8 MiB adapter byte budget".into(),
            ));
        }
        Ok(())
    }
}

fn is_forbidden_raw_command(command: &str) -> bool {
    matches!(
        command,
        "ACL"
            | "ASKING"
            | "AUTH"
            | "BGREWRITEAOF"
            | "BGSAVE"
            | "CLIENT"
            | "CLUSTER"
            | "COMMAND"
            | "CONFIG"
            | "DEBUG"
            | "DISCARD"
            | "DUMP"
            | "EVAL"
            | "EVALSHA"
            | "EVAL_RO"
            | "EVALSHA_RO"
            | "EXEC"
            | "FAILOVER"
            | "FCALL"
            | "FCALL_RO"
            | "FLUSHALL"
            | "FLUSHDB"
            | "FUNCTION"
            | "INFO"
            | "LATENCY"
            | "MEMORY"
            | "MIGRATE"
            | "MODULE"
            | "MONITOR"
            | "MOVE"
            | "MULTI"
            | "PSUBSCRIBE"
            | "PSYNC"
            | "PUBLISH"
            | "PUBSUB"
            | "PUNSUBSCRIBE"
            | "READONLY"
            | "READWRITE"
            | "REPLICAOF"
            | "RESTORE"
            | "ROLE"
            | "SAVE"
            | "SCRIPT"
            | "SELECT"
            | "SHUTDOWN"
            | "SLOWLOG"
            | "SLAVEOF"
            | "SSUBSCRIBE"
            | "SUBSCRIBE"
            | "SUNSUBSCRIBE"
            | "SWAPDB"
            | "SYNC"
            | "UNSUBSCRIBE"
            | "UNWATCH"
            | "WATCH"
    )
}

fn expect_raw_arity(args: &[String], accepted: &[usize], command: &str) -> Result<()> {
    if accepted.contains(&args.len()) {
        Ok(())
    } else {
        Err(Error::Config(format!(
            "Redis raw {command} received {} argument(s); accepted total argument counts: {accepted:?}",
            args.len()
        )))
    }
}

fn expect_raw_min_arity(args: &[String], minimum: usize, command: &str) -> Result<()> {
    if args.len() >= minimum {
        Ok(())
    } else {
        Err(Error::Config(format!(
            "Redis raw {command} requires at least {} argument(s) after the command",
            minimum - 1
        )))
    }
}

fn check_raw_item_budget(command: &str, count: usize) -> Result<()> {
    if count == 0 || count > RAW_ADAPTER_ITEM_LIMIT {
        return Err(Error::Config(format!(
            "Redis raw {command} item count {count} is outside the adapter range 1..={RAW_ADAPTER_ITEM_LIMIT}"
        )));
    }
    Ok(())
}

fn parse_raw_i64(args: &[String], index: usize, command: &str) -> Result<i64> {
    args[index].parse::<i64>().map_err(|_| {
        Error::Config(format!(
            "Redis raw {command} argument {index} must be an i64 integer"
        ))
    })
}

fn parse_raw_positive_usize(args: &[String], index: usize, command: &str) -> Result<usize> {
    let value = args[index].parse::<usize>().map_err(|_| {
        Error::Config(format!(
            "Redis raw {command} argument {index} must be a positive integer"
        ))
    })?;
    if value == 0 {
        return Err(Error::Config(format!(
            "Redis raw {command} argument {index} must be greater than zero"
        )));
    }
    Ok(value)
}

fn check_raw_index_range(
    args: &[String],
    start_index: usize,
    stop_index: usize,
    command: &str,
) -> Result<()> {
    let start = parse_raw_i64(args, start_index, command)?;
    let stop = parse_raw_i64(args, stop_index, command)?;
    if start < 0 || stop < 0 {
        return Err(Error::Config(format!(
            "Redis raw {command} rejects negative indexes because the result size depends on remote state"
        )));
    }
    if stop < start {
        return Ok(());
    }
    let count = stop
        .checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| Error::Config(format!("Redis raw {command} range size overflow")))?;
    check_raw_item_budget(command, count)
}

fn validate_raw_simple_zadd(args: &[String], command: &str) -> Result<()> {
    expect_raw_min_arity(args, 4, command)?;
    if !(args.len() - 2).is_multiple_of(2) {
        return Err(Error::Config(
            "Redis raw ZADD accepts only score/member pairs in portable mode".into(),
        ));
    }
    for score_index in (2..args.len()).step_by(2) {
        let score = args[score_index]
            .parse::<f64>()
            .map_err(|_| Error::Config("Redis raw ZADD score must be numeric".into()))?;
        if !score.is_finite() {
            return Err(Error::Config("Redis raw ZADD score must be finite".into()));
        }
    }
    check_raw_item_budget(command, (args.len() - 2) / 2)
}

async fn xreadgroup(
    connection: &mut MultiplexedConnection,
    stream: &str,
    group: &str,
    member: &str,
    count: usize,
    start: &str,
    block_ms: Option<usize>,
) -> Result<Vec<RedisStreamEntry>> {
    let mut command = redis::cmd("XREADGROUP");
    command
        .arg("GROUP")
        .arg(group)
        .arg(member)
        .arg("COUNT")
        .arg(count);
    if let Some(block_ms) = block_ms {
        command.arg("BLOCK").arg(block_ms);
    }
    let reply: redis::Value = command
        .arg("STREAMS")
        .arg(stream)
        .arg(start)
        .query_async(connection)
        .await
        .map_err(|e| Error::Query(e.to_string()))?;
    parse_stream_read_reply(reply, stream)
}

fn extend_unique_stream_entries(
    entries: &mut Vec<RedisStreamEntry>,
    seen_ids: &mut HashSet<String>,
    reply: Vec<RedisStreamEntry>,
    max: usize,
) -> Result<()> {
    for entry in reply {
        if entries.len() == max {
            return Err(Error::Serialization(format!(
                "Redis Stream read exceeded the requested {max}-entry batch"
            )));
        }
        if !seen_ids.insert(entry.id.clone()) {
            return Err(Error::Serialization(
                "Redis Stream read returned a duplicate entry ID".into(),
            ));
        }
        entries.push(entry);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedisStreamEntry {
    id: String,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

fn parse_stream_read_reply(
    value: redis::Value,
    expected_stream: &str,
) -> Result<Vec<RedisStreamEntry>> {
    let rows = match value {
        redis::Value::Nil => return Ok(vec![]),
        redis::Value::Array(rows) => rows
            .into_iter()
            .map(parse_stream_row_array)
            .collect::<Result<Vec<_>>>()?,
        redis::Value::Map(rows) => rows,
        other => {
            return Err(Error::Serialization(format!(
                "Redis Stream read returned unexpected top-level {}",
                redis_value_shape(&other)
            )))
        }
    };

    let mut matched = false;
    let mut entries = Vec::new();
    for (raw_stream, raw_entries) in rows {
        let stream = redis_stream_raw_bytes(raw_stream, "stream name")?;
        if stream.as_slice() != expected_stream.as_bytes() {
            return Err(Error::Serialization(format!(
                "Redis Stream read returned a different stream name ({} bytes returned, {} expected)",
                stream.len(),
                expected_stream.len()
            )));
        }
        if matched {
            return Err(Error::Serialization(
                "Redis Stream read returned a duplicate stream row".into(),
            ));
        }
        matched = true;
        entries.extend(parse_stream_entries(raw_entries)?);
    }
    Ok(entries)
}

fn parse_stream_row_array(value: redis::Value) -> Result<(redis::Value, redis::Value)> {
    match value {
        redis::Value::Array(mut values) if values.len() == 2 => {
            let entries = values.pop().expect("length was checked");
            let stream = values.pop().expect("length was checked");
            Ok((stream, entries))
        }
        redis::Value::Array(values) => Err(Error::Serialization(format!(
            "Redis Stream read row must contain exactly stream and entries, got {} elements",
            values.len()
        ))),
        other => Err(Error::Serialization(format!(
            "Redis Stream read row must be an array, got {}",
            redis_value_shape(&other)
        ))),
    }
}

fn parse_stream_entries(value: redis::Value) -> Result<Vec<RedisStreamEntry>> {
    let values = match value {
        redis::Value::Nil => return Ok(vec![]),
        redis::Value::Array(values) => values,
        redis::Value::Map(pairs) => pairs
            .into_iter()
            .map(|(id, fields)| redis::Value::Map(vec![(id, fields)]))
            .collect(),
        other => {
            return Err(Error::Serialization(format!(
                "Redis Stream entries must be an array or map, got {}",
                redis_value_shape(&other)
            )))
        }
    };
    values.into_iter().map(parse_stream_entry).collect()
}

fn parse_stream_entry(value: redis::Value) -> Result<RedisStreamEntry> {
    let (raw_id, raw_fields) = match value {
        redis::Value::Array(mut values) if values.len() == 2 => {
            let fields = values.pop().expect("length was checked");
            let id = values.pop().expect("length was checked");
            (id, fields)
        }
        redis::Value::Map(mut pairs) if pairs.len() == 1 => {
            pairs.pop().expect("length was checked")
        }
        redis::Value::Array(values) => {
            return Err(Error::Serialization(format!(
                "Redis Stream entry must contain exactly ID and fields, got {} elements",
                values.len()
            )))
        }
        redis::Value::Map(pairs) => {
            return Err(Error::Serialization(format!(
                "Redis Stream entry map must contain exactly one ID, got {} entries",
                pairs.len()
            )))
        }
        other => {
            return Err(Error::Serialization(format!(
                "Redis Stream entry must be an ID/fields array or map, got {}",
                redis_value_shape(&other)
            )))
        }
    };
    let id = String::from_utf8(redis_stream_raw_bytes(raw_id, "entry ID")?)
        .map_err(|_| Error::Serialization("Redis Stream entry ID is not valid UTF-8".into()))?;
    let fields = parse_stream_field_pairs(raw_fields)?;
    Ok(RedisStreamEntry { id, fields })
}

fn parse_stream_field_pairs(value: redis::Value) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let pairs = match value {
        redis::Value::Array(values) => {
            if !values.len().is_multiple_of(2) {
                return Err(Error::Serialization(
                    "Redis Stream entry contains an odd number of field/value elements".into(),
                ));
            }
            let mut values = values.into_iter();
            let mut pairs = Vec::new();
            while let Some(field) = values.next() {
                let value = values.next().expect("even length was checked");
                pairs.push((field, value));
            }
            pairs
        }
        redis::Value::Map(pairs) => pairs,
        other => {
            return Err(Error::Serialization(format!(
                "Redis Stream entry fields must be an ordered array or map, got {}",
                redis_value_shape(&other)
            )))
        }
    };

    pairs
        .into_iter()
        .map(|(field, value)| {
            Ok((
                redis_stream_raw_bytes(field, "field name")?,
                redis_stream_raw_bytes(value, "field value")?,
            ))
        })
        .collect()
}

fn redis_stream_raw_bytes(value: redis::Value, label: &str) -> Result<Vec<u8>> {
    match value {
        redis::Value::BulkString(bytes) => Ok(bytes),
        redis::Value::SimpleString(value) => Ok(value.into_bytes()),
        other => Err(Error::Serialization(format!(
            "Redis Stream {label} must be a byte string, got {}",
            redis_value_shape(&other)
        ))),
    }
}

fn redis_value_shape(value: &redis::Value) -> String {
    match value {
        redis::Value::Nil => "nil".to_owned(),
        redis::Value::Int(_) => "integer".to_owned(),
        redis::Value::BulkString(bytes) => format!("bulk string ({} bytes)", bytes.len()),
        redis::Value::Array(values) => format!("array ({} elements)", values.len()),
        redis::Value::SimpleString(value) => {
            format!("simple string ({} bytes)", value.len())
        }
        redis::Value::Okay => "OK status".to_owned(),
        redis::Value::Map(values) => format!("map ({} entries)", values.len()),
        redis::Value::Attribute { attributes, .. } => {
            format!("attribute ({} attributes)", attributes.len())
        }
        redis::Value::Set(values) => format!("set ({} elements)", values.len()),
        redis::Value::Double(_) => "double".to_owned(),
        redis::Value::Boolean(_) => "boolean".to_owned(),
        redis::Value::VerbatimString { text, .. } => {
            format!("verbatim string ({} bytes)", text.len())
        }
        redis::Value::BigNumber(_) => "big number".to_owned(),
        redis::Value::Push { data, .. } => format!("push ({} elements)", data.len()),
        redis::Value::ServerError(_) => "server error".to_owned(),
    }
}

async fn detect_consumer_lag_support(
    kind: &ConnectorKind,
    connection: &mut MultiplexedConnection,
) -> bool {
    if kind.0 == "keydb" {
        return false;
    }
    let Ok(info) = redis::cmd("INFO")
        .arg("server")
        .query_async::<String>(connection)
        .await
    else {
        return false;
    };
    redis_info_supports_consumer_lag(kind, &info)
}

fn redis_info_supports_consumer_lag(kind: &ConnectorKind, info: &str) -> bool {
    kind.0 != "keydb" && redis_protocol_major(info).is_some_and(|major| major >= 7)
}

fn redis_protocol_major(info: &str) -> Option<u64> {
    let mut versions = info.lines().filter_map(|line| {
        line.strip_suffix('\r')
            .unwrap_or(line)
            .strip_prefix("redis_version:")
    });
    let version = versions.next()?;
    if versions.next().is_some() || version.trim() != version {
        return None;
    }
    let core = version.split_once('-').map_or(version, |(core, _)| core);
    let mut parts = core.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    parts.next()?.parse::<u64>().ok()?;
    parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(major)
}

fn redis_operations(
    capabilities: Capabilities,
    consumer_lag_supported: bool,
) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::KeyValueGetWithExpiry,
        CapabilityOperation::KeyValueRestoreWithExpiry,
        CapabilityOperation::MessageConsumeGroup,
        CapabilityOperation::MessageConsumeAck,
        CapabilityOperation::MessageAdminListTopics,
        CapabilityOperation::MessageAdminListTopicsBounded,
        CapabilityOperation::MessageAdminTopicDetail,
        CapabilityOperation::MessageAdminTopicDetailBounded,
        CapabilityOperation::MessageAdminDelete,
    ]);
    if consumer_lag_supported {
        operations.push(CapabilityOperation::MessageAdminConsumerLag);
        operations.push(CapabilityOperation::MessageAdminConsumerLagBounded);
    }
    operations
}

fn enforce_redis_topic_detail_budget(
    detail: TopicDetail,
    budget: MetadataBudget,
) -> Result<TopicDetail> {
    let mut limiter = MetadataLimiter::new(budget, "Redis topic detail")?;
    for item in &detail.config {
        limiter.observe(&item)?;
    }
    for watermark in &detail.watermarks {
        limiter.observe(watermark)?;
    }
    limiter.ensure_complete(&detail)?;
    Ok(detail)
}

fn metadata_work_probe(observed: usize, budget: MetadataBudget) -> Result<usize> {
    budget
        .max_items
        .saturating_sub(observed)
        .checked_add(1)
        .ok_or_else(|| Error::Config("metadata work budget cannot reserve a probe item".into()))
}

fn observe_metadata_work(
    observed: &mut usize,
    budget: MetadataBudget,
    subject: &str,
) -> Result<()> {
    if *observed >= budget.max_items {
        return Err(Error::MetadataBudgetExceeded {
            subject: subject.to_owned(),
            unit: "items",
            limit: budget.max_items,
        });
    }
    *observed = observed
        .checked_add(1)
        .ok_or_else(|| Error::Query(format!("{subject} item count overflow")))?;
    Ok(())
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

fn redis_lag_dimensions(
    last_delivered_id: &str,
    entries_read: Option<usize>,
    pending: usize,
    server_lag: Option<usize>,
) -> Result<(i64, i64, i64)> {
    let entries_read = match entries_read {
        Some(entries_read) => entries_read,
        None if last_delivered_id == "0-0" => 0,
        None => {
            return Err(Error::Query(
                "Redis server did not report consumer-group entries-read after delivery began"
                    .into(),
            ))
        }
    };
    let server_lag = server_lag
        .ok_or_else(|| Error::Query("Redis server did not report consumer-group lag".into()))?;
    let committed = entries_read.checked_sub(pending).ok_or_else(|| {
        Error::Serialization(format!(
            "Redis consumer-group pending count {pending} exceeds entries-read {entries_read}"
        ))
    })?;
    let latest = entries_read.checked_add(server_lag).ok_or_else(|| {
        Error::Serialization("Redis consumer-group logical latest count overflow".into())
    })?;
    let lag = pending.checked_add(server_lag).ok_or_else(|| {
        Error::Serialization("Redis consumer-group outstanding lag overflow".into())
    })?;

    Ok((
        i64::try_from(latest).map_err(|_| {
            Error::Serialization("Redis consumer-group logical latest count exceeds i64".into())
        })?,
        i64::try_from(committed)
            .map_err(|_| Error::Serialization("Redis committed count exceeds i64".into()))?,
        i64::try_from(lag).map_err(|_| {
            Error::Serialization("Redis consumer-group outstanding lag exceeds i64".into())
        })?,
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
    if message.cursor.is_some() || message.metadata.is_some() {
        return Err(Error::Config(
            "Redis Stream producer does not accept consumer cursor or delivery metadata".into(),
        ));
    }
    if message.headers.contains_key("redis_stream_id") {
        return Err(Error::Config(
            "Redis Stream producer reserves the redis_stream_id header for native identity".into(),
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
    if message.cursor.is_some() || message.metadata.is_some() {
        return Err(Error::Config(
            "Redis PubSub producer does not accept consumer cursor or delivery metadata".into(),
        ));
    }
    Ok(())
}

fn validate_stream_consume_options(kind: &ConnectorKind, options: &ConsumeOptions) -> Result<()> {
    options
        .validate()
        .map_err(|message| Error::Config(format!("Redis Stream consume: {message}")))?;
    validate_stream_partition(options.partition, "consumer")?;
    match &options.cursor {
        None => {}
        Some(cursor @ ConsumeCursor::RedisStream { .. }) => {
            cursor.validate().map_err(Error::Config)?;
            if options.partition.is_some() || options.offset.is_some() {
                return Err(Error::Config(
                    "Redis Stream exact cursor cannot be combined with legacy partition or offset fields"
                        .into(),
                ));
            }
        }
        Some(cursor) => {
            return Err(Error::Config(format!(
                "Redis Stream consumer cannot use {cursor:?} cursor"
            )))
        }
    }
    match (&options.identity, options.ack) {
        (ConsumerIdentity::Stateless, AckMode::None) => {}
        (ConsumerIdentity::Stateless, AckMode::OnSuccess) => {
            return Err(Error::Config(
                "Redis Stream --ack on-success requires a consumer group".into(),
            ))
        }
        (ConsumerIdentity::Group { member: None, .. }, _) => {
            return Err(Error::Config(
                "Redis Stream consumer groups require an explicit consumer member".into(),
            ))
        }
        (ConsumerIdentity::Group { .. }, _) => {}
        (ConsumerIdentity::Durable { .. }, _) => {
            return Err(Error::UnsupportedCapability {
                kind: kind.0.clone(),
                needed: CapabilityOperation::MessageConsumeDurable.as_str(),
            })
        }
    }
    Ok(())
}

fn redis_stream_start(kind: &ConnectorKind, options: &ConsumeOptions) -> Result<String> {
    validate_stream_consume_options(kind, options)?;
    Ok(match &options.cursor {
        Some(ConsumeCursor::RedisStream { id }) => redis_stream_predecessor(id)?,
        Some(_) => unreachable!("cursor kind was validated"),
        None => options
            .offset
            .map(|offset| format!("{offset}-0"))
            .unwrap_or_else(|| "0-0".to_owned()),
    })
}

/// XREAD treats its ID as the last-seen (exclusive) position. Convert dbtool's
/// inclusive native cursor into the immediately preceding Redis Stream ID.
fn redis_stream_predecessor(id: &str) -> Result<String> {
    ConsumeCursor::RedisStream { id: id.to_owned() }
        .validate()
        .map_err(Error::Config)?;
    let (millis, sequence) = id
        .split_once('-')
        .expect("validated Redis Stream IDs contain a separator");
    let millis = millis
        .parse::<u64>()
        .expect("validated Redis Stream milliseconds are numeric");
    let sequence = sequence
        .parse::<u64>()
        .expect("validated Redis Stream sequences are numeric");
    if sequence > 0 {
        Ok(format!("{millis}-{}", sequence - 1))
    } else {
        Ok(format!("{}-{}", millis - 1, u64::MAX))
    }
}

fn validate_pubsub_consume_options(options: &ConsumeOptions) -> Result<()> {
    options
        .validate()
        .map_err(|message| Error::Config(format!("Redis PubSub consume: {message}")))?;
    if options.identity.is_stateful() {
        return Err(Error::Config(
            "Redis PubSub consumer does not support group or durable identities".into(),
        ));
    }
    if options.ack == AckMode::OnSuccess {
        return Err(Error::Config(
            "Redis PubSub consumer does not support acknowledgement".into(),
        ));
    }
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
    if options.cursor.is_some() {
        return Err(Error::Config(
            "Redis PubSub consumer does not support exact cursors".into(),
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

fn stream_id_to_message(stream: &str, entry: RedisStreamEntry) -> Result<Message> {
    let RedisStreamEntry { id, fields } = entry;
    ConsumeCursor::RedisStream { id: id.clone() }
        .validate()
        .map_err(|message| {
            Error::Serialization(format!("invalid Redis Stream entry ID: {message}"))
        })?;
    let mut payload = None;
    let mut key = None;
    let mut headers = HashMap::from([("redis_stream_id".to_owned(), id.clone())]);

    for (field, value) in fields {
        if field == b"payload" {
            if payload.replace(value).is_some() {
                return Err(Error::Serialization(
                    "Redis Stream entry contains duplicate payload fields".into(),
                ));
            }
            continue;
        }
        if field == b"key" {
            if key.replace(bytes::Bytes::from(value)).is_some() {
                return Err(Error::Serialization(
                    "Redis Stream entry contains duplicate key fields".into(),
                ));
            }
            continue;
        }
        let Some(raw_header) = field.strip_prefix(b"h:") else {
            return Err(Error::Serialization(format!(
                "Redis Stream entry contains an unsupported field name ({} bytes)",
                field.len()
            )));
        };
        let header = String::from_utf8(raw_header.to_vec()).map_err(|_| {
            Error::Serialization("Redis Stream entry contains a non-UTF-8 header name".into())
        })?;
        if header == "redis_stream_id" {
            return Err(Error::Serialization(
                "Redis Stream entry contains reserved header redis_stream_id".into(),
            ));
        }
        let header_value = String::from_utf8(value).map_err(|_| {
            Error::Serialization(format!(
                "Redis Stream entry header value is not valid UTF-8 (header name {} bytes)",
                header.len()
            ))
        })?;
        if headers.insert(header.clone(), header_value).is_some() {
            return Err(Error::Serialization(format!(
                "Redis Stream entry contains a duplicate header ({}-byte name)",
                header.len()
            )));
        }
    }
    let payload = payload.ok_or_else(|| {
        Error::Serialization("Redis Stream entry is missing the payload field".into())
    })?;

    let offset = redis_stream_legacy_offset(&id)?;
    Ok(Message {
        key,
        payload: payload.into(),
        headers,
        partition: Some(0),
        offset,
        timestamp: offset.filter(|value| *value > 0),
        cursor: Some(MessageCursor::RedisStream {
            stream: stream.to_owned(),
            id,
        }),
        metadata: None,
    })
}

fn redis_stream_id_millis(id: &str) -> Result<u64> {
    let (millis, sequence) = id.split_once('-').ok_or_else(|| {
        Error::Serialization("Redis Stream ID must contain milliseconds and sequence".into())
    })?;
    let millis = millis.parse::<u64>().map_err(|_| {
        Error::Serialization("Redis Stream ID contains invalid milliseconds".into())
    })?;
    sequence
        .parse::<u64>()
        .map_err(|_| Error::Serialization("Redis Stream ID contains an invalid sequence".into()))?;
    Ok(millis)
}

fn redis_stream_legacy_offset(id: &str) -> Result<Option<i64>> {
    redis_stream_id_millis(id).map(|millis| i64::try_from(millis).ok())
}

fn redis_stream_offset(id: &str) -> Result<i64> {
    redis_stream_legacy_offset(id)?.ok_or_else(|| {
        Error::Serialization(
            "Redis Stream ID milliseconds exceed the required i64 metadata range".into(),
        )
    })
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

fn redis_value_to_core(value: redis::Value) -> Result<Value> {
    Ok(match value {
        redis::Value::Nil => Value::Null,
        redis::Value::Int(value) => Value::Int(value),
        redis::Value::BulkString(bytes) => bytes_to_value(bytes),
        redis::Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(redis_value_to_core)
                .collect::<Result<Vec<_>>>()?,
        ),
        redis::Value::Set(_) => {
            return Err(Error::Serialization(
                "RESP set values cannot preserve set identity in the portable Value model; use a dedicated bounded command"
                    .into(),
            ))
        }
        redis::Value::SimpleString(value) => Value::Text(value),
        redis::Value::Okay => Value::Text("OK".to_owned()),
        redis::Value::Map(values) => redis_pairs_to_map(values)?,
        redis::Value::Attribute { data, attributes } => {
            let mut map = BTreeMap::new();
            map.insert("data".to_owned(), redis_value_to_core(*data)?);
            map.insert("attributes".to_owned(), redis_pairs_to_map(attributes)?);
            Value::Map(map)
        }
        redis::Value::Double(value) if value.is_finite() => Value::Float(value),
        redis::Value::Double(_) => {
            return Err(Error::Serialization(
                "RESP double is non-finite and cannot be represented portably".into(),
            ))
        }
        redis::Value::Boolean(value) => Value::Bool(value),
        redis::Value::VerbatimString { .. } => {
            return Err(Error::Serialization(
                "RESP verbatim strings carry a format tag that the portable Value model cannot preserve"
                    .into(),
            ))
        }
        redis::Value::BigNumber(_) => {
            return Err(Error::Serialization(
                "RESP big numbers exceed the portable Value integer contract".into(),
            ))
        }
        redis::Value::Push { .. } => {
            return Err(Error::Serialization(
                "RESP push values are connection-mode events and are forbidden in kv raw".into(),
            ))
        }
        redis::Value::ServerError(error) => {
            return Err(Error::Query(format!("Redis server error: {error:?}")))
        }
    })
}

fn redis_pairs_to_map(values: Vec<(redis::Value, redis::Value)>) -> Result<Value> {
    let mut map = BTreeMap::new();
    for (raw_key, raw_value) in values {
        let key = redis_key_to_string(raw_key)?;
        let value = redis_value_to_core(raw_value)?;
        if map.insert(key.clone(), value).is_some() {
            return Err(Error::Serialization(format!(
                "RESP map contains duplicate portable key {key:?}"
            )));
        }
    }
    Ok(Value::Map(map))
}

fn redis_key_to_string(value: redis::Value) -> Result<String> {
    match value {
        redis::Value::SimpleString(value) => Ok(value),
        redis::Value::BulkString(bytes) => String::from_utf8(bytes).map_err(|_| {
            Error::Serialization(
                "RESP map contains a non-UTF-8 bulk-string key that cannot be represented portably"
                    .into(),
            )
        }),
        redis::Value::Okay => Ok("OK".to_owned()),
        _ => Err(Error::Serialization(
            "RESP map key is not a portable UTF-8 string".into(),
        )),
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
            cursor: None,
            metadata: None,
        }
    }

    fn redis_kind() -> ConnectorKind {
        ConnectorKind("redis".to_owned())
    }

    fn redis_dsn_for_database(raw: &str, database: u8) -> String {
        let query_at = raw.find('?').unwrap_or(raw.len());
        let (base, query) = raw.split_at(query_at);
        let authority_start = base.find("://").map_or(0, |index| index + 3);
        let path_at = base[authority_start..]
            .find('/')
            .map(|index| authority_start + index)
            .unwrap_or(base.len());
        format!("{}/{database}{query}", &base[..path_at])
    }

    fn stream_entry(id: &str, fields: &[(&[u8], &[u8])]) -> RedisStreamEntry {
        RedisStreamEntry {
            id: id.to_owned(),
            fields: fields
                .iter()
                .map(|(field, value)| (field.to_vec(), value.to_vec()))
                .collect(),
        }
    }

    fn resp2_stream_reply(stream: &str, id: &str, fields: Vec<redis::Value>) -> redis::Value {
        redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::BulkString(stream.as_bytes().to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(id.as_bytes().to_vec()),
                redis::Value::Array(fields),
            ])]),
        ])])
    }

    #[test]
    fn raw_command_validation_is_fail_closed_and_bounded() {
        for forbidden in [
            "FLUSHALL",
            "SELECT",
            "EVAL",
            "FUNCTION",
            "FCALL",
            "MULTI",
            "MIGRATE",
            "RESTORE",
            "SUBSCRIBE",
        ] {
            assert!(matches!(
                validate_raw_command(&[forbidden.to_owned()]),
                Err(Error::Config(_))
            ));
        }
        assert!(matches!(
            validate_raw_command(&["UNKNOWN_COMMAND".to_owned()]),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            validate_raw_command(&["KEYS".to_owned(), "*".to_owned()]),
            Err(Error::Config(_))
        ));
        assert!(validate_raw_command(&["XLEN".to_owned(), "stream".to_owned()]).is_ok());
        assert!(validate_raw_command(&[
            "XRANGE".to_owned(),
            "stream".to_owned(),
            "-".to_owned(),
            "+".to_owned(),
            "COUNT".to_owned(),
            "10".to_owned(),
        ])
        .is_ok());
        assert!(validate_raw_command(&[
            "LRANGE".to_owned(),
            "list".to_owned(),
            "-1".to_owned(),
            "-1".to_owned(),
        ])
        .is_err());
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
    fn lifetime_read_script_orders_time_before_pttl_and_decodes_exact_values() {
        let get_position = GET_WITH_EXPIRY_SCRIPT.find("'GET'").unwrap();
        let time_position = GET_WITH_EXPIRY_SCRIPT.find("'TIME'").unwrap();
        let pttl_position = GET_WITH_EXPIRY_SCRIPT.find("'PTTL'").unwrap();
        assert!(get_position < time_position && time_position < pttl_position);

        assert_eq!(
            decode_get_with_expiry_response((0, vec![], 1, 0, -2)).unwrap(),
            None
        );
        assert_eq!(
            decode_get_with_expiry_response((1, vec![], 1, 0, -1)).unwrap(),
            Some(KeyValueSnapshot {
                value: bytes::Bytes::new(),
                expiry: KeyExpiry::Persistent,
            })
        );
        assert_eq!(
            decode_get_with_expiry_response((1, vec![0, 0xff], 1, 500_000, 23)).unwrap(),
            Some(KeyValueSnapshot {
                value: bytes::Bytes::from_static(&[0, 0xff]),
                expiry: KeyExpiry::ExpiresAtUnixMs(1_523),
            })
        );
    }

    #[test]
    fn lifetime_read_fails_closed_on_invalid_pttl_time_or_status() {
        for response in [
            (0, vec![1], 1, 0, -2),
            (0, vec![], 1, 0, -1),
            (1, vec![], 1, 0, -3),
            (9, vec![], 1, 0, -1),
            (1, vec![], -1, 0, -1),
            (1, vec![], 1, 1_000_000, -1),
            (1, vec![], i64::MAX, 0, 1),
            (1, vec![], 9_007_199_254_740, 991_000, 1),
        ] {
            assert!(decode_get_with_expiry_response(response).is_err());
        }
        assert!(checked_redis_time_ms(i64::MAX, 0).is_err());
    }

    #[test]
    fn lifetime_restore_outcomes_and_lua_integer_limits_are_explicit() {
        assert_eq!(
            decode_restore_with_expiry_status(1).unwrap(),
            KeyValueRestoreOutcome::Stored
        );
        assert_eq!(
            decode_restore_with_expiry_status(0).unwrap(),
            KeyValueRestoreOutcome::ConditionNotMet
        );
        assert_eq!(
            decode_restore_with_expiry_status(2).unwrap(),
            KeyValueRestoreOutcome::Expired
        );
        assert!(decode_restore_with_expiry_status(-1).is_err());
        assert!(decode_restore_with_expiry_status(-2).is_err());
        assert!(decode_restore_with_expiry_status(3).is_err());

        assert!(validate_restore_expiry(KeyExpiry::Persistent).is_ok());
        assert!(validate_restore_expiry(KeyExpiry::ExpiresAtUnixMs(i64::MIN)).is_ok());
        assert!(
            validate_restore_expiry(KeyExpiry::ExpiresAtUnixMs(REDIS_LUA_SAFE_INTEGER_MAX_MS))
                .is_ok()
        );
        assert!(validate_restore_expiry(KeyExpiry::ExpiresAtUnixMs(
            REDIS_LUA_SAFE_INTEGER_MAX_MS + 1
        ))
        .is_err());

        let time_position = RESTORE_WITH_EXPIRY_SCRIPT.find("'TIME'").unwrap();
        let expired_position = RESTORE_WITH_EXPIRY_SCRIPT
            .find("deadline <= now_ms")
            .unwrap();
        let expiring_set_position = RESTORE_WITH_EXPIRY_SCRIPT.find("'PX', remaining").unwrap();
        assert!(time_position < expired_position && expired_position < expiring_set_position);
    }

    #[tokio::test]
    async fn redis_live_bounded_lag_counts_sparse_scan_work_exactly() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_REDIS_DSN") else {
            return;
        };
        // Use a dedicated logical database so stale streams from other live
        // tests cannot change the exact stream+group work count below.
        let isolated_dsn = redis_dsn_for_database(&raw_dsn, 15);
        let dsn = Dsn::parse(&isolated_dsn).unwrap();
        let client = Client::open(dsn.raw_with_scheme("redis").unwrap()).unwrap();
        let mut direct = client.get_multiplexed_async_connection().await.unwrap();
        redis::cmd("FLUSHDB")
            .query_async::<()>(&mut direct)
            .await
            .unwrap();

        let target_stream = "dbtool_it_exact_target";
        let sparse_stream = "dbtool_it_exact_sparse";
        let target_group = "dbtool_it_exact_group";
        for (stream, group) in [
            (target_stream, target_group),
            (sparse_stream, "dbtool_it_unrelated_group"),
        ] {
            redis::cmd("XADD")
                .arg(stream)
                .arg("*")
                .arg("payload")
                .arg("fixture")
                .query_async::<String>(&mut direct)
                .await
                .unwrap();
            redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream)
                .arg(group)
                .arg("0")
                .query_async::<()>(&mut direct)
                .await
                .unwrap();
        }

        let connector = factory(dsn).await.unwrap();
        let admin = connector.as_admin().unwrap();
        // Work is exactly two stream candidates plus one group candidate per
        // stream. The unrelated stream/group proves a sparse non-result still
        // consumes caller-owned scan work.
        let lag = admin
            .consumer_lag_bounded(target_group, MetadataBudget::with_default_bytes(4).unwrap())
            .await
            .expect("exact stream+group work budget should prove completeness");
        assert_eq!(lag.len(), 1);
        assert_eq!(lag[0].topic, target_stream);
        assert!(matches!(
            admin
                .consumer_lag_bounded(target_group, MetadataBudget::with_default_bytes(3).unwrap(),)
                .await,
            Err(Error::MetadataBudgetExceeded {
                unit: "items",
                limit: 3,
                ..
            })
        ));

        redis::cmd("FLUSHDB")
            .query_async::<()>(&mut direct)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn redis_live_lifetime_contract_preserves_bytes_deadlines_and_write_conditions() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_REDIS_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let persistent_key = format!("dbtool_it_lifetime_{suffix}:persistent");
        let expiring_key = format!("dbtool_it_lifetime_{suffix}:expiring");
        let past_key = format!("dbtool_it_lifetime_{suffix}:past");
        let keys = vec![
            persistent_key.clone(),
            expiring_key.clone(),
            past_key.clone(),
        ];

        let dsn = Dsn::parse(&raw_dsn).unwrap();
        let driver_url = dsn.raw_with_scheme("redis").unwrap();
        let client = Client::open(driver_url.as_str()).unwrap();
        let mut direct = client.get_multiplexed_async_connection().await.unwrap();
        redis::cmd("DEL")
            .arg(&keys)
            .query_async::<u64>(&mut direct)
            .await
            .unwrap();

        let connector = factory(dsn).await.unwrap();
        let kv = connector.as_kv().unwrap();
        assert_eq!(kv.get_with_expiry(&persistent_key).await.unwrap(), None);

        assert_eq!(
            kv.restore_with_expiry(
                &persistent_key,
                &[0, 0xff, b'Z'],
                KeyExpiry::Persistent,
                false,
            )
            .await
            .unwrap(),
            KeyValueRestoreOutcome::Stored
        );
        assert_eq!(
            kv.get_with_expiry(&persistent_key).await.unwrap(),
            Some(KeyValueSnapshot {
                value: bytes::Bytes::from_static(&[0, 0xff, b'Z']),
                expiry: KeyExpiry::Persistent,
            })
        );
        assert_eq!(
            kv.restore_with_expiry(&persistent_key, b"replacement", KeyExpiry::Persistent, true,)
                .await
                .unwrap(),
            KeyValueRestoreOutcome::ConditionNotMet
        );
        assert_eq!(
            kv.get(&persistent_key).await.unwrap(),
            Some(bytes::Bytes::from_static(&[0, 0xff, b'Z']))
        );

        let (seconds, micros): (i64, i64) =
            redis::cmd("TIME").query_async(&mut direct).await.unwrap();
        let deadline = checked_redis_time_ms(seconds, micros)
            .unwrap()
            .checked_add(60_000)
            .unwrap();
        assert_eq!(
            kv.restore_with_expiry(
                &expiring_key,
                b"",
                KeyExpiry::ExpiresAtUnixMs(deadline),
                false,
            )
            .await
            .unwrap(),
            KeyValueRestoreOutcome::Stored
        );
        let expiring = kv.get_with_expiry(&expiring_key).await.unwrap().unwrap();
        assert!(expiring.value.is_empty());
        let KeyExpiry::ExpiresAtUnixMs(observed_deadline) = expiring.expiry else {
            panic!("expiring restore became persistent")
        };
        assert!(observed_deadline <= deadline);
        assert!(observed_deadline >= deadline - 1_000);

        kv.set(&past_key, b"original", SetOptions::default())
            .await
            .unwrap();
        let (seconds, micros): (i64, i64) =
            redis::cmd("TIME").query_async(&mut direct).await.unwrap();
        let now = checked_redis_time_ms(seconds, micros).unwrap();
        assert_eq!(
            kv.restore_with_expiry(
                &past_key,
                b"must-not-be-written",
                KeyExpiry::ExpiresAtUnixMs(now),
                true,
            )
            .await
            .unwrap(),
            KeyValueRestoreOutcome::Expired
        );
        assert_eq!(
            kv.get(&past_key).await.unwrap(),
            Some(bytes::Bytes::from_static(b"original"))
        );

        assert_eq!(kv.delete(&keys).await.unwrap(), 3);
        assert_eq!(
            kv.scan(&format!("dbtool_it_lifetime_{suffix}:*"), 10)
                .await
                .unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn redis_live_stream_groups_replay_ack_and_report_complete_lag() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_REDIS_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let stream = format!("dbtool_it_stream_group_{suffix}");
        let group = format!("dbtool_it_group_{suffix}");
        let member = format!("worker-{suffix}");

        let dsn = Dsn::parse(&raw_dsn).unwrap();
        let driver_url = dsn.raw_with_scheme("redis").unwrap();
        let client = Client::open(driver_url.as_str()).unwrap();
        let mut direct = client.get_multiplexed_async_connection().await.unwrap();
        redis::cmd("DEL")
            .arg(&stream)
            .query_async::<u64>(&mut direct)
            .await
            .unwrap();

        let connector = factory(dsn).await.unwrap();
        let operations = connector.operations();
        assert!(operations.contains(&CapabilityOperation::MessageConsumeGroup));
        assert!(operations.contains(&CapabilityOperation::MessageConsumeAck));
        assert!(!operations.contains(&CapabilityOperation::MessageConsumeDurable));
        let lag_supported = operations.contains(&CapabilityOperation::MessageAdminConsumerLag);

        let producer = connector.as_producer().unwrap();
        let fixtures = [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()]
            .into_iter()
            .map(|payload| Message {
                payload: bytes::Bytes::copy_from_slice(payload),
                ..message()
            })
            .collect();
        assert_eq!(
            producer.produce(&stream, fixtures).await.unwrap().produced,
            3
        );
        redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg("0")
            .query_async::<()>(&mut direct)
            .await
            .unwrap();

        let consume_options = |group: &str, member: Option<&str>, ack, max| ConsumeOptions {
            max,
            timeout: std::time::Duration::from_secs(2),
            partition: None,
            offset: None,
            cursor: None,
            identity: ConsumerIdentity::Group {
                group: group.to_owned(),
                member: member.map(str::to_owned),
            },
            ack,
        };
        let message_ids = |messages: &[Message]| {
            messages
                .iter()
                .map(|message| match message.cursor.as_ref().unwrap() {
                    MessageCursor::RedisStream { stream: owner, id } => {
                        assert_eq!(owner, &stream);
                        id.clone()
                    }
                    cursor => panic!("unexpected Redis cursor {cursor:?}"),
                })
                .collect::<Vec<_>>()
        };
        let consumer = connector.as_consumer().unwrap();
        let admin = connector.as_admin().unwrap();
        let detail = admin
            .topic_detail_bounded(&stream, MetadataBudget::with_default_bytes(5).unwrap())
            .await
            .expect("bounded Stream detail should use the fixed-shape Lua response");
        assert_eq!(detail.info.name, stream);
        assert_eq!(detail.config["length"], "3");
        assert!(matches!(
            admin
                .topic_detail_bounded(&stream, MetadataBudget::with_default_bytes(4).unwrap())
                .await,
            Err(Error::MetadataBudgetExceeded {
                unit: "items",
                limit: 4,
                ..
            })
        ));
        if lag_supported {
            let initial_lag = admin.consumer_lag(&group).await.unwrap();
            assert_eq!(initial_lag.len(), 1);
            assert_eq!(
                (
                    initial_lag[0].latest,
                    initial_lag[0].committed,
                    initial_lag[0].lag,
                ),
                (3, 0, 3)
            );
            let bounded_lag = admin
                .consumer_lag_bounded(&group, MetadataBudget::with_default_bytes(10_000).unwrap())
                .await
                .expect("bounded group lag should use server-side group filtering");
            let bounded_lag = bounded_lag
                .iter()
                .find(|item| item.topic == stream)
                .expect("bounded lag should include the fixture stream");
            assert_eq!(
                (bounded_lag.latest, bounded_lag.committed, bounded_lag.lag,),
                (3, 0, 3)
            );
        } else {
            assert!(matches!(
                admin.consumer_lag(&group).await,
                Err(Error::UnsupportedCapability { needed, .. })
                    if needed == CapabilityOperation::MessageAdminConsumerLag.as_str()
            ));
        }

        let missing_group = format!("{group}_missing");
        let error = consumer
            .consume(
                &stream,
                consume_options(&missing_group, Some(&member), AckMode::None, 1),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Query(message) if message.contains("NOGROUP")));
        let groups: StreamInfoGroupsReply = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&stream)
            .query_async(&mut direct)
            .await
            .unwrap();
        assert_eq!(
            groups.groups.len(),
            1,
            "consume must not create missing groups"
        );

        assert!(matches!(
            consumer
                .consume(
                    &stream,
                    consume_options(&group, None, AckMode::None, 1),
                )
                .await,
            Err(Error::Config(message)) if message.contains("explicit consumer member")
        ));

        let first = consumer
            .consume(
                &stream,
                consume_options(&group, Some(&member), AckMode::None, 2),
            )
            .await
            .unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(
            first
                .iter()
                .map(|message| message.payload.as_ref())
                .collect::<Vec<_>>(),
            [b"one".as_slice(), b"two".as_slice()]
        );
        let first_ids = message_ids(&first);

        let replayed = consumer
            .consume(
                &stream,
                consume_options(&group, Some(&member), AckMode::None, 2),
            )
            .await
            .unwrap();
        assert_eq!(message_ids(&replayed), first_ids);

        if lag_supported {
            let lag = admin.consumer_lag(&group).await.unwrap();
            assert_eq!(lag.len(), 1);
            assert_eq!((lag[0].latest, lag[0].committed, lag[0].lag), (3, 0, 3));
        }

        let acknowledged = consumer
            .consume(
                &stream,
                consume_options(&group, Some(&member), AckMode::OnSuccess, 2),
            )
            .await
            .unwrap();
        assert_eq!(message_ids(&acknowledged), first_ids);
        if lag_supported {
            let lag = admin.consumer_lag(&group).await.unwrap();
            assert_eq!((lag[0].latest, lag[0].committed, lag[0].lag), (3, 2, 1));
        }

        let final_message = consumer
            .consume(
                &stream,
                consume_options(&group, Some(&member), AckMode::OnSuccess, 2),
            )
            .await
            .unwrap();
        assert_eq!(final_message.len(), 1);
        assert_eq!(final_message[0].payload.as_ref(), b"three");
        if lag_supported {
            let lag = admin.consumer_lag(&group).await.unwrap();
            assert_eq!((lag[0].latest, lag[0].committed, lag[0].lag), (3, 3, 0));
        }

        let boundary_id = format!("{}-0", i64::MAX as u64 + 1);
        let stored_boundary_id: String = redis::cmd("XADD")
            .arg(&stream)
            .arg(&boundary_id)
            .arg("payload")
            .arg("boundary")
            .query_async(&mut direct)
            .await
            .unwrap();
        assert_eq!(stored_boundary_id, boundary_id);
        let boundary_message = consumer
            .consume(
                &stream,
                consume_options(&group, Some(&member), AckMode::OnSuccess, 1),
            )
            .await
            .unwrap();
        assert_eq!(boundary_message.len(), 1);
        assert_eq!(boundary_message[0].payload.as_ref(), b"boundary");
        assert_eq!(boundary_message[0].offset, None);
        assert_eq!(boundary_message[0].timestamp, None);
        assert_eq!(
            boundary_message[0].cursor,
            Some(MessageCursor::RedisStream {
                stream: stream.clone(),
                id: boundary_id,
            })
        );

        let malformed_id: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("payload")
            .arg("first")
            .arg("payload")
            .arg("second")
            .query_async(&mut direct)
            .await
            .unwrap();
        let result = consumer
            .consume(
                &stream,
                consume_options(&group, Some(&member), AckMode::OnSuccess, 1),
            )
            .await;
        assert!(matches!(
            result,
            Err(Error::Serialization(message)) if message.contains("duplicate payload")
        ));
        let pending: redis::streams::StreamPendingCountReply = redis::cmd("XPENDING")
            .arg(&stream)
            .arg(&group)
            .arg(&malformed_id)
            .arg(&malformed_id)
            .arg(1)
            .query_async(&mut direct)
            .await
            .unwrap();
        assert_eq!(
            pending.ids.len(),
            1,
            "failed conversion must not XACK the entry"
        );
        if lag_supported {
            let lag = admin.consumer_lag(&group).await.unwrap();
            assert_eq!((lag[0].latest, lag[0].committed, lag[0].lag), (5, 4, 1));
        }

        let outcome = connector
            .as_admin_mutate()
            .unwrap()
            .delete_resource(
                MessageResource {
                    kind: MessageResourceKind::RedisStream,
                    name: stream.clone(),
                },
                DeleteResourceOptions::default(),
            )
            .await
            .unwrap();
        assert!(outcome.acknowledged && outcome.verified_absent);
        assert_eq!(outcome.messages_before, Some(5));
        let resource_type: String = redis::cmd("TYPE")
            .arg(&stream)
            .query_async(&mut direct)
            .await
            .unwrap();
        assert_eq!(resource_type, "none");
    }

    #[test]
    fn scan_collector_deduplicates_across_pages_and_stops_at_zero() {
        let mut collector = ScanCollector::new(4).unwrap();
        assert_eq!(
            collector
                .push_page(17, vec![b"one".to_vec(), b"two".to_vec()])
                .unwrap(),
            ScanProgress::Continue(17)
        );
        assert_eq!(
            collector
                .push_page(
                    0,
                    vec![b"two".to_vec(), b"three".to_vec(), b"three".to_vec()]
                )
                .unwrap(),
            ScanProgress::Complete
        );
        assert_eq!(collector.into_keys(), ["one", "two", "three"]);
    }

    #[test]
    fn scan_collector_counts_unique_keys_toward_the_limit() {
        let mut collector = ScanCollector::new(3).unwrap();
        assert_eq!(
            collector
                .push_page(9, vec![b"one".to_vec(), b"one".to_vec()])
                .unwrap(),
            ScanProgress::Continue(9)
        );
        assert_eq!(
            collector
                .push_page(11, vec![b"two".to_vec(), b"three".to_vec()])
                .unwrap(),
            ScanProgress::Complete
        );
        assert_eq!(collector.into_keys(), ["one", "two", "three"]);
    }

    #[test]
    fn scan_collector_rejects_non_utf8_without_returning_a_partial_page() {
        let mut collector = ScanCollector::new(3).unwrap();
        assert!(matches!(
            collector.push_page(0, vec![b"valid".to_vec(), vec![0xff]]),
            Err(Error::Serialization(message)) if message.contains("non-UTF-8 key")
        ));
        assert!(collector.keys.is_empty());
    }

    #[test]
    fn scan_collector_rejects_cursor_cycles() {
        let mut collector = ScanCollector::new(3).unwrap();
        assert_eq!(
            collector.push_page(7, vec![]).unwrap(),
            ScanProgress::Continue(7)
        );
        assert!(matches!(
            collector.push_page(7, vec![]),
            Err(Error::Query(message)) if message.contains("repeated")
        ));
    }

    #[test]
    fn scan_limits_are_positive_and_platform_overflow_safe() {
        assert!(matches!(redis_scan_count(0), Err(Error::Config(_))));
        assert_eq!(redis_scan_count(1).unwrap(), 1);
        assert_eq!(
            redis_scan_count(usize::MAX).unwrap(),
            u64::try_from(REDIS_SCAN_COUNT).unwrap()
        );
    }

    #[tokio::test]
    async fn redis_live_scan_rejects_non_utf8_keys_without_partial_success() {
        let Ok(raw_dsn) = std::env::var("DBTOOL_IT_REDIS_DSN") else {
            return;
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!("dbtool_it_non_utf8_{suffix}:");
        let mut binary_key = prefix.as_bytes().to_vec();
        binary_key.push(0xff);

        let dsn = Dsn::parse(&raw_dsn).unwrap();
        let driver_url = dsn.raw_with_scheme("redis").unwrap();
        let client = Client::open(driver_url.as_str()).unwrap();
        let mut direct = client.get_multiplexed_async_connection().await.unwrap();
        redis::cmd("SET")
            .arg(&binary_key)
            .arg(b"value")
            .query_async::<()>(&mut direct)
            .await
            .unwrap();

        let connector = factory(dsn).await.unwrap();
        let result = connector
            .as_kv()
            .unwrap()
            .scan(&format!("{prefix}*"), 10)
            .await;

        let deleted = redis::cmd("DEL")
            .arg(&binary_key)
            .query_async::<u64>(&mut direct)
            .await
            .unwrap();
        assert_eq!(deleted, 1);
        assert!(matches!(
            result,
            Err(Error::Serialization(message)) if message.contains("non-UTF-8 key")
        ));
    }

    #[test]
    fn redis_values_convert_to_typed_core_values() {
        let value = redis_value_to_core(redis::Value::Array(vec![
            redis::Value::Int(42),
            redis::Value::BulkString(b"hello".to_vec()),
            redis::Value::BulkString(vec![0, 255]),
            redis::Value::Boolean(true),
        ]))
        .unwrap();

        assert_eq!(
            value,
            Value::Array(vec![
                Value::Int(42),
                Value::Text("hello".to_owned()),
                Value::Bytes(vec![0, 255]),
                Value::Bool(true),
            ])
        );
    }

    #[test]
    fn redis_maps_reject_nonportable_or_colliding_keys() {
        assert!(matches!(
            redis_value_to_core(redis::Value::Map(vec![(
                redis::Value::BulkString(vec![0xff]),
                redis::Value::Int(1),
            )])),
            Err(Error::Serialization(message)) if message.contains("non-UTF-8")
        ));
        assert!(matches!(
            redis_value_to_core(redis::Value::Map(vec![(
                redis::Value::Int(1),
                redis::Value::Int(1),
            )])),
            Err(Error::Serialization(message)) if message.contains("map key")
        ));
        assert!(matches!(
            redis_value_to_core(redis::Value::Map(vec![
                (
                    redis::Value::SimpleString("same".into()),
                    redis::Value::Int(1),
                ),
                (
                    redis::Value::BulkString(b"same".to_vec()),
                    redis::Value::Int(2),
                ),
            ])),
            Err(Error::Serialization(message)) if message.contains("duplicate")
        ));
    }

    #[test]
    fn redis_protocol_shapes_without_portable_identity_are_errors() {
        assert!(matches!(
            redis_value_to_core(redis::Value::Set(vec![redis::Value::Int(1)])),
            Err(Error::Serialization(message)) if message.contains("set values")
        ));
        assert!(matches!(
            redis_value_to_core(redis::Value::BigNumber("123".parse().unwrap())),
            Err(Error::Serialization(message)) if message.contains("big numbers")
        ));
    }

    #[test]
    fn raw_response_budget_rejects_oversized_bytes_and_collections() {
        assert!(matches!(
            validate_raw_response_budget(&redis::Value::BulkString(vec![
                0;
                RAW_MAX_REQUEST_BYTES + 1
            ])),
            Err(Error::Serialization(message)) if message.contains("byte budget")
        ));
        assert!(matches!(
            validate_raw_response_budget(&redis::Value::Array(
                (0..RAW_ADAPTER_ITEM_LIMIT)
                    .map(|_| redis::Value::Nil)
                    .collect()
            )),
            Err(Error::Serialization(message)) if message.contains("item budget")
        ));
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
        assert_eq!(
            redis_stream_offset("1710000000000-3").unwrap(),
            1_710_000_000_000
        );
        assert_eq!(
            redis_stream_legacy_offset("9223372036854775808-0").unwrap(),
            None
        );
        assert!(redis_stream_offset("9223372036854775808-0").is_err());
        assert!(redis_stream_offset("bad-id").is_err());
    }

    #[test]
    fn redis_stream_messages_keep_the_full_native_id() {
        let message = stream_id_to_message(
            "orders",
            stream_entry("1710000000000-42", &[(b"payload", b"payload")]),
        )
        .unwrap();

        assert_eq!(message.offset, Some(1_710_000_000_000));
        assert_eq!(
            message.cursor,
            Some(MessageCursor::RedisStream {
                stream: "orders".to_owned(),
                id: "1710000000000-42".to_owned(),
            })
        );

        let boundary = stream_id_to_message(
            "orders",
            stream_entry("9223372036854775808-7", &[(b"payload", b"boundary")]),
        )
        .unwrap();
        assert_eq!(boundary.offset, None);
        assert_eq!(boundary.timestamp, None);
        assert_eq!(
            boundary.cursor,
            Some(MessageCursor::RedisStream {
                stream: "orders".to_owned(),
                id: "9223372036854775808-7".to_owned(),
            })
        );
    }

    #[test]
    fn redis_stream_conversion_is_lossless_or_fails_closed() {
        let valid = stream_id_to_message(
            "orders",
            RedisStreamEntry {
                id: "1710000000000-1".to_owned(),
                fields: vec![
                    (b"payload".to_vec(), vec![0, 0xff]),
                    (b"key".to_vec(), vec![0xfe, 1]),
                    (b"h:trace".to_vec(), b"abc".to_vec()),
                ],
            },
        )
        .unwrap();
        assert_eq!(valid.payload.as_ref(), &[0, 0xff]);
        assert_eq!(valid.key.as_deref(), Some(&[0xfe, 1][..]));
        assert_eq!(valid.headers.get("trace").map(String::as_str), Some("abc"));

        let invalid = |fields| {
            stream_id_to_message(
                "orders",
                RedisStreamEntry {
                    id: "1710000000000-2".to_owned(),
                    fields,
                },
            )
        };
        assert!(matches!(
            invalid(vec![]),
            Err(Error::Serialization(message)) if message.contains("missing the payload")
        ));
        assert!(matches!(
            invalid(vec![
                (b"payload".to_vec(), b"ok".to_vec()),
                (b"h:trace".to_vec(), vec![0xff]),
            ]),
            Err(Error::Serialization(message)) if message.contains("not valid UTF-8")
        ));
        assert!(matches!(
            invalid(vec![
                (b"payload".to_vec(), b"ok".to_vec()),
                (b"unknown".to_vec(), b"lost".to_vec()),
            ]),
            Err(Error::Serialization(message)) if message.contains("unsupported field")
        ));
        assert!(matches!(
            invalid(vec![
                (b"payload".to_vec(), b"ok".to_vec()),
                (b"h:redis_stream_id".to_vec(), b"spoofed".to_vec()),
            ]),
            Err(Error::Serialization(message)) if message.contains("reserved header")
        ));
    }

    #[test]
    fn raw_stream_reply_retains_and_rejects_duplicate_fields() {
        let duplicate_payload = parse_stream_read_reply(
            resp2_stream_reply(
                "orders",
                "1710000000000-3",
                vec![
                    redis::Value::BulkString(b"payload".to_vec()),
                    redis::Value::BulkString(b"one".to_vec()),
                    redis::Value::BulkString(b"payload".to_vec()),
                    redis::Value::BulkString(b"two".to_vec()),
                ],
            ),
            "orders",
        )
        .unwrap();
        assert_eq!(duplicate_payload[0].fields.len(), 2);
        assert!(matches!(
            stream_id_to_message("orders", duplicate_payload.into_iter().next().unwrap()),
            Err(Error::Serialization(message)) if message.contains("duplicate payload")
        ));

        let duplicate_header = parse_stream_read_reply(
            resp2_stream_reply(
                "orders",
                "1710000000000-4",
                vec![
                    redis::Value::BulkString(b"payload".to_vec()),
                    redis::Value::BulkString(b"ok".to_vec()),
                    redis::Value::BulkString(b"h:trace".to_vec()),
                    redis::Value::BulkString(b"one".to_vec()),
                    redis::Value::BulkString(b"h:trace".to_vec()),
                    redis::Value::BulkString(b"two".to_vec()),
                ],
            ),
            "orders",
        )
        .unwrap();
        assert!(matches!(
            stream_id_to_message("orders", duplicate_header.into_iter().next().unwrap()),
            Err(Error::Serialization(message)) if message.contains("duplicate header")
        ));

        let duplicate_key = stream_entry(
            "1710000000000-5",
            &[(b"payload", b"ok"), (b"key", b"one"), (b"key", b"two")],
        );
        assert!(matches!(
            stream_id_to_message("orders", duplicate_key),
            Err(Error::Serialization(message)) if message.contains("duplicate key")
        ));

        let resp3 = redis::Value::Map(vec![(
            redis::Value::BulkString(b"orders".to_vec()),
            redis::Value::Array(vec![redis::Value::Map(vec![(
                redis::Value::BulkString(b"1710000000000-6".to_vec()),
                redis::Value::Map(vec![(
                    redis::Value::BulkString(b"payload".to_vec()),
                    redis::Value::BulkString(b"resp3".to_vec()),
                )]),
            )])]),
        )]);
        let parsed = parse_stream_read_reply(resp3, "orders").unwrap();
        assert_eq!(
            stream_id_to_message("orders", parsed.into_iter().next().unwrap())
                .unwrap()
                .payload
                .as_ref(),
            b"resp3"
        );
    }

    #[test]
    fn raw_stream_parser_errors_redact_values_and_duplicate_ids_fail_closed() {
        const MARKER: &[u8] = b"DBTOOL_STREAM_SECRET_MARKER";
        let top_level =
            parse_stream_read_reply(redis::Value::BulkString(MARKER.to_vec()), "orders")
                .unwrap_err()
                .to_string();
        assert!(!top_level.contains("DBTOOL_STREAM_SECRET_MARKER"));

        let wrong_stream = parse_stream_read_reply(
            resp2_stream_reply(
                std::str::from_utf8(MARKER).unwrap(),
                "1710000000000-6",
                vec![
                    redis::Value::BulkString(b"payload".to_vec()),
                    redis::Value::BulkString(b"ok".to_vec()),
                ],
            ),
            "orders",
        )
        .unwrap_err()
        .to_string();
        assert!(!wrong_stream.contains("DBTOOL_STREAM_SECRET_MARKER"));

        let unknown = stream_id_to_message(
            "orders",
            RedisStreamEntry {
                id: "1710000000000-7".to_owned(),
                fields: vec![
                    (b"payload".to_vec(), b"ok".to_vec()),
                    (MARKER.to_vec(), MARKER.to_vec()),
                ],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(!unknown.contains("DBTOOL_STREAM_SECRET_MARKER"));

        let duplicate = stream_entry("1710000000000-8", &[(b"payload", b"ok")]);
        let mut entries = Vec::new();
        let mut ids = HashSet::new();
        extend_unique_stream_entries(&mut entries, &mut ids, vec![duplicate.clone()], 2).unwrap();
        assert!(matches!(
            extend_unique_stream_entries(&mut entries, &mut ids, vec![duplicate], 2),
            Err(Error::Serialization(message)) if message.contains("duplicate entry ID")
        ));
    }

    #[test]
    fn redis_server_version_gates_group_lag_capability() {
        assert_eq!(
            redis_protocol_major("# Server\r\nredis_version:7.4.9\r\n"),
            Some(7)
        );
        assert_eq!(redis_protocol_major("redis_version:7.0.0-rc1\n"), Some(7));
        assert_eq!(redis_protocol_major("redis_version:6.3.4\n"), Some(6));
        assert_eq!(redis_protocol_major("redis_version:invalid\n"), None);
        assert_eq!(
            redis_protocol_major("redis_version:7.4.9\nredis_version:7.4.9\n"),
            None
        );
        assert!(redis_info_supports_consumer_lag(
            &ConnectorKind("redis".into()),
            "redis_version:7.4.9\n"
        ));
        assert!(!redis_info_supports_consumer_lag(
            &ConnectorKind("redis".into()),
            "redis_version:6.2.14\n"
        ));
        assert!(!redis_info_supports_consumer_lag(
            &ConnectorKind("keydb".into()),
            "redis_version:7.4.9\n"
        ));
    }

    #[test]
    fn redis_exact_cursor_is_not_compressed_to_millisecond_offset() {
        let options = ConsumeOptions {
            cursor: Some(ConsumeCursor::RedisStream {
                id: "1710000000000-42".to_owned(),
            }),
            ..Default::default()
        };
        assert_eq!(
            redis_stream_start(&redis_kind(), &options).unwrap(),
            "1710000000000-41"
        );
        assert_eq!(
            redis_stream_predecessor("1710000000000-0").unwrap(),
            format!("1709999999999-{}", u64::MAX)
        );
        assert_eq!(redis_stream_predecessor("0-1").unwrap(), "0-0");
        assert!(redis_stream_predecessor("0-0").is_err());

        let conflict = ConsumeOptions {
            offset: Some(1_710_000_000_000),
            cursor: options.cursor,
            ..Default::default()
        };
        assert!(matches!(
            redis_stream_start(&redis_kind(), &conflict),
            Err(Error::Config(message)) if message.contains("cannot be combined")
        ));
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
                cursor: None,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("partitions")
        ));
        assert!(matches!(
            validate_pubsub_consume_options(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: None,
                offset: Some(0),
                cursor: None,
                ..Default::default()
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
            cursor: None,
            ..Default::default()
        };

        assert!(validate_stream_consume_options(&redis_kind(), &options(None)).is_ok());
        assert!(validate_stream_consume_options(&redis_kind(), &options(Some(0))).is_ok());
        assert!(matches!(
            validate_stream_consume_options(&redis_kind(), &options(Some(1))),
            Err(Error::Config(message)) if message.contains("partition 0")
        ));

        let mut negative_offset = options(Some(0));
        negative_offset.offset = Some(-1);
        assert!(matches!(
            validate_stream_consume_options(&redis_kind(), &negative_offset),
            Err(Error::Config(message)) if message.contains("non-negative")
        ));
    }

    #[test]
    fn redis_stream_groups_require_members_and_ack_requires_a_group() {
        let mut options = ConsumeOptions {
            identity: ConsumerIdentity::Group {
                group: "workers".to_owned(),
                member: None,
            },
            ..Default::default()
        };
        assert!(matches!(
            validate_stream_consume_options(&redis_kind(), &options),
            Err(Error::Config(message)) if message.contains("explicit consumer member")
        ));

        options.identity = ConsumerIdentity::Group {
            group: "workers".to_owned(),
            member: Some("worker-1".to_owned()),
        };
        assert!(validate_stream_consume_options(&redis_kind(), &options).is_ok());
        options.ack = AckMode::OnSuccess;
        assert!(validate_stream_consume_options(&redis_kind(), &options).is_ok());

        options.identity = ConsumerIdentity::Stateless;
        assert!(matches!(
            validate_stream_consume_options(&redis_kind(), &options),
            Err(Error::Config(message)) if message.contains("requires a consumer group")
        ));

        options.identity = ConsumerIdentity::Durable {
            name: "durable".to_owned(),
        };
        assert!(matches!(
            validate_stream_consume_options(&ConnectorKind("valkey".into()), &options),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == CapabilityOperation::MessageConsumeDurable.as_str()
        ));
    }

    #[test]
    fn redis_pubsub_rejects_stateful_identity_and_acknowledgement() {
        let mut options = ConsumeOptions {
            identity: ConsumerIdentity::Group {
                group: "workers".to_owned(),
                member: Some("worker-1".to_owned()),
            },
            ..Default::default()
        };
        assert!(matches!(
            validate_pubsub_consume_options(&options),
            Err(Error::Config(message)) if message.contains("group or durable")
        ));

        options.identity = ConsumerIdentity::Stateless;
        options.ack = AckMode::OnSuccess;
        assert!(matches!(
            validate_pubsub_consume_options(&options),
            Err(Error::Config(message)) if message.contains("acknowledgement")
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
        let capabilities = Capabilities {
            key_value: true,
            producer: true,
            consumer: true,
            admin: true,
            ..Default::default()
        };
        let operations = redis_operations(capabilities, true);

        for operation in [
            CapabilityOperation::KeyValueGetWithExpiry,
            CapabilityOperation::KeyValueRestoreWithExpiry,
            CapabilityOperation::MessageConsumeGroup,
            CapabilityOperation::MessageConsumeAck,
            CapabilityOperation::MessageAdminListTopics,
            CapabilityOperation::MessageAdminListTopicsBounded,
            CapabilityOperation::MessageAdminTopicDetail,
            CapabilityOperation::MessageAdminTopicDetailBounded,
            CapabilityOperation::MessageAdminConsumerLag,
            CapabilityOperation::MessageAdminConsumerLagBounded,
            CapabilityOperation::MessageAdminDelete,
        ] {
            assert!(operations.contains(&operation));
        }
        assert!(operations.contains(&CapabilityOperation::KeyValueGet));
        assert!(operations.contains(&CapabilityOperation::MessageProduce));
        assert!(operations.contains(&CapabilityOperation::MessageConsume));

        let keydb_operations = redis_operations(capabilities, false);
        assert!(keydb_operations.contains(&CapabilityOperation::MessageConsumeGroup));
        assert!(keydb_operations.contains(&CapabilityOperation::MessageConsumeAck));
        assert!(keydb_operations.contains(&CapabilityOperation::MessageAdminTopicDetailBounded));
        assert!(!keydb_operations.contains(&CapabilityOperation::MessageAdminConsumerLag));
        assert!(!keydb_operations.contains(&CapabilityOperation::MessageAdminConsumerLagBounded));
    }

    #[test]
    fn redis_lag_work_budget_reserves_an_exact_probe() {
        let budget = MetadataBudget::new(2, 1024).unwrap();
        let mut observed = 0;
        assert_eq!(metadata_work_probe(observed, budget).unwrap(), 3);
        observe_metadata_work(&mut observed, budget, "test scan").unwrap();
        assert_eq!(metadata_work_probe(observed, budget).unwrap(), 2);
        observe_metadata_work(&mut observed, budget, "test scan").unwrap();
        assert_eq!(metadata_work_probe(observed, budget).unwrap(), 1);
        assert!(matches!(
            observe_metadata_work(&mut observed, budget, "test scan"),
            Err(Error::MetadataBudgetExceeded {
                unit: "items",
                limit: 2,
                ..
            })
        ));
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
    fn redis_lag_includes_pending_and_undelivered_work() {
        assert_eq!(
            redis_lag_dimensions("7-0", Some(7), 2, Some(3)).unwrap(),
            (10, 5, 5)
        );
        assert_eq!(
            redis_lag_dimensions("0-0", None, 0, Some(3)).unwrap(),
            (3, 0, 3)
        );
        assert!(matches!(
            redis_lag_dimensions("7-0", None, 0, Some(3)),
            Err(Error::Query(message)) if message.contains("entries-read")
        ));
        assert!(matches!(
            redis_lag_dimensions("7-0", Some(7), 0, None),
            Err(Error::Query(message)) if message.contains("consumer-group lag")
        ));
        assert!(matches!(
            redis_lag_dimensions("2-0", Some(2), 3, Some(0)),
            Err(Error::Serialization(message)) if message.contains("exceeds entries-read")
        ));
    }
}
