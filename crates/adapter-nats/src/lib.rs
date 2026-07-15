use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        ConsumeOptions, LagInfo, Message, PartitionWatermark, ProduceOutcome, TopicDetail,
        TopicInfo,
    },
    port::{
        capability::{AdminInspect, MessageConsumer, MessageProducer},
        connector::{Capabilities, Connector, ConnectorKind},
    },
};
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use std::{collections::HashMap, path::PathBuf, str::FromStr};
use tokio::time::{timeout, Instant};

pub struct NatsAdapter {
    client: async_nats::Client,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let driver_url = nats_driver_url(&dsn)?;
        let mut options = async_nats::ConnectOptions::new();
        if dsn.scheme == "nats+tls" {
            options = options.require_tls(true);
        }
        if let Some(path) = nats_tls_ca(&dsn) {
            options = options.add_root_certificates(PathBuf::from(path));
        }
        let client = options
            .connect(driver_url)
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;
        Ok(Box::new(NatsAdapter {
            client,
            kind: ConnectorKind(dsn.scheme),
        }) as Box<dyn Connector>)
    })
}

#[async_trait::async_trait]
impl Connector for NatsAdapter {
    fn kind(&self) -> ConnectorKind {
        self.kind.clone()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            producer: true,
            consumer: true,
            admin: true,
            ..Default::default()
        }
    }
    async fn ping(&self) -> Result<()> {
        self.client
            .flush()
            .await
            .map_err(|e| Error::Connection(e.to_string()))
    }
    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
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
impl MessageProducer for NatsAdapter {
    async fn produce(&self, target: &str, messages: Vec<Message>) -> Result<ProduceOutcome> {
        validate_subject(target)?;
        for message in &messages {
            validate_produce_message(message)?;
        }
        let mut produced = 0;
        for message in messages {
            let headers = nats_headers_from_core(&message.headers)?;
            if headers.is_empty() {
                self.client
                    .publish(target.to_owned(), message.payload)
                    .await
                    .map_err(|e| Error::Query(e.to_string()))?;
            } else {
                self.client
                    .publish_with_headers(target.to_owned(), headers, message.payload)
                    .await
                    .map_err(|e| Error::Query(e.to_string()))?;
            }
            produced += 1;
        }
        self.client
            .flush()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        Ok(ProduceOutcome {
            produced,
            placements: vec![],
        })
    }
}

#[async_trait::async_trait]
impl MessageConsumer for NatsAdapter {
    async fn consume(&self, source: &str, options: ConsumeOptions) -> Result<Vec<Message>> {
        validate_subject(source)?;
        validate_consume_position(&options)?;
        if options.max == 0 {
            return Ok(vec![]);
        }

        let deadline = checked_deadline(options.timeout)?;
        let mut subscriber = self
            .client
            .subscribe(source.to_owned())
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        self.client
            .flush()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let mut messages = Vec::new();
        while messages.len() < options.max {
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            match timeout(deadline - now, subscriber.next()).await {
                Ok(Some(message)) => {
                    let headers = nats_headers_to_core(message.headers.as_ref())?;
                    messages.push(Message {
                        key: None,
                        payload: message.payload,
                        headers,
                        partition: None,
                        offset: None,
                        timestamp: None,
                    });
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        Ok(messages)
    }
}

#[async_trait::async_trait]
impl AdminInspect for NatsAdapter {
    async fn list_topics(&self) -> Result<Vec<TopicInfo>> {
        let mut streams = self.jetstream().streams();
        let mut topics = Vec::new();

        while let Some(info) = streams
            .try_next()
            .await
            .map_err(|e| Error::Query(e.to_string()))?
        {
            topics.push(nats_topic_info(&info));
        }

        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    async fn topic_detail(&self, name: &str) -> Result<TopicDetail> {
        validate_jetstream_name("stream", name)?;
        let stream = self
            .jetstream()
            .get_stream(name)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        Ok(nats_topic_detail(stream.cached_info()))
    }

    async fn consumer_lag(&self, group: &str) -> Result<Vec<LagInfo>> {
        validate_jetstream_name("consumer", group)?;
        let mut streams = self.jetstream().streams();
        let mut lag = Vec::new();

        while let Some(info) = streams
            .try_next()
            .await
            .map_err(|e| Error::Query(e.to_string()))?
        {
            let stream = self
                .jetstream()
                .get_stream(&info.config.name)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
            let mut consumer_names = stream.consumer_names();
            let mut has_consumer = false;

            while let Some(name) = consumer_names
                .try_next()
                .await
                .map_err(|e| Error::Query(e.to_string()))?
            {
                if name == group {
                    has_consumer = true;
                    break;
                }
            }

            if has_consumer {
                let consumer = stream
                    .consumer_info(group)
                    .await
                    .map_err(|e| Error::Query(e.to_string()))?;
                lag.push(LagInfo {
                    topic: info.config.name,
                    partition: 0,
                    group: group.to_owned(),
                    committed: u64_to_i64(consumer.ack_floor.stream_sequence),
                    latest: u64_to_i64(info.state.last_sequence),
                    lag: u64_to_i64(consumer.num_pending),
                });
            }
        }

        Ok(lag)
    }
}

impl NatsAdapter {
    fn jetstream(&self) -> async_nats::jetstream::Context {
        async_nats::jetstream::new(self.client.clone())
    }
}

fn validate_subject(subject: &str) -> Result<()> {
    if subject.is_empty()
        || subject
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(Error::Query(format!("invalid NATS subject: {subject:?}")));
    }

    Ok(())
}

fn validate_produce_message(message: &Message) -> Result<()> {
    if message.key.is_some() {
        return Err(Error::Config(
            "Core NATS producer does not support message keys".into(),
        ));
    }
    if message.partition.is_some() {
        return Err(Error::Config(
            "Core NATS producer does not support partitions".into(),
        ));
    }
    if message.offset.is_some() {
        return Err(Error::Config(
            "Core NATS producer does not support producer offsets".into(),
        ));
    }
    if message.timestamp.is_some() {
        return Err(Error::Config(
            "Core NATS producer does not support producer timestamps".into(),
        ));
    }

    // Validate header syntax before any message is published so a batch cannot
    // partially succeed due to a later invalid header.
    nats_headers_from_core(&message.headers)?;
    Ok(())
}

fn validate_consume_position(options: &ConsumeOptions) -> Result<()> {
    if options.partition.is_some() {
        return Err(Error::Config(
            "Core NATS consumer does not support partitions".into(),
        ));
    }
    if options.offset.is_some() {
        return Err(Error::Config(
            "Core NATS consumer does not support offsets".into(),
        ));
    }
    Ok(())
}

fn nats_headers_from_core(headers: &HashMap<String, String>) -> Result<async_nats::HeaderMap> {
    let mut mapped = async_nats::HeaderMap::new();
    for (key, value) in headers {
        let name = async_nats::HeaderName::from_str(key).map_err(|error| {
            Error::Config(format!("invalid Core NATS header name {key:?}: {error}"))
        })?;
        let value = async_nats::HeaderValue::from_str(value).map_err(|error| {
            Error::Config(format!(
                "invalid Core NATS header value for {key:?}: {error}"
            ))
        })?;
        mapped.insert(name, value);
    }
    Ok(mapped)
}

fn nats_headers_to_core(
    headers: Option<&async_nats::HeaderMap>,
) -> Result<HashMap<String, String>> {
    let Some(headers) = headers else {
        return Ok(HashMap::new());
    };

    headers
        .iter()
        .map(|(name, values)| {
            if values.len() != 1 {
                return Err(Error::Serialization(format!(
                    "Core NATS header {name:?} has {} values; the message model requires exactly one",
                    values.len()
                )));
            }
            Ok((name.to_string(), values[0].as_str().to_owned()))
        })
        .collect()
}

fn checked_deadline(timeout: std::time::Duration) -> Result<Instant> {
    Instant::now().checked_add(timeout).ok_or_else(|| {
        Error::Config("Core NATS consume timeout is too large for this platform".into())
    })
}

fn validate_jetstream_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty()
        || name
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b == b'.' || b == b'*' || b == b'>')
    {
        return Err(Error::Query(format!(
            "invalid NATS JetStream {kind} name: {name:?}"
        )));
    }

    Ok(())
}

fn nats_driver_url(dsn: &Dsn) -> Result<String> {
    match dsn.scheme.as_str() {
        "nats" => Ok(dsn.raw.clone()),
        "nats+tls" => dsn.raw_with_scheme("tls"),
        scheme => Err(Error::Dsn(format!(
            "NATS DSN must use nats:// or nats+tls://, got {scheme}"
        ))),
    }
}

fn nats_tls_ca(dsn: &Dsn) -> Option<&str> {
    dsn.params
        .get("tls-ca")
        .or_else(|| dsn.params.get("ssl-ca"))
        .map(String::as_str)
}

fn nats_topic_info(info: &async_nats::jetstream::stream::Info) -> TopicInfo {
    TopicInfo {
        name: info.config.name.clone(),
        partitions: 1,
        replicas: usize_to_i16(info.config.num_replicas),
    }
}

fn nats_topic_detail(info: &async_nats::jetstream::stream::Info) -> TopicDetail {
    let mut config = HashMap::new();
    config.insert("kind".to_owned(), "jetstream".to_owned());
    config.insert("subjects".to_owned(), info.config.subjects.join(","));
    config.insert("messages".to_owned(), info.state.messages.to_string());
    config.insert("bytes".to_owned(), info.state.bytes.to_string());
    config.insert(
        "consumer_count".to_owned(),
        info.state.consumer_count.to_string(),
    );
    config.insert("storage".to_owned(), format!("{:?}", info.config.storage));
    config.insert(
        "retention".to_owned(),
        format!("{:?}", info.config.retention),
    );
    config.insert(
        "max_messages".to_owned(),
        info.config.max_messages.to_string(),
    );
    config.insert("max_bytes".to_owned(), info.config.max_bytes.to_string());

    TopicDetail {
        info: nats_topic_info(info),
        config,
        watermarks: vec![PartitionWatermark {
            partition: 0,
            low: u64_to_i64(info.state.first_sequence),
            high: u64_to_i64(info.state.last_sequence),
        }],
    }
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn usize_to_i16(value: usize) -> i16 {
    value.min(i16::MAX as usize) as i16
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message() -> Message {
        Message {
            key: None,
            payload: bytes::Bytes::from_static(b"payload"),
            headers: HashMap::from([
                ("trace".to_owned(), "abc".to_owned()),
                ("content-type".to_owned(), "text/plain".to_owned()),
            ]),
            partition: None,
            offset: None,
            timestamp: None,
        }
    }

    #[test]
    fn nats_tls_alias_rewrites_to_async_nats_tls_scheme() {
        let dsn = Dsn::parse("nats+tls://127.0.0.1:4222?tls-ca=/tmp/ca.pem").unwrap();

        assert_eq!(
            nats_driver_url(&dsn).unwrap(),
            "tls://127.0.0.1:4222?tls-ca=/tmp/ca.pem"
        );
        assert_eq!(nats_tls_ca(&dsn), Some("/tmp/ca.pem"));
    }

    #[test]
    fn jetstream_names_reject_subject_wildcards_and_dots() {
        assert!(validate_jetstream_name("stream", "EVENTS").is_ok());
        assert!(validate_jetstream_name("stream", "events.data").is_err());
        assert!(validate_jetstream_name("stream", "events.*").is_err());
        assert!(validate_jetstream_name("stream", "").is_err());
    }

    #[test]
    fn core_nats_string_headers_round_trip_exactly() {
        let message = message();
        let mapped = nats_headers_from_core(&message.headers).unwrap();

        assert_eq!(
            nats_headers_to_core(Some(&mapped)).unwrap(),
            message.headers
        );
    }

    #[test]
    fn core_nats_rejects_unrepresentable_metadata_and_positions() {
        let mut candidate = message();
        candidate.key = Some(bytes::Bytes::from_static(b"key"));
        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("message keys")
        ));

        let mut candidate = message();
        candidate.partition = Some(0);
        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("partitions")
        ));

        let mut candidate = message();
        candidate.offset = Some(1);
        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("producer offsets")
        ));

        let mut candidate = message();
        candidate.timestamp = Some(1_710_000_000_123);
        assert!(matches!(
            validate_produce_message(&candidate),
            Err(Error::Config(message)) if message.contains("producer timestamps")
        ));

        assert!(matches!(
            validate_consume_position(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: Some(0),
                offset: None,
            }),
            Err(Error::Config(message)) if message.contains("partitions")
        ));
        assert!(matches!(
            validate_consume_position(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: None,
                offset: Some(0),
            }),
            Err(Error::Config(message)) if message.contains("offsets")
        ));
    }

    #[test]
    fn core_nats_rejects_invalid_or_multi_value_headers() {
        assert!(matches!(
            nats_headers_from_core(&HashMap::from([(
                "bad:name".to_owned(),
                "value".to_owned()
            )])),
            Err(Error::Config(message)) if message.contains("header name")
        ));
        assert!(matches!(
            nats_headers_from_core(&HashMap::from([(
                "trace".to_owned(),
                "bad\nvalue".to_owned()
            )])),
            Err(Error::Config(message)) if message.contains("header value")
        ));

        let mut headers = async_nats::HeaderMap::new();
        headers.append("trace", "one");
        headers.append("trace", "two");
        assert!(matches!(
            nats_headers_to_core(Some(&headers)),
            Err(Error::Serialization(message)) if message.contains("2 values")
        ));
    }
}
