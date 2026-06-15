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
use std::collections::HashMap;
use tokio::time::{timeout, Instant};

pub struct NatsAdapter {
    client: async_nats::Client,
    kind: ConnectorKind,
}

pub fn factory(dsn: Dsn) -> BoxFuture<'static, Result<Box<dyn Connector>>> {
    Box::pin(async move {
        let client = async_nats::connect(dsn.raw)
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
        let mut produced = 0;
        for message in messages {
            self.client
                .publish(target.to_owned(), message.payload)
                .await
                .map_err(|e| Error::Query(e.to_string()))?;
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
        if options.max == 0 {
            return Ok(vec![]);
        }

        let mut subscriber = self
            .client
            .subscribe(source.to_owned())
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        self.client
            .flush()
            .await
            .map_err(|e| Error::Connection(e.to_string()))?;

        let deadline = Instant::now() + options.timeout;
        let mut messages = Vec::new();
        while messages.len() < options.max {
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            match timeout(deadline - now, subscriber.next()).await {
                Ok(Some(message)) => messages.push(Message {
                    key: None,
                    payload: message.payload,
                    headers: HashMap::new(),
                    partition: None,
                    offset: None,
                    timestamp: None,
                }),
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

    #[test]
    fn jetstream_names_reject_subject_wildcards_and_dots() {
        assert!(validate_jetstream_name("stream", "EVENTS").is_ok());
        assert!(validate_jetstream_name("stream", "events.data").is_err());
        assert!(validate_jetstream_name("stream", "events.*").is_err());
        assert!(validate_jetstream_name("stream", "").is_err());
    }
}
