use dbtool_core::{
    dsn::Dsn,
    error::{Error, Result},
    model::{
        AckMode, BoundedList, ConsumeCursor, ConsumeOptions, ConsumerIdentity,
        DeleteResourceOptions, DeleteResourceOutcome, LagInfo, Message, MessageCursor,
        MessageMetadata, MessageResource, MessageResourceKind, PartitionWatermark, ProduceOutcome,
        TopicDetail, TopicInfo,
    },
    port::{
        capability::{AdminInspect, AdminMutate, MessageConsumer, MessageProducer},
        connector::{Capabilities, CapabilityOperation, Connector, ConnectorKind},
    },
    service::limiter::ListLimiter,
};
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    str::FromStr,
};
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

    fn operations(&self) -> Vec<CapabilityOperation> {
        nats_operations(self.capabilities())
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

    fn as_admin_mutate(&self) -> Option<&dyn AdminMutate> {
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
        validate_nats_consume_options(&options)?;

        match &options.identity {
            ConsumerIdentity::Stateless => {
                if let Some(ConsumeCursor::NatsJetstream { stream_sequence }) = options.cursor {
                    self.consume_jetstream_stateless(
                        source,
                        options.max,
                        options.timeout,
                        stream_sequence,
                    )
                    .await
                } else {
                    self.consume_core_nats(source, None, options.max, options.timeout)
                        .await
                }
            }
            ConsumerIdentity::Group { group, member } => {
                debug_assert!(member.is_none());
                self.consume_core_nats(source, Some(group.as_str()), options.max, options.timeout)
                    .await
            }
            ConsumerIdentity::Durable { name } => {
                validate_jetstream_name("consumer", name)?;
                self.consume_jetstream_durable(source, name, &options).await
            }
        }
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

    async fn list_topics_bounded(&self, max_items: usize) -> Result<BoundedList<TopicInfo>> {
        let limiter = ListLimiter::new(max_items);
        let probe_items = limiter.probe_items()?;
        let mut streams = self.jetstream().streams();
        let mut topics = Vec::new();
        let mut names = HashSet::new();

        while topics.len() < probe_items {
            let Some(info) = streams
                .try_next()
                .await
                .map_err(|e| Error::Query(e.to_string()))?
            else {
                break;
            };
            let topic = nats_topic_info(&info);
            if !names.insert(topic.name.clone()) {
                return Err(Error::Serialization(format!(
                    "NATS JetStream paginated catalog repeated stream {:?}",
                    topic.name
                )));
            }
            topics.push(topic);
        }

        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(limiter.finish(topics))
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
                let (committed, latest, outstanding) = nats_lag_dimensions(
                    consumer.ack_floor.stream_sequence,
                    info.state.last_sequence,
                    consumer.num_ack_pending,
                    consumer.num_pending,
                )?;
                lag.push(LagInfo {
                    topic: info.config.name,
                    partition: 0,
                    group: group.to_owned(),
                    committed,
                    latest,
                    lag: outstanding,
                });
            }
        }

        Ok(lag)
    }
}

#[async_trait::async_trait]
impl AdminMutate for NatsAdapter {
    async fn delete_resource(
        &self,
        resource: MessageResource,
        options: DeleteResourceOptions,
    ) -> Result<DeleteResourceOutcome> {
        validate_nats_delete_request(&resource, options)?;

        let jetstream = self.jetstream();
        let stream = jetstream
            .get_stream(&resource.name)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        let messages_before = stream.cached_info().state.messages;
        let consumers_before = u64::try_from(stream.cached_info().state.consumer_count)
            .map_err(|_| Error::Serialization("NATS consumer count exceeds u64".into()))?;
        let status = jetstream
            .delete_stream(&resource.name)
            .await
            .map_err(|e| Error::Query(e.to_string()))?;
        if !status.success {
            return Err(Error::Query(format!(
                "NATS did not acknowledge deletion of JetStream {:?}",
                resource.name
            )));
        }
        match jetstream.get_stream(&resource.name).await {
            Ok(_) => {
                return Err(Error::Query(format!(
                    "NATS acknowledged deletion of JetStream {:?}, but it still exists",
                    resource.name
                )))
            }
            Err(error) if nats_stream_not_found(&error) => {}
            Err(error) => return Err(Error::Query(error.to_string())),
        }

        Ok(DeleteResourceOutcome {
            resource,
            acknowledged: true,
            verified_absent: true,
            messages_before: Some(messages_before),
            consumers_before: Some(consumers_before),
        })
    }
}

impl NatsAdapter {
    fn jetstream(&self) -> async_nats::jetstream::Context {
        async_nats::jetstream::new(self.client.clone())
    }

    async fn consume_core_nats(
        &self,
        subject: &str,
        queue_group: Option<&str>,
        max: usize,
        wait: std::time::Duration,
    ) -> Result<Vec<Message>> {
        let deadline = checked_deadline(wait)?;
        let mut subscriber = match queue_group {
            Some(group) => self
                .client
                .queue_subscribe(subject.to_owned(), group.to_owned())
                .await
                .map_err(|error| Error::Query(error.to_string()))?,
            None => self
                .client
                .subscribe(subject.to_owned())
                .await
                .map_err(|error| Error::Query(error.to_string()))?,
        };
        self.client
            .flush()
            .await
            .map_err(|error| Error::Connection(error.to_string()))?;

        let mut messages = Vec::new();
        while messages.len() < max {
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
                        cursor: None,
                        metadata: None,
                    });
                }
                Ok(None) | Err(_) => break,
            }
        }

        Ok(messages)
    }

    async fn consume_jetstream_stateless(
        &self,
        subject: &str,
        max: usize,
        wait: std::time::Duration,
        start_sequence: u64,
    ) -> Result<Vec<Message>> {
        use async_nats::jetstream::consumer::{pull, AckPolicy, DeliverPolicy};

        let deadline = checked_deadline(wait)?;
        let jetstream = self.jetstream();
        let stream_name = jetstream
            .stream_by_subject(subject)
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        let stream = jetstream
            .get_stream(&stream_name)
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        let consumer = stream
            .create_consumer(pull::Config {
                deliver_policy: DeliverPolicy::ByStartSequence { start_sequence },
                ack_policy: AckPolicy::None,
                filter_subject: subject.to_owned(),
                inactive_threshold: wait
                    .checked_add(std::time::Duration::from_secs(5))
                    .ok_or_else(|| {
                        Error::Config(
                            "NATS JetStream consume timeout is too large for cleanup".into(),
                        )
                    })?,
                ..Default::default()
            })
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        let consumer_name = consumer.cached_info().name.clone();

        let consume_result = async {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| Error::Query("NATS JetStream consume deadline elapsed".into()))?;
            let mut batch = consumer
                .fetch()
                .max_messages(max)
                .expires(remaining)
                .messages()
                .await
                .map_err(|error| Error::Query(error.to_string()))?;
            let mut messages = Vec::new();
            while messages.len() < max {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                let item = match timeout(remaining, batch.next()).await {
                    Ok(Some(item)) => item.map_err(|error| Error::Query(error.to_string()))?,
                    Ok(None) | Err(_) => break,
                };
                messages.push(jetstream_message_to_core(item)?);
            }
            Ok(messages)
        }
        .await;

        let cleanup_result = jetstream
            .delete_consumer_from_stream(&consumer_name, &stream_name)
            .await
            .map(|status| status.success)
            .map_err(|error| Error::Query(error.to_string()));
        finish_temporary_consumer(consume_result, cleanup_result)
    }

    async fn consume_jetstream_durable(
        &self,
        subject: &str,
        durable_name: &str,
        options: &ConsumeOptions,
    ) -> Result<Vec<Message>> {
        let deadline = checked_deadline(options.timeout)?;
        let jetstream = self.jetstream();
        let stream_name = jetstream
            .stream_by_subject(subject)
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        let stream = jetstream
            .get_stream(&stream_name)
            .await
            .map_err(|error| Error::Query(error.to_string()))?;
        let consumer =
            compatible_durable_consumer(&jetstream, &stream, durable_name, subject).await?;
        // An ACKing call reserves one third of its caller-owned deadline for
        // server-confirmed acknowledgements. Otherwise a partially filled pull
        // batch could consume the whole timeout waiting for more deliveries and
        // leave no time to acknowledge the messages it already received.
        let fetch_deadline = if options.ack == AckMode::OnSuccess {
            deadline - (options.timeout / 3)
        } else {
            deadline
        };
        let native_messages = fetch_jetstream_batch(&consumer, options.max, fetch_deadline).await?;

        // Conversion is deliberately complete before the first ACK. A malformed
        // header or delivery envelope therefore leaves the whole fetched batch
        // unacknowledged and eligible for redelivery.
        let messages = native_messages
            .iter()
            .cloned()
            .map(jetstream_message_to_core)
            .collect::<Result<Vec<_>>>()?;

        if options.ack == AckMode::OnSuccess {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    Error::Query(
                        "NATS JetStream consume deadline elapsed before acknowledgements completed"
                            .into(),
                    )
                })?;
            timeout(
                remaining,
                futures::future::try_join_all(
                    native_messages.iter().map(|message| message.double_ack()),
                ),
            )
            .await
            .map_err(|_| {
                Error::Query(
                    "NATS JetStream verified acknowledgements exceeded consume timeout".into(),
                )
            })?
            .map_err(|error| Error::Query(error.to_string()))?;
        }

        Ok(messages)
    }
}

async fn compatible_durable_consumer(
    jetstream: &async_nats::jetstream::Context,
    stream: &async_nats::jetstream::stream::Stream,
    durable_name: &str,
    subject: &str,
) -> Result<async_nats::jetstream::consumer::PullConsumer> {
    use async_nats::jetstream::consumer::{pull, AckPolicy, DeliverPolicy, ReplayPolicy};
    use async_nats::jetstream::stream::{ConsumerCreateStrictErrorKind, ConsumerErrorKind};

    let stream_name = stream.cached_info().config.name.as_str();
    match jetstream
        .get_consumer_from_stream::<async_nats::jetstream::consumer::Config, _, _>(
            durable_name,
            stream_name,
        )
        .await
    {
        Ok(consumer) => {
            validate_durable_consumer_config(
                &consumer.cached_info().config,
                durable_name,
                subject,
            )?;
        }
        Err(error)
            if matches!(
                error.kind(),
                ConsumerErrorKind::JetStream(ref jetstream_error)
                    if jetstream_error.error_code()
                        == async_nats::jetstream::ErrorCode::CONSUMER_NOT_FOUND
            ) =>
        {
            let config = pull::Config {
                durable_name: Some(durable_name.to_owned()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: subject.to_owned(),
                replay_policy: ReplayPolicy::Instant,
                ..Default::default()
            };
            match stream.create_consumer_strict(config).await {
                Ok(consumer) => return Ok(consumer),
                Err(error) if error.kind() == ConsumerCreateStrictErrorKind::AlreadyExists => {
                    // A concurrent creator won the race. Re-read and validate it;
                    // never update an existing consumer to match our request.
                }
                Err(error) => return Err(Error::Query(error.to_string())),
            }
        }
        Err(error) => return Err(Error::Query(error.to_string())),
    }

    let consumer = jetstream
        .get_consumer_from_stream::<pull::Config, _, _>(durable_name, stream_name)
        .await
        .map_err(|error| Error::Query(error.to_string()))?;
    validate_durable_consumer_config(&consumer.cached_info().config, durable_name, subject)?;
    Ok(consumer)
}

fn validate_durable_consumer_config(
    config: &async_nats::jetstream::consumer::Config,
    durable_name: &str,
    subject: &str,
) -> Result<()> {
    use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, ReplayPolicy};

    let compatible = config.deliver_subject.is_none()
        && config.durable_name.as_deref() == Some(durable_name)
        && config.deliver_policy == DeliverPolicy::All
        && config.ack_policy == AckPolicy::Explicit
        && config.filter_subject == subject
        && config.filter_subjects.is_empty()
        && config.replay_policy == ReplayPolicy::Instant
        && !config.headers_only
        && config.max_deliver <= 0;
    if compatible {
        Ok(())
    } else {
        Err(Error::Config(format!(
            "existing NATS JetStream durable {durable_name:?} is incompatible: dbtool requires a pull consumer with deliver=all, ack=explicit, replay=instant, unlimited redelivery, full payloads, and the exact filter subject {subject:?}; the existing consumer was not modified"
        )))
    }
}

async fn fetch_jetstream_batch(
    consumer: &async_nats::jetstream::consumer::PullConsumer,
    max: usize,
    deadline: Instant,
) -> Result<Vec<async_nats::jetstream::Message>> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| Error::Query("NATS JetStream consume deadline elapsed".into()))?;
    let mut batch = consumer
        .fetch()
        .max_messages(max)
        .expires(remaining)
        .messages()
        .await
        .map_err(|error| Error::Query(error.to_string()))?;
    let mut messages = Vec::new();
    while messages.len() < max {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        let item = match timeout(remaining, batch.next()).await {
            Ok(Some(item)) => item.map_err(|error| Error::Query(error.to_string()))?,
            Ok(None) | Err(_) => break,
        };
        messages.push(item);
    }
    Ok(messages)
}

fn finish_temporary_consumer<T>(consume: Result<T>, cleanup: Result<bool>) -> Result<T> {
    let cleanup = cleanup.and_then(|success| {
        if success {
            Ok(())
        } else {
            Err(Error::Query(
                "NATS JetStream reported unsuccessful temporary consumer cleanup".into(),
            ))
        }
    });
    match (consume, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
        (Err(consume_error), Ok(())) => Err(consume_error),
        (Err(consume_error), Err(cleanup_error)) => Err(Error::Query(format!(
            "NATS JetStream consume failed: {consume_error}; temporary consumer cleanup also failed: {cleanup_error}"
        ))),
    }
}

fn jetstream_message_to_core(message: async_nats::jetstream::Message) -> Result<Message> {
    let info = message
        .info()
        .map_err(|error| Error::Serialization(error.to_string()))?;
    let stream = info.stream.to_owned();
    let consumer = info.consumer.to_owned();
    let stream_sequence = info.stream_sequence;
    let consumer_sequence = info.consumer_sequence;
    let delivery_attempt = info.delivered;
    let pending = info.pending;
    let timestamp = i64::try_from(info.published.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| Error::Serialization("NATS publish timestamp exceeds i64 millis".into()))?;
    let headers = nats_headers_to_core(message.headers.as_ref())?;

    Ok(Message {
        key: None,
        payload: message.payload.clone(),
        headers,
        partition: None,
        offset: None,
        timestamp: Some(timestamp),
        cursor: Some(MessageCursor::NatsJetstream {
            stream,
            stream_sequence,
        }),
        metadata: Some(MessageMetadata::NatsJetstream {
            consumer,
            consumer_sequence,
            delivery_attempt,
            pending,
        }),
    })
}

fn nats_operations(capabilities: Capabilities) -> Vec<CapabilityOperation> {
    let mut operations = capabilities.operations();
    operations.extend([
        CapabilityOperation::MessageConsumeGroup,
        CapabilityOperation::MessageConsumeDurable,
        CapabilityOperation::MessageConsumeAck,
        CapabilityOperation::MessageAdminListTopics,
        CapabilityOperation::MessageAdminListTopicsBounded,
        CapabilityOperation::MessageAdminTopicDetail,
        CapabilityOperation::MessageAdminConsumerLag,
        CapabilityOperation::MessageAdminDelete,
    ]);
    operations
}

fn validate_nats_delete_request(
    resource: &MessageResource,
    options: DeleteResourceOptions,
) -> Result<()> {
    if resource.kind != MessageResourceKind::NatsJetstream {
        return Err(Error::Config(format!(
            "NATS can delete only nats-jetstream resources, not {}",
            resource.kind.as_str()
        )));
    }
    if options.if_empty || options.if_unused {
        return Err(Error::Config(
            "NATS JetStream deletion does not support AMQP if-empty/if-unused options".into(),
        ));
    }
    validate_jetstream_name("stream", &resource.name)
}

fn nats_stream_not_found(error: &async_nats::jetstream::context::GetStreamError) -> bool {
    match error.kind() {
        async_nats::jetstream::context::GetStreamErrorKind::JetStream(error) => {
            // STREAM.INFO returns HTTP-style status 404 only when the named
            // stream is absent; other JetStream failures must remain visible.
            error.code() == 404
        }
        _ => false,
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

fn validate_queue_group(group: &str) -> Result<()> {
    if group.is_empty()
        || group
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(Error::Config(format!(
            "invalid Core NATS queue group: {group:?}"
        )));
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
    if message.cursor.is_some() || message.metadata.is_some() {
        return Err(Error::Config(
            "Core NATS producer messages cannot set consumer cursor or delivery metadata".into(),
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
    match &options.cursor {
        None => {}
        Some(cursor @ ConsumeCursor::NatsJetstream { .. }) => {
            cursor.validate().map_err(Error::Config)?;
        }
        Some(cursor) => {
            return Err(Error::Config(format!(
                "NATS consumer cannot use {cursor:?} cursor"
            )))
        }
    }
    Ok(())
}

fn validate_nats_consume_options(options: &ConsumeOptions) -> Result<()> {
    options.validate().map_err(Error::Config)?;
    validate_consume_position(options)?;
    match &options.identity {
        ConsumerIdentity::Stateless => {
            if options.ack != AckMode::None {
                return Err(Error::Config(
                    "stateless NATS consumption cannot acknowledge broker state; use --durable"
                        .into(),
                ));
            }
        }
        ConsumerIdentity::Group { group, member } => {
            validate_queue_group(group)?;
            if member.is_some() {
                return Err(Error::Config(
                    "Core NATS queue groups do not expose stable member identities; omit --consumer"
                        .into(),
                ));
            }
            if options.ack != AckMode::None {
                return Err(Error::Config(
                    "Core NATS queue groups have no broker acknowledgement or replay progress"
                        .into(),
                ));
            }
        }
        ConsumerIdentity::Durable { name } => validate_jetstream_name("consumer", name)?,
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

fn nats_lag_dimensions(
    ack_floor_stream_sequence: u64,
    stream_last_sequence: u64,
    num_ack_pending: usize,
    num_pending: u64,
) -> Result<(i64, i64, i64)> {
    let num_ack_pending = u64::try_from(num_ack_pending)
        .map_err(|_| Error::Serialization("NATS ack-pending count exceeds u64".into()))?;
    let outstanding = num_ack_pending.checked_add(num_pending).ok_or_else(|| {
        Error::Serialization("NATS total outstanding message count exceeds u64".into())
    })?;
    Ok((
        exact_nats_lag_i64("ACK floor stream sequence", ack_floor_stream_sequence)?,
        exact_nats_lag_i64("stream last sequence", stream_last_sequence)?,
        exact_nats_lag_i64("outstanding message count", outstanding)?,
    ))
}

fn exact_nats_lag_i64(label: &str, value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| Error::Serialization(format!("NATS {label} exceeds portable i64")))
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
    use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, ReplayPolicy};

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
            cursor: None,
            metadata: None,
        }
    }

    fn durable_config(
        durable_name: &str,
        subject: &str,
    ) -> async_nats::jetstream::consumer::Config {
        async_nats::jetstream::consumer::Config {
            durable_name: Some(durable_name.to_owned()),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            filter_subject: subject.to_owned(),
            replay_policy: ReplayPolicy::Instant,
            ..Default::default()
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
                cursor: None,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("partitions")
        ));
        assert!(matches!(
            validate_consume_position(&ConsumeOptions {
                max: 1,
                timeout: std::time::Duration::from_secs(1),
                partition: None,
                offset: Some(0),
                cursor: None,
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("offsets")
        ));

        assert!(validate_consume_position(&ConsumeOptions {
            cursor: Some(ConsumeCursor::NatsJetstream {
                stream_sequence: 42
            }),
            ..Default::default()
        })
        .is_ok());
        assert!(matches!(
            validate_consume_position(&ConsumeOptions {
                cursor: Some(ConsumeCursor::NatsJetstream { stream_sequence: 0 }),
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
        assert!(matches!(
            validate_consume_position(&ConsumeOptions {
                cursor: Some(ConsumeCursor::RedisStream {
                    id: "1-0".to_owned(),
                }),
                ..Default::default()
            }),
            Err(Error::Config(message)) if message.contains("cannot use")
        ));
    }

    #[test]
    fn nats_stateful_identity_rules_do_not_invent_protocol_semantics() {
        let group = ConsumeOptions {
            identity: ConsumerIdentity::Group {
                group: "workers.eu".to_owned(),
                member: None,
            },
            ack: AckMode::None,
            ..Default::default()
        };
        assert!(validate_nats_consume_options(&group).is_ok());

        let mut invalid = group.clone();
        invalid.identity = ConsumerIdentity::Group {
            group: "workers.eu".to_owned(),
            member: Some("member-1".to_owned()),
        };
        assert!(matches!(
            validate_nats_consume_options(&invalid),
            Err(Error::Config(message)) if message.contains("stable member")
        ));

        let mut invalid = group;
        invalid.ack = AckMode::OnSuccess;
        assert!(matches!(
            validate_nats_consume_options(&invalid),
            Err(Error::Config(message)) if message.contains("no broker acknowledgement")
        ));

        let durable = ConsumeOptions {
            identity: ConsumerIdentity::Durable {
                name: "DBTOOL_WORKER".to_owned(),
            },
            ack: AckMode::OnSuccess,
            ..Default::default()
        };
        assert!(validate_nats_consume_options(&durable).is_ok());

        let stateless_ack = ConsumeOptions {
            ack: AckMode::OnSuccess,
            ..Default::default()
        };
        assert!(matches!(
            validate_nats_consume_options(&stateless_ack),
            Err(Error::Config(message)) if message.contains("use --durable")
        ));
    }

    #[test]
    fn durable_consumers_must_match_without_server_side_mutation() {
        let valid = durable_config("DBTOOL_WORKER", "events.us");
        assert!(validate_durable_consumer_config(&valid, "DBTOOL_WORKER", "events.us").is_ok());

        let mut wrong_filter = valid.clone();
        wrong_filter.filter_subject = "events.eu".to_owned();
        assert!(matches!(
            validate_durable_consumer_config(
                &wrong_filter,
                "DBTOOL_WORKER",
                "events.us"
            ),
            Err(Error::Config(message))
                if message.contains("incompatible") && message.contains("not modified")
        ));

        let mut wrong_delivery = valid.clone();
        wrong_delivery.deliver_policy = DeliverPolicy::New;
        assert!(
            validate_durable_consumer_config(&wrong_delivery, "DBTOOL_WORKER", "events.us")
                .is_err()
        );

        let mut wrong_ack = valid;
        wrong_ack.ack_policy = AckPolicy::All;
        assert!(
            validate_durable_consumer_config(&wrong_ack, "DBTOOL_WORKER", "events.us").is_err()
        );

        let mut finite_redelivery = durable_config("DBTOOL_WORKER", "events.us");
        finite_redelivery.max_deliver = 1;
        assert!(matches!(
            validate_durable_consumer_config(
                &finite_redelivery,
                "DBTOOL_WORKER",
                "events.us"
            ),
            Err(Error::Config(message)) if message.contains("unlimited redelivery")
        ));
    }

    #[test]
    fn jetstream_lag_includes_delivered_unacknowledged_and_not_yet_delivered() {
        assert_eq!(nats_lag_dimensions(4, 11, 3, 4).unwrap(), (4, 11, 7));
        assert!(matches!(
            nats_lag_dimensions(u64::MAX, u64::MAX, 1, u64::MAX),
            Err(Error::Serialization(message)) if message.contains("outstanding")
        ));
        assert!(matches!(
            nats_lag_dimensions(i64::MAX as u64 + 1, 1, 0, 0),
            Err(Error::Serialization(message)) if message.contains("ACK floor")
        ));
    }

    #[test]
    fn temporary_consumer_cleanup_must_succeed_and_preserves_dual_failures() {
        assert_eq!(finish_temporary_consumer(Ok(7), Ok(true)).unwrap(), 7);
        assert!(matches!(
            finish_temporary_consumer::<()>(Ok(()), Ok(false)),
            Err(Error::Query(message)) if message.contains("unsuccessful")
        ));
        assert!(matches!(
            finish_temporary_consumer::<()>(
                Err(Error::Query("consume broke".into())),
                Err(Error::Query("cleanup broke".into())),
            ),
            Err(Error::Query(message))
                if message.contains("consume broke") && message.contains("cleanup broke")
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

    #[test]
    fn nats_declares_jetstream_admin_profile_with_core_defaults() {
        let operations = nats_operations(Capabilities {
            producer: true,
            consumer: true,
            admin: true,
            ..Default::default()
        });

        for operation in [
            CapabilityOperation::MessageProduce,
            CapabilityOperation::MessageConsume,
            CapabilityOperation::MessageConsumeGroup,
            CapabilityOperation::MessageConsumeDurable,
            CapabilityOperation::MessageConsumeAck,
            CapabilityOperation::MessageAdminListTopics,
            CapabilityOperation::MessageAdminListTopicsBounded,
            CapabilityOperation::MessageAdminTopicDetail,
            CapabilityOperation::MessageAdminConsumerLag,
            CapabilityOperation::MessageAdminDelete,
        ] {
            assert!(operations.contains(&operation));
        }
    }

    #[test]
    fn nats_delete_accepts_only_jetstreams_without_amqp_options() {
        let stream = MessageResource {
            kind: MessageResourceKind::NatsJetstream,
            name: "EVENTS".to_owned(),
        };
        assert!(validate_nats_delete_request(&stream, DeleteResourceOptions::default()).is_ok());
        assert!(matches!(
            validate_nats_delete_request(
                &stream,
                DeleteResourceOptions {
                    if_empty: false,
                    if_unused: true,
                }
            ),
            Err(Error::Config(message)) if message.contains("AMQP")
        ));

        let queue = MessageResource {
            kind: MessageResourceKind::AmqpQueue,
            name: "EVENTS".to_owned(),
        };
        assert!(matches!(
            validate_nats_delete_request(&queue, DeleteResourceOptions::default()),
            Err(Error::Config(message)) if message.contains("nats-jetstream")
        ));
    }
}
