use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{ConsumeOptions, Message},
    service::formatter::Formatter,
    Result,
};
use std::time::Duration;

#[derive(Args)]
pub struct MqCmd {
    #[command(subcommand)]
    pub action: MqAction,
}

#[derive(Subcommand)]
pub enum MqAction {
    /// Produce messages to a topic/queue
    Produce {
        topic: String,
        /// JSON payload
        payload: String,
    },
    /// Consume messages (always bounded)
    Consume {
        topic: String,
        #[arg(long, default_value = "10")]
        max: usize,
        #[arg(long, default_value = "5")]
        timeout: u64,
    },
    /// List topics
    Topics,
    /// Show topic/queue detail when the backend exposes admin metadata
    Detail { topic: String },
    /// Show consumer group lag
    Lag { group: String },
}

pub async fn run(ctx: &Context, cmd: MqCmd) -> Result<String> {
    if matches!(cmd.action, MqAction::Produce { .. }) {
        ensure_write_allowed(ctx)?;
    }

    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let start = std::time::Instant::now();
    let elapsed = || start.elapsed().as_millis() as u64;
    let kind = conn.kind().0.clone();

    Ok(match cmd.action {
        MqAction::Produce { topic, payload } => {
            let producer = conn
                .as_producer()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "MessageProducer",
                })?;
            let msg = Message {
                key: None,
                payload: payload.into_bytes().into(),
                headers: Default::default(),
                partition: None,
                offset: None,
                timestamp: None,
            };
            let outcome = producer.produce(&topic, vec![msg]).await?;
            Formatter::success(&kind, outcome, elapsed(), false)
        }
        MqAction::Consume {
            topic,
            max,
            timeout,
        } => {
            let consumer = conn
                .as_consumer()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "MessageConsumer",
                })?;
            let opts = ConsumeOptions {
                max,
                timeout: Duration::from_secs(timeout),
                ..Default::default()
            };
            let msgs = consumer.consume(&topic, opts).await?;
            let truncated = msgs.len() >= max;
            Formatter::success(&kind, msgs, elapsed(), truncated)
        }
        MqAction::Topics => {
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            let topics = admin.list_topics().await?;
            Formatter::success(&kind, topics, elapsed(), false)
        }
        MqAction::Detail { topic } => {
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            let detail = admin.topic_detail(&topic).await?;
            Formatter::success(&kind, detail, elapsed(), false)
        }
        MqAction::Lag { group } => {
            let admin = conn
                .as_admin()
                .ok_or_else(|| Error::UnsupportedCapability {
                    kind: kind.clone(),
                    needed: "AdminInspect",
                })?;
            let lag = admin.consumer_lag(&group).await?;
            Formatter::success(&kind, lag, elapsed(), false)
        }
    })
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    if ctx.allow_write {
        Ok(())
    } else {
        Err(Error::WriteNotAllowed)
    }
}
