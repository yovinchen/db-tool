use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{Point, TimeRange},
    Result,
};
use std::collections::HashMap;

#[derive(Args)]
#[command(
    about = "Read and write Prometheus-compatible time-series data.",
    long_about = "Time-series commands list metric names, run bounded range queries, and write single samples through Prometheus remote write behind --allow-write."
)]
pub struct TsCmd {
    #[command(subcommand)]
    pub action: TsAction,
}

#[derive(Subcommand)]
pub enum TsAction {
    /// List metric names from a Prometheus-compatible backend.
    Measurements,
    /// Run a range query over the last N minutes.
    Query {
        /// PromQL expression to evaluate over the requested range.
        query: String,
        /// Number of minutes back from now to include in the range query.
        #[arg(long, default_value = "60")]
        last_minutes: i64,
    },
    /// Write one sample through Prometheus remote write.
    Write {
        /// Metric name to write.
        measurement: String,
        /// Numeric sample value.
        value: f64,
        /// Field label used by dbtool's generic point model.
        #[arg(long, default_value = "value")]
        field: String,
        /// Metric tag in key=value form. Repeat for multiple tags.
        #[arg(long = "tag")]
        tags: Vec<String>,
        /// Optional Unix timestamp in milliseconds; defaults to current time.
        #[arg(long = "timestamp-ms")]
        timestamp_ms: Option<i64>,
    },
}

pub async fn run(ctx: &Context, cmd: TsCmd) -> Result<String> {
    if matches!(cmd.action, TsAction::Write { .. }) {
        ensure_write_allowed(ctx)?;
    }

    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let ts = conn
        .as_timeseries()
        .ok_or_else(|| Error::UnsupportedCapability {
            kind: conn.kind().0.clone(),
            needed: "TimeSeriesStore",
        })?;
    let start = std::time::Instant::now();
    let kind = conn.kind().0.clone();

    Ok(match cmd.action {
        TsAction::Measurements => ctx.render_success(
            &kind,
            ts.list_measurements().await?,
            start.elapsed().as_millis() as u64,
            false,
        ),
        TsAction::Query {
            query,
            last_minutes,
        } => {
            let range = TimeRange::last_n_minutes(last_minutes);
            let result = ts.query_range(&query, range).await?;
            let truncated = result.truncated;
            ctx.render_success(&kind, result, start.elapsed().as_millis() as u64, truncated)
        }
        TsAction::Write {
            measurement,
            value,
            field,
            tags,
            timestamp_ms,
        } => {
            let point = Point {
                measurement,
                tags: parse_tags(&tags)?,
                fields: HashMap::from([(field, value)]),
                timestamp: timestamp_ms.unwrap_or_else(now_millis),
            };
            ts.write_points(vec![point]).await?;
            ctx.render_success(
                &kind,
                serde_json::json!({ "written_points": 1, "written_samples": 1 }),
                start.elapsed().as_millis() as u64,
                false,
            )
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

fn parse_tags(tags: &[String]) -> Result<HashMap<String, String>> {
    let mut parsed = HashMap::new();
    for tag in tags {
        let (key, value) = tag
            .split_once('=')
            .ok_or_else(|| Error::Config(format!("invalid tag '{tag}', expected key=value")))?;
        if key.trim().is_empty() {
            return Err(Error::Config("tag key must not be empty".into()));
        }
        parsed.insert(key.to_owned(), value.to_owned());
    }
    Ok(parsed)
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbtool_core::service::formatter::Format;

    fn test_context(allow_write: bool) -> Context {
        Context {
            registry: dbtool_core::registry::Registry::default(),
            conn: None,
            dsn: None,
            format: Format::Json,
            limit: 100,
            throttle_overrides: Default::default(),
            allow_write,
            confirm: None,
        }
    }

    #[test]
    fn ts_write_requires_write_flag() {
        assert!(matches!(
            ensure_write_allowed(&test_context(false)),
            Err(Error::WriteNotAllowed)
        ));
        assert!(ensure_write_allowed(&test_context(true)).is_ok());
    }

    #[test]
    fn parses_tags_as_key_value_pairs() {
        let tags = parse_tags(&["job=dbtool".to_owned(), "instance=local".to_owned()]).unwrap();

        assert_eq!(tags["job"], "dbtool");
        assert_eq!(tags["instance"], "local");
        assert!(matches!(
            parse_tags(&["bad".to_owned()]),
            Err(Error::Config(message)) if message.contains("expected key=value")
        ));
    }
}
