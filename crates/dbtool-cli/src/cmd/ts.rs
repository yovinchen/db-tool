use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{Point, SeriesSet, TimeRange},
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
    if let TsAction::Query { last_minutes, .. } = &cmd.action {
        validate_last_minutes(*last_minutes)?;
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
            let result = limit_series_set(ts.query_range(&query, range).await?, ctx.limit);
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
    ctx.ensure_write_allowed()
}

fn validate_last_minutes(last_minutes: i64) -> Result<()> {
    if last_minutes <= 0 {
        return Err(Error::Config(
            "--last-minutes must be greater than zero".into(),
        ));
    }
    last_minutes.checked_mul(60_000).ok_or_else(|| {
        Error::Config("--last-minutes is too large to represent in milliseconds".into())
    })?;
    Ok(())
}

/// Apply the CLI row budget across all samples in all returned series.
///
/// Prometheus range queries return one row vector per series, so limiting every
/// series independently would allow high-cardinality queries to multiply the
/// configured limit. Empty trailing series are removed only when truncation is
/// required, keeping the returned shape bounded along with the sample count.
fn limit_series_set(mut result: SeriesSet, limit: usize) -> SeriesSet {
    let total_samples = result.series.iter().fold(0usize, |total, series| {
        total.saturating_add(series.values.len())
    });

    if total_samples <= limit {
        return result;
    }

    let mut remaining = limit;
    for series in &mut result.series {
        if remaining == 0 {
            series.values.clear();
            continue;
        }
        if series.values.len() > remaining {
            series.values.truncate(remaining);
        }
        remaining -= series.values.len();
    }
    result.series.retain(|series| !series.values.is_empty());
    result.truncated = true;
    result
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
    use dbtool_core::model::series::Series;
    use dbtool_core::service::formatter::Format;
    use serde_json::json;

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

    #[test]
    fn rejects_non_positive_and_overflowing_query_windows() {
        assert!(matches!(
            validate_last_minutes(0),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
        assert!(matches!(
            validate_last_minutes(-1),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
        assert!(matches!(
            validate_last_minutes(i64::MAX),
            Err(Error::Config(message)) if message.contains("too large")
        ));
        assert!(validate_last_minutes(1).is_ok());
    }

    #[test]
    fn limits_samples_across_all_series_and_marks_truncation() {
        let result = SeriesSet {
            series: vec![series("first", &[1, 2]), series("second", &[3, 4])],
            truncated: false,
        };

        let limited = limit_series_set(result, 3);

        assert_eq!(limited.series.len(), 2);
        assert_eq!(limited.series[0].values.len(), 2);
        assert_eq!(limited.series[1].values, vec![vec![json!(3)]]);
        assert!(limited.truncated);
    }

    #[test]
    fn preserves_backend_truncation_without_over_limit() {
        let result = SeriesSet {
            series: vec![series("only", &[1, 2])],
            truncated: true,
        };

        let limited = limit_series_set(result, 2);

        assert_eq!(limited.series[0].values.len(), 2);
        assert!(limited.truncated);
    }

    #[test]
    fn zero_limit_returns_no_samples_and_marks_truncation() {
        let result = SeriesSet {
            series: vec![series("only", &[1])],
            truncated: false,
        };

        let limited = limit_series_set(result, 0);

        assert!(limited.series.is_empty());
        assert!(limited.truncated);
    }

    fn series(name: &str, values: &[i64]) -> Series {
        Series {
            name: name.to_owned(),
            columns: vec!["value".to_owned()],
            values: values.iter().map(|value| vec![json!(value)]).collect(),
        }
    }
}
