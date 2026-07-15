use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{Point, SeriesSet, TimeRange},
    Result,
};
use std::collections::HashMap;

const DEFAULT_LAST_MINUTES: i64 = 60;
const MAX_QUERY_SAMPLES: usize = 1_000_000;

#[derive(Args)]
#[command(
    about = "Read and write Prometheus-compatible time-series data.",
    long_about = "Time-series commands list metric names, run bounded range queries, and write single samples through Prometheus remote write behind --allow-write. Query ranges use either --last-minutes (60 by default) or an explicit --start-ms/--end-ms pair in Unix epoch milliseconds."
)]
pub struct TsCmd {
    #[command(subcommand)]
    pub action: TsAction,
}

#[derive(Subcommand)]
pub enum TsAction {
    /// List metric names from a Prometheus-compatible backend.
    Measurements,
    /// Run a bounded range query.
    #[command(
        long_about = "Run a PromQL range query. Select either a relative window with --last-minutes (60 minutes by default) or an inclusive explicit range with both --start-ms and --end-ms in Unix epoch milliseconds. Explicit bounds cannot be combined with --last-minutes. The global --limit must be between 1 and 1,000,000 samples and is applied across all returned series."
    )]
    Query {
        /// PromQL expression to evaluate over the requested range.
        query: String,
        /// Relative minutes back from now; defaults to 60 when no range is supplied.
        #[arg(long, value_name = "MINUTES")]
        last_minutes: Option<i64>,
        /// Inclusive range start as Unix epoch milliseconds; requires --end-ms.
        #[arg(long, value_name = "EPOCH_MILLIS")]
        start_ms: Option<i64>,
        /// Inclusive range end as Unix epoch milliseconds; requires --start-ms.
        #[arg(long, value_name = "EPOCH_MILLIS")]
        end_ms: Option<i64>,
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
    let query_range = match &cmd.action {
        TsAction::Query {
            last_minutes,
            start_ms,
            end_ms,
            ..
        } => {
            validate_query_limit(ctx.limit)?;
            Some(resolve_query_range(*last_minutes, *start_ms, *end_ms)?)
        }
        _ => None,
    };

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
            last_minutes: _,
            start_ms: _,
            end_ms: _,
        } => {
            let range = query_range.ok_or_else(|| {
                Error::Internal("validated time-series query range is missing".into())
            })?;
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
    let window_ms = last_minutes.checked_mul(60_000).ok_or_else(|| {
        Error::Config("--last-minutes is too large to represent in milliseconds".into())
    })?;
    now_millis().checked_sub(window_ms).ok_or_else(|| {
        Error::Config("--last-minutes is too large to represent a valid time range".into())
    })?;
    Ok(())
}

fn validate_query_limit(limit: usize) -> Result<()> {
    if limit == 0 {
        return Err(Error::Config(
            "time-series query --limit must be greater than zero".into(),
        ));
    }
    if limit > MAX_QUERY_SAMPLES {
        return Err(Error::Config(format!(
            "time-series query --limit must not exceed {MAX_QUERY_SAMPLES} samples"
        )));
    }
    Ok(())
}

fn resolve_query_range(
    last_minutes: Option<i64>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> Result<TimeRange> {
    match (last_minutes, start_ms, end_ms) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => Err(Error::Config(
            "--last-minutes cannot be combined with --start-ms or --end-ms".into(),
        )),
        (None, Some(start), Some(end)) if start > end => Err(Error::Config(
            "--start-ms must be less than or equal to --end-ms".into(),
        )),
        (None, Some(start), Some(end)) => Ok(TimeRange {
            start: Some(start),
            end: Some(end),
        }),
        (None, Some(_), None) | (None, None, Some(_)) => Err(Error::Config(
            "--start-ms and --end-ms must be provided together".into(),
        )),
        (last_minutes, None, None) => {
            let last_minutes = last_minutes.unwrap_or(DEFAULT_LAST_MINUTES);
            validate_last_minutes(last_minutes)?;
            let end = now_millis();
            let window_ms = last_minutes.checked_mul(60_000).ok_or_else(|| {
                Error::Config("--last-minutes is too large to represent in milliseconds".into())
            })?;
            let start = end.checked_sub(window_ms).ok_or_else(|| {
                Error::Config("--last-minutes is too large to represent a valid time range".into())
            })?;
            Ok(TimeRange {
                start: Some(start),
                end: Some(end),
            })
        }
    }
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
    fn resolves_relative_and_explicit_query_ranges() {
        let relative = resolve_query_range(Some(5), None, None).unwrap();
        assert_eq!(relative.end.unwrap() - relative.start.unwrap(), 300_000);

        let default = resolve_query_range(None, None, None).unwrap();
        assert_eq!(
            default.end.unwrap() - default.start.unwrap(),
            DEFAULT_LAST_MINUTES * 60_000
        );

        let explicit =
            resolve_query_range(None, Some(1_710_000_000_000), Some(1_710_000_060_000)).unwrap();
        assert_eq!(explicit.start, Some(1_710_000_000_000));
        assert_eq!(explicit.end, Some(1_710_000_060_000));
    }

    #[test]
    fn rejects_ambiguous_or_invalid_explicit_ranges() {
        assert!(matches!(
            resolve_query_range(Some(5), Some(1), Some(2)),
            Err(Error::Config(message)) if message.contains("cannot be combined")
        ));
        assert!(matches!(
            resolve_query_range(None, Some(2), Some(1)),
            Err(Error::Config(message)) if message.contains("less than or equal")
        ));
        assert!(matches!(
            resolve_query_range(None, Some(1), None),
            Err(Error::Config(message)) if message.contains("provided together")
        ));
        assert!(matches!(
            resolve_query_range(None, None, Some(1)),
            Err(Error::Config(message)) if message.contains("provided together")
        ));
    }

    #[test]
    fn validates_time_series_query_sample_budget() {
        assert!(matches!(
            validate_query_limit(0),
            Err(Error::Config(message)) if message.contains("greater than zero")
        ));
        assert!(validate_query_limit(1).is_ok());
        assert!(validate_query_limit(MAX_QUERY_SAMPLES).is_ok());
        assert!(matches!(
            validate_query_limit(MAX_QUERY_SAMPLES + 1),
            Err(Error::Config(message)) if message.contains("must not exceed")
        ));
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
