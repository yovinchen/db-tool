use super::Context;
use clap::{Args, Subcommand};
use dbtool_core::{
    error::Error,
    model::{Point, ReadBudget, TimeRange, TimeSeriesReadBudget},
    port::CapabilityOperation,
    service::InputLimiter,
    Result,
};
use std::collections::HashMap;

const DEFAULT_LAST_MINUTES: i64 = 60;
const MAX_QUERY_SERIES: usize = 1_000_000;
const MAX_QUERY_SAMPLES: usize = 1_000_000;

#[derive(Args)]
#[command(
    about = "Read and write Prometheus-compatible time-series data.",
    long_about = "Time-series commands list metric names, run bounded range queries, and write single samples through Prometheus remote write behind --allow-write. Metric catalogs honor both the global --limit item budget and --max-bytes response budget. Query ranges use either --last-minutes (60 by default) or an explicit --start-ms/--end-ms pair in Unix epoch milliseconds."
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
        long_about = "Run a PromQL range query. Select either a relative window with --last-minutes (60 minutes by default) or an inclusive explicit range with both --start-ms and --end-ms in Unix epoch milliseconds. Explicit bounds cannot be combined with --last-minutes. The global --limit must be between 1 and 1,000,000 cumulative samples. --max-series independently bounds returned series and defaults to the global limit. The global --max-bytes bounds the complete portable response."
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
        /// Maximum returned series; defaults to the global sample limit.
        #[arg(long, value_name = "COUNT")]
        max_series: Option<usize>,
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
    let measurements_budget = match &cmd.action {
        TsAction::Measurements => Some(ReadBudget::new(ctx.limit, ctx.max_bytes)?),
        TsAction::Query { .. } | TsAction::Write { .. } => None,
    };
    let query_request = match &cmd.action {
        TsAction::Query {
            last_minutes,
            start_ms,
            end_ms,
            max_series,
            ..
        } => {
            let budget = time_series_query_budget(
                ctx.limit,
                max_series.unwrap_or(ctx.limit),
                ctx.max_bytes,
            )?;
            Some((
                resolve_query_range(*last_minutes, *start_ms, *end_ms)?,
                budget,
            ))
        }
        TsAction::Measurements => None,
        TsAction::Write { .. } => None,
    };
    let write_request = match &cmd.action {
        TsAction::Write {
            measurement,
            value,
            field,
            tags,
            timestamp_ms,
        } => {
            let budget = ctx.input_budget()?;
            let points = vec![Point {
                measurement: measurement.clone(),
                tags: parse_tags(tags)?,
                fields: HashMap::from([(field.clone(), *value)]),
                timestamp: timestamp_ms.unwrap_or_else(now_millis),
            }];
            InputLimiter::new(budget, "CLI time-series write input")?.validate_batch(&points)?;
            Some((points, budget))
        }
        TsAction::Measurements | TsAction::Query { .. } => None,
    };

    let dsn = ctx.resolve_dsn()?;
    let conn = ctx.registry.connect(&dsn).await?;
    let operations = conn.operations();
    let kind = conn.kind().0.clone();
    let (operation, needed) = time_series_operation_for_action(&cmd.action);
    require_time_series_operation(&operations, operation, &kind, needed)?;
    let ts = conn
        .as_timeseries()
        .ok_or_else(|| Error::UnsupportedCapability {
            kind: kind.clone(),
            needed: "TimeSeriesStore",
        })?;
    let start = std::time::Instant::now();

    Ok(match cmd.action {
        TsAction::Measurements => {
            let measurements = ts
                .list_measurements_budgeted(measurements_budget.ok_or_else(|| {
                    Error::Internal("measurement-list budget was not initialized".into())
                })?)
                .await?;
            let truncated = measurements.truncated;
            ctx.render_success(
                &kind,
                measurements.items,
                start.elapsed().as_millis() as u64,
                truncated,
            )
        }
        TsAction::Query {
            query,
            last_minutes: _,
            start_ms: _,
            end_ms: _,
            max_series: _,
        } => {
            let (range, budget) = query_request.ok_or_else(|| {
                Error::Internal("validated time-series query request is missing".into())
            })?;
            let result = ts.query_range_bounded(&query, range, budget).await?;
            let truncated = result.truncated;
            ctx.render_success(&kind, result, start.elapsed().as_millis() as u64, truncated)
        }
        TsAction::Write {
            measurement: _,
            value: _,
            field: _,
            tags: _,
            timestamp_ms: _,
        } => {
            let (points, budget) = write_request.ok_or_else(|| {
                Error::Internal("validated time-series write request is missing".into())
            })?;
            ts.write_points_budgeted(points, budget).await?;
            ctx.render_success(
                &kind,
                serde_json::json!({ "written_points": 1, "written_samples": 1 }),
                start.elapsed().as_millis() as u64,
                false,
            )
        }
    })
}

fn time_series_operation_for_action(action: &TsAction) -> (CapabilityOperation, &'static str) {
    match action {
        TsAction::Measurements => (
            CapabilityOperation::TimeSeriesListMeasurementsBudgeted,
            "TimeSeriesStore.list_measurements_budgeted",
        ),
        TsAction::Query { .. } => (
            CapabilityOperation::TimeSeriesQueryRangeBounded,
            "TimeSeriesStore.query_range_bounded",
        ),
        TsAction::Write { .. } => (
            CapabilityOperation::TimeSeriesWritePointsBudgeted,
            "TimeSeriesStore.write_points_budgeted",
        ),
    }
}

fn ensure_write_allowed(ctx: &Context) -> Result<()> {
    ctx.ensure_write_allowed()
}

fn require_time_series_operation(
    operations: &[CapabilityOperation],
    operation: CapabilityOperation,
    kind: &str,
    needed: &'static str,
) -> Result<()> {
    if operations.contains(&operation) {
        Ok(())
    } else {
        Err(Error::UnsupportedCapability {
            kind: kind.to_owned(),
            needed,
        })
    }
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

fn time_series_query_budget(
    max_samples: usize,
    max_series: usize,
    max_bytes: usize,
) -> Result<TimeSeriesReadBudget> {
    validate_query_limit(max_samples)?;
    if max_series == 0 {
        return Err(Error::Config(
            "time-series query --max-series must be greater than zero".into(),
        ));
    }
    if max_series > MAX_QUERY_SERIES {
        return Err(Error::Config(format!(
            "time-series query --max-series must not exceed {MAX_QUERY_SERIES} series"
        )));
    }
    TimeSeriesReadBudget::new(max_series, max_samples, max_bytes)
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
            max_bytes: dbtool_core::model::DEFAULT_READ_BYTES,
            max_item_bytes: dbtool_core::model::DEFAULT_INPUT_ITEM_BYTES,
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
    fn coarse_time_series_capability_does_not_authorize_budgeted_catalog_reads() {
        assert!(matches!(
            require_time_series_operation(
                CapabilityOperation::TIME_SERIES,
                CapabilityOperation::TimeSeriesListMeasurementsBudgeted,
                "legacy-time-series",
                "TimeSeriesStore.list_measurements_budgeted",
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-time-series"
                    && needed == "TimeSeriesStore.list_measurements_budgeted"
        ));
        assert!(matches!(
            require_time_series_operation(
                CapabilityOperation::TIME_SERIES,
                CapabilityOperation::TimeSeriesQueryRangeBounded,
                "legacy-time-series",
                "TimeSeriesStore.query_range_bounded",
            ),
            Err(Error::UnsupportedCapability { kind, needed })
                if kind == "legacy-time-series"
                    && needed == "TimeSeriesStore.query_range_bounded"
        ));

        let query = TsAction::Query {
            query: "up".to_owned(),
            last_minutes: None,
            start_ms: None,
            end_ms: None,
            max_series: Some(3),
        };
        assert_eq!(
            time_series_operation_for_action(&query),
            (
                CapabilityOperation::TimeSeriesQueryRangeBounded,
                "TimeSeriesStore.query_range_bounded"
            )
        );
    }

    #[test]
    fn time_series_write_requires_the_exact_input_budgeted_operation() {
        let action = TsAction::Write {
            measurement: "requests_total".to_owned(),
            value: 1.0,
            field: "value".to_owned(),
            tags: Vec::new(),
            timestamp_ms: Some(1),
        };
        assert_eq!(
            time_series_operation_for_action(&action),
            (
                CapabilityOperation::TimeSeriesWritePointsBudgeted,
                "TimeSeriesStore.write_points_budgeted"
            )
        );
        assert!(matches!(
            require_time_series_operation(
                &[CapabilityOperation::TimeSeriesWritePoints],
                CapabilityOperation::TimeSeriesWritePointsBudgeted,
                "legacy-time-series",
                "TimeSeriesStore.write_points_budgeted",
            ),
            Err(Error::UnsupportedCapability { needed, .. })
                if needed == "TimeSeriesStore.write_points_budgeted"
        ));
    }

    #[tokio::test]
    async fn time_series_write_budget_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context(true);
        ctx.max_item_bytes = 1;
        let error = run(
            &ctx,
            TsCmd {
                action: TsAction::Write {
                    measurement: "requests_total".to_owned(),
                    value: 1.0,
                    field: "value".to_owned(),
                    tags: Vec::new(),
                    timestamp_ms: Some(1),
                },
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::InputBudgetExceeded { .. }));
    }

    #[tokio::test]
    async fn measurement_limit_is_rejected_before_dsn_resolution() {
        let mut ctx = test_context(false);
        ctx.limit = usize::MAX;
        let error = run(
            &ctx,
            TsCmd {
                action: TsAction::Measurements,
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::Config(message) if message.contains("too large")));

        let mut ctx = test_context(false);
        ctx.max_bytes = 0;
        let error = run(
            &ctx,
            TsCmd {
                action: TsAction::Measurements,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("byte budget")));
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

        let budget = time_series_query_budget(5, 2, 4096).unwrap();
        assert_eq!(budget.max_samples, 5);
        assert_eq!(budget.max_series, 2);
        assert_eq!(budget.max_bytes, 4096);
        assert!(matches!(
            time_series_query_budget(1, 0, 4096),
            Err(Error::Config(message)) if message.contains("--max-series")
        ));
        assert!(matches!(
            time_series_query_budget(1, MAX_QUERY_SERIES + 1, 4096),
            Err(Error::Config(message)) if message.contains("must not exceed")
        ));
    }
}
