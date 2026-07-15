use crate::{Error, Result};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, time::sleep};

#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    pub max_concurrency: usize,
    pub rate: Option<Rate>,
    pub acquire_timeout: Duration,
    pub request_timeout: Duration,
    pub overall_deadline: Option<Duration>,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rate {
    PerSecond(NonZeroU32),
    PerMinute(NonZeroU32),
}

impl Rate {
    pub fn per_second(count: u32) -> Option<Self> {
        NonZeroU32::new(count).map(Self::PerSecond)
    }

    pub fn per_minute(count: u32) -> Option<Self> {
        NonZeroU32::new(count).map(Self::PerMinute)
    }

    fn quota(self) -> Quota {
        match self {
            Rate::PerSecond(count) => Quota::per_second(count),
            Rate::PerMinute(count) => Quota::per_minute(count),
        }
    }
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 8,
            rate: None,
            acquire_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(10),
            overall_deadline: Some(Duration::from_secs(15)),
            max_retries: 3,
        }
    }
}

pub struct FlowControl {
    sem: Arc<Semaphore>,
    rate: Option<Arc<DefaultDirectRateLimiter>>,
    config: ThrottleConfig,
}

impl FlowControl {
    pub fn new(config: ThrottleConfig) -> Self {
        let sem = Arc::new(Semaphore::new(config.max_concurrency.max(1)));
        let rate = config
            .rate
            .map(|rate| Arc::new(RateLimiter::direct(rate.quota())));
        Self { sem, rate, config }
    }

    pub async fn run<F, Fut, T>(&self, mk: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.validate_durations()?;
        let deadline_at = deadline_from_now(self.config.overall_deadline)?;
        let mut attempt = 0u32;

        loop {
            match self.run_once(deadline_at, mk()).await {
                Ok(v) => return Ok(v),
                Err(e) if e.is_retryable() && attempt < self.config.max_retries => {
                    attempt += 1;
                    // Exponential backoff with jitter, capped by remaining budget.
                    let backoff = Duration::from_millis(100 * (1u64 << attempt.min(6)));
                    let nap = remaining(deadline_at)?.map_or(backoff, |rem| backoff.min(rem));
                    if nap.is_zero() {
                        return Err(Error::DeadlineExceeded);
                    }
                    sleep(nap).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Run one operation under the configured rate/concurrency/time budgets.
    ///
    /// This intentionally does not retry, so CLI callers can protect one-shot
    /// operations without replaying writes.
    pub async fn run_single<F, T>(&self, op: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        self.validate_durations()?;
        let deadline_at = deadline_from_now(self.config.overall_deadline)?;
        self.run_once(deadline_at, op).await
    }

    async fn run_once<F, T>(&self, deadline_at: Option<Instant>, op: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        if let Some(rate) = &self.rate {
            let wait = budget_with_deadline(self.config.acquire_timeout, deadline_at)?;
            tokio::time::timeout(wait, rate.until_ready())
                .await
                .map_err(|_| Error::RateLimited)?;
        }

        // Acquire concurrency permit with timeout to avoid infinite blocking.
        let wait = budget_with_deadline(self.config.acquire_timeout, deadline_at)?;
        let _permit = tokio::time::timeout(wait, self.sem.clone().acquire_owned())
            .await
            .map_err(|_| Error::Overloaded)?
            .map_err(|_| Error::Internal("semaphore closed".into()))?;

        // Execute op under a budget = min(remaining, single request timeout).
        let budget = budget_with_deadline(self.config.request_timeout, deadline_at)?;
        tokio::time::timeout(budget, op)
            .await
            .map_err(|_| Error::Timeout)?
    }

    fn validate_durations(&self) -> Result<()> {
        validate_duration(self.config.acquire_timeout, "acquire_timeout")?;
        validate_duration(self.config.request_timeout, "request_timeout")?;
        if let Some(deadline) = self.config.overall_deadline {
            validate_duration(deadline, "overall_deadline")?;
        }
        Ok(())
    }
}

#[inline]
fn deadline_from_now(duration: Option<Duration>) -> Result<Option<Instant>> {
    duration
        .map(|duration| {
            Instant::now().checked_add(duration).ok_or_else(|| {
                Error::Config("overall_deadline exceeds the platform timer range".into())
            })
        })
        .transpose()
}

#[inline]
fn remaining(deadline_at: Option<Instant>) -> Result<Option<Duration>> {
    let Some(deadline_at) = deadline_at else {
        return Ok(None);
    };
    let now = Instant::now();
    if now >= deadline_at {
        Err(Error::DeadlineExceeded)
    } else {
        Ok(Some(deadline_at - now))
    }
}

#[inline]
fn budget_with_deadline(budget: Duration, deadline_at: Option<Instant>) -> Result<Duration> {
    Ok(remaining(deadline_at)?.map_or(budget, |remaining| budget.min(remaining)))
}

fn validate_duration(duration: Duration, field: &str) -> Result<()> {
    if duration.is_zero() {
        return Err(Error::Config(format!("{field} must be greater than 0")));
    }
    if Instant::now().checked_add(duration).is_none() {
        return Err(Error::Config(format!(
            "{field} exceeds the platform timer range"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::time::sleep;

    fn test_config() -> ThrottleConfig {
        ThrottleConfig {
            acquire_timeout: Duration::from_millis(25),
            request_timeout: Duration::from_millis(50),
            overall_deadline: Some(Duration::from_millis(200)),
            max_retries: 0,
            ..ThrottleConfig::default()
        }
    }

    #[tokio::test]
    async fn returns_successful_operation_value() {
        let flow = FlowControl::new(test_config());

        let result = flow.run(|| async { Ok::<_, Error>(42) }).await.unwrap();

        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn request_timeout_aborts_slow_operations() {
        let flow = FlowControl::new(ThrottleConfig {
            request_timeout: Duration::from_millis(10),
            overall_deadline: Some(Duration::from_millis(100)),
            max_retries: 0,
            ..test_config()
        });

        let err = flow
            .run(|| async {
                sleep(Duration::from_millis(100)).await;
                Ok::<_, Error>(())
            })
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Timeout));
    }

    #[tokio::test]
    async fn concurrency_acquire_timeout_returns_overloaded() {
        let flow = Arc::new(FlowControl::new(ThrottleConfig {
            max_concurrency: 1,
            acquire_timeout: Duration::from_millis(15),
            request_timeout: Duration::from_millis(200),
            overall_deadline: Some(Duration::from_millis(250)),
            max_retries: 0,
            ..ThrottleConfig::default()
        }));

        let holder = {
            let flow = Arc::clone(&flow);
            tokio::spawn(async move {
                flow.run(|| async {
                    sleep(Duration::from_millis(75)).await;
                    Ok::<_, Error>(())
                })
                .await
            })
        };
        sleep(Duration::from_millis(5)).await;

        let err = flow.run(|| async { Ok::<_, Error>(()) }).await.unwrap_err();
        let holder_result = holder.await.expect("holder task should not panic");

        assert!(matches!(err, Error::Overloaded));
        assert!(holder_result.is_ok());
    }

    #[tokio::test]
    async fn rate_limit_wait_is_bounded_by_acquire_timeout() {
        let flow = FlowControl::new(ThrottleConfig {
            rate: Some(Rate::per_second(1).unwrap()),
            acquire_timeout: Duration::from_millis(15),
            request_timeout: Duration::from_millis(50),
            overall_deadline: Some(Duration::from_millis(100)),
            max_retries: 0,
            ..ThrottleConfig::default()
        });

        flow.run(|| async { Ok::<_, Error>(()) }).await.unwrap();
        let err = flow.run(|| async { Ok::<_, Error>(()) }).await.unwrap_err();

        assert!(matches!(err, Error::RateLimited));
    }

    #[tokio::test]
    async fn retry_budget_uses_one_overall_deadline() {
        let flow = FlowControl::new(ThrottleConfig {
            acquire_timeout: Duration::from_millis(5),
            request_timeout: Duration::from_millis(100),
            overall_deadline: Some(Duration::from_millis(30)),
            max_retries: 10,
            ..ThrottleConfig::default()
        });
        let attempts = Arc::new(AtomicUsize::new(0));

        let err = flow
            .run(|| {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(Error::Connection("transient".to_owned()))
                }
            })
            .await
            .unwrap_err();

        assert!(matches!(err, Error::DeadlineExceeded));
        assert!(
            attempts.load(Ordering::SeqCst) >= 1,
            "at least one operation attempt should run"
        );
    }

    #[tokio::test]
    async fn disabled_overall_deadline_gives_admission_and_request_separate_budgets() {
        let flow = Arc::new(FlowControl::new(ThrottleConfig {
            max_concurrency: 1,
            acquire_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_millis(50),
            overall_deadline: None,
            max_retries: 0,
            ..ThrottleConfig::default()
        }));

        let holder = {
            let flow = Arc::clone(&flow);
            tokio::spawn(async move {
                flow.run_single(async {
                    sleep(Duration::from_millis(35)).await;
                    Ok::<_, Error>(())
                })
                .await
            })
        };
        sleep(Duration::from_millis(5)).await;

        let result = flow
            .run_single(async {
                sleep(Duration::from_millis(40)).await;
                Ok::<_, Error>("completed")
            })
            .await;

        assert_eq!(result.unwrap(), "completed");
        assert!(holder.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn programmatic_oversized_duration_returns_error_instead_of_panicking() {
        let flow = FlowControl::new(ThrottleConfig {
            request_timeout: Duration::from_secs(u64::MAX),
            overall_deadline: None,
            ..test_config()
        });

        let error = flow
            .run_single(async { Ok::<_, Error>(()) })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(message) if message.contains("timer range")));
    }
}
