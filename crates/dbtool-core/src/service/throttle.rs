use crate::{Error, Result};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, time::sleep};

#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    pub max_concurrency: usize,
    pub acquire_timeout: Duration,
    pub request_timeout: Duration,
    pub overall_deadline: Option<Duration>,
    pub max_retries: u32,
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 8,
            acquire_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(10),
            overall_deadline: Some(Duration::from_secs(15)),
            max_retries: 3,
        }
    }
}

pub struct FlowControl {
    sem: Arc<Semaphore>,
    config: ThrottleConfig,
}

impl FlowControl {
    pub fn new(config: ThrottleConfig) -> Self {
        let sem = Arc::new(Semaphore::new(config.max_concurrency));
        Self { sem, config }
    }

    pub async fn run<F, Fut, T>(&self, mk: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let deadline_at = Instant::now()
            + self
                .config
                .overall_deadline
                .unwrap_or(self.config.request_timeout);
        let mut attempt = 0u32;

        loop {
            match self.run_once(deadline_at, mk()).await {
                Ok(v) => return Ok(v),
                Err(e) if e.is_retryable() && attempt < self.config.max_retries => {
                    attempt += 1;
                    let rem = remaining(deadline_at)?;
                    // Exponential backoff with jitter, capped by remaining budget.
                    let backoff = Duration::from_millis(100 * (1u64 << attempt.min(6)));
                    let nap = backoff.min(rem);
                    if nap.is_zero() {
                        return Err(Error::DeadlineExceeded);
                    }
                    sleep(nap).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn run_once<F, T>(&self, deadline_at: Instant, op: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        // Acquire concurrency permit with timeout to avoid infinite blocking.
        let wait = self.config.acquire_timeout.min(remaining(deadline_at)?);
        let _permit = tokio::time::timeout(wait, self.sem.clone().acquire_owned())
            .await
            .map_err(|_| Error::Overloaded)?
            .map_err(|_| Error::Internal("semaphore closed".into()))?;

        // Execute op under a budget = min(remaining, single request timeout).
        let budget = remaining(deadline_at)?.min(self.config.request_timeout);
        tokio::time::timeout(budget, op)
            .await
            .map_err(|_| Error::Timeout)?
    }
}

#[inline]
fn remaining(deadline_at: Instant) -> Result<Duration> {
    let now = Instant::now();
    if now >= deadline_at {
        Err(Error::DeadlineExceeded)
    } else {
        Ok(deadline_at - now)
    }
}
