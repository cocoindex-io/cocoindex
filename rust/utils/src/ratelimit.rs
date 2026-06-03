use governor::{Jitter, Quota};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use tokio::sync::Semaphore;

use crate::api_bail;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RateLimit {
    pub max_rows_per_second: f64,
    pub burst_window: std::time::Duration,
}

pub struct RateLimiter {
    limiter: governor::RateLimiter<
        governor::state::NotKeyed,
        governor::state::InMemoryState,
        governor::clock::DefaultClock,
    >,
    jitter: Option<governor::Jitter>,
    semaphore: Semaphore,
}

impl RateLimiter {
    pub fn new(rate_limit: &RateLimit) -> anyhow::Result<Self> {
        if rate_limit.max_rows_per_second <= 0.0 {
            api_bail!("`max_rows_per_second` must be positive");
        }

        // Prefer building a quota based on an exact period derived from the desired per-second rate.
        // This supports fractional rates as well (e.g., 2.5 ops/s => period of 0.4s).
        let period_secs = 1.0 / rate_limit.max_rows_per_second;
        let period = std::time::Duration::from_secs_f64(period_secs);

        let (quota, jitter) = if let Some(q) = Quota::with_period(period) {
            (q, None)
        } else {
            // Fallback for extremely high rates where period cannot be represented.
            let tokens_per_second = rate_limit
                .max_rows_per_second
                .floor()
                .max(1.0)
                .min(u32::MAX as f64) as u32;
            (
                Quota::per_second(NonZeroU32::new(tokens_per_second).unwrap()),
                Some(Jitter::up_to(std::time::Duration::from_secs(1))),
            )
        };

        // Configure burst capacity based on the burst window.
        let quota = if rate_limit.burst_window.as_nanos() > 0 {
            let burst_tokens = (rate_limit.max_rows_per_second
                * rate_limit.burst_window.as_secs_f64())
            .floor()
            .max(1.0)
            .min(u32::MAX as f64) as u32;
            if let Some(nz) = NonZeroU32::new(burst_tokens) {
                quota.allow_burst(nz)
            } else {
                quota
            }
        } else {
            quota
        };

        Ok(Self {
            limiter: governor::RateLimiter::direct(quota),
            jitter,
            semaphore: Semaphore::new(1),
        })
    }
}

impl RateLimiter {
    /// Acquire `n` tokens, waiting as needed.
    ///
    /// Tokens are drawn one at a time, so this never fails on the quota's
    /// burst capacity the way `governor`'s `until_n_ready` does — a caller
    /// may request more tokens than the bucket can hold at once. The
    /// internal semaphore is held for the whole acquisition, so concurrent
    /// callers are served in FIFO order rather than racing token-by-token.
    pub async fn until_ready_n(&self, n: u32) -> Result<(), crate::error::Error> {
        let _permit = self.semaphore.acquire().await?;
        for _ in 0..n {
            if let Some(jitter) = self.jitter {
                self.limiter.until_ready_with_jitter(jitter).await;
            } else {
                self.limiter.until_ready().await;
            }
        }
        Ok(())
    }
}
