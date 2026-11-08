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

/// The longest time span the bucket may cover, the replenish period times the
/// burst size. `governor` keeps times in `u64` nanoseconds (about 584 years)
/// and panics or overflows beyond that; half of it leaves room to add the span
/// to a clock reading.
const MAX_BUCKET_SPAN_NANOS: u128 = (u64::MAX / 2) as u128;

impl RateLimiter {
    pub fn new(rate_limit: &RateLimit) -> anyhow::Result<Self> {
        let rate = rate_limit.max_rows_per_second;
        // Written to reject NaN too, for which every comparison is false.
        if rate.is_nan() || rate <= 0.0 {
            api_bail!("`max_rows_per_second` must be positive, got {rate}");
        }

        // Prefer building a quota based on an exact period derived from the desired per-second rate.
        // This supports fractional rates as well (e.g., 2.5 ops/s => period of 0.4s).
        let period = match std::time::Duration::try_from_secs_f64(1.0 / rate) {
            Ok(period) if period.as_nanos() <= MAX_BUCKET_SPAN_NANOS => period,
            _ => api_bail!(
                "`max_rows_per_second` {rate} is too small: the limiter needs at least one row per {} years",
                MAX_BUCKET_SPAN_NANOS / 1_000_000_000 / 3600 / 24 / 365
            ),
        };

        let (quota, jitter) = if let Some(q) = Quota::with_period(period) {
            (q, None)
        } else {
            // Fallback for extremely high rates where period cannot be represented.
            let tokens_per_second = rate.floor().max(1.0).min(u32::MAX as f64) as u32;
            (
                Quota::per_second(NonZeroU32::new(tokens_per_second).unwrap()),
                Some(Jitter::up_to(std::time::Duration::from_secs(1))),
            )
        };

        // Configure burst capacity based on the burst window, capped so that
        // the bucket's span stays within what `governor` can represent.
        let quota = if rate_limit.burst_window.as_nanos() > 0 {
            let replenish_nanos = quota.replenish_interval().as_nanos().max(1);
            let max_burst_tokens = (MAX_BUCKET_SPAN_NANOS / replenish_nanos).min(u32::MAX as u128);
            let burst_tokens = (rate * rate_limit.burst_window.as_secs_f64())
                .floor()
                .max(1.0)
                .min(max_burst_tokens as f64) as u32;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn limiter(max_rows_per_second: f64, burst_window: Duration) -> anyhow::Result<RateLimiter> {
        RateLimiter::new(&RateLimit {
            max_rows_per_second,
            burst_window,
        })
    }

    #[test]
    fn rejects_rates_that_are_not_positive() {
        for rate in [0.0, -1.0, f64::NEG_INFINITY, f64::NAN] {
            let err = limiter(rate, Duration::from_secs(1)).err();
            assert!(err.is_some(), "rate {rate} should be rejected");
        }
    }

    #[test]
    fn rejects_a_rate_too_small_to_represent() {
        for rate in [1e-300, f64::MIN_POSITIVE, 1e-12] {
            let err = limiter(rate, Duration::from_secs(1))
                .err()
                .unwrap_or_else(|| panic!("rate {rate} should be rejected"));
            assert!(err.to_string().contains("too small"), "{err}");
        }
    }

    #[test]
    fn accepts_extreme_rates_and_burst_windows() {
        let windows = [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_secs(1),
            Duration::from_secs(u64::MAX / 4),
            Duration::MAX,
        ];
        for rate in [
            1e-9,
            0.01,
            0.5,
            1.0,
            1e6,
            1e9,
            1e10,
            1e30,
            f64::MAX,
            f64::INFINITY,
        ] {
            for window in windows {
                limiter(rate, window)
                    .unwrap_or_else(|err| panic!("rate {rate}, window {window:?}: {err}"));
            }
        }
    }

    #[tokio::test]
    async fn a_huge_rate_never_waits() {
        let limiter = limiter(1e12, Duration::from_secs(1)).unwrap();
        tokio::time::timeout(Duration::from_secs(5), limiter.until_ready_n(100_000))
            .await
            .expect("a huge rate should not throttle")
            .unwrap();
    }

    #[tokio::test]
    async fn requesting_more_than_the_burst_waits_instead_of_failing() {
        // Burst of one token, 1000 tokens per second: 50 tokens take ~49 ms.
        let limiter = limiter(1000.0, Duration::ZERO).unwrap();
        let start = std::time::Instant::now();
        limiter.until_ready_n(50).await.unwrap();
        assert!(start.elapsed() >= Duration::from_millis(40));
        limiter.until_ready_n(0).await.unwrap();
    }

    #[tokio::test]
    async fn contended_callers_share_the_rate() {
        // 200 rows/s with a burst of 20: 20 callers x 10 tokens = 200 tokens,
        // of which 180 wait for replenishment, so at least ~0.9 s overall.
        let limiter = std::sync::Arc::new(limiter(200.0, Duration::from_millis(100)).unwrap());
        let start = std::time::Instant::now();
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let limiter = limiter.clone();
                tokio::spawn(async move { limiter.until_ready_n(10).await })
            })
            .collect();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(850), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    }
}
