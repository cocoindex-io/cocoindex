//! Token-bucket rate limiter.
//!
//! Mirrors Python's `cocoindex.resources.rate_limit.RateLimiter`. Throttles
//! outbound work — e.g. API calls in a source / target connector — to stay
//! within an external service's rate limit. Backed by the `governor`-based
//! limiter in `cocoindex_utils`.

use std::sync::Arc;
use std::time::Duration;

use cocoindex_utils::ratelimit::{RateLimit, RateLimiter as CoreRateLimiter};

use crate::error::{Error, Result};

/// A token-bucket rate limiter.
///
/// [`acquire`](RateLimiter::acquire) asynchronously waits until `n` tokens are
/// available; concurrent callers are served in FIFO order. Cheap to clone — all
/// clones share one underlying bucket.
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<CoreRateLimiter>,
}

impl RateLimiter {
    /// Create a limiter allowing `max_rows_per_second`, with burst capacity sized
    /// to `burst_window` (how much unused quota may accumulate). Pass a
    /// `burst_window` of one second to match the Python default.
    pub fn new(max_rows_per_second: f64, burst_window: Duration) -> Result<Self> {
        let limiter = CoreRateLimiter::new(&RateLimit {
            max_rows_per_second,
            burst_window,
        })
        .map_err(|e| Error::engine(format!("{e}")))?;
        Ok(Self {
            inner: Arc::new(limiter),
        })
    }

    /// Wait until `n` tokens are available.
    pub async fn acquire(&self, n: u32) -> Result<()> {
        self.inner.until_ready_n(n).await?;
        Ok(())
    }
}
