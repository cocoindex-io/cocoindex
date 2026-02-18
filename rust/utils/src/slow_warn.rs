use std::future::Future;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{Level, warn};

/// Runs an async operation and logs a warning if it takes longer than the threshold.
/// The operation continues running and its result is returned normally.
///
/// The message closure is only called if:
/// 1. The operation exceeds the threshold, AND
/// 2. Warn-level logging is enabled
pub async fn warn_if_slow<F, T, M>(msg_fn: &M, threshold: Duration, future: F) -> T
where
    F: Future<Output = T>,
    M: Fn() -> String,
{
    if !tracing::enabled!(Level::WARN) {
        return future.await;
    }

    tokio::pin!(future);

    tokio::select! {
        biased;
        result = &mut future => result,
        _ = sleep(threshold) => {
            let start = Instant::now();
            let msg = msg_fn();
            warn!("Taking longer than {}s: {msg}", threshold.as_secs_f32());
            let result = future.await;
            warn!("Finished after {}s: {msg}", start.elapsed().as_secs_f32() + threshold.as_secs_f32());
            result
        }
    }
}
