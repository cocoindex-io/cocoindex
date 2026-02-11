//! Memoization: skip re-execution when inputs haven't changed.

use std::future::Future;

use cocoindex_core::engine::context::FnCallContext;
use cocoindex_core::engine::function::{reserve_memoization, FnCallMemoGuard};
use cocoindex_utils::fingerprint::Fingerprint;
use serde::{Deserialize, Serialize};

use crate::app::Ctx;
use crate::error::{Error, Result};
use crate::internal::profile::Value;

/// Execute `f` with memoization. If `key` hasn't changed since the last run,
/// returns the cached result from LMDB without executing `f`.
///
/// The key is serialized and fingerprinted (Blake2b). The result is
/// serialized to MessagePack for storage. Both must implement Serde.
///
/// When called outside an `App::update()` pipeline (no LMDB context),
/// falls back to direct execution without caching.
///
/// # Examples
/// ```ignore
/// let html = memo::cached(&ctx, &file, || async {
///     Ok(render_markdown(&file.read_text()?))
/// }).await?;
/// ```
pub async fn cached<K, T, F, Fut>(ctx: &Ctx, key: &K, f: F) -> Result<T>
where
    K: Serialize,
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let fp = Fingerprint::from(key)
        .map_err(|e| Error::engine(format!("fingerprint error: {e}")))?;

    // If we have a pipeline context, use LMDB-backed memoization.
    let Some(comp_ctx) = &ctx.comp_ctx else {
        // No pipeline context — execute directly (standalone mode).
        return f().await;
    };

    let guard = reserve_memoization(comp_ctx, fp)
        .await
        .map_err(|e| Error::engine(format!("reserve_memoization: {e}")))?;

    match guard {
        FnCallMemoGuard::Ready(ready) => {
            // Cache hit — deserialize the stored result.
            if let Some(memo) = &*ready {
                let value: T = memo.ret.deserialize()?;
                return Ok(value);
            }
            // Memoization was disabled for this entry (e.g., has side effects).
            // Fall through to execute.
            f().await
        }
        FnCallMemoGuard::Pending(pending) => {
            // Cache miss — we are the resolver. Execute and commit.
            let fn_ctx = FnCallContext::default();
            let result = f().await?;
            let value = Value::from_serializable(&result)?;
            pending.resolve(&fn_ctx, || value);
            comp_ctx.join_fn_call(&fn_ctx);
            Ok(result)
        }
    }
}
