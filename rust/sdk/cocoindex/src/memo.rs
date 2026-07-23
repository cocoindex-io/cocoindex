//! Memoization: skip re-execution when inputs haven't changed.
//!
//! Prefer `#[cocoindex::function(memo)]` for memoizing a whole pipeline
//! function. The attribute builds the argument key, tracks the function body's
//! logic, and passes an owned, memo-scoped [`Ctx`] into the body. Context
//! resources can therefore be read normally with [`Ctx::get_or_err`] or
//! [`Ctx::get_key`]. Use `memo_key(parameter = skip)` for an `Any + Clone`
//! parameter that should not participate in the key; it does not need to
//! implement Serde.
//!
//! [`Ctx::memo`] and [`cached`] are intended for block-level memoization inside
//! a function. Their closure body is not logic-tracked. When a manual memo block
//! must invalidate after its enclosing `#[cocoindex::function]` changes, include
//! that function's generated `__COCO_FN_HASH_<NAME>` constant in the manual key.

use std::any::Any;
use std::future::Future;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex_core::engine::context::{FnCallContext, MemoStatesPayload};
use cocoindex_core::engine::function::reserve_memoization;
use cocoindex_utils::fingerprint::{Fingerprint, Fingerprinter};
use serde::{Deserialize, Serialize};

use crate::ctx::{Ctx, fn_call_guard};
use crate::error::{Error, Result};
use crate::profile::Value;
use crate::resources::file::FileLike;

#[derive(Clone)]
pub struct MemoStateValue(Value);

pub struct MemoStateDecision {
    state: MemoStateValue,
    memo_valid: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FileMemoState {
    modified_nanos: u128,
    content_fingerprint: Fingerprint,
}

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
/// let html = memo::cached(&ctx, &file, |_ctx| async {
///     Ok(render_markdown(&file.read_text()?))
/// }).await?;
/// ```
pub async fn cached<K, T, F, Fut>(ctx: &Ctx, key: &K, f: F) -> Result<T>
where
    K: Serialize,
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    F: FnOnce(Ctx) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let fp = key_fingerprint_result(key)?;
    cached_by_fingerprint(ctx, fp, true, f).await
}

/// Fast path for generated macros that have already built the memo fingerprint.
///
/// `f` receives a `Ctx` scoped to this memo call; use it for `get_key` so
/// change-detection dependencies attach to this memo entry.
///
/// `propagate_children_fn_logic` is the function's `logic_tracking` mode: `true`
/// for `"full"` (transitively-called functions' logic changes invalidate this
/// memo entry), `false` for `"self"`/`"none"` (only this function's own body).
#[doc(hidden)]
pub async fn cached_by_fingerprint<T, F, Fut>(
    ctx: &Ctx,
    fp: Fingerprint,
    propagate_children_fn_logic: bool,
    f: F,
) -> Result<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    F: FnOnce(Ctx) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    ctx.check_cancellation()?;
    // If we have a pipeline context, use LMDB-backed memoization.
    let Some(comp_ctx) = &ctx.comp_ctx else {
        // No pipeline context — execute directly (standalone mode).
        let result = f(ctx.clone()).await?;
        ctx.check_cancellation()?;
        return Ok(result);
    };

    let guard = reserve_memoization(comp_ctx, fp)
        .await
        .map_err(|e| Error::engine(format!("reserve_memoization: {e}")))?;

    // Cache hit — deserialize and return the stored value.
    if let Some(cached) = guard.cached() {
        let value: T = cached.ret.deserialize()?;
        ctx.check_cancellation()?;
        return Ok(value);
    }

    // Cache miss (or memo disabled) — we are the resolver. Execute and commit.
    let fn_ctx = Arc::new(FnCallContext::new(propagate_children_fn_logic));
    let _guard = fn_call_guard(comp_ctx, fn_ctx.clone());
    let result = f(ctx.with_fn_ctx(fn_ctx.clone())).await?;
    ctx.check_cancellation()?;
    let value = Value::from_serializable(&result)
        .map_err(|e| Error::engine(format!("failed to serialize memo result: {e}")))?;
    let memo_states = MemoStatesPayload {
        positional: Vec::new(),
        by_context_fp: fn_ctx.collect_context_initial_states(comp_ctx.app_ctx().env()),
    };
    guard
        .resolve(&fn_ctx, || value, memo_states)
        .map_err(|e| Error::engine(format!("{e}")))?;
    Ok(result)
}

#[doc(hidden)]
pub async fn cached_by_fingerprint_with_state<T, F, Fut, S, SFut>(
    ctx: &Ctx,
    fp: Fingerprint,
    propagate_children_fn_logic: bool,
    state_fn: S,
    f: F,
) -> Result<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    F: FnOnce(Ctx) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
    S: FnOnce(Option<Vec<MemoStateValue>>) -> SFut,
    SFut: Future<Output = Result<Vec<MemoStateDecision>>> + Send,
{
    ctx.check_cancellation()?;
    let Some(comp_ctx) = &ctx.comp_ctx else {
        let result = f(ctx.clone()).await?;
        ctx.check_cancellation()?;
        return Ok(result);
    };

    let mut guard = reserve_memoization(comp_ctx, fp)
        .await
        .map_err(|e| Error::engine(format!("reserve_memoization: {e}")))?;

    let cached_states = guard.cached().map(|cached| {
        cached
            .memo_states
            .iter()
            .cloned()
            .map(MemoStateValue)
            .collect::<Vec<_>>()
    });
    let state_decisions = state_fn(cached_states).await?;
    let memo_states_for_resolve = state_decisions
        .iter()
        .map(|decision| decision.state.0.clone())
        .collect::<Vec<_>>();

    if let Some(cached) = guard.cached()
        && state_decisions.iter().all(|decision| decision.memo_valid)
    {
        let states_changed = state_decisions
            .iter()
            .map(|decision| &decision.state.0.0)
            .ne(cached.memo_states.iter().map(|value| &value.0));
        let cached_context_states = cached.context_memo_states.to_vec();
        let value: T = cached.ret.deserialize()?;
        ctx.check_cancellation()?;
        if states_changed {
            guard.update_memo_states(MemoStatesPayload {
                positional: memo_states_for_resolve,
                by_context_fp: cached_context_states,
            });
        }
        return Ok(value);
    }

    let fn_ctx = Arc::new(FnCallContext::new(propagate_children_fn_logic));
    let _guard = fn_call_guard(comp_ctx, fn_ctx.clone());
    let result = f(ctx.with_fn_ctx(fn_ctx.clone())).await?;
    ctx.check_cancellation()?;
    let value = Value::from_serializable(&result)
        .map_err(|e| Error::engine(format!("failed to serialize memo result: {e}")))?;
    let memo_states = MemoStatesPayload {
        positional: memo_states_for_resolve,
        by_context_fp: fn_ctx.collect_context_initial_states(comp_ctx.app_ctx().env()),
    };
    guard
        .resolve(&fn_ctx, || value, memo_states)
        .map_err(|e| Error::engine(format!("{e}")))?;
    Ok(result)
}

/// Internal helper for stable memo keys in generated macros.
///
/// Returns an error instead of panicking so callers can fail fast with a typed
/// error when serialization is not supported.
#[doc(hidden)]
pub fn key_bytes_result<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(value).map_err(Error::from)
}

#[doc(hidden)]
pub fn key_fingerprint_result<T: Serialize + ?Sized>(value: &T) -> Result<Fingerprint> {
    Fingerprint::from(value).map_err(|e| Error::engine(format!("fingerprint error: {e}")))
}

#[doc(hidden)]
pub fn new_key_fingerprinter() -> Fingerprinter {
    Fingerprinter::default()
}

#[doc(hidden)]
pub fn write_key_fingerprint_part<T: Serialize + ?Sized>(
    fingerprinter: &mut Fingerprinter,
    value: &T,
) -> Result<()> {
    fingerprinter
        .write(value)
        .map_err(|e| Error::engine(format!("fingerprint error: {e}")))
}

#[doc(hidden)]
pub fn write_key_fingerprint_part_for_arg<T: Any + Serialize>(
    fingerprinter: &mut Fingerprinter,
    value: &T,
) -> Result<()> {
    if let Some(file) = as_file_like(value) {
        let file_path = file.file_path();
        return write_key_fingerprint_part(fingerprinter, &file_path.memo_key());
    }
    write_key_fingerprint_part(fingerprinter, value)
}

#[doc(hidden)]
pub fn finish_key_fingerprinter(fingerprinter: Fingerprinter) -> Fingerprint {
    fingerprinter.into_fingerprint()
}

#[doc(hidden)]
pub async fn collect_memo_arg_state<T: Any>(
    value: &T,
    prev: Option<&MemoStateValue>,
) -> Result<Option<MemoStateDecision>> {
    if let Some(file) = as_file_like(value) {
        return file_memo_state(file, prev).await.map(Some);
    }
    Ok(None)
}

fn as_file_like(value: &dyn Any) -> Option<&dyn FileLike> {
    if let Some(file) = value.downcast_ref::<crate::resources::fs::FileEntry>() {
        return Some(file);
    }
    #[cfg(feature = "amazon_s3")]
    if let Some(file) = value.downcast_ref::<crate::connectors::amazon_s3::S3File>() {
        return Some(file);
    }
    #[cfg(feature = "google_drive")]
    if let Some(file) = value.downcast_ref::<crate::connectors::gdrive::DriveFile>() {
        return Some(file);
    }
    #[cfg(feature = "oci_object_storage")]
    if let Some(file) = value.downcast_ref::<crate::connectors::oci_object_storage::OciFile>() {
        return Some(file);
    }
    None
}

async fn file_memo_state(
    file: &dyn FileLike,
    prev: Option<&MemoStateValue>,
) -> Result<MemoStateDecision> {
    let metadata = file.metadata().await?;
    let modified_nanos = system_time_nanos(metadata.modified);
    let prev_state = prev.and_then(|value| value.0.deserialize::<FileMemoState>().ok());

    if let Some(ref prev_state) = prev_state
        && prev_state.modified_nanos == modified_nanos
    {
        return Ok(MemoStateDecision {
            state: MemoStateValue(Value::from_serializable(&prev_state)?),
            memo_valid: true,
        });
    }

    let content_fingerprint = file.content_fingerprint().await?;
    let state = FileMemoState {
        modified_nanos,
        content_fingerprint,
    };
    let memo_valid = prev_state
        .as_ref()
        .is_some_and(|prev_state| prev_state.content_fingerprint == content_fingerprint);
    Ok(MemoStateDecision {
        state: MemoStateValue(Value::from_serializable(&state)?),
        memo_valid,
    })
}

fn system_time_nanos(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
