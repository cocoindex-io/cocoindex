//! Memoization: skip re-execution when inputs haven't changed.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex_core::engine::context::{FnCallContext, MemoStatesPayload};
use cocoindex_core::engine::function::{FnCallMemoGuard, reserve_memoization};
use cocoindex_utils::fingerprint::{Fingerprint, Fingerprinter};
use serde::{Deserialize, Serialize};

use crate::ctx::{Ctx, fn_call_guard};
use crate::error::{Error, Result};
use crate::file::FileLike;
use crate::profile::{RustProfile, Value};

#[derive(Clone)]
pub struct MemoStateValue(Value);

pub struct MemoStateDecision {
    state: MemoStateValue,
    memo_valid: bool,
}

#[doc(hidden)]
pub type MemoStateBoxFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<MemoStateDecision>>> + Send + 'a>>;

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
    cached_by_fingerprint(ctx, fp, f).await
}

/// Fast path for generated macros that have already built the memo fingerprint.
///
/// `f` receives a `Ctx` scoped to this memo call; use it for `get_key` so
/// change-detection dependencies attach to this memo entry.
#[doc(hidden)]
pub async fn cached_by_fingerprint<T, F, Fut>(ctx: &Ctx, fp: Fingerprint, f: F) -> Result<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    F: FnOnce(Ctx) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    // If we have a pipeline context, use LMDB-backed memoization.
    let Some(comp_ctx) = &ctx.comp_ctx else {
        // No pipeline context — execute directly (standalone mode).
        return f(ctx.clone()).await;
    };

    let guard = reserve_memoization(comp_ctx, fp)
        .await
        .map_err(|e| Error::engine(format!("reserve_memoization: {e}")))?;

    // Cache hit — deserialize and return the stored value.
    if let Some(cached) = guard.cached() {
        let value: T = cached.ret.deserialize()?;
        return Ok(value);
    }

    // Cache miss (or memo disabled) — we are the resolver. Execute and commit.
    let fn_ctx = Arc::new(FnCallContext::default());
    let _guard = fn_call_guard(comp_ctx, fn_ctx.clone());
    let result = f(ctx.with_fn_ctx(fn_ctx.clone())).await?;
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
    let Some(comp_ctx) = &ctx.comp_ctx else {
        return f(ctx.clone()).await;
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
        if states_changed {
            guard.update_memo_states(MemoStatesPayload {
                positional: memo_states_for_resolve,
                by_context_fp: cached_context_states,
            });
        }
        return Ok(value);
    }

    let fn_ctx = Arc::new(FnCallContext::default());
    let _guard = fn_call_guard(comp_ctx, fn_ctx.clone());
    let result = f(ctx.with_fn_ctx(fn_ctx.clone())).await?;
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
/// Serializes key parts immediately into owned bytes to avoid leaking
/// temporary references from generated closures.
///
/// This infallible helper never panics. Unsupported key types produce a unique,
/// stable fallback payload that includes the key type and an incrementing
/// sequence identifier to avoid collisions.
#[doc(hidden)]
pub fn key_bytes<T: Serialize + ?Sized>(value: &T) -> Vec<u8> {
    match key_bytes_result(value) {
        Ok(bytes) => bytes,
        Err(err) => fallback_key_bytes::<T>(err),
    }
}

/// Internal helper for stable memo keys in generated macros.
///
/// This variant returns an error instead of panicking so callers can fail
/// fast with a typed error when serialization is not supported.
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
    if let Some(file) = value.downcast_ref::<crate::fs::FileEntry>() {
        return Some(file);
    }
    #[cfg(feature = "amazon_s3")]
    if let Some(file) = value.downcast_ref::<crate::amazon_s3::S3File>() {
        return Some(file);
    }
    #[cfg(feature = "google_drive")]
    if let Some(file) = value.downcast_ref::<crate::gdrive::DriveFile>() {
        return Some(file);
    }
    #[cfg(feature = "oci_object_storage")]
    if let Some(file) = value.downcast_ref::<crate::oci_object_storage::OciFile>() {
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

fn fallback_key_bytes<T: ?Sized>(error: Error) -> Vec<u8> {
    static KEY_BYTES_FALLBACK_SEQ: AtomicU64 = AtomicU64::new(0);

    let seq = KEY_BYTES_FALLBACK_SEQ.fetch_add(1, Ordering::AcqRel);
    match rmp_serde::to_vec_named(&(
        "__cocoindex_key_bytes_unsupported__",
        std::any::type_name::<T>(),
        seq,
        error.to_string(),
    )) {
        Ok(bytes) => bytes,
        Err(_) => {
            let mut bytes = b"__cocoindex_key_bytes_unsupported__".to_vec();
            bytes.extend_from_slice(std::any::type_name::<T>().as_bytes());
            bytes.extend_from_slice(&seq.to_le_bytes());
            bytes.extend_from_slice(error.to_string().as_bytes());
            bytes
        }
    }
}

/// Batch-process items with per-item memoization.
///
/// For each item, computes a cache key via `key_fn` and probes the memo cache.
/// Items with cache hits get their stored values immediately. Items with cache
/// misses are collected and passed to `f` as a single batch. Results are stored
/// back in the cache and merged with hits in original order.
///
/// This is the key optimization for LLM pipelines: instead of N individual API
/// calls, you get 1 batch call for the cache misses only.
///
/// # Contract
///
/// `f` receives the cache-miss items and **must** return exactly one result per
/// item, in the same order.
pub async fn batch<I, K, T, F, KF, Fut>(ctx: &Ctx, items: I, key_fn: KF, f: F) -> Result<Vec<T>>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    K: Serialize,
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    KF: Fn(&I::Item) -> Result<K>,
    F: FnOnce(Ctx, Vec<I::Item>) -> Fut + Send,
    Fut: Future<Output = Result<Vec<T>>> + Send,
{
    batch_by_fingerprint(
        ctx,
        items,
        move |item| {
            let key = key_fn(item)?;
            key_fingerprint_result(&key)
        },
        f,
    )
    .await
}

/// Same as [`batch`] but lets callers supply precomputed per-item fingerprints.
#[doc(hidden)]
pub async fn batch_by_fingerprint<I, T, F, KF, Fut>(
    ctx: &Ctx,
    items: I,
    key_fn: KF,
    f: F,
) -> Result<Vec<T>>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    KF: Fn(&I::Item) -> Result<Fingerprint>,
    F: FnOnce(Ctx, Vec<I::Item>) -> Fut + Send,
    Fut: Future<Output = Result<Vec<T>>> + Send,
{
    batch_by_fingerprint_with_state(
        ctx,
        items,
        key_fn,
        |_item, _prev_states| Box::pin(async { Ok(Vec::new()) }),
        f,
    )
    .await
}

#[doc(hidden)]
pub async fn batch_by_fingerprint_with_state<I, T, F, KF, SF, Fut>(
    ctx: &Ctx,
    items: I,
    key_fn: KF,
    state_fn: SF,
    f: F,
) -> Result<Vec<T>>
where
    I: IntoIterator,
    I::Item: Send + 'static,
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    KF: Fn(&I::Item) -> Result<Fingerprint>,
    SF: for<'a> Fn(&'a I::Item, Option<Vec<MemoStateValue>>) -> MemoStateBoxFuture<'a>,
    F: FnOnce(Ctx, Vec<I::Item>) -> Fut + Send,
    Fut: Future<Output = Result<Vec<T>>> + Send,
{
    let items: Vec<I::Item> = items.into_iter().collect();
    let total = items.len();

    let Some(comp_ctx) = &ctx.comp_ctx else {
        // No pipeline context — execute all directly.
        return f(ctx.clone(), items).await;
    };

    let mut results: Vec<Option<T>> = (0..total).map(|_| None).collect();
    let mut miss_items: Vec<I::Item> = Vec::with_capacity(total);
    let mut miss_indices: Vec<usize> = Vec::with_capacity(total);
    let mut miss_states: Vec<Vec<Value>> = Vec::with_capacity(total);
    // Parallel vec: guard for cache-miss items that still hold the write lock.
    let mut miss_guards: Vec<FnCallMemoGuard<RustProfile>> = Vec::with_capacity(total);

    let mut fps_seen = rustc_hash::FxHashSet::default();

    for (idx, item) in items.into_iter().enumerate() {
        let fp = key_fn(&item)?;

        if !fps_seen.insert(fp) {
            // Engine rejects mapping duplicate memo keys per function invocation.
            return Err(Error::engine(
                "duplicate cache keys generated in memo batch execution",
            ));
        }

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
        let state_decisions = state_fn(&item, cached_states).await?;
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
            results[idx] = Some(cached.ret.deserialize()?);
            if states_changed {
                guard.update_memo_states(MemoStatesPayload {
                    positional: memo_states_for_resolve,
                    by_context_fp: cached_context_states,
                });
            }
        } else {
            // Cache miss — collect for batch execution; keep the guard for resolve.
            miss_indices.push(idx);
            miss_states.push(memo_states_for_resolve);
            miss_items.push(item);
            miss_guards.push(guard);
        }
    }

    if !miss_items.is_empty() {
        let fn_ctx = Arc::new(FnCallContext::default());
        let _guard = fn_call_guard(comp_ctx, fn_ctx.clone());
        let miss_results = f(ctx.with_fn_ctx(fn_ctx.clone()), miss_items).await?;
        if miss_results.len() != miss_indices.len() {
            return Err(Error::engine(format!(
                "batch function returned {} results for {} items",
                miss_results.len(),
                miss_indices.len()
            )));
        }

        for (((idx, miss_result), guard), memo_states_for_resolve) in miss_indices
            .into_iter()
            .zip(miss_results)
            .zip(miss_guards)
            .zip(miss_states)
        {
            let value = Value::from_serializable(&miss_result)
                .map_err(|e| Error::engine(format!("failed to serialize memo result: {e}")))?;
            let memo_states = MemoStatesPayload {
                positional: memo_states_for_resolve,
                by_context_fp: fn_ctx.collect_context_initial_states(comp_ctx.app_ctx().env()),
            };
            guard
                .resolve(&fn_ctx, || value, memo_states)
                .map_err(|e| Error::engine(format!("{e}")))?;
            results[idx] = Some(miss_result);
        }
    }

    let mut resolved = Vec::with_capacity(total);
    for (idx, value) in results.into_iter().enumerate() {
        let value = value.ok_or_else(|| {
            Error::engine(format!(
                "memo batch internal inconsistency: missing result for index {idx}"
            ))
        })?;
        resolved.push(value);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::Error as SerError;
    use serde::{Serialize, Serializer};

    #[derive(Debug)]
    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S: Serializer>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error> {
            Err(S::Error::custom("forced serialization failure"))
        }
    }

    #[test]
    fn key_bytes_result_serializes_supported_types() {
        let value = ("hello", 123);
        let bytes = key_bytes_result(&value).unwrap();
        let decoded: (String, i32) = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded, (value.0.to_string(), value.1));
    }

    #[test]
    fn key_bytes_uses_fallback_on_serialization_error() {
        let bytes = key_bytes(&FailingSerialize);
        let (marker, type_name, _, message) =
            rmp_serde::from_slice::<(String, String, u64, String)>(&bytes).unwrap();

        assert_eq!(marker, "__cocoindex_key_bytes_unsupported__");
        assert_eq!(type_name, std::any::type_name::<FailingSerialize>());
        assert!(message.contains("forced serialization failure"));
    }

    #[test]
    fn key_bytes_fallback_is_unique_per_failure() {
        let first = key_bytes(&FailingSerialize);
        let second = key_bytes(&FailingSerialize);

        let (_, _, first_seq, _) =
            rmp_serde::from_slice::<(String, String, u64, String)>(&first).unwrap();
        let (_, _, second_seq, _) =
            rmp_serde::from_slice::<(String, String, u64, String)>(&second).unwrap();

        assert_ne!(first_seq, second_seq);
    }
}
