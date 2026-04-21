//! Memoization: skip re-execution when inputs haven't changed.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use cocoindex_core::engine::context::{FnCallContext, MemoStatesPayload};
use cocoindex_core::engine::function::{FnCallMemoGuard, reserve_memoization};
use cocoindex_utils::fingerprint::{Fingerprint, Fingerprinter};
use serde::{Deserialize, Serialize};

use crate::ctx::{Ctx, fn_call_guard};
use crate::error::{Error, Result};
use crate::profile::{RustProfile, Value};

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
    let fp = key_fingerprint_result(key)?;
    cached_by_fingerprint(ctx, fp, f).await
}

/// Fast path for generated macros that have already built the memo fingerprint.
#[doc(hidden)]
pub async fn cached_by_fingerprint<T, F, Fut>(ctx: &Ctx, fp: Fingerprint, f: F) -> Result<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    // If we have a pipeline context, use LMDB-backed memoization.
    let Some(comp_ctx) = &ctx.comp_ctx else {
        // No pipeline context — execute directly (standalone mode).
        return f().await;
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
    let fn_ctx = FnCallContext::default();
    let _guard = fn_call_guard(comp_ctx, &fn_ctx);
    let result = f().await?;
    let value = Value::from_serializable(&result)
        .map_err(|e| Error::engine(format!("failed to serialize memo result: {e}")))?;
    guard
        .resolve(&fn_ctx, || value, MemoStatesPayload::default())
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
pub fn finish_key_fingerprinter(fingerprinter: Fingerprinter) -> Fingerprint {
    fingerprinter.into_fingerprint()
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
    F: FnOnce(Vec<I::Item>) -> Fut + Send,
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
    F: FnOnce(Vec<I::Item>) -> Fut + Send,
    Fut: Future<Output = Result<Vec<T>>> + Send,
{
    let items: Vec<I::Item> = items.into_iter().collect();
    let total = items.len();

    let Some(comp_ctx) = &ctx.comp_ctx else {
        // No pipeline context — execute all directly.
        return f(items).await;
    };

    let mut results: Vec<Option<T>> = (0..total).map(|_| None).collect();
    let mut miss_items: Vec<I::Item> = Vec::with_capacity(total);
    let mut miss_indices: Vec<usize> = Vec::with_capacity(total);
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

        let guard = reserve_memoization(comp_ctx, fp)
            .await
            .map_err(|e| Error::engine(format!("reserve_memoization: {e}")))?;

        if let Some(cached) = guard.cached() {
            // Cache hit — deserialize stored result.
            results[idx] = Some(cached.ret.deserialize()?);
        } else {
            // Cache miss — collect for batch execution; keep the guard for resolve.
            miss_indices.push(idx);
            miss_items.push(item);
            miss_guards.push(guard);
        }
    }

    if !miss_items.is_empty() {
        let fn_ctx = FnCallContext::default();
        let _guard = fn_call_guard(comp_ctx, &fn_ctx);
        let miss_results = f(miss_items).await?;
        if miss_results.len() != miss_indices.len() {
            return Err(Error::engine(format!(
                "batch function returned {} results for {} items",
                miss_results.len(),
                miss_indices.len()
            )));
        }

        for ((idx, miss_result), guard) in miss_indices
            .into_iter()
            .zip(miss_results.into_iter())
            .zip(miss_guards.into_iter())
        {
            let value = Value::from_serializable(&miss_result)
                .map_err(|e| Error::engine(format!("failed to serialize memo result: {e}")))?;
            guard
                .resolve(&fn_ctx, || value, MemoStatesPayload::default())
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
