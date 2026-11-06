//! Dependencies a function declares beyond its arguments: [`LogicDep`] (a
//! logic dependency decided per call), [`DataDep`] (data a body reads while it
//! runs) and [`PropagatedCallback`] (a callback whose logic rides through
//! `logic_tracking = "self"` layers). They mirror the Python SDK's
//! `coco.LogicDep` / `use_logic_dep`, `coco.DataDep` / `use_data_dep` and
//! `coco.logic_propagated_to_callers`; the recording methods live on
//! [`Ctx`](crate::Ctx) because Rust has no ambient component context.
//!
//! For a value every call of a function depends on, use the `deps = ...`
//! argument of `#[cocoindex::function]` instead.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock, RwLock, Weak};

use cocoindex_core::engine::context::{DataDepRecord, FnCallContext};
use cocoindex_core::engine::logic_registry;
use cocoindex_utils::fingerprint::Fingerprint;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ctx::Ctx;
use crate::error::{Error, Result};
use crate::memo::{MemoInput, MemoKeyWriter, MemoStateDecision, memo_value_fingerprint};
use crate::profile::Value;

// ---------------------------------------------------------------------------
// LogicDep
// ---------------------------------------------------------------------------

/// A logic dependency declared once and recorded per call with
/// [`Ctx::use_logic_dep`].
///
/// `deps = ...` on `#[cocoindex::function]` folds a value into a function's
/// logic fingerprint, so every call of the function depends on it. Some
/// dependencies are decided per call: a tree walk's `visit_file` depends on the
/// parser of *that file's* language, and a change to one parser should re-run
/// the files it parsed, not every file the walk saw. A `LogicDep` declares such
/// a value and `use_logic_dep` records it as part of the current call's logic,
/// so only the memo entries that recorded it move when the value changes.
///
/// The value is fingerprinted through [`MemoInput`], exactly as a `deps`
/// value is, and the fingerprint is registered in the process's logic set for
/// as long as this object (or a clone of it) lives. A memo entry that recorded
/// it stays valid while a live `LogicDep` with the same value exists; when a
/// later release constructs the dependency with a different value, the
/// entries that recorded the old one are invalidated.
///
/// **Construct before any component runs, never per call or on first use.**
/// An entry is validated against the fingerprints registered at that moment:
/// a dependency that registers only when first needed evicts every entry
/// recorded with it that is probed earlier in the process, and one dropped at
/// the end of a call evicts them all, every pass. A `static` behind
/// `LazyLock` is fine if it is forced before the app runs (in `main` or a
/// lifespan); a dependency built inside a function body is not.
///
/// As a memoized function's argument a `LogicDep` keys by its fingerprint.
#[derive(Clone)]
pub struct LogicDep(Arc<LogicDepInner>);

struct LogicDepInner {
    fp: Fingerprint,
}

impl Drop for LogicDepInner {
    fn drop(&mut self) {
        logic_registry::unregister(&self.fp);
    }
}

impl LogicDep {
    /// Declare a logic dependency on `value` and register it in the process's
    /// logic set until the last clone is dropped.
    pub fn new<T: MemoInput + ?Sized>(value: &T) -> Result<Self> {
        // Its own namespace in the fingerprint space: a function's logic
        // fingerprint hashes its module, name and code hash, and a `deps`
        // value is folded into that; a dependency recorded per call must never
        // collide with either.
        let fp = Fingerprint::from(&("<logic_dep>", memo_value_fingerprint(value)?))?;
        logic_registry::register(fp);
        Ok(Self(Arc::new(LogicDepInner { fp })))
    }

    pub(crate) fn fingerprint(&self) -> Fingerprint {
        self.0.fp
    }
}

impl std::fmt::Debug for LogicDep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LogicDep").field(&self.0.fp).finish()
    }
}

impl PartialEq for LogicDep {
    fn eq(&self, other: &Self) -> bool {
        self.0.fp == other.0.fp
    }
}

impl Eq for LogicDep {}

impl std::hash::Hash for LogicDep {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.fp.hash(state);
    }
}

#[async_trait::async_trait]
impl MemoInput for LogicDep {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&("coco.LogicDep", self.0.fp))
    }
}

// ---------------------------------------------------------------------------
// DataDep
// ---------------------------------------------------------------------------

/// The erased validator of one kind: decodes a stored key and state (a decode
/// failure is the outer error, before any validator runs), then validates.
type ErasedValidator = dyn Fn(&bytes::Bytes, &bytes::Bytes) -> Result<BoxFuture<'static, Result<MemoStateDecision>>>
    + Send
    + Sync;

struct DataDepInner {
    fp: Fingerprint,
    name: String,
    validate: Box<ErasedValidator>,
}

/// Kinds alive in the process, by fingerprint. Weak: a kind is registered
/// while the user's program keeps it alive and leaves when the last clone
/// goes, so the registry never keeps a kind alive by itself.
static REGISTRY: LazyLock<RwLock<HashMap<Fingerprint, Weak<DataDepInner>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

fn registered_kind(fp: &Fingerprint) -> Option<Arc<DataDepInner>> {
    REGISTRY
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(fp)
        .and_then(Weak::upgrade)
}

impl Drop for DataDepInner {
    fn drop(&mut self) {
        let mut registry = REGISTRY.write().unwrap_or_else(|e| e.into_inner());
        // Keep the entry when a later kind of the same name replaced this one.
        if registry
            .get(&self.fp)
            .is_some_and(|live| live.upgrade().is_none())
        {
            registry.remove(&self.fp);
        }
    }
}

/// A kind of data dependency, declared once, with a validator; one read of
/// that kind is recorded into the current call with [`Ctx::use_data_dep`].
///
/// **Experimental:** the API and its semantics may change in a later release.
///
/// Logic, arguments and context values are all known before a body runs. Some
/// bodies read things decided only while they run: an agent session opens the
/// files the model chose, a test runner touches the files the tests import. A
/// `DataDep` declares such a *kind* of read with a validator, and
/// `use_data_dep` records one read as a key and the state observed. The next
/// probe validates each record from the stored key and state alone, through
/// the kind's validator, so the entry can be reused even though nothing in its
/// arguments covers the read.
///
/// `name` is a stable string. It is hashed into the kind's fingerprint in its
/// own namespace and identifies the kind in stored memo entries, so renaming a
/// kind invalidates every entry that recorded it; that doubles as the version
/// bump. `validate(key, prev_state)` takes a recorded key and the state stored
/// with it and returns a [`MemoStateDecision`]: the state now observed (stored
/// in place of the old one when it differs) and whether the entry is still
/// valid. `K` and `S` are the key and state types; both are stored serialized.
///
/// **Construct before any component runs.** A memo entry is validated against
/// the kinds registered at that moment, so a kind constructed inside a
/// component registers too late for the entries that recorded it. A second
/// live kind with the same name replaces the registration and logs a warning:
/// the most recently constructed live kind validates records of that name.
///
/// As a memoized function's argument a `DataDep` keys by its fingerprint.
pub struct DataDep<K, S> {
    inner: Arc<DataDepInner>,
    _types: PhantomData<fn(K, S)>,
}

impl<K, S> Clone for DataDep<K, S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _types: PhantomData,
        }
    }
}

impl<K, S> DataDep<K, S>
where
    K: DeserializeOwned + Send + 'static,
    S: DeserializeOwned + Send + 'static,
{
    /// Declare a kind named `name`, validated by `validate`, and register it
    /// until the last clone is dropped.
    pub fn new<F, Fut>(name: impl Into<String>, validate: F) -> Result<Self>
    where
        F: Fn(K, S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<MemoStateDecision>> + Send + 'static,
    {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::engine("DataDep takes a non-empty name"));
        }
        // Its own namespace in the fingerprint space, as `<logic_dep>` is for
        // `LogicDep`: a kind must never collide with a function's logic
        // fingerprint or a `deps` value.
        let fp = Fingerprint::from(&("<data_dep>", name.as_str()))?;
        let validate = Box::new(move |key: &bytes::Bytes, state: &bytes::Bytes| {
            let key: K = rmp_serde::from_slice(key)?;
            let state: S = rmp_serde::from_slice(state)?;
            Ok(Box::pin(validate(key, state)) as BoxFuture<'static, _>)
        });
        let inner = Arc::new(DataDepInner { fp, name, validate });
        let mut registry = REGISTRY.write().unwrap_or_else(|e| e.into_inner());
        if registry
            .get(&fp)
            .is_some_and(|existing| existing.upgrade().is_some())
        {
            tracing::warn!(
                "DataDep({:?}) replaces a live kind of the same name; the new kind now validates records of that name",
                inner.name
            );
        }
        registry.insert(fp, Arc::downgrade(&inner));
        Ok(Self {
            inner,
            _types: PhantomData,
        })
    }
}

impl<K, S> DataDep<K, S> {
    /// The kind's name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub(crate) fn fingerprint(&self) -> Fingerprint {
        self.inner.fp
    }
}

impl<K, S> std::fmt::Debug for DataDep<K, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DataDep").field(&self.inner.name).finish()
    }
}

impl<K, S> PartialEq for DataDep<K, S> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.fp == other.inner.fp
    }
}

impl<K, S> Eq for DataDep<K, S> {}

impl<K, S> std::hash::Hash for DataDep<K, S> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.fp.hash(state);
    }
}

#[async_trait::async_trait]
impl<K: Send + Sync, S: Send + Sync> MemoInput for DataDep<K, S> {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&("coco.DataDep", self.inner.fp))
    }
}

/// Build the record `use_data_dep` stores: `key` and `state` serialized once,
/// with the record's identity and equality taken as fingerprints of those
/// bytes. The memo-key pipeline is deliberately not involved, so two unequal
/// observations never collapse into one.
pub(crate) fn data_dep_record<K: Serialize, S: Serialize>(
    kind_fp: Fingerprint,
    key: &K,
    state: &S,
) -> Result<DataDepRecord> {
    let key = Value::from_serializable(key)?.0;
    let state = Value::from_serializable(state)?.0;
    Ok(DataDepRecord {
        kind_fp,
        key_fp: Fingerprint::from_bytes(&key),
        state_fp: Fingerprint::from_bytes(&state),
        key,
        state,
    })
}

/// The outcome of validating stored data-dependency records: the records to
/// store back (refreshed where a validator reported a changed but valid
/// state), whether the entry can be reused, and whether any state moved.
pub(crate) struct DataDepsOutcome {
    pub(crate) records: Vec<DataDepRecord>,
    pub(crate) can_reuse: bool,
    pub(crate) states_changed: bool,
}

impl DataDepsOutcome {
    fn miss(records: Vec<DataDepRecord>) -> Self {
        Self {
            records,
            can_reuse: false,
            states_changed: false,
        }
    }
}

/// Validate the records a memo entry stored. Two phases, as in the Python
/// SDK: decode (a kind no longer registered or a stored key or state that no
/// longer deserializes is a logged miss, and no validator runs), then the
/// validators, concurrently; a validator's error propagates.
pub(crate) async fn validate_data_deps(
    records: Vec<DataDepRecord>,
    where_: &str,
) -> Result<DataDepsOutcome> {
    let mut validations = Vec::with_capacity(records.len());
    for record in &records {
        let Some(kind) = registered_kind(&record.kind_fp) else {
            tracing::warn!(
                "{where_}: memo misses on a data dependency of kind {}, which is no longer registered (renamed or removed); the body re-runs",
                record.kind_fp
            );
            return Ok(DataDepsOutcome::miss(records));
        };
        match (kind.validate)(&record.key, &record.state) {
            Ok(fut) => validations.push(fut),
            Err(err) => {
                tracing::warn!(
                    "{where_}: memo misses on a data dependency of kind {:?} whose stored key or state failed to deserialize ({err}); the body re-runs",
                    kind.name
                );
                return Ok(DataDepsOutcome::miss(records));
            }
        }
    }
    let decisions = futures::future::try_join_all(validations).await?;
    let mut can_reuse = true;
    let mut states_changed = false;
    let refreshed = records
        .into_iter()
        .zip(decisions)
        .map(|(record, decision)| {
            let (state, memo_valid) = decision.into_parts();
            can_reuse &= memo_valid;
            if state.0 == record.state {
                record
            } else {
                states_changed = true;
                DataDepRecord {
                    state_fp: Fingerprint::from_bytes(&state.0),
                    state: state.0,
                    ..record
                }
            }
        })
        .collect();
    Ok(DataDepsOutcome {
        records: refreshed,
        can_reuse,
        states_changed,
    })
}

// ---------------------------------------------------------------------------
// PropagatedCallback
// ---------------------------------------------------------------------------

/// A callback whose logic is propagated to its callers' memo entries through
/// `logic_tracking = "self"` / `"none"` layers. Built by
/// [`Ctx::logic_propagated_to_callers`] in the function that hands the
/// callback down (the **owner**); every call site runs it with
/// [`call`](Self::call).
///
/// Those modes hide the logic of everything a function calls, including
/// callbacks it merely received. The wrapper lets the owner have the
/// callback's logic recorded in every memo entry between the call site and
/// the owner, regardless of their modes. It decides nothing about *what* is
/// tracked: the callback's own `#[cocoindex::function]` and `logic_tracking`
/// do, exactly as for any function.
pub struct PropagatedCallback<F> {
    f: F,
    owner: Arc<FnCallContext>,
}

impl<F> PropagatedCallback<F> {
    pub(crate) fn new(f: F, owner: Arc<FnCallContext>) -> Self {
        Self { f, owner }
    }

    /// Run the callback with `arg` from the frame of `ctx`, the call site's
    /// context. The callback receives a `Ctx` scoped to its own frame; what it
    /// offers is recorded in every memo entry between `ctx` and the owner.
    ///
    /// # Errors
    ///
    /// Returns an error when `ctx` has no pipeline context, or when the
    /// function that built the wrapper has already returned: a propagated
    /// callback is only valid inside the dynamic extent of that call, so
    /// mount the components that call it with `use_mount!` or wait for them
    /// inside the owner.
    pub async fn call<A, T, Fut>(&self, ctx: &Ctx, arg: A) -> Result<T>
    where
        F: Fn(Ctx, A) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        if ctx.comp_ctx.is_none() {
            return Err(Error::engine(
                "a propagated callback was called with no active pipeline context; it must run inside the component that wrapped it",
            ));
        }
        if self.owner.is_closed() {
            return Err(Error::engine(
                "a propagated callback was called after the function that wrapped it returned; a propagated callback is only valid inside the dynamic extent of that call",
            ));
        }
        // The collector propagates everything, so its fn_logic_deps is exactly
        // what the callback offers under the callback's own logic_tracking.
        let collector = Arc::new(FnCallContext::new(true));
        let _frame = ctx.tunnel_guard(collector.clone(), self.owner.id());
        (self.f)(ctx.with_fn_ctx(collector), arg).await
    }
}

impl<F> std::fmt::Debug for PropagatedCallback<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PropagatedCallback").finish_non_exhaustive()
    }
}
