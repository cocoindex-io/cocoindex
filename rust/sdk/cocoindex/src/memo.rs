//! Memoization: skip re-execution when inputs haven't changed.
//!
//! Prefer `#[cocoindex::function(memo)]` for memoizing a whole pipeline
//! function. The attribute builds the argument key, tracks the function body's
//! logic, and passes an owned, memo-scoped [`Ctx`] into the body. Context
//! resources can therefore be read normally with [`Ctx::get_or_err`] or
//! [`Ctx::get_key`]. Use `memo_key(parameter = skip)` for a `Clone` parameter
//! that should not participate in the key; it does not need to implement
//! [`MemoInput`] or Serde. A transform override similarly replaces the
//! parameter's normal [`MemoInput`] identity and suppresses its type-owned
//! external-state validation.
//!
//! `MemoInput` composes recursively through `Option`, sequences, tuples,
//! maps, sets, `Box`, and `Arc`. The SDK also implements it for its standard
//! scalar schema types, including UUIDs and, when their corresponding SDK
//! features are enabled, `serde_json::Value`, Chrono values, and
//! `rust_decimal::Decimal`.
//! Other third-party types need a local newtype or a declaration/call-site
//! `memo_key` projection because Rust's orphan rules prevent downstream crates
//! from implementing this trait for a foreign type.
//!
//! `Vec<u8>` uses the same recursive sequence representation as every other
//! `Vec<T>`. For large byte payloads, prefer [`bytes::Bytes`] for bulk hashing,
//! or pass a precomputed [`Fingerprint`] when the content is already hashed.
//!
//! [`Ctx::memo`] and [`cached`] are intended for block-level memoization inside
//! a function. Their closure body is not logic-tracked. When a manual memo block
//! must invalidate after its enclosing `#[cocoindex::function]` changes, include
//! that function's generated `__COCO_FN_HASH_<NAME>` constant in the manual key.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::hash::{BuildHasher, Hash};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cocoindex_core::engine::context::{FnCallContext, MemoStatesPayload};
use cocoindex_core::engine::function::reserve_memoization;
use cocoindex_utils::fingerprint::{Fingerprint, Fingerprinter};
use serde::{Deserialize, Serialize};

use crate::ctx::{Ctx, fn_call_guard};
use crate::error::{Error, Result};
use crate::profile::Value;
use crate::resources::file::FileLike;

/// Type-owned memoization behavior for function arguments and tracked context
/// values.
///
/// Use the [`crate::MemoInput`] derive for normal structs and enums. It adds a
/// `MemoInput` bound to each field type and recursively visits every field; the
/// containing type itself does not need to implement [`Serialize`]. Nested
/// resources therefore preserve their type-owned identity and freshness
/// validation. Resource types can implement this trait directly to provide a
/// small stable identity and asynchronous validation.
#[async_trait]
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as a memoized input",
    note = "add `#[derive(cocoindex::MemoInput)]`, implement `MemoInput` manually, or provide a `memo_key` override"
)]
pub trait MemoInput: Send + Sync {
    /// Add this value's semantic identity to a function memo key.
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()>;

    /// Validate external state before a matching memo is reused.
    ///
    /// `None` means that this value needs no state validation.
    async fn memo_state(
        &self,
        _previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        Ok(None)
    }
}

macro_rules! impl_serde_memo_input {
    ($($ty:ty),+ $(,)?) => {
        $(
            #[async_trait]
            impl MemoInput for $ty {
                fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
                    writer.write(self)
                }
            }
        )+
    };
}

impl_serde_memo_input!(
    (),
    bool,
    char,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    f32,
    f64,
    String,
    str,
    PathBuf,
    Path,
    Duration,
    SystemTime,
    uuid::Uuid,
    Fingerprint,
);

#[async_trait]
impl MemoInput for bytes::Bytes {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        writer.write(self)
    }
}

#[cfg(feature = "serde_json")]
impl_serde_memo_input!(serde_json::Value);

#[cfg(feature = "chrono")]
impl_serde_memo_input!(chrono::NaiveDate, chrono::NaiveTime, chrono::NaiveDateTime);

#[cfg(feature = "chrono")]
#[async_trait]
impl<Tz> MemoInput for chrono::DateTime<Tz>
where
    Tz: chrono::TimeZone + Send + Sync,
    Tz::Offset: Send + Sync,
    chrono::DateTime<Tz>: Serialize,
{
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        writer.write(self)
    }
}

#[cfg(feature = "rust_decimal")]
impl_serde_memo_input!(rust_decimal::Decimal);

#[async_trait]
impl<T: MemoInput + ?Sized> MemoInput for &T {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        (*self).write_memo_key(writer)
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        (*self).memo_state(previous).await
    }
}

#[async_trait]
impl<T: MemoInput + ?Sized> MemoInput for Box<T> {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        self.as_ref().write_memo_key(writer)
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        self.as_ref().memo_state(previous).await
    }
}

#[async_trait]
impl<T: MemoInput + ?Sized> MemoInput for Arc<T> {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        self.as_ref().write_memo_key(writer)
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        self.as_ref().memo_state(previous).await
    }
}

#[async_trait]
impl<T: MemoInput> MemoInput for Option<T> {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        match self {
            Some(value) => {
                writer.write(&("option", true))?;
                writer.write_input(value)
            }
            None => writer.write(&("option", false)),
        }
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        match self {
            Some(value) => value.memo_state(previous).await,
            None => Ok(None),
        }
    }
}

#[async_trait]
impl<T: MemoInput> MemoInput for [T] {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        writer.write(&("sequence", self.len()))?;
        for item in self {
            writer.write_input(item)?;
        }
        Ok(())
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        let children = self
            .iter()
            .map(|item| item as &dyn MemoInput)
            .collect::<Vec<_>>();
        memo_state_group(&children, previous, false).await
    }
}

#[async_trait]
impl<T: MemoInput> MemoInput for Vec<T> {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        self.as_slice().write_memo_key(writer)
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        self.as_slice().memo_state(previous).await
    }
}

#[async_trait]
impl<T: MemoInput, const N: usize> MemoInput for [T; N] {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        self.as_slice().write_memo_key(writer)
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        self.as_slice().memo_state(previous).await
    }
}

fn memo_value_fingerprint<T: MemoInput + ?Sized>(value: &T) -> Result<Fingerprint> {
    let mut fingerprinter = Fingerprinter::default();
    value.write_memo_key(&mut MemoKeyWriter {
        fingerprinter: &mut fingerprinter,
    })?;
    Ok(fingerprinter.into_fingerprint())
}

type SortedMapEntries<'a, K, V> = (Vec<(Fingerprint, Fingerprint, &'a K, &'a V)>, bool);

fn sorted_map_entries<'a, K, V>(
    entries: impl Iterator<Item = (&'a K, &'a V)>,
) -> Result<SortedMapEntries<'a, K, V>>
where
    K: MemoInput + 'a,
    V: MemoInput + 'a,
{
    let mut entries = entries
        .map(|(key, value)| {
            Ok((
                memo_value_fingerprint(key)?,
                memo_value_fingerprint(value)?,
                key,
                value,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by_key(|(key_fp, value_fp, _, _)| (*key_fp, *value_fp));
    let collision = entries.windows(2).any(|pair| pair[0].0 == pair[1].0);
    Ok((entries, collision))
}

macro_rules! impl_map_memo_input {
    ($ty:ty) => {
        #[async_trait]
        impl<K, V, S> MemoInput for $ty
        where
            K: MemoInput + Eq + Hash,
            V: MemoInput,
            S: BuildHasher + Send + Sync,
        {
            fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
                let (entries, collision) = sorted_map_entries(self.iter())?;
                writer.write(&("map", entries.len(), collision))?;
                for (_, _, key, value) in entries {
                    writer.write_input(key)?;
                    writer.write_input(value)?;
                }
                Ok(())
            }

            async fn memo_state(
                &self,
                previous: Option<&MemoStateValue>,
            ) -> Result<Option<MemoStateDecision>> {
                let (entries, collision) = sorted_map_entries(self.iter())?;
                let children = entries
                    .iter()
                    .flat_map(|(_, _, key, value)| {
                        [*key as &dyn MemoInput, *value as &dyn MemoInput]
                    })
                    .collect::<Vec<_>>();
                memo_state_group(&children, previous, collision).await
            }
        }
    };
}

impl_map_memo_input!(HashMap<K, V, S>);

#[async_trait]
impl<K: MemoInput + Ord, V: MemoInput> MemoInput for BTreeMap<K, V> {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        let (entries, collision) = sorted_map_entries(self.iter())?;
        writer.write(&("map", entries.len(), collision))?;
        for (_, _, key, value) in entries {
            writer.write_input(key)?;
            writer.write_input(value)?;
        }
        Ok(())
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        let (entries, collision) = sorted_map_entries(self.iter())?;
        let children = entries
            .iter()
            .flat_map(|(_, _, key, value)| [*key as &dyn MemoInput, *value as &dyn MemoInput])
            .collect::<Vec<_>>();
        memo_state_group(&children, previous, collision).await
    }
}

fn sorted_set_entries<'a, T: MemoInput + 'a>(
    entries: impl Iterator<Item = &'a T>,
) -> Result<(Vec<(Fingerprint, &'a T)>, bool)> {
    let mut entries = entries
        .map(|value| Ok((memo_value_fingerprint(value)?, value)))
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by_key(|(fingerprint, _)| *fingerprint);
    let collision = entries.windows(2).any(|pair| pair[0].0 == pair[1].0);
    Ok((entries, collision))
}

#[async_trait]
impl<T, S> MemoInput for HashSet<T, S>
where
    T: MemoInput + Eq + Hash,
    S: BuildHasher + Send + Sync,
{
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        let (entries, collision) = sorted_set_entries(self.iter())?;
        writer.write(&("set", entries.len(), collision))?;
        for (_, value) in entries {
            writer.write_input(value)?;
        }
        Ok(())
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        let (entries, collision) = sorted_set_entries(self.iter())?;
        let children = entries
            .iter()
            .map(|(_, value)| *value as &dyn MemoInput)
            .collect::<Vec<_>>();
        memo_state_group(&children, previous, collision).await
    }
}

#[async_trait]
impl<T: MemoInput + Ord> MemoInput for BTreeSet<T> {
    fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
        let (entries, collision) = sorted_set_entries(self.iter())?;
        writer.write(&("set", entries.len(), collision))?;
        for (_, value) in entries {
            writer.write_input(value)?;
        }
        Ok(())
    }

    async fn memo_state(
        &self,
        previous: Option<&MemoStateValue>,
    ) -> Result<Option<MemoStateDecision>> {
        let (entries, collision) = sorted_set_entries(self.iter())?;
        let children = entries
            .iter()
            .map(|(_, value)| *value as &dyn MemoInput)
            .collect::<Vec<_>>();
        memo_state_group(&children, previous, collision).await
    }
}

macro_rules! impl_tuple_memo_input {
    ($len:expr; $(($index:tt, $name:ident)),+ $(,)?) => {
        #[async_trait]
        impl<$($name: MemoInput),+> MemoInput for ($($name,)+) {
            fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
                writer.write(&("tuple", $len as usize))?;
                $(writer.write_input(&self.$index)?;)+
                Ok(())
            }

            async fn memo_state(
                &self,
                previous: Option<&MemoStateValue>,
            ) -> Result<Option<MemoStateDecision>> {
                let children: &[&dyn MemoInput] = &[$(&self.$index),+];
                memo_state_group(children, previous, false).await
            }
        }
    };
}

impl_tuple_memo_input!(1; (0, A));
impl_tuple_memo_input!(2; (0, A), (1, B));
impl_tuple_memo_input!(3; (0, A), (1, B), (2, C));
impl_tuple_memo_input!(4; (0, A), (1, B), (2, C), (3, D));
impl_tuple_memo_input!(5; (0, A), (1, B), (2, C), (3, D), (4, E));
impl_tuple_memo_input!(6; (0, A), (1, B), (2, C), (3, D), (4, E), (5, F));
impl_tuple_memo_input!(7; (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G));
impl_tuple_memo_input!(8; (0, A), (1, B), (2, C), (3, D), (4, E), (5, F), (6, G), (7, H));

/// Opaque writer passed to [`MemoInput::write_memo_key`].
pub struct MemoKeyWriter<'a> {
    fingerprinter: &'a mut Fingerprinter,
}

impl MemoKeyWriter<'_> {
    /// Write a serializable value as one framed part of the memo identity.
    pub fn write<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        write_key_fingerprint_part(self.fingerprinter, value)
    }

    /// Recursively write another memo input into the same framed fingerprint
    /// stream.
    pub fn write_input<T: MemoInput + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.write_memo_key(self)
    }
}

#[derive(Clone)]
pub struct MemoStateValue(Value);

pub struct MemoStateDecision {
    state: MemoStateValue,
    memo_valid: bool,
}

#[derive(Serialize, Deserialize)]
struct NestedMemoStatesV1 {
    version: u8,
    children: Vec<Option<Vec<u8>>>,
}

impl MemoStateValue {
    /// Serialize a state value for storage with a memo entry.
    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self> {
        Ok(Self(Value::from_serializable(value)?))
    }

    /// Deserialize a state stored by a previous memo run.
    pub fn deserialize<T: for<'de> Deserialize<'de>>(&self) -> Result<T> {
        self.0.deserialize()
    }

    pub(crate) fn from_profile_value(value: Value) -> Self {
        Self(value)
    }
}

impl MemoStateDecision {
    /// Build a state-validation decision.
    pub fn new<T: Serialize>(state: &T, memo_valid: bool) -> Result<Self> {
        Ok(Self {
            state: MemoStateValue::from_serializable(state)?,
            memo_valid,
        })
    }

    pub(crate) fn into_parts(self) -> (Value, bool) {
        (self.state.0, self.memo_valid)
    }
}

/// Validate the memo states of recursively nested inputs and bundle them into
/// one opaque state value for the containing top-level argument or context
/// value.
#[doc(hidden)]
pub async fn memo_state_group(
    children: &[&dyn MemoInput],
    previous: Option<&MemoStateValue>,
    force_miss: bool,
) -> Result<Option<MemoStateDecision>> {
    let previous_group = previous
        .and_then(|value| value.deserialize::<NestedMemoStatesV1>().ok())
        .filter(|group| group.version == 1);
    let shape_matches = previous_group
        .as_ref()
        .is_some_and(|group| group.children.len() == children.len());

    let previous_values = (0..children.len())
        .map(|index| {
            previous_group
                .as_ref()
                .and_then(|group| group.children.get(index))
                .and_then(|state| state.as_ref())
                .map(|bytes| MemoStateValue(Value(bytes::Bytes::copy_from_slice(bytes.as_slice()))))
        })
        .collect::<Vec<_>>();

    let decisions = futures::future::try_join_all(
        children
            .iter()
            .zip(&previous_values)
            .map(|(child, previous)| child.memo_state(previous.as_ref())),
    )
    .await?;

    let mut memo_valid = previous.is_some() && shape_matches && !force_miss;
    // Preserve a marker after malformed/legacy state so this slot forces one
    // miss without shifting the following top-level argument states.
    let mut has_state = force_miss || previous.is_some();
    let mut new_children = Vec::with_capacity(decisions.len());
    for (index, decision) in decisions.into_iter().enumerate() {
        let previous_present = previous_values[index].is_some();
        match decision {
            Some(decision) => {
                has_state = true;
                memo_valid &= previous_present && decision.memo_valid;
                new_children.push(Some(decision.state.0.0.to_vec()));
            }
            None => {
                memo_valid &= !previous_present;
                new_children.push(None);
            }
        }
    }

    if !has_state {
        return Ok(None);
    }

    MemoStateDecision::new(
        &NestedMemoStatesV1 {
            version: 1,
            children: new_children,
        },
        memo_valid,
    )
    .map(Some)
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

    let mut guard = reserve_memoization(comp_ctx, fp)
        .await
        .map_err(|e| Error::engine(format!("reserve_memoization: {e}")))?;

    let cached_context_states = guard
        .cached()
        .map(|cached| cached.context_memo_states.to_vec());
    if let Some(cached_context_states) = cached_context_states {
        let context_validation = ctx
            .state
            .context
            .validate_memo_states(&cached_context_states)
            .await?;
        if context_validation.memo_valid {
            let cached = guard
                .cached()
                .expect("cached memo disappeared while reserved");
            let value: T = cached.ret.deserialize()?;
            let positional_states = cached.memo_states.to_vec();
            ctx.check_cancellation()?;
            if context_validation.states_changed {
                guard.update_memo_states(MemoStatesPayload {
                    positional: positional_states,
                    by_context_fp: context_validation.states,
                });
            }
            return Ok(value);
        }
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
    let cached_context_states = guard
        .cached()
        .map(|cached| cached.context_memo_states.to_vec())
        .unwrap_or_default();
    let context_validation = ctx
        .state
        .context
        .validate_memo_states(&cached_context_states)
        .await?;

    if let Some(cached) = guard.cached()
        && state_decisions.iter().all(|decision| decision.memo_valid)
        && context_validation.memo_valid
    {
        let states_changed = state_decisions
            .iter()
            .map(|decision| &decision.state.0.0)
            .ne(cached.memo_states.iter().map(|value| &value.0));
        let value: T = cached.ret.deserialize()?;
        ctx.check_cancellation()?;
        if states_changed || context_validation.states_changed {
            guard.update_memo_states(MemoStatesPayload {
                positional: memo_states_for_resolve,
                by_context_fp: context_validation.states,
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
pub fn write_key_fingerprint_part_for_arg<T: MemoInput + ?Sized>(
    fingerprinter: &mut Fingerprinter,
    value: &T,
) -> Result<()> {
    value.write_memo_key(&mut MemoKeyWriter { fingerprinter })
}

#[doc(hidden)]
pub fn finish_key_fingerprinter(fingerprinter: Fingerprinter) -> Fingerprint {
    fingerprinter.into_fingerprint()
}

pub(crate) fn memo_input_fingerprint<T: MemoInput + ?Sized>(
    namespace: &str,
    name: &str,
    value: &T,
) -> Result<Fingerprint> {
    let mut fingerprinter = new_key_fingerprinter();
    write_key_fingerprint_part(&mut fingerprinter, namespace)?;
    write_key_fingerprint_part(&mut fingerprinter, name)?;
    write_key_fingerprint_part_for_arg(&mut fingerprinter, value)?;
    Ok(finish_key_fingerprinter(fingerprinter))
}

pub(crate) fn write_file_memo_key(
    file: &dyn FileLike,
    writer: &mut MemoKeyWriter<'_>,
) -> Result<()> {
    writer.write(&file.file_path().memo_key())
}

#[doc(hidden)]
pub async fn collect_memo_arg_state<T: MemoInput + ?Sized>(
    value: &T,
    prev: Option<&MemoStateValue>,
) -> Result<Option<MemoStateDecision>> {
    value.memo_state(prev).await
}

pub(crate) async fn file_memo_state(
    file: &dyn FileLike,
    prev: Option<&MemoStateValue>,
) -> Result<MemoStateDecision> {
    let metadata = file.metadata().await?;
    let modified_nanos = system_time_nanos(metadata.modified);
    let prev_state = prev.and_then(|value| value.deserialize::<FileMemoState>().ok());

    if let Some(ref prev_state) = prev_state
        && prev_state.modified_nanos == modified_nanos
    {
        return MemoStateDecision::new(&prev_state, true);
    }

    let content_fingerprint = file.content_fingerprint().await?;
    let state = FileMemoState {
        modified_nanos,
        content_fingerprint,
    };
    let memo_valid = prev_state
        .as_ref()
        .is_some_and(|prev_state| prev_state.content_fingerprint == content_fingerprint);
    MemoStateDecision::new(&state, memo_valid)
}

macro_rules! impl_file_memo_input {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl $crate::memo::MemoInput for $ty {
            fn write_memo_key(
                &self,
                writer: &mut $crate::memo::MemoKeyWriter<'_>,
            ) -> $crate::Result<()> {
                $crate::memo::write_file_memo_key(self, writer)
            }

            async fn memo_state(
                &self,
                previous: Option<&$crate::memo::MemoStateValue>,
            ) -> $crate::Result<Option<$crate::memo::MemoStateDecision>> {
                $crate::memo::file_memo_state(self, previous)
                    .await
                    .map(Some)
            }
        }
    };
}

pub(crate) use impl_file_memo_input;

fn system_time_nanos(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Tracked {
        id: &'static str,
        version: u64,
    }

    #[derive(Eq, Hash, PartialEq)]
    struct CollidingKey(&'static str);

    #[async_trait]
    impl MemoInput for Tracked {
        fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
            writer.write(&self.id)
        }

        async fn memo_state(
            &self,
            previous: Option<&MemoStateValue>,
        ) -> Result<Option<MemoStateDecision>> {
            let previous_version = previous.and_then(|value| value.deserialize::<u64>().ok());
            MemoStateDecision::new(&self.version, previous_version == Some(self.version)).map(Some)
        }
    }

    #[async_trait]
    impl MemoInput for CollidingKey {
        fn write_memo_key(&self, writer: &mut MemoKeyWriter<'_>) -> Result<()> {
            writer.write(&"deliberate collision")
        }
    }

    #[derive(crate::MemoInput)]
    struct TrackedBatch {
        name: String,
        items: Vec<Arc<Tracked>>,
    }

    #[derive(crate::MemoInput)]
    enum TrackedSource {
        Empty,
        Batch(TrackedBatch),
        Named { item: Tracked },
    }

    #[derive(crate::MemoInput)]
    enum ShadowedNames {
        Named { writer: String, previous: u64 },
    }

    fn fingerprint(value: &impl MemoInput) -> Fingerprint {
        memo_value_fingerprint(value).unwrap()
    }

    fn previous_state(decision: &MemoStateDecision) -> MemoStateValue {
        decision.state.clone()
    }

    #[test]
    fn recursive_sequence_fingerprints_are_framed_and_ordered() {
        assert_ne!(
            fingerprint(&vec!["ab".to_string(), "c".to_string()]),
            fingerprint(&vec!["a".to_string(), "bc".to_string()]),
        );
        assert_ne!(
            fingerprint(&vec!["a".to_string(), "b".to_string()]),
            fingerprint(&vec!["b".to_string(), "a".to_string()]),
        );
    }

    #[test]
    fn bytes_use_bulk_encoding_while_vec_u8_remains_a_sequence() {
        let value = bytes::Bytes::from_static(b"bulk bytes");
        let mut expected = Fingerprinter::default();
        serde::Serializer::serialize_bytes(&mut expected, value.as_ref()).unwrap();

        assert_eq!(fingerprint(&value), expected.into_fingerprint());
        assert_ne!(fingerprint(&value), fingerprint(&value.to_vec()));
    }

    #[test]
    fn all_advertised_integer_leaves_are_fingerprintable() {
        assert_ne!(fingerprint(&i128::MIN), fingerprint(&i128::MAX));
        assert_ne!(fingerprint(&u128::MIN), fingerprint(&u128::MAX));
        assert_ne!(fingerprint(&i128::MAX), fingerprint(&(i128::MAX as u128)));
    }

    #[test]
    fn tuple_identity_is_tagged_ordered_and_distinct_from_vec() {
        let tuple = ("a".to_string(), "b".to_string());
        assert_ne!(
            fingerprint(&tuple),
            fingerprint(&(tuple.1.clone(), tuple.0.clone()))
        );
        assert_ne!(fingerprint(&tuple), fingerprint(&vec![tuple.0, tuple.1]));
    }

    #[test]
    fn option_pointer_and_set_framing_are_structural() {
        assert_ne!(
            fingerprint(&None::<String>),
            fingerprint(&Vec::<String>::new())
        );
        assert_ne!(
            fingerprint(&None::<String>),
            fingerprint(&Some(String::new()))
        );

        let value = "value".to_string();
        assert_eq!(fingerprint(&value), fingerprint(&Box::new(value.clone())));
        assert_eq!(fingerprint(&value), fingerprint(&Arc::new(value)));

        let first = HashSet::from(["a".to_string(), "b".to_string()]);
        let mut second = HashSet::new();
        second.insert("b".to_string());
        second.insert("a".to_string());
        assert_eq!(fingerprint(&first), fingerprint(&second));
        let ordered = BTreeSet::from(["a".to_string(), "b".to_string()]);
        assert_eq!(fingerprint(&first), fingerprint(&ordered));
    }

    #[test]
    fn derive_tags_types_and_handles_reserved_named_enum_fields() {
        #[derive(crate::MemoInput)]
        struct First {
            value: String,
        }

        #[derive(crate::MemoInput)]
        struct Second {
            value: String,
        }

        assert_ne!(
            fingerprint(&First {
                value: "same".to_string(),
            }),
            fingerprint(&Second {
                value: "same".to_string(),
            }),
        );
        let _ = fingerprint(&ShadowedNames::Named {
            writer: "value".to_string(),
            previous: 1,
        });
    }

    #[test]
    fn hash_map_fingerprint_is_insertion_order_independent() {
        let first = HashMap::from([("a".to_string(), 1_u64), ("b".to_string(), 2)]);
        let mut second = HashMap::new();
        second.insert("b".to_string(), 2_u64);
        second.insert("a".to_string(), 1_u64);
        assert_eq!(fingerprint(&first), fingerprint(&second));
    }

    #[tokio::test]
    async fn unordered_identity_collision_forces_recomputation_without_error() {
        let values = HashMap::from([
            (CollidingKey("a"), "first".to_string()),
            (CollidingKey("b"), "second".to_string()),
        ]);
        let first = values.memo_state(None).await.unwrap().unwrap();
        assert!(!first.memo_valid);
        let second = values
            .memo_state(Some(&previous_state(&first)))
            .await
            .unwrap()
            .unwrap();
        assert!(!second.memo_valid);
    }

    #[tokio::test]
    async fn nested_sequence_preserves_child_state_validation() {
        let initial = vec![
            Tracked {
                id: "a",
                version: 1,
            },
            Tracked {
                id: "b",
                version: 1,
            },
        ];
        let first = initial.memo_state(None).await.unwrap().unwrap();
        assert!(!first.memo_valid);

        let unchanged = initial
            .memo_state(Some(&previous_state(&first)))
            .await
            .unwrap()
            .unwrap();
        assert!(unchanged.memo_valid);

        let changed = vec![
            Tracked {
                id: "a",
                version: 1,
            },
            Tracked {
                id: "b",
                version: 2,
            },
        ]
        .memo_state(Some(&previous_state(&unchanged)))
        .await
        .unwrap()
        .unwrap();
        assert!(!changed.memo_valid);
    }

    #[tokio::test]
    async fn unordered_map_reordering_preserves_child_state_alignment() {
        let mut initial = HashMap::new();
        initial.insert(
            "a".to_string(),
            Tracked {
                id: "a",
                version: 1,
            },
        );
        initial.insert(
            "b".to_string(),
            Tracked {
                id: "b",
                version: 1,
            },
        );
        let first = initial.memo_state(None).await.unwrap().unwrap();

        let mut reordered = HashMap::new();
        reordered.insert(
            "b".to_string(),
            Tracked {
                id: "b",
                version: 1,
            },
        );
        reordered.insert(
            "a".to_string(),
            Tracked {
                id: "a",
                version: 1,
            },
        );
        let unchanged = reordered
            .memo_state(Some(&previous_state(&first)))
            .await
            .unwrap()
            .unwrap();
        assert!(unchanged.memo_valid);

        reordered.get_mut("b").unwrap().version = 2;
        let changed = reordered
            .memo_state(Some(&previous_state(&unchanged)))
            .await
            .unwrap()
            .unwrap();
        assert!(!changed.memo_valid);
    }

    #[tokio::test]
    async fn derive_recurses_through_struct_enum_vec_and_arc() {
        let source = TrackedSource::Batch(TrackedBatch {
            name: "batch".to_string(),
            items: vec![Arc::new(Tracked {
                id: "item",
                version: 3,
            })],
        });
        let first = source.memo_state(None).await.unwrap().unwrap();
        let unchanged = source
            .memo_state(Some(&previous_state(&first)))
            .await
            .unwrap()
            .unwrap();
        assert!(unchanged.memo_valid);

        let changed = TrackedSource::Batch(TrackedBatch {
            name: "batch".to_string(),
            items: vec![Arc::new(Tracked {
                id: "item",
                version: 4,
            })],
        })
        .memo_state(Some(&previous_state(&unchanged)))
        .await
        .unwrap()
        .unwrap();
        assert!(!changed.memo_valid);

        assert_ne!(fingerprint(&TrackedSource::Empty), fingerprint(&source));
        assert_ne!(
            fingerprint(&TrackedSource::Named {
                item: Tracked {
                    id: "item",
                    version: 3,
                },
            }),
            fingerprint(&source),
        );
    }

    #[tokio::test]
    async fn malformed_nested_state_causes_a_miss_not_an_error() {
        let values = vec![Tracked {
            id: "item",
            version: 1,
        }];
        let malformed = MemoStateValue::from_serializable(&"not a nested state").unwrap();
        let decision = values.memo_state(Some(&malformed)).await.unwrap().unwrap();
        assert!(!decision.memo_valid);
    }

    #[tokio::test]
    async fn malformed_state_for_stateless_container_still_causes_one_miss() {
        let values = vec!["item".to_string()];
        let malformed = MemoStateValue::from_serializable(&"not a nested state").unwrap();
        let replacement = values.memo_state(Some(&malformed)).await.unwrap().unwrap();
        assert!(!replacement.memo_valid);

        let unchanged = values
            .memo_state(Some(&previous_state(&replacement)))
            .await
            .unwrap()
            .unwrap();
        assert!(unchanged.memo_valid);
    }

    #[tokio::test]
    async fn stateless_nested_values_do_not_store_state() {
        let values = vec![Some("a".to_string()), None];
        assert!(values.memo_state(None).await.unwrap().is_none());
    }

    #[cfg(all(feature = "serde_json", feature = "chrono", feature = "rust_decimal"))]
    #[test]
    fn ecosystem_schema_leaf_types_compose_in_derived_inputs() {
        #[derive(crate::MemoInput)]
        struct EcosystemLeaves {
            timestamp: chrono::DateTime<chrono::Utc>,
            date: chrono::NaiveDate,
            id: uuid::Uuid,
            json: serde_json::Value,
            decimal: rust_decimal::Decimal,
        }

        let value = EcosystemLeaves {
            timestamp: "2024-01-02T03:04:05Z".parse().unwrap(),
            date: chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            id: uuid::Uuid::nil(),
            json: serde_json::json!({"nested": [1, 2, 3]}),
            decimal: rust_decimal::Decimal::new(1234, 2),
        };
        let _ = fingerprint(&value);
    }
}
