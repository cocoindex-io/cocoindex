use crate::engine::context::{
    ComponentProcessorContext, FnCallContext, FnCallMemo, FnCallMemoEntry, MemoStatesPayload,
};
use crate::engine::execution::read_fn_call_memo;
use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use cocoindex_utils::fingerprint::Fingerprint;

/// Builds a `FnCallMemo` from a completed function call's context, return value, and states.
fn build_fn_call_memo<Prof: EngineProfile>(
    fn_ctx: &FnCallContext,
    ret: Prof::FunctionData,
    memo_states: MemoStatesPayload<Prof>,
) -> Option<FnCallMemo<Prof>> {
    fn_ctx.update(|inner| {
        let mut logic_deps = inner.fn_logic_deps.clone();
        logic_deps.extend(inner.context_tracked_deps.iter().cloned());
        Some(FnCallMemo {
            ret,
            target_state_paths: inner.target_state_paths.clone(),
            dependency_memo_entries: inner.dependency_memo_entries.clone(),
            logic_deps,
            memo_states: memo_states.positional,
            context_memo_states: memo_states.by_context_fp,
            already_stored: false,
        })
    })
}

/// Guard for a function-call memo entry. Always holds a write lock.
///
/// Returned by [`reserve_memoization`]. Use [`cached()`](Self::cached) to check for a
/// cache hit, then either return the cached value directly or call
/// [`resolve()`](Self::resolve) after (re-)execution. Dropping the guard without calling
/// `resolve()` releases the write lock and leaves the entry unchanged.
///
/// Concurrent callers for the same fingerprint serialize through the write lock. This
/// prevents duplicate re-execution when memo states are stale: only one caller validates
/// and potentially re-executes per key; subsequent callers see the updated result.
pub struct FnCallMemoGuard<Prof: EngineProfile> {
    guard: tokio::sync::OwnedRwLockWriteGuard<FnCallMemoEntry<Prof>>,
}

/// Cached memo view returned on a cache hit.
pub struct CachedFnCallMemo<'a, Prof: EngineProfile> {
    pub ret: &'a Prof::FunctionData,
    pub memo_states: &'a [Prof::FunctionData],
    pub context_memo_states: &'a [(Fingerprint, Vec<Prof::FunctionData>)],
}

impl<Prof: EngineProfile> FnCallMemoGuard<Prof> {
    /// Returns the cached return value and memo states if this is a cache hit.
    /// Returns `None` on cache miss or if memoization is disabled for this entry.
    pub fn cached(&self) -> Option<CachedFnCallMemo<'_, Prof>> {
        match &*self.guard {
            FnCallMemoEntry::Ready(Some(memo)) => Some(CachedFnCallMemo {
                ret: &memo.ret,
                memo_states: &memo.memo_states,
                context_memo_states: &memo.context_memo_states,
            }),
            _ => None,
        }
    }

    /// Update memo states on a cache hit without re-execution.
    ///
    /// Used when the state function indicates `can_reuse=true` but the state value itself
    /// has changed (e.g. mtime changed but content fingerprint is the same). Sets
    /// `already_stored = false` so the entry gets persisted with updated states at
    /// finalization time.
    pub fn update_memo_states(&mut self, memo_states: MemoStatesPayload<Prof>) {
        if let FnCallMemoEntry::Ready(Some(ref mut memo)) = *self.guard {
            memo.memo_states = memo_states.positional;
            memo.context_memo_states = memo_states.by_context_fp;
            memo.already_stored = false;
        }
    }

    /// Store the function's return value and memo states after execution.
    ///
    /// Works for both cache miss (initial execution) and cache hit with stale memo states
    /// (re-execution after validation). Consumes `self`, transitioning the entry to `Ready`
    /// and releasing the write lock.
    pub fn resolve(
        mut self,
        fn_ctx: &FnCallContext,
        ret: impl FnOnce() -> Prof::FunctionData,
        memo_states: MemoStatesPayload<Prof>,
    ) -> Result<bool> {
        let has_child_components = fn_ctx.update(|inner| inner.has_child_components);
        if has_child_components {
            *self.guard = FnCallMemoEntry::Ready(None);
            client_bail!(
                "A function with memo=True mounted child components. \
                 Either mount the function as a component, or set memo=False."
            );
        }
        let memo_ret = build_fn_call_memo(fn_ctx, ret(), memo_states);
        let resolved = memo_ret.is_some();
        *self.guard = FnCallMemoEntry::Ready(memo_ret);
        Ok(resolved)
    }
}

/// Reserve a memoization slot for a function call, returning a guard.
///
/// If a cached result exists (from a previous run or earlier in this run), the guard's
/// [`cached()`](FnCallMemoGuard::cached) method will return it. Otherwise, the caller
/// should execute the function and call [`resolve()`](FnCallMemoGuard::resolve).
pub async fn reserve_memoization<Prof: EngineProfile>(
    comp_exec_ctx: &ComponentProcessorContext<Prof>,
    memo_fp: Fingerprint,
) -> Result<FnCallMemoGuard<Prof>> {
    // Clone the Arc so we don't hold building_state's mutex across `.await`.
    let memo_entry = comp_exec_ctx.update_building_state(|building_state| {
        Ok(building_state
            .fn_call_memos
            .entry(memo_fp)
            .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(FnCallMemoEntry::Pending)))
            .clone())
    })?;

    let mut guard = memo_entry.write_owned().await;

    // If still pending (first caller, or retry after a previous caller failed),
    // try loading from the database.
    if let FnCallMemoEntry::Pending = &*guard {
        if !comp_exec_ctx.full_reprocess() {
            if let Some(fn_call_memo) = read_fn_call_memo(comp_exec_ctx, memo_fp)? {
                *guard = FnCallMemoEntry::Ready(Some(fn_call_memo));
            }
        }
    }

    Ok(FnCallMemoGuard { guard })
}
