use crate::engine::context::{
    ComponentProcessorContext, FnCallContext, FnCallMemo, FnCallMemoEntry,
};
use crate::engine::execution::read_fn_call_memo;
use crate::engine::profile::EngineProfile;
use crate::prelude::*;

use cocoindex_utils::fingerprint::Fingerprint;
use tokio::sync::OwnedRwLockReadGuard;

pub struct PendingFnCallMemo<Prof: EngineProfile> {
    // `FnCallMemoEntry` expected to be in Pending state.
    guard: tokio::sync::OwnedRwLockWriteGuard<FnCallMemoEntry<Prof>>,
}

impl<Prof: EngineProfile> PendingFnCallMemo<Prof> {
    pub fn resolve(
        mut self,
        fn_ctx: &FnCallContext,
        ret: impl FnOnce() -> Prof::FunctionData,
    ) -> bool {
        let memo_ret = fn_ctx.update(|inner| {
            Some(FnCallMemo {
                ret: ret(),
                child_components: std::mem::take(&mut inner.child_components),
                target_state_paths: std::mem::take(&mut inner.target_state_paths),
                dependency_memo_entries: std::mem::take(&mut inner.dependency_memo_entries),
                already_stored: false,
            })
        });
        let resolved = memo_ret.is_some();
        *self.guard = FnCallMemoEntry::Ready(memo_ret);
        resolved
    }
}

pub enum FnCallMemoGuard<Prof: EngineProfile> {
    Ready(tokio::sync::OwnedRwLockReadGuard<FnCallMemoEntry<Prof>, Option<FnCallMemo<Prof>>>),
    Pending(PendingFnCallMemo<Prof>),
}

pub async fn reserve_memoization<Prof: EngineProfile>(
    comp_exec_ctx: &ComponentProcessorContext<Prof>,
    memo_fp: Fingerprint,
) -> Result<FnCallMemoGuard<Prof>> {
    let mut try_write = false;
    loop {
        // We clone out the Arc so we don't hold any mutexes across `.await`.
        let memo_entry =
            comp_exec_ctx.update_building_state(|building_state| {
                match building_state.fn_call_memos.entry(memo_fp) {
                    std::collections::hash_map::Entry::Occupied(e) => Ok(e.get().clone()),
                    std::collections::hash_map::Entry::Vacant(e) => {
                        try_write = true;
                        let entry = Arc::new(tokio::sync::RwLock::new(FnCallMemoEntry::Pending));
                        e.insert(entry.clone());
                        Ok(entry)
                    }
                }
            })?;

        let result = if try_write {
            // If pending, attempt to become the resolver by acquiring a write lock.
            let mut guard = memo_entry.write_owned().await;
            if let FnCallMemoEntry::Pending = &*guard {
                let stored_fn_call_memo = read_fn_call_memo(comp_exec_ctx, memo_fp)?;
                if let Some(fn_call_memo) = stored_fn_call_memo {
                    *guard = FnCallMemoEntry::Ready(Some(fn_call_memo));
                }
            }
            match &mut *guard {
                FnCallMemoEntry::Ready(_) => {
                    let ready_guard =
                        tokio::sync::OwnedRwLockReadGuard::map(guard.downgrade(), |mem_entry| {
                            match mem_entry {
                                FnCallMemoEntry::Ready(memo) => memo,
                                _ => unreachable!(),
                            }
                        });
                    FnCallMemoGuard::Ready(ready_guard)
                }
                FnCallMemoEntry::Pending => FnCallMemoGuard::Pending(PendingFnCallMemo { guard }),
            }
        } else {
            let read_guard = memo_entry.read_owned().await;
            let ready_guard =
                OwnedRwLockReadGuard::try_map(read_guard, |mem_entry| match mem_entry {
                    FnCallMemoEntry::Ready(memo) => Some(memo),
                    _ => None,
                });
            match ready_guard {
                Ok(ready_guard) => FnCallMemoGuard::Ready(ready_guard),
                Err(_) => {
                    // Edge case: The initial call that creates the pending entry doesn't finish, e.g. it can be an exception.
                    // We need to read the entry from the map again and try to grab the write lock.
                    try_write = true;
                    continue;
                }
            }
        };
        return Ok(result);
    }
}
