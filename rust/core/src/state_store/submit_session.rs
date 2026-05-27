//! Per-component submit session: drives the state-store side of one
//! `submit()` call through pre-commit and commit phases.
//!
//! Spec: [`specs/store/submit_session.md`](../../../../specs/store/submit_session.md).
//!
//! ## Lifecycle
//!
//! Phase 1 (eager `__cex` upsert) is not a session method — see
//! `engine::execution::eager_existence_upsert`, which runs at the
//! start of every Build before the user processor. The session owns
//! phases 2 and 4:
//!
//! ```text
//!   [eager_existence_upsert]          ← Phase 1: eager __cex upsert
//!        ↓                              before user processor runs
//!   [user processor runs]
//!        ↓
//!   AppStore::begin_submit(component_path, process_token)
//!        ↓
//!   precommit_read(PrecommitReadPlan) ← Phase 2 step a: snapshot read of
//!                                       existing tracking info, target
//!                                       owners, and preempted-owner state
//!        ↓
//!   [engine reconciles in-memory, builds plan]
//!        ↓
//!   precommit_write(PrecommitWritePlan) ← Phase 2 step b: write tracking_info
//!                                       + owner upserts + preempted updates
//!        ↓
//!   [sink_apply runs]
//!        ↓
//!   commit(CommitPlan, reconciler)    ← Phase 4: apply finalized writes,
//!                                       invoke reconciler for the child-
//!                                       existence diff, all in one txn.
//!                                       Or:
//!   cleanup()                         ← Phase 4 failure: clear stage marker.
//!
//!   [Phase 5 + 6 driven separately via AppStore::cleanup_tombstone and
//!    AppStore::finalize_memoization — each its own small txn with no
//!    session-scoped state.]
//! ```

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use futures::future::BoxFuture;

use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, DbEntryKey, StablePathEntryKey, StablePathEntryTrackingInfo,
    StablePathNodeType, TargetStateOwnerInfo,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathRef};
use crate::state::stable_path_set::{ChildStablePathSet, StablePathSet};
use crate::state::target_state_path::TargetStatePath;
use crate::state_store::AppStore;
use crate::state_store::WriteTxn;

// ---------------------------------------------------------------------------
// Phase 2 — Pre-commit (split into read + write to bracket engine's reconcile)
// ---------------------------------------------------------------------------
//
// Phase 1 (eager `__cex` existence bit) is *not* a session phase. The
// engine writes the component's own existence row (and any missing
// ancestors) before the user processor runs — see
// `engine::execution::eager_existence_upsert`. Doing it outside the
// session keeps the session API focused on precommit/commit, and
// matches `internal_states.md §3.1`.

/// Inputs to [`SubmitSession::precommit_read`].
pub struct PrecommitReadPlan {
    /// Self path, for filtering "owner != self" entries before
    /// surfacing them as preempts.
    pub self_path: StablePath,
    /// This process's stage token (`u128`). Carried in the
    /// `pending_process_token` field of the on-disk tracking-info
    /// blob.
    pub self_token: u128,
    /// Target-state paths this component declared in the current build.
    /// Backend bulk-reads `__target` to learn current owners; for any
    /// owner != `self_path`, also bulk-reads that owner's `__track` row
    /// so the engine can decide preempt vs. PendingRetry.
    pub declared_target_states: Vec<TargetStatePath>,
}

/// Output of [`SubmitSession::precommit_read`].
pub struct PrecommitReadResult {
    /// Existing committed tracking-info bytes for `self_path`. `None`
    /// if no row yet.
    pub existing_tracking_info: Option<Vec<u8>>,
    /// Pending-stage token observed on `self_path` — `self_token` if
    /// no other writer is in flight, otherwise the other writer's
    /// token. The engine compares it to `self_token` to detect the
    /// "another process is in flight on this same component" case.
    /// (Intra-process is already serialized by the per-component
    /// semaphore, so a mismatch implies a different process.)
    pub post_stage_token: u128,
    /// For each declared target-state path: the current owner, or
    /// `None` if no row exists yet.
    pub current_owners: BTreeMap<TargetStatePath, Option<StablePath>>,
    /// For each unique non-self owner discovered in `current_owners`:
    /// their tracking-info bytes and pending-stage token. Engine
    /// combines `staged_token == Some(self_token)` with its own
    /// per-item `is_pending()` check to decide live-writer vs.
    /// preemptable.
    pub preempted_owner_states: BTreeMap<StablePath, OwnerStateForPreempt>,
}

/// One preempted other-component owner's relevant state.
pub struct OwnerStateForPreempt {
    /// Committed tracking-info bytes for that owner. `None` if the
    /// row is missing.
    pub tracking_info: Option<Vec<u8>>,
    /// The owner's `pending_process_token` from its tracking-info
    /// blob.
    pub staged_token: Option<u128>,
}

/// Inputs to [`SubmitSession::precommit_write`].
pub struct PrecommitWritePlan {
    /// Self path (echo of `PrecommitReadPlan::self_path`).
    pub self_path: StablePath,
    /// New `__track.value` bytes for this component. `None` skips the
    /// write (e.g. nothing to track yet).
    pub new_tracking_info: Option<Vec<u8>>,
    /// `(target_state_path, owner_component_path)` upserts on `__target`.
    pub target_owners_to_upsert: BTreeMap<TargetStatePath, StablePath>,
    /// For each preempted other-owner: their new tracking-info bytes
    /// (with the preempted item removed by the engine).
    pub preempted_owner_updates: BTreeMap<StablePath, Vec<u8>>,
}

// ---------------------------------------------------------------------------
// Phase 4 — Commit
// ---------------------------------------------------------------------------

/// Inputs to [`SubmitSession::commit`].
pub struct CommitPlan {
    /// Final `StablePathEntryTrackingInfo` bytes for self. `None` in
    /// `Delete` mode (backend deletes the `__track` row instead).
    pub new_tracking_info: Option<Vec<u8>>,
    /// Bulk upserts on `__target`.
    pub target_owners_to_upsert: Vec<(TargetStatePath, StablePath)>,
    /// Bulk deletes from `__target` (pruned-item cleanup).
    pub target_owners_to_delete: Vec<TargetStatePath>,
    /// If true, prefix-delete all `__fnmemo` rows for self before
    /// applying the writes/deletes below.
    pub fn_memo_clear_all_first: bool,
    pub fn_memo_writes: Vec<(Fingerprint, Vec<u8>)>,
    pub fn_memo_deletes: Vec<Fingerprint>,
    /// In-memory child tree after this build. Backend feeds it to
    /// `existence_reconciler` (see [`SubmitSession::commit`]) inside
    /// its commit txn, so the children-`__cex` read + tombstone
    /// writes happen atomically with the other commit writes.
    /// `None` skips existence reconciliation (e.g. `demote_component_only`).
    pub child_path_set: Option<Arc<ChildStablePathSet>>,
}

/// Callback the backend invokes inside its commit txn to run the
/// child-existence diff. Engine constructs this closure with all
/// captures it needs (component path, child_path_set, an `AppStore`
/// clone) and passes it to [`SubmitSession::commit`]. The closure
/// receives the open `WriteTxn` and walks the in-memory tree, reads
/// `__cex` per parent, writes deltas + tombstones — see
/// [`crate::engine::existence::reconcile_child_existence`].
///
/// Lifetime: the closure runs strictly inside the backend's commit
/// txn, so the `&'a mut WriteTxn<'env>` borrow is bounded by the
/// callback's await suspension scope. The body's future is `'a`-tied
/// so it can't outlive the borrow.
pub type ExistenceReconciler =
    Box<dyn for<'a, 'env> FnOnce(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<()>> + Send>;

// ---------------------------------------------------------------------------
// SubmitSession (concrete)
// ---------------------------------------------------------------------------

/// Per-component submit session. Drives Phase 2 (pre-commit read +
/// write) and Phase 4 (commit / cleanup) against the LMDB AppStore.
/// Open with [`AppStore::begin_submit`].
pub struct SubmitSession {
    app_store: AppStore,
    component_path: StablePath,
    process_token: u128,
}

impl SubmitSession {
    pub(in crate::state_store) fn new(
        app_store: AppStore,
        component_path: StablePath,
        process_token: u128,
    ) -> Self {
        Self {
            app_store,
            component_path,
            process_token,
        }
    }

    /// Phase 2 step a: snapshot read of existing tracking_info,
    /// declared target owners, and preempted-owner tracking-info bytes.
    /// Returns everything the engine needs to reconcile in-memory.
    ///
    /// LMDB uses MVCC, so the snapshot is consistent. Per-component
    /// intra-process exclusivity rules out a concurrent writer
    /// touching these rows between this read and the subsequent
    /// `precommit_write`.
    pub async fn precommit_read(&mut self, plan: PrecommitReadPlan) -> Result<PrecommitReadResult> {
        let rtxn = self.app_store.read_txn().await?;
        let db = self.app_store.db();

        // 1. Read existing tracking_info + extract pending token.
        let tracking_key = key_tracking_info(&plan.self_path)?;
        let raw = db.get(&rtxn, &tracking_key)?.map(<[u8]>::to_vec);
        let existing_pending = match raw.as_deref() {
            Some(bytes) => {
                let info: StablePathEntryTrackingInfo<'_> = from_msgpack_slice(bytes)?;
                info.pending_process_token
            }
            None => None,
        };
        let post_stage_token = existing_pending.unwrap_or(plan.self_token);

        // 2. Read current owners for each declared target.
        let mut current_owners: BTreeMap<TargetStatePath, Option<StablePath>> = BTreeMap::new();
        let mut preempted_set: BTreeSet<StablePath> = BTreeSet::new();
        for tsp in &plan.declared_target_states {
            let key = key_target_state_owner(tsp)?;
            let owner_path = match db.get(&rtxn, &key)? {
                Some(bytes) => {
                    let info: TargetStateOwnerInfo = from_msgpack_slice(bytes)?;
                    if info.component_path != plan.self_path {
                        preempted_set.insert(info.component_path.clone());
                    }
                    Some(info.component_path)
                }
                None => None,
            };
            current_owners.insert(tsp.clone(), owner_path);
        }

        // 3. Read tracking_info for each preempted owner.
        let mut preempted_owner_states: BTreeMap<StablePath, OwnerStateForPreempt> =
            BTreeMap::new();
        for owner_path in &preempted_set {
            let key = key_tracking_info(owner_path)?;
            let owner_raw = db.get(&rtxn, &key)?.map(<[u8]>::to_vec);
            let staged_token = match owner_raw.as_deref() {
                Some(bytes) => {
                    let info: StablePathEntryTrackingInfo<'_> = from_msgpack_slice(bytes)?;
                    info.pending_process_token
                }
                None => None,
            };
            preempted_owner_states.insert(
                owner_path.clone(),
                OwnerStateForPreempt {
                    tracking_info: owner_raw,
                    staged_token,
                },
            );
        }

        Ok(PrecommitReadResult {
            existing_tracking_info: raw,
            post_stage_token,
            current_owners,
            preempted_owner_states,
        })
    }

    /// Phase 2 step b: apply engine's writes (tracking_info + target
    /// owners + preempted-owner updates). Routed through the
    /// single-writer batcher.
    pub async fn precommit_write(&mut self, plan: PrecommitWritePlan) -> Result<()> {
        let app_store = self.app_store.clone();
        self.app_store
            .run_in_batcher(move |wtxn| {
                Box::pin(async move {
                    if let Some(bytes) = plan.new_tracking_info.as_ref() {
                        app_store
                            .write_tracking_info_raw(wtxn, &plan.self_path, bytes)
                            .await?;
                    }
                    for (target_path, owner_path) in plan.target_owners_to_upsert {
                        app_store
                            .upsert_target_state_owner(wtxn, &target_path, &owner_path)
                            .await?;
                    }
                    for (owner_path, bytes) in plan.preempted_owner_updates {
                        app_store
                            .write_tracking_info_raw(wtxn, &owner_path, &bytes)
                            .await?;
                    }
                    Ok(())
                })
            })
            .await
    }

    /// Phase 4 success: apply finalized writes, then invoke
    /// `existence_reconciler` for the child-existence diff — all in
    /// one write txn so the reconciler's per-parent `__cex` reads see
    /// the same snapshot as the plan writes that just happened.
    ///
    /// Consumes self.
    pub async fn commit(
        self,
        plan: CommitPlan,
        existence_reconciler: ExistenceReconciler,
    ) -> Result<()> {
        let app_store = self.app_store.clone();
        let component_path = self.component_path.clone();
        self.app_store
            .storage
            .run_txn(move |wtxn: &mut WriteTxn<'_>| {
                Box::pin(async move {
                    if let Some(bytes) = plan.new_tracking_info.as_ref() {
                        app_store
                            .write_tracking_info_raw(wtxn, &component_path, bytes)
                            .await?;
                    } else {
                        app_store
                            .delete_tracking_info(wtxn, &component_path)
                            .await?;
                    }
                    for (target_path, owner_path) in plan.target_owners_to_upsert {
                        app_store
                            .upsert_target_state_owner(wtxn, &target_path, &owner_path)
                            .await?;
                    }
                    for target_path in plan.target_owners_to_delete {
                        app_store
                            .delete_target_state_owner(wtxn, &target_path)
                            .await?;
                    }
                    if plan.fn_memo_clear_all_first {
                        app_store.delete_all_fn_memos(wtxn, &component_path).await?;
                    }
                    for fp in plan.fn_memo_deletes {
                        app_store.delete_fn_memo(wtxn, &component_path, fp).await?;
                    }
                    for (fp, bytes) in plan.fn_memo_writes {
                        app_store
                            .write_fn_memo_raw(wtxn, &component_path, fp, &bytes)
                            .await?;
                    }
                    existence_reconciler(wtxn).await?;
                    Ok(())
                })
            })
            .await
    }

    /// Phase 4 failure: clear the stage marker so a subsequent
    /// pre-commit doesn't see our stale token as a live in-flight
    /// writer. Idempotent. Consumes self.
    pub async fn cleanup(self) -> Result<()> {
        self.app_store
            .clear_staged_tracking(&self.component_path, self.process_token)
            .await
    }
}

// LMDB key encoders for the session's snapshot reads. Mirror the
// private helpers in `app_store.rs` (kept tiny; if they ever drift,
// fix at the source).

fn key_tracking_info(path: &StablePath) -> Result<Vec<u8>> {
    storekey::encode_vec(&DbEntryKey::StablePath(
        path.clone(),
        StablePathEntryKey::TrackingInfo,
    ))
    .map_err(|e| internal_error!("encode tracking_info key: {e}"))
}

fn key_target_state_owner(path: &TargetStatePath) -> Result<Vec<u8>> {
    storekey::encode_vec(&DbEntryKey::TargetState(path.clone()))
        .map_err(|e| internal_error!("encode target_state_owner key: {e}"))
}

// ---------------------------------------------------------------------------
// Child-existence reconciliation
// ---------------------------------------------------------------------------

/// Walk the in-memory child tree under `component_path`, diff against
/// the on-disk `__cex` rows per parent, and apply the deltas:
///
/// * declared-not-present → write `__cex` row, recurse into Directory.
/// * present-not-declared → delete `__cex` row; for Component leaves,
///   write a tombstone under `component_path`; for Directory existing
///   nodes, recurse into the on-disk subtree to find more leaves to
///   tombstone.
/// * declared+present with changed node_type → upsert `__cex` row.
/// * declared+present, both Directory → recurse with merged children.
///
/// Sibling-by-sibling sorted-merge per level so each `__cex` read is
/// O(N children) per parent and the writes are bounded by changes.
///
/// Used by [`SubmitSession::commit`] implementations: the backend opens
/// the commit txn, then invokes the engine-supplied
/// [`ExistenceReconciler`] which in turn calls this function. Lives on
/// the storage side because the entire logic only depends on the state
/// layer (no engine concepts).
pub async fn reconcile_child_existence<'a>(
    wtxn: &mut WriteTxn<'_>,
    app_store: &AppStore,
    component_path: &StablePath,
    child_path_set: Option<&'a ChildStablePathSet>,
) -> Result<()> {
    let mut queue: VecDeque<Level<'a>> = VecDeque::new();
    queue.push_back(Level {
        path: component_path.clone(),
        child_path_set,
    });

    let mut buffered_tombstones: Vec<StablePath> = Vec::new();
    while let Some(level) = queue.pop_front() {
        let mut curr_iter = level
            .child_path_set
            .into_iter()
            .flat_map(|set| set.children.iter());
        let existing_children = app_store
            .list_child_existence_in_txn(&mut *wtxn, &level.path)
            .await?;
        let mut existing_iter = existing_children.into_iter();

        let mut curr_next = curr_iter.next();
        let mut existing_next = existing_iter.next();
        let mut children_to_add: Vec<(&StableKey, &StablePathSet)> = Vec::new();

        loop {
            match (&curr_next, &existing_next) {
                (None, None) => break,
                (Some(_), None) => {
                    if let Some(entry) = curr_next.take() {
                        children_to_add.push(entry);
                    }
                    children_to_add.extend(curr_iter.by_ref());
                    break;
                }
                (None, Some(_)) => {
                    if let Some((key, info)) = existing_next.take() {
                        app_store
                            .delete_child_existence(wtxn, &level.path, &key)
                            .await?;
                        del_child(
                            &key,
                            &info,
                            &level.path,
                            component_path,
                            &mut queue,
                            &mut buffered_tombstones,
                        )?;
                    }
                    for (key, info) in existing_iter.by_ref() {
                        app_store
                            .delete_child_existence(wtxn, &level.path, &key)
                            .await?;
                        del_child(
                            &key,
                            &info,
                            &level.path,
                            component_path,
                            &mut queue,
                            &mut buffered_tombstones,
                        )?;
                    }
                    break;
                }
                (Some((curr_key, _)), Some((existing_key, _))) => match curr_key.cmp(&existing_key)
                {
                    Ordering::Less => {
                        children_to_add.push(
                            curr_next
                                .take()
                                .ok_or_else(|| internal_error!("invariance violation"))?,
                        );
                        curr_next = curr_iter.next();
                    }
                    Ordering::Greater => {
                        let (key, info) = existing_next
                            .take()
                            .ok_or_else(|| internal_error!("invariance violation"))?;
                        app_store
                            .delete_child_existence(wtxn, &level.path, &key)
                            .await?;
                        del_child(
                            &key,
                            &info,
                            &level.path,
                            component_path,
                            &mut queue,
                            &mut buffered_tombstones,
                        )?;
                        existing_next = existing_iter.next();
                    }
                    Ordering::Equal => {
                        let (curr_key, curr_path_set) = curr_next
                            .take()
                            .ok_or_else(|| internal_error!("invariance violation"))?;
                        let (_, existing_info) = existing_next
                            .take()
                            .ok_or_else(|| internal_error!("invariance violation"))?;
                        let new_node_type = node_type_for(curr_path_set);

                        if existing_info.node_type != new_node_type {
                            app_store
                                .write_child_existence(
                                    wtxn,
                                    &level.path,
                                    curr_key,
                                    &ChildExistenceInfo {
                                        node_type: new_node_type,
                                    },
                                )
                                .await?;
                        }

                        if let StablePathSet::Directory(curr_dir_set) = curr_path_set {
                            if existing_info.node_type == StablePathNodeType::Component {
                                buffered_tombstones.push(
                                    relative_to(&level.path, component_path)?
                                        .concat_part(curr_key.clone()),
                                );
                            }
                            queue.push_back(Level {
                                path: level.path.concat_part(curr_key.clone()),
                                child_path_set: Some(curr_dir_set),
                            });
                        }
                        curr_next = curr_iter.next();
                        existing_next = existing_iter.next();
                    }
                },
            }
        }

        for (stable_key, path_set) in children_to_add {
            let node_type = node_type_for(path_set);
            app_store
                .write_child_existence(
                    wtxn,
                    &level.path,
                    stable_key,
                    &ChildExistenceInfo { node_type },
                )
                .await?;
            if let StablePathSet::Directory(child_path_set) = path_set {
                queue.push_back(Level {
                    path: level.path.concat_part(stable_key.clone()),
                    child_path_set: Some(child_path_set),
                });
            }
        }

        for relative_path in std::mem::take(&mut buffered_tombstones) {
            app_store
                .write_tombstone(wtxn, component_path, &relative_path)
                .await?;
        }
    }
    Ok(())
}

struct Level<'a> {
    path: StablePath,
    child_path_set: Option<&'a ChildStablePathSet>,
}

fn del_child<'a>(
    stable_key: &StableKey,
    info: &ChildExistenceInfo,
    parent_path: &StablePath,
    component_path: &StablePath,
    queue: &mut VecDeque<Level<'a>>,
    buffered_tombstones: &mut Vec<StablePath>,
) -> Result<()> {
    match info.node_type {
        StablePathNodeType::Directory => {
            queue.push_back(Level {
                path: parent_path.concat_part(stable_key.clone()),
                child_path_set: None,
            });
        }
        StablePathNodeType::Component => {
            buffered_tombstones
                .push(relative_to(parent_path, component_path)?.concat_part(stable_key.clone()));
        }
    }
    Ok(())
}

fn node_type_for(path_set: &StablePathSet) -> StablePathNodeType {
    match path_set {
        StablePathSet::Directory(_) => StablePathNodeType::Directory,
        StablePathSet::Component => StablePathNodeType::Component,
    }
}

fn relative_to<'p>(path: &'p StablePath, base: &StablePath) -> Result<StablePathRef<'p>> {
    path.as_ref().strip_parent(base.as_ref())
}
