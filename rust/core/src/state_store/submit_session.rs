//! Per-component submit lifecycle on the state-store side: the engine
//! drives Phase 2 (precommit) and Phase 4 (commit / cleanup) through
//! three [`AppStore`] methods — `precommit`, `commit`,
//! `clear_stage_marker`.
//!
//! Spec: [`specs/store/submit_session.md`](../../../../specs/store/submit_session.md).
//!
//! ## Two scopes
//!
//! The lifecycle splits into two **disjoint scopes** with different
//! ownership rules:
//!
//! - **Txn-scoped** — [`PrecommitSession`] is a *value* that lives only
//!   between BEGIN and COMMIT of the precommit txn. Its two methods
//!   (`precommit_read`, `precommit_claim_targets`) are meaningful only
//!   while the txn is open; holding `&mut PrecommitSession` is the
//!   type-level proof of that. Per-attempt mutable state (`paths_to_claim`
//!   stash) lives directly on `&mut self` — no `Mutex`, no `Arc` —
//!   because no other task ever touches it.
//! - **Call-scoped on [`AppStore`]** — `precommit`, `commit`,
//!   `clear_stage_marker` are inherent methods on `AppStore` (the per-app
//!   handle). Each call is a standalone, self-contained operation
//!   parameterized by `component_path` (and, for `clear_stage_marker`,
//!   `process_token`) and opens/closes its own txn(s). No persistent
//!   submit handle threads across the three methods — what ties them
//!   together is the engine code in `engine::execution::submit`, which
//!   calls them with matching args.
//!
//! ## Lifecycle
//!
//! Phase 1 (eager `__cex` upsert) is not an `AppStore`-submit method —
//! see `engine::execution::eager_existence_upsert`, which runs at the
//! start of every Build before the user processor. The `AppStore` owns
//! phases 2 and 4:
//!
//! ```text
//!   [eager_existence_upsert]          ← Phase 1: eager __cex upsert
//!        ↓                              before user processor runs
//!   [user processor runs]
//!        ↓
//!   AppStore::precommit(component_path,
//!     callback(&mut WriteTxn, &mut PrecommitSession) -> Option<(plan, output)>)
//!     ├─ opens batched precommit WTxn (single-writer, coalesced)
//!     ├─ engine callback runs inside it:
//!     │   ├─ session.precommit_read
//!     │   ├─ [engine reconciles in-memory]
//!     │   ├─ session.precommit_claim_targets
//!     │   └─ → Some((plan, output)) | None (PendingRetry)
//!     └─ applies plan writes + commits, or contributes no writes on None.
//!        ↓
//!   [sink_apply runs]
//!        ↓
//!   AppStore::commit(component_path, CommitPlan, reconciler)
//!                                     ← Phase 4 success: open commit txn,
//!                                       apply finalized writes, invoke
//!                                       reconciler for child-existence
//!                                       diff, COMMIT.
//!                                     Or:
//!   AppStore::clear_stage_marker(component_path, process_token)
//!                                     ← Phase 4 failure: clear stage marker.
//!
//!   [Phase 5 + 6 driven separately via AppStore methods — each its own
//!    small txn with no submit-scoped state.]
//! ```

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use futures::future::BoxFuture;

use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, StablePathEntryTrackingInfo, StablePathNodeType,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathRef};
use crate::state::stable_path_set::{ChildStablePathSet, StablePathSet};
use crate::state::target_state_path::TargetStatePath;
use crate::state_store::AppStore;
use crate::state_store::WriteTxn;

// ---------------------------------------------------------------------------
// Phase 2 — Pre-commit plans + session
// ---------------------------------------------------------------------------
//
// Phase 1 (eager `__cex` existence bit) is *not* a session phase. The
// engine writes the component's own existence row (and any missing
// ancestors) before the user processor runs — see
// `engine::execution::eager_existence_upsert`. Doing it outside the
// session keeps the AppStore API focused on precommit/commit and
// matches `internal_states.md §3.1`.

/// Inputs to [`PrecommitSession::precommit_read`].
pub struct PrecommitReadPlan {
    /// Self path. Stage marker scope.
    pub self_path: StablePath,
    /// This process's stage token. Stored in the on-disk tracking-info
    /// blob's `pending_process_token` field.
    pub self_token: u128,
}

/// Output of [`PrecommitSession::precommit_read`]. Pure read — the
/// engine uses `existing_tracking_info` to determine which declared
/// target-state paths are new (i.e. not already self-owned in
/// `__target`) before calling
/// [`PrecommitSession::precommit_claim_targets`].
pub struct PrecommitReadResult {
    /// Existing committed tracking-info bytes for `self_path`. `None`
    /// if no row yet.
    pub existing_tracking_info: Option<Vec<u8>>,
    /// Pending-stage token observed on `self_path` — `self_token` if no
    /// other writer is in flight, otherwise the other writer's token.
    /// The engine compares it to `self_token` to detect the "another
    /// process is in flight on this same component" case. (Intra-process
    /// is already serialized by the per-component semaphore, so a
    /// mismatch implies a different process.)
    pub post_stage_token: u128,
}

/// Inputs to [`PrecommitSession::precommit_claim_targets`].
pub struct PrecommitClaimTargetsPlan {
    pub self_path: StablePath,
    /// Subset of the engine's declared target-state paths that the
    /// engine knows are **not** already in self's existing tracking
    /// record (i.e. not already self-owned in `__target`). Under
    /// per-component exclusivity, self's tracking record is the
    /// authoritative source for what self owns in `__target`, so
    /// already-owned paths don't need a touch and we save the lookup.
    pub paths_to_claim: Vec<TargetStatePath>,
}

/// Output of [`PrecommitSession::precommit_claim_targets`].
pub struct PrecommitClaimTargetsResult {
    /// For each entry in `paths_to_claim`: the prior owner that the
    /// claim displaced.
    ///
    /// - `None` — no row existed before; fresh insert.
    /// - `Some(self_path)` — `__target` already held self (crash-recovery
    ///   path: a previous attempt landed the upsert but didn't finalize).
    /// - `Some(other)` — cross-component preempt; engine consults
    ///   `preempted_owner_states` to decide PendingRetry vs. takeover.
    ///
    /// Paths **not** in `paths_to_claim` are absent from the map; the
    /// engine treats their owner as self by construction.
    pub prior_owners: BTreeMap<TargetStatePath, Option<StablePath>>,
    /// For each unique non-self owner discovered in `prior_owners`:
    /// their tracking-info bytes and pending-stage token. Engine
    /// combines `staged_token == Some(self_token)` with its own per-item
    /// `is_pending()` check to decide live-writer vs. preemptable.
    pub preempted_owner_states: BTreeMap<StablePath, OwnerStateForPreempt>,
}

/// One preempted other-component owner's relevant state.
pub struct OwnerStateForPreempt {
    /// Committed tracking-info bytes for that owner. `None` if the row
    /// is missing.
    pub tracking_info: Option<Vec<u8>>,
    /// The owner's `pending_process_token` from its tracking-info blob.
    pub staged_token: Option<u128>,
}

/// Engine's "go ahead and commit" output from the precommit callback.
/// The backend applies these writes inside the same txn the callback
/// ran in, then commits.
///
/// `__target` claims do **not** appear here — they're already known to
/// the AppStore from `precommit_claim_targets` (LMDB stashed
/// `paths_to_claim` on the `PrecommitSession`; the apply step drains
/// it).
pub struct PrecommitWritePlan {
    /// Self path.
    pub self_path: StablePath,
    /// New tracking-info bytes for this component. `None` skips the
    /// write (e.g. nothing to track yet).
    pub new_tracking_info: Option<Vec<u8>>,
    /// For each preempted other-owner: their new tracking-info bytes
    /// (with the preempted item removed by the engine).
    pub preempted_owner_updates: BTreeMap<StablePath, Vec<u8>>,
}

// ---------------------------------------------------------------------------
// PrecommitSession — txn-scoped Phase 2 calls
// ---------------------------------------------------------------------------

/// Txn-scoped handle for the Phase 2 read calls. Lives only inside the
/// precommit txn body opened by [`AppStore::precommit`]; holding the
/// `&mut PrecommitSession` is the type-level proof that the txn is open.
///
/// Per-attempt mutable state (`paths_to_claim`) lives directly on the
/// `&mut self` — no `Mutex`, no `Arc` — because the engine callback is
/// the only caller and runs single-threaded inside the txn body.
pub struct PrecommitSession {
    app_store: AppStore,
    /// Populated by [`Self::precommit_claim_targets`] and drained by
    /// [`AppStore::precommit`]'s apply step. LMDB has no per-body
    /// savepoints, so `precommit_claim_targets` can't write the
    /// `__target` upserts directly — if the engine then returns `None`
    /// (PendingRetry), those writes would already be in the WTxn with
    /// no way to roll them back. The apply step is the first point
    /// where the go/no-go is known, so the writes wait there.
    paths_to_claim: Option<Vec<TargetStatePath>>,
}

impl PrecommitSession {
    fn new(app_store: AppStore) -> Self {
        Self {
            app_store,
            paths_to_claim: None,
        }
    }

    /// Phase 2 step a: stage self's tracking-info row and return the
    /// existing committed `value` plus the pending-stage token. Pure
    /// read in LMDB (single-writer; the next `__track` write happens at
    /// the apply step); `__target` is not touched here so the engine
    /// can use `existing_tracking_info` to filter the upcoming claim
    /// set down to paths self doesn't already own.
    pub async fn precommit_read(
        &mut self,
        wtxn: &mut WriteTxn<'_>,
        plan: PrecommitReadPlan,
    ) -> Result<PrecommitReadResult> {
        // Read tracking-info from the engine-owned WTxn. Under
        // per-component exclusivity, the row can't be modified by
        // another writer between phases, but reading from the WTxn gets
        // us read-your-own-writes for free.
        let raw = self
            .app_store
            .read_tracking_info_in_txn(wtxn, &plan.self_path)
            .await?;
        let existing_pending = match raw.as_deref() {
            Some(bytes) => {
                let info: StablePathEntryTrackingInfo<'_> = from_msgpack_slice(bytes)?;
                info.pending_process_token
            }
            None => None,
        };
        let post_stage_token = existing_pending.unwrap_or(plan.self_token);

        Ok(PrecommitReadResult {
            existing_tracking_info: raw,
            post_stage_token,
        })
    }

    /// Phase 2 step b: read prior owners and preempted-owner tracking
    /// rows for `paths_to_claim`. The actual `__target` upserts are
    /// deferred to [`AppStore::precommit`]'s apply step (LMDB has no
    /// per-body savepoints — if the engine then returns `None`, those
    /// writes can't be rolled back, so we wait until the go/no-go is
    /// known). The reads here use the engine-owned WTxn for
    /// read-your-own-writes.
    pub async fn precommit_claim_targets(
        &mut self,
        wtxn: &mut WriteTxn<'_>,
        plan: PrecommitClaimTargetsPlan,
    ) -> Result<PrecommitClaimTargetsResult> {
        self.paths_to_claim = Some(plan.paths_to_claim.clone());

        if plan.paths_to_claim.is_empty() {
            return Ok(PrecommitClaimTargetsResult {
                prior_owners: BTreeMap::new(),
                preempted_owner_states: BTreeMap::new(),
            });
        }

        let mut prior_owners: BTreeMap<TargetStatePath, Option<StablePath>> = BTreeMap::new();
        let mut preempted_set: BTreeSet<StablePath> = BTreeSet::new();
        for tsp in &plan.paths_to_claim {
            let prior_owner = self
                .app_store
                .read_target_state_owner_in_txn(wtxn, tsp)
                .await?
                .map(|info| info.component_path);
            if let Some(ref op) = prior_owner
                && op != &plan.self_path
            {
                preempted_set.insert(op.clone());
            }
            prior_owners.insert(tsp.clone(), prior_owner);
        }

        let mut preempted_owner_states: BTreeMap<StablePath, OwnerStateForPreempt> =
            BTreeMap::new();
        for owner_path in &preempted_set {
            let owner_raw = self
                .app_store
                .read_tracking_info_in_txn(wtxn, owner_path)
                .await?;
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

        Ok(PrecommitClaimTargetsResult {
            prior_owners,
            preempted_owner_states,
        })
    }
}

// ---------------------------------------------------------------------------
// Phase 4 — Commit
// ---------------------------------------------------------------------------

/// Inputs to [`AppStore::commit`].
pub struct CommitPlan {
    /// Final tracking-info bytes for self. `None` in `Delete` mode
    /// (AppStore deletes the row instead).
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
    /// If true, prefix-delete all user-state rows for self before writing.
    pub user_state_clear_all_first: bool,
    pub user_state_writes: Vec<(StableKey, Vec<u8>)>,
    pub user_state_deletes: Vec<StableKey>,
    /// In-memory child tree after this build. AppStore feeds it to
    /// `existence_reconciler` (see [`AppStore::commit`]) inside its
    /// commit txn, so the children-`__cex` read + tombstone writes
    /// happen atomically with the other commit writes. `None` skips
    /// existence reconciliation (e.g. `demote_component_only`).
    pub child_path_set: Option<Arc<ChildStablePathSet>>,
}

/// Callback the AppStore invokes inside its commit txn to run the
/// child-existence diff. Engine constructs this closure with all
/// captures it needs (component path, child_path_set, an `AppStore`
/// clone) and passes it to [`AppStore::commit`]. The closure receives
/// the open `WriteTxn` and walks the in-memory tree, reads `__cex` per
/// parent, writes deltas + tombstones — see
/// [`reconcile_child_existence`].
///
/// Lifetime: the closure runs strictly inside the commit txn, so the
/// `&'a mut WriteTxn<'env>` borrow is bounded by the callback's await
/// suspension scope. The body's future is `'a`-tied so it can't outlive
/// the borrow.
///
/// `Fn` (not `FnOnce`) so a backend that re-runs its commit txn can
/// invoke the reconciler more than once; it therefore clones or
/// `Arc`-shares its captures rather than moving them in. The LMDB
/// AppStore invokes it exactly once.
pub type ExistenceReconciler =
    Box<dyn for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<()>> + Send + Sync>;

// ---------------------------------------------------------------------------
// AppStore methods — Phase 2 driver + Phase 4 commit / cleanup
// ---------------------------------------------------------------------------

impl AppStore {
    /// Phase 2 driver. Opens a batched precommit WTxn, runs `callback`
    /// inside it with a fresh [`PrecommitSession`], applies the
    /// returned [`PrecommitWritePlan`] and commits — or contributes no
    /// writes if the callback returns `None` (PendingRetry; the batched
    /// WTxn still commits whatever other bodies wrote).
    ///
    /// Routes through [`crate::state_store::Storage::run_txn`] so
    /// concurrent precommits coalesce into one WTxn → one fsync (per
    /// AGENTS.md "LMDB write paths"). LMDB has no savepoints — to
    /// "abort" on PendingRetry, the body contributes no writes.
    ///
    /// The callback is `Fn` (not `FnOnce`) for shape parity with
    /// retry-capable backends; LMDB's batcher never retries the body,
    /// so it's invoked at most once per call here. Captures consumed by
    /// the body must be cloned inside the closure (typically a few
    /// `Arc::clone`s).
    pub async fn precommit<T, F>(
        &self,
        component_path: &StablePath,
        callback: F,
    ) -> Result<Option<T>>
    where
        T: Send + 'static,
        F: for<'a, 'env> Fn(
                &'a mut WriteTxn<'env>,
                &'a mut PrecommitSession,
            ) -> BoxFuture<'a, Result<Option<(PrecommitWritePlan, T)>>>
            + Send
            + Sync
            + 'static,
    {
        let app_store = self.clone();
        let component_path_outer = component_path.clone();
        let callback = Arc::new(callback);

        self.storage
            .run_txn(move |wtxn: &mut WriteTxn<'_>| {
                let app_store = app_store.clone();
                let component_path = component_path_outer.clone();
                let callback = Arc::clone(&callback);
                Box::pin(async move {
                    let mut session = PrecommitSession::new(app_store.clone());
                    let cb_result = callback(wtxn, &mut session).await?;
                    match cb_result {
                        Some((plan, output)) => {
                            // Apply __target claims (the work deferred from
                            // `precommit_claim_targets`).
                            if let Some(paths) = session.paths_to_claim.take() {
                                for path in paths {
                                    app_store
                                        .upsert_target_state_owner(wtxn, &path, &component_path)
                                        .await?;
                                }
                            }
                            if let Some(bytes) = plan.new_tracking_info.as_ref() {
                                app_store
                                    .write_tracking_info_raw(wtxn, &plan.self_path, bytes)
                                    .await?;
                            }
                            for (owner_path, bytes) in plan.preempted_owner_updates {
                                app_store
                                    .write_tracking_info_raw(wtxn, &owner_path, &bytes)
                                    .await?;
                            }
                            Ok(Some(output))
                        }
                        None => Ok(None),
                    }
                })
            })
            .await
    }

    /// Phase 4 success: open commit txn, apply finalized writes, invoke
    /// `existence_reconciler` for the child-existence diff — all in one
    /// write txn so the reconciler's per-parent `__cex` reads see the
    /// same snapshot as the plan writes that just happened.
    pub async fn commit(
        &self,
        component_path: &StablePath,
        plan: CommitPlan,
        existence_reconciler: ExistenceReconciler,
    ) -> Result<()> {
        let app_store = self.clone();
        let component_path = component_path.clone();
        self.storage
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
                    if plan.user_state_clear_all_first {
                        app_store
                            .delete_all_user_states(wtxn, &component_path)
                            .await?;
                    }
                    for key in plan.user_state_deletes {
                        app_store
                            .delete_user_state(wtxn, &component_path, &key)
                            .await?;
                    }
                    for (key, bytes) in plan.user_state_writes {
                        app_store
                            .write_user_state(wtxn, &component_path, &key, &bytes)
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
    /// writer. Idempotent.
    pub async fn clear_stage_marker(
        &self,
        component_path: &StablePath,
        process_token: u128,
    ) -> Result<()> {
        self.clear_staged_tracking(component_path, process_token)
            .await
    }
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
/// Used by [`AppStore::commit`]: the AppStore opens the commit txn,
/// then invokes the engine-supplied [`ExistenceReconciler`] which in
/// turn calls this function. Lives on the storage side because the
/// entire logic only depends on the state layer (no engine concepts).
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
