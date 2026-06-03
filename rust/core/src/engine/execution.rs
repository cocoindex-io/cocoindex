use crate::engine::component::ComponentProcessor;
use crate::prelude::*;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque, btree_map};

use crate::engine::context::{
    ComponentProcessingAction, ComponentProcessingMode, ComponentProcessorContext,
    DeclaredTargetState, MemoStatesPayload, TARGET_ID_KEY,
};
use crate::engine::context::{
    FnCallContext, FnCallMemoEntry, FnMemoCache, UserStateCache, decode_stored_entry,
};
use crate::engine::logic_registry;
use crate::engine::profile::{EngineProfile, Persist};
use crate::engine::target_state::{
    ChildInvalidation, TargetActionSink, TargetHandler, TargetStateProvider,
    TargetStateProviderRegistry,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathRef};
use crate::state::stable_path_set::ChildStablePathSet;
use crate::state::target_state_path::{
    TargetStatePath, TargetStatePathWithProviderId, TargetStateProviderGeneration,
};
use crate::state_store::{
    AppStore, CommitPlan, ExistenceReconciler, OwnerStateForPreempt, PrecommitClaimTargetsPlan,
    PrecommitReadPlan, PrecommitWritePlan, WriteTxn, reconcile_child_existence,
};
use cocoindex_utils::deser::from_msgpack_slice;
use cocoindex_utils::fingerprint::Fingerprint;

/// Deserialize a `Vec<MemoizedValue>` into a `Vec<Prof::FunctionData>`.
pub(crate) fn deserialize_memo_values<Prof: EngineProfile>(
    values: &[db_schema::MemoizedValue<'_>],
) -> Result<Vec<Prof::FunctionData>> {
    values
        .iter()
        .map(|s| {
            let bytes = match s {
                db_schema::MemoizedValue::Inlined(b) => b,
            };
            Prof::FunctionData::from_bytes(bytes.as_ref())
        })
        .collect()
}

/// Serialize a `&[Prof::FunctionData]` into a `Vec<MemoizedValue<'static>>`.
/// The returned values own their bytes (`Cow::Owned`), so they're independent of
/// the input lifetime.
pub(crate) fn serialize_memo_values<Prof: EngineProfile>(
    values: &[Prof::FunctionData],
) -> Result<Vec<db_schema::MemoizedValue<'static>>> {
    values
        .iter()
        .map(|s| {
            let bytes = s.to_bytes()?;
            Ok(db_schema::MemoizedValue::Inlined(Cow::Owned(bytes.into())))
        })
        .collect()
}

/// Deserialize the context-borne memo states (fp-tagged list of value blobs).
pub(crate) fn deserialize_context_memo_states<Prof: EngineProfile>(
    entries: &[(Fingerprint, Vec<db_schema::MemoizedValue<'_>>)],
) -> Result<Vec<(Fingerprint, Vec<Prof::FunctionData>)>> {
    entries
        .iter()
        .map(|(fp, values)| Ok((*fp, deserialize_memo_values::<Prof>(values)?)))
        .collect()
}

/// Serialize the context-borne memo states into the on-disk representation.
pub(crate) fn serialize_context_memo_states<Prof: EngineProfile>(
    entries: &[(Fingerprint, Vec<Prof::FunctionData>)],
) -> Result<Vec<(Fingerprint, Vec<db_schema::MemoizedValue<'static>>)>> {
    entries
        .iter()
        .map(|(fp, values)| Ok((*fp, serialize_memo_values::<Prof>(values)?)))
        .collect()
}

pub(crate) async fn use_or_invalidate_component_memoization<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    processor_fp: Option<Fingerprint>,
) -> Result<Option<(Prof::FunctionData, MemoStatesPayload<Prof>)>> {
    // Short-circuit to miss under full_reprocess
    if comp_ctx.full_reprocess() {
        return Ok(None);
    }

    let app_store = comp_ctx.app_ctx().app_store();
    let path = comp_ctx.stable_path();
    {
        let Some(memo_bytes) = app_store.read_component_memo(path).await? else {
            return Ok(None);
        };
        let memo_info: db_schema::ComponentMemoizationInfo<'_> = from_msgpack_slice(&memo_bytes)?;
        if let Some(processor_fp) = processor_fp {
            if memo_info.processor_fp == processor_fp
                && logic_registry::all_contained_with_env(
                    &memo_info.logic_deps,
                    comp_ctx.app_ctx().env(),
                )
            {
                let bytes = match memo_info.return_value {
                    db_schema::MemoizedValue::Inlined(b) => b,
                };
                let ret = Prof::FunctionData::from_bytes(bytes.as_ref());
                match ret {
                    Ok(ret) => {
                        let memo_states = deserialize_memo_values::<Prof>(&memo_info.memo_states)?;
                        let context_memo_states = deserialize_context_memo_states::<Prof>(
                            &memo_info.context_memo_states,
                        )?;
                        return Ok(Some((
                            ret,
                            MemoStatesPayload {
                                positional: memo_states,
                                by_context_fp: context_memo_states,
                            },
                        )));
                    }
                    Err(e) => {
                        warn!(
                            "Skip memoized return value because it failed in deserialization: {:?}",
                            e
                        );
                    }
                }
            }
        }
    }

    // Invalidate the memoization.
    {
        let app_store = comp_ctx.app_ctx().app_store().clone();
        let path = path.clone();
        comp_ctx
            .app_ctx()
            .env()
            .run_txn(move |wtxn| {
                let app_store = app_store.clone();
                let path = path.clone();
                Box::pin(async move { app_store.delete_component_memo_in_txn(wtxn, &path).await })
            })
            .await?;
    }

    Ok(None)
}

/// Update only the memo states of an existing component memoization entry.
///
/// Used when memo state validation indicates `can_reuse=true` but states have changed
/// (e.g. mtime changed but content fingerprint is unchanged). Reads the existing entry,
/// replaces the `memo_states` / `context_memo_states` fields, and writes it back —
/// preserving `processor_fp`, `return_value`, and `logic_deps`.
pub(crate) async fn update_component_memo_states<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    new_states: &MemoStatesPayload<Prof>,
) -> Result<()> {
    let app_store = comp_ctx.app_ctx().app_store().clone();
    let path = comp_ctx.stable_path().clone();

    // Serialize new states once outside the (potentially retried) txn.
    // `MemoizedValue<'static>` holds `Cow::Owned(Vec<u8>)`, so deep-
    // cloning per attempt would copy every byte. `Arc`-wrap the
    // owned vectors; inside each attempt construct a borrowed view
    // (`Vec<MemoizedValue<'_>>` with `Cow::Borrowed` wrappers) that
    // shares the underlying bytes.
    let memo_states_serialized = Arc::new(serialize_memo_values::<Prof>(&new_states.positional)?);
    let context_memo_states_serialized = Arc::new(serialize_context_memo_states::<Prof>(
        &new_states.by_context_fp,
    )?);

    comp_ctx
        .app_ctx()
        .env()
        .run_txn(move |wtxn| {
            let app_store = app_store.clone();
            let path = path.clone();
            let memo_states_serialized = Arc::clone(&memo_states_serialized);
            let context_memo_states_serialized = Arc::clone(&context_memo_states_serialized);
            Box::pin(async move {
                let encoded = {
                    let Some(existing_bytes) =
                        app_store.read_component_memo_in_txn(wtxn, &path).await?
                    else {
                        return Ok(());
                    };
                    let existing: db_schema::ComponentMemoizationInfo<'_> =
                        from_msgpack_slice(&existing_bytes)?;
                    let memo_states_borrowed = borrow_memo_values(&memo_states_serialized);
                    let context_memo_states_borrowed =
                        borrow_context_memo_states(&context_memo_states_serialized);
                    let new_info = db_schema::ComponentMemoizationInfo {
                        processor_fp: existing.processor_fp,
                        return_value: existing.return_value,
                        logic_deps: existing.logic_deps,
                        memo_states: memo_states_borrowed,
                        context_memo_states: context_memo_states_borrowed,
                    };
                    rmp_serde::to_vec_named(&new_info)?
                };
                app_store
                    .write_component_memo_raw(wtxn, &path, &encoded)
                    .await
            })
        })
        .await?;
    Ok(())
}

/// Create a borrowed view of `Vec<MemoizedValue<'static>>` — each element
/// re-wrapped as `MemoizedValue::Inlined(Cow::Borrowed(...))` referencing
/// the original's bytes. Cheap (no byte copying); used to feed retry-safe
/// txn closures whose `ComponentMemoizationInfo` lifetime is local to the
/// attempt.
fn borrow_memo_values<'a>(
    values: &'a [db_schema::MemoizedValue<'static>],
) -> Vec<db_schema::MemoizedValue<'a>> {
    values
        .iter()
        .map(|v| {
            let db_schema::MemoizedValue::Inlined(bytes) = v;
            db_schema::MemoizedValue::Inlined(Cow::Borrowed(bytes.as_ref()))
        })
        .collect()
}

/// Same as [`borrow_memo_values`] but for the context-borne nested shape
/// (`Vec<(Fingerprint, Vec<MemoizedValue>)>`).
fn borrow_context_memo_states<'a>(
    values: &'a [(Fingerprint, Vec<db_schema::MemoizedValue<'static>>)],
) -> Vec<(Fingerprint, Vec<db_schema::MemoizedValue<'a>>)> {
    values
        .iter()
        .map(|(fp, vals)| (*fp, borrow_memo_values(vals)))
        .collect()
}

pub fn declare_target_state<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    fn_ctx: &FnCallContext,
    provider: TargetStateProvider<Prof>,
    key: StableKey,
    value: Prof::TargetStateValue,
) -> Result<()> {
    let target_state_path = provider.target_state_path().concat(&key);
    let declared_target_state = DeclaredTargetState {
        provider,
        item_key: key,
        value,
        child_provider: None,
    };
    comp_ctx.update_building_state(|building_state| {
        match building_state
            .target_states
            .declared_target_states
            .entry(target_state_path.clone())
        {
            btree_map::Entry::Occupied(entry) => {
                client_bail!(
                    "Target state already declared with key: {:?}",
                    entry.get().item_key
                );
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(declared_target_state);
            }
        }
        Ok(())
    })?;
    fn_ctx.update(|inner| inner.target_state_paths.push(target_state_path));
    Ok(())
}

pub fn register_root_target_state_provider<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    name: String,
    handler: Prof::TargetHdl,
) -> Result<TargetStateProvider<Prof>> {
    comp_ctx.update_building_state(|building_state| {
        building_state
            .target_states
            .provider_registry
            .register_root(name, handler)
    })
}

pub fn declare_target_state_with_child<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    fn_ctx: &FnCallContext,
    provider: TargetStateProvider<Prof>,
    key: StableKey,
    value: Prof::TargetStateValue,
) -> Result<TargetStateProvider<Prof>> {
    let child_provider = comp_ctx.update_building_state(|building_state| {
        let child_provider = building_state
            .target_states
            .provider_registry
            .register_lazy(&provider, key.clone())?;
        let declared_target_state = DeclaredTargetState {
            provider,
            item_key: key,
            value,
            child_provider: Some(child_provider.clone()),
        };
        match building_state
            .target_states
            .declared_target_states
            .entry(child_provider.target_state_path().clone())
        {
            btree_map::Entry::Occupied(entry) => {
                client_bail!(
                    "Target state already declared with key: {:?}",
                    entry.get().item_key
                );
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(declared_target_state);
            }
        }
        Ok(child_provider)
    })?;
    fn_ctx.update(|inner| {
        inner
            .target_state_paths
            .push(child_provider.target_state_path().clone());
    });
    Ok(child_provider)
}

struct Committer<Prof: EngineProfile> {
    component_ctx: ComponentProcessorContext<Prof>,
    app_store: AppStore,
    target_states_providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,

    component_path: StablePath,

    demote_component_only: bool,
}

impl<Prof: EngineProfile> Committer<Prof> {
    fn new(
        component_ctx: &ComponentProcessorContext<Prof>,
        target_states_providers: &rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        demote_component_only: bool,
    ) -> Result<Self> {
        let component_path = component_ctx.stable_path().clone();
        Ok(Self {
            component_ctx: component_ctx.clone(),
            app_store: component_ctx.app_ctx().app_store().clone(),
            target_states_providers: target_states_providers.clone(),
            component_path,
            demote_component_only,
        })
    }

    /// Build the engine-side decisions for Phase 4 commit and run
    /// them through [`AppStore::commit`](crate::state_store::AppStore::commit).
    /// The AppStore opens its own write txn, applies the plan's writes
    /// (tracking-info, fn-memo flush, user-state flush, target-owner cleanup),
    /// and invokes the `ExistenceReconciler` callback to walk the
    /// child-existence tree atomically inside the same txn. Then
    /// launches Phase 5 GC.

    async fn commit(
        self,
        child_path_set: Option<ChildStablePathSet>,
        fn_memos: FnMemoCache<Prof>,
        user_states: UserStateCache,
        curr_version: Option<u64>,
    ) -> Result<()> {
        // Consume FnMemoCache once (drains each entry's RwLock to Pending).
        let fn_memo_plan = fn_memos.into_flush_plan()?;
        let user_state_plan = user_states.into_flush_plan();

        // Engine-side decisions: read existing tracking_info, prune by
        // version retention, clear pending_process_token, version-
        // converge — produce final bytes the AppStore will write. Per-
        // component exclusivity means no concurrent writer can change
        // `__track` between this standalone read and `app_store.commit`.
        let (new_tracking_info, target_owners_to_delete) =
            self.build_commit_writes(curr_version).await?;

        let child_path_set = if self.demote_component_only {
            None
        } else {
            child_path_set.map(Arc::new)
        };

        let plan = CommitPlan {
            new_tracking_info,
            target_owners_to_upsert: Vec::new(),
            target_owners_to_delete,
            fn_memo_clear_all_first: fn_memo_plan.clear_all_first,
            fn_memo_writes: fn_memo_plan.writes,
            fn_memo_deletes: fn_memo_plan.deletes,
            user_state_clear_all_first: user_state_plan.clear_all_first,
            user_state_writes: user_state_plan.writes,
            user_state_deletes: user_state_plan.deletes,
            child_path_set: child_path_set.clone(),
        };

        // Reconciler closure: walks `child_path_set` against on-disk
        // `__cex` rows, writes diffs and tombstones. Runs inside the
        // AppStore's commit txn so the existence diff is atomic with
        // the rest of the commit plan.
        let app_store = self.app_store.clone();
        let component_path = self.component_path.clone();
        let cps = child_path_set;
        // `Fn` (not `FnOnce`) so a backend that re-runs its commit txn can
        // re-invoke it — clone the (cheap, `Arc`/owned) captures per call
        // rather than moving them into the future.
        let reconciler: ExistenceReconciler = Box::new(move |wtxn| {
            let app_store = app_store.clone();
            let component_path = component_path.clone();
            let cps = cps.clone();
            Box::pin(async move {
                reconcile_child_existence(wtxn, &app_store, &component_path, cps.as_deref()).await
            })
        });

        self.app_store
            .commit(&self.component_path, plan, reconciler)
            .await?;

        // Phase 5 GC: snapshot-read tombstones and spawn child delete
        // operations. Outside the commit txn — tombstones are durable
        // and the GC sweep is idempotent.
        self.launch_child_component_gc().await
    }

    /// Engine-side reconcile that produces the `(new_tracking_info,
    /// target_owners_to_delete)` portion of [`CommitPlan`]. Delete
    /// mode short-circuits to `(None, vec![])`, signalling the
    /// session to delete the `__track` row.
    async fn build_commit_writes(
        &self,
        curr_version: Option<u64>,
    ) -> Result<(Option<Vec<u8>>, Vec<TargetStatePath>)> {
        if self.component_ctx.mode() == ComponentProcessingMode::Delete {
            return Ok((None, Vec::new()));
        }
        let curr_version = curr_version
            .ok_or_else(|| internal_error!("curr_version is required for Build mode"))?;
        let tracking_info_bytes = self
            .app_store
            .read_tracking_info(&self.component_path)
            .await?
            .ok_or_else(|| internal_error!("tracking info not found for commit"))?;
        let mut tracking_info: db_schema::StablePathEntryTrackingInfo<'_> =
            from_msgpack_slice(&tracking_info_bytes)?;

        for item in tracking_info.target_state_items.values_mut() {
            item.states.retain(|(version, state)| {
                *version > curr_version || *version == curr_version && !state.is_deleted()
            });
        }
        // Prune entries with empty states and collect their paths for
        // inverted-tracking cleanup. Component-level
        // `pending_process_token` is cleared here — pre_commit →
        // sink_apply → commit is succeeding, so any token written by
        // pre_commit is no longer "pending".
        let mut pruned_paths: HashSet<TargetStatePath> = HashSet::new();
        tracking_info
            .target_state_items
            .retain(|path_with_pid, item| {
                if item.states.is_empty() {
                    pruned_paths.insert(path_with_pid.target_state_path.clone());
                    false
                } else {
                    true
                }
            });
        tracking_info.pending_process_token = None;
        // Don't delete inverted tracking if a surviving entry shares the
        // same target_state_path (provider_id change — old entry pruned,
        // new entry survives under different provider_id).
        if !pruned_paths.is_empty() {
            for path_with_pid in tracking_info.target_state_items.keys() {
                pruned_paths.remove(&path_with_pid.target_state_path);
            }
        }
        for (path_with_pid, item) in tracking_info.target_state_items.iter_mut() {
            if let Some(parent_provider) = self
                .target_states_providers
                .get(path_with_pid.target_state_path.provider_path())
                && let Some(pg) = parent_provider.provider_generation()
            {
                item.provider_schema_version = pg.provider_schema_version;
            }
        }

        let is_version_converged = tracking_info.target_state_items.iter().all(|(_, item)| {
            item.states
                .iter()
                .all(|(version, _)| *version == curr_version)
        });
        if is_version_converged {
            tracking_info.version = 1;
            for item in tracking_info.target_state_items.values_mut() {
                for (version, _) in item.states.iter_mut() {
                    *version = 1;
                }
            }
        }

        let data_bytes = rmp_serde::to_vec_named(&tracking_info)?;
        let owners_to_delete: Vec<TargetStatePath> = pruned_paths.into_iter().collect();
        Ok((Some(data_bytes), owners_to_delete))
    }

    async fn launch_child_component_gc(&self) -> Result<()> {
        // Cascade the parent's on_error to descendant orphan deletes.
        //
        // - Delete-mode parent (recursive cascade from `App.drop()`'s
        //   root delete): the raising on_error propagates so any
        //   descendant failure surfaces back through `handle.ready()`.
        // - Build-mode parent (orphan deletes during a normal update,
        //   triggered by the parent's `process()` no longer declaring a
        //   previously-existing child): the on_error installed on the
        //   parent's build context — same handler `Component::mount`
        //   wires for the child's own task failure — sees orphan-delete
        //   failures too.
        // - No installed handler (root `App.update`, `use_mount`,
        //   `operator.delete` without a chain): `None` preserves the
        //   "log + swallow" default.
        //
        // The `Arc` makes cloning cheap regardless of how many
        // descendants we spawn.
        let cascaded_on_error = self.component_ctx.processing_action_on_error();
        // Standalone snapshot read — `list_tombstones` opens its own
        // fresh `RoTxn` internally.
        let tombstones = self.app_store.list_tombstones(&self.component_path).await?;
        let mut handles = Vec::with_capacity(tombstones.len());
        for relative_path in tombstones {
            let stable_path = self.component_path.concat(relative_path.as_ref());
            let component = self.component_ctx.component().get_child(stable_path);
            let delete_ctx = component.new_processor_context_for_delete(
                self.target_states_providers.clone(),
                Some(&self.component_ctx),
                self.component_ctx.processing_stats().clone(),
                self.component_ctx.host_ctx().clone(),
                cascaded_on_error.clone(),
            );
            handles.push(component.delete(delete_ctx, None)?);
        }
        // Await each handle so descendant failures (when on_error
        // propagates) reach our own task_result, which the parent
        // delete's spawned task surfaces via `handle.ready()` —
        // eventually back to `app.drop()`. Short-circuits on first Err;
        // remaining children continue running (orphan tasks), but their
        // tombstones survive for the next reconcile to retry. With
        // `on_error = None`, every handle resolves Ok regardless of
        // child failures, so this is a no-op cost in that case.
        for handle in handles {
            handle.ready().await?;
        }
        Ok(())
    }
}

struct SinkInput<Prof: EngineProfile> {
    actions: Vec<Prof::TargetAction>,
    child_providers: Option<Vec<Option<TargetStateProvider<Prof>>>>,
}

impl<Prof: EngineProfile> Default for SinkInput<Prof> {
    fn default() -> Self {
        Self {
            actions: Vec::new(),
            child_providers: None,
        }
    }
}

impl<Prof: EngineProfile> SinkInput<Prof> {
    fn add_action(
        &mut self,
        action: Prof::TargetAction,
        child_provider: Option<TargetStateProvider<Prof>>,
    ) {
        self.actions.push(action);
        if let Some(child_providers) = self.child_providers.as_mut() {
            child_providers.push(child_provider);
        } else if let Some(child_provider) = child_provider {
            let mut v = Vec::with_capacity(self.actions.len());
            v.extend(std::iter::repeat(None).take(self.actions.len() - 1));
            v.push(Some(child_provider));
            self.child_providers = Some(v);
        }
    }
}

struct PreCommitOutput<Prof: EngineProfile> {
    curr_version: Option<u64>,
    previously_exists: bool,
    actions_by_sinks: HashMap<Prof::TargetActionSink, SinkInput<Prof>>,
    /// Name of the processor to be deleted; caller passes it to `collect_processor_name_name_for_del`.
    processor_name_for_del: Option<String>,
    /// Provider generations to apply (via
    /// `TargetStateProvider::set_provider_generation`) after the
    /// precommit txn has committed. Buffered so a retry of the
    /// precommit doesn't trip the `OnceLock` "already set" guard.
    deferred_provider_generations: Vec<(TargetStateProvider<Prof>, TargetStateProviderGeneration)>,
}

/// Either a completed pre_commit (with optional output for skip-cases) or a
/// "back off and retry" signal triggered by detecting a concurrent
/// pre_commit's live `pending_process_token` on disk. See
/// `specs/target_state_ownership_transfer/concurrent_preempt_race_fix.md`.
///
/// `pre_commit` borrows `declared_target_states` (via a
/// `tokio::sync::MutexGuard` held by the caller for the duration of one
/// attempt). On `PendingRetry` the outer loop just re-locks and calls
/// again — no clones, no consumed state to restore. `TargetStateValue`s
/// are borrowed directly into `TargetHandler::reconcile` from within
/// the lock scope; reconcile impls decide whether (and how) to clone.
enum PreCommitOutcome<Prof: EngineProfile> {
    Done {
        output: PreCommitOutput<Prof>,
        write_plan: PrecommitWritePlan,
    },
    PendingRetry,
}

/// Captures bundle shared into the precommit callback closure. Every
/// field is `O(1)` to clone (Arc-internal or persistent data structure)
/// so the body's per-call `Arc::clone(&captures)` is cheap. LMDB never
/// retries the callback, but the bundle's `Fn`-friendly shape keeps the
/// closure structurally aligned with retry-capable backends.
struct PreCommitCaptures<Prof: EngineProfile> {
    app_store: AppStore,
    stable_path: StablePath,
    processor_name: Option<Arc<str>>,
    contained_target_state_paths: Arc<HashSet<TargetStatePath>>,
    target_states_providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    declared_target_states:
        Arc<tokio::sync::Mutex<BTreeMap<TargetStatePath, DeclaredTargetState<Prof>>>>,
}

/// Engine-side reconcile body. Takes precomputed reads from
/// [`PrecommitSession::precommit_read`] and
/// [`PrecommitSession::precommit_claim_targets`] (existing tracking_info,
/// declared-target owners, preempted-owner tracking blobs) and returns
/// the [`PrecommitWritePlan`] that the caller returns from the
/// [`AppStore::precommit`](crate::state_store::AppStore::precommit)
/// callback for the AppStore to apply + commit.
///
/// Delete-mode preflight (`delete_component_memo` + node-type check)
/// runs outside, in [`submit`], before the precommit txn is opened —
/// the `demote_component_only` decision lives there too.
#[allow(clippy::too_many_arguments)]
async fn pre_commit<'tracking, Prof: EngineProfile>(
    app_store: &AppStore,
    wtxn: &mut WriteTxn<'_>,
    process_token: u128,
    stable_path: &StablePath,
    full_reprocess: bool,
    processor_name: Option<&str>,
    contained_target_state_paths: &HashSet<TargetStatePath>,
    target_states_providers: &rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    declared_target_states: Arc<
        tokio::sync::Mutex<BTreeMap<TargetStatePath, DeclaredTargetState<Prof>>>,
    >,
    declared_paths_all: Vec<TargetStatePath>,
    mut tracking_info: Option<db_schema::StablePathEntryTrackingInfo<'tracking>>,
    prior_owners: BTreeMap<TargetStatePath, Option<StablePath>>,
    preempted_owner_states: BTreeMap<StablePath, OwnerStateForPreempt>,
) -> Result<PreCommitOutcome<Prof>> {
    let mut actions_by_sinks = HashMap::<Prof::TargetActionSink, SinkInput<Prof>>::new();
    let mut processor_name_for_del: Option<String> = None;

    // Flatten `prior_owners` to drop `None` entries (paths with no
    // existing owner row). The detection sub-pass + Phase 1 preempt
    // branch only look at non-self owners; storing `Option` would
    // force every lookup to double-deref. Note: `prior_owners` only
    // contains entries for `paths_to_claim` (the subset of declared
    // paths the engine just decided to claim); paths self already owns
    // per `tracking_info` are absent from the map and treated as
    // "owner == self" by construction.
    let bulk_target_owners: HashMap<TargetStatePath, StablePath> = prior_owners
        .into_iter()
        .filter_map(|(k, v)| v.map(|owner| (k, owner)))
        .collect();

    // Old-owner tracking_info bytes were prefetched by the session.
    // The cache is per-owner-path; the detection sub-pass and Phase 1
    // reconcile both read from it. The Phase 1 preempt branch may
    // re-encode an owner's bytes after removing the preempted item;
    // those updates are emitted as `preempted_owner_updates` in the
    // write plan, applied inside the precommit txn by the session.
    let mut old_tracking_cache: HashMap<StablePath, Vec<u8>> = preempted_owner_states
        .iter()
        .filter_map(|(path, state)| {
            state
                .tracking_info
                .as_ref()
                .map(|bytes| (path.clone(), bytes.clone()))
        })
        .collect();

    // Detection sub-pass — runs before any `TargetStateValue` is consumed by
    // reconcile, so a `PendingRetry` return leaves the input `declared_target_states`
    // intact and the surrounding txn write-free for the retry.
    //
    // We're only looking for one thing: a *live* in-flight pre_commit from
    // this process on an old owner whose item we want to preempt. The signal
    // is `old.tracking.pending_process_token == self AND item.is_pending()`
    // — the component-level token says the lifecycle is in flight, the
    // per-item multi-state signal filters to just the items that lifecycle
    // actually touched. Without the per-item filter, C2 would back off
    // preempting item I from C1 even when C1's pre_commit only modified
    // item J — over-conservative.
    //
    // Crashed-prior-process and rolled-back states are *not* detected here.
    // Both leave multi-state items on disk (a token from a dead process, or
    // no token after `clear_staged_tracking` ran), and the main pass picks
    // them up uniformly via `prev_item.is_pending()` → force
    // `prev_may_be_missing = true` on reconcile.
    let mut pending_retry = false;
    for target_state_path in &declared_paths_all {
        let parent_provider_gen = target_states_providers
            .get(target_state_path.provider_path())
            .and_then(|p| p.provider_generation());
        let lookup_key = TargetStatePathWithProviderId {
            target_state_path: target_state_path.clone(),
            provider_id: parent_provider_gen.map(|g| g.provider_id),
        };
        if tracking_info
            .as_ref()
            .is_some_and(|t| t.target_state_items.contains_key(&lookup_key))
        {
            continue;
        }
        let Some(owner_path) = bulk_target_owners.get(target_state_path) else {
            continue;
        };
        if owner_path == stable_path {
            continue;
        }
        let Some(cached) = old_tracking_cache.get(owner_path) else {
            continue;
        };
        let old: db_schema::StablePathEntryTrackingInfo<'_> = from_msgpack_slice(cached)?;
        if old.pending_process_token == Some(process_token) {
            if let Some(item) = old.target_state_items.get(&lookup_key) {
                if item.is_pending() {
                    pending_retry = true;
                    break;
                }
            }
        }
    }
    if pending_retry {
        return Ok(PreCommitOutcome::PendingRetry);
    }
    let mut modified_old_owners: HashSet<StablePath> = HashSet::new();
    let previously_exists = tracking_info.is_some();
    if let Some(tracking_info) = &mut tracking_info {
        if let Some(processor_name) = processor_name {
            tracking_info.processor_name = Cow::Borrowed(processor_name);
        } else {
            processor_name_for_del = Some(tracking_info.processor_name.as_ref().to_owned());
        }
    } else if let Some(processor_name) = processor_name {
        tracking_info = Some(db_schema::StablePathEntryTrackingInfo::new(Cow::Borrowed(
            processor_name,
        )));
    }
    // Provider generation updates deferred to after Phase 1 + Phase 2 complete
    // — `TargetStateProvider::set_provider_generation` is OnceLock-backed and
    // would error on a hypothetical retry. The detection sub-pass already
    // returned PendingRetry before any reconcile ran, so by the time we
    // reach here we're committed to this attempt; collecting and applying at
    // the end keeps the invariant "set at most once per successful lifecycle"
    // explicit.
    let mut deferred_provider_generations: Vec<(
        TargetStateProvider<Prof>,
        TargetStateProviderGeneration,
    )> = Vec::new();
    let curr_version = if let Some(mut tracking_info) = tracking_info {
        let curr_version = tracking_info.version + 1;
        tracking_info.version = curr_version;

        // Entries to insert/re-insert into target_state_items after both phases.
        // Collected separately so Phase 2 doesn't see items added by Phase 1.
        let mut items_to_insert: Vec<(
            TargetStatePathWithProviderId,
            db_schema::TargetStateInfoItem,
        )> = Vec::new();

        // Phase 1: Insert + Update — iterate declared target states.
        // For each declared target state, find and remove any existing tracked entry,
        // then reconcile. This unifies the insert and update code paths.
        //
        // Materialize keys first so the lock isn't held across awaits inside
        // the loop body. Per-entry extracts re-lock briefly; the reconcile
        // call itself runs inside that lock and borrows `&decl.value`
        // directly (no engine-level clone — host-specific reconcile impl
        // decides whether and how to clone).
        // Reuse the `declared_paths_all` materialized at the top for
        // the bulk-read step — same set, no need to re-lock + re-clone.
        for target_state_path in declared_paths_all.iter().cloned() {
            // Look up existing tracked entry using exact key (provider_id from current providers).
            let parent_provider_gen = target_states_providers
                .get(target_state_path.provider_path())
                .and_then(|p| p.provider_generation());
            let parent_provider_id = parent_provider_gen.map(|g| g.provider_id);
            let lookup_key = TargetStatePathWithProviderId {
                target_state_path: target_state_path.clone(),
                provider_id: parent_provider_id,
            };
            let existing_item = tracking_info.target_state_items.remove(&lookup_key);

            // Whether this target state path is new to this component's forward tracking
            // (either fresh insert or preempted from another component).
            // When provider_id changed, the old entry (under old_pid) stays for Phase 2
            // to skip (stale) and commit to prune.

            // Obtain prev_item: either from this component's existing entry or via preempt.
            // Owner info comes from the pre-fetched `bulk_target_owners` map (no
            // plain SELECT in this loop — same SIReadLock avoidance reason as
            // the detection sub-pass above).
            let mut prev_item = if let Some(existing_item) = existing_item {
                Some(existing_item)
            } else {
                match bulk_target_owners.get(&target_state_path) {
                    Some(owner_path) if owner_path != stable_path => {
                        let old_owner_path = owner_path.clone();
                        if let Some(cached_bytes) = old_tracking_cache.get(&old_owner_path) {
                            let mut old_tracking: db_schema::StablePathEntryTrackingInfo<'_> =
                                from_msgpack_slice(cached_bytes)?;
                            let len_before = old_tracking.target_state_items.len();
                            // Look up the entry matching current provider_id.
                            // `into_owned()` releases the borrow on the cached
                            // bytes so `prev_item` outlives this scope.
                            let prev_item = old_tracking
                                .target_state_items
                                .remove(&lookup_key)
                                .map(|item| {
                                    let mut item = item.into_owned();
                                    // Reset version numbers so the new component's commit
                                    // retention prunes them. The old owner's versions are from
                                    // a different version space and may collide with
                                    // curr_version.
                                    for (version, _) in item.states.iter_mut() {
                                        *version = 0;
                                    }
                                    item
                                });
                            // Also remove any stale entries (different provider_ids)
                            // to prevent them from clobbering inverted tracking on prune.
                            old_tracking
                                .target_state_items
                                .retain(|k, _| k.target_state_path != target_state_path);
                            if old_tracking.target_state_items.len() < len_before {
                                let new_bytes = rmp_serde::to_vec_named(&old_tracking)?;
                                drop(old_tracking);
                                old_tracking_cache.insert(old_owner_path.clone(), new_bytes);
                                modified_old_owners.insert(old_owner_path);
                            }
                            prev_item
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            };

            // Compute prev_states and prev_may_be_missing uniformly from prev_item.
            // A `Deleted` entry among the states means the sink may be absent —
            // e.g. a prior delete whose sink_apply succeeded but whose commit
            // didn't finish (crash, or a `rollback_pending_tokens` after a later
            // failure). Multi-state on its own does NOT imply missing: every
            // value the sink could hold is already among `prev_states`, so the
            // handler's own `all(prev == desired)` check decides whether to act.
            let (prev_states, prev_may_be_missing) = if let Some(ref prev_item) = prev_item {
                let schema_version_mismatch = match parent_provider_gen {
                    Some(pg) => prev_item.provider_schema_version != pg.provider_schema_version,
                    None => false,
                };
                let prev_may_be_missing = full_reprocess
                    || schema_version_mismatch
                    || prev_item.states.iter().any(|(_, s)| s.is_deleted());
                let prev_states = prev_item
                    .states
                    .iter()
                    .filter_map(|(_, s)| s.as_ref())
                    .map(|s_bytes| Prof::TargetStateTrackingRecord::from_bytes(s_bytes))
                    .collect::<Result<Vec<_>>>()?;
                (prev_states, prev_may_be_missing)
            } else {
                (vec![], true)
            };

            // Lock the shared map to run `reconcile` against `&decl.value`,
            // then extract the post-reconcile data we'll need below
            // (`target_state_key_bytes`, `recon_output`, `child_provider`).
            // The guard drops at the end of this scope so subsequent awaits
            // in this iteration aren't carrying a `!Send` borrow.
            let (target_state_key_bytes, recon_output, child_provider) = {
                let guard = declared_target_states.lock().await;
                let decl = guard.get(&target_state_path).ok_or_else(|| {
                    internal_error!("declared entry vanished mid-pre_commit: {target_state_path}")
                })?;
                let target_state_key_bytes = storekey::encode_vec(&decl.item_key)
                    .map_err(|e| internal_error!("Failed to encode StableKey: {e}"))?;
                let recon_output = decl
                    .provider
                    .handler()
                    .ok_or_else(|| {
                        internal_error!(
                            "provider not ready for target state with key {:?}",
                            decl.item_key
                        )
                    })?
                    .reconcile(
                        decl.item_key.clone(),
                        Some(&decl.value),
                        &prev_states,
                        prev_may_be_missing,
                    )?;
                (
                    target_state_key_bytes,
                    recon_output,
                    decl.child_provider.clone(),
                )
            };

            if let Some(recon_output) = recon_output {
                let mut provider_generation = prev_item
                    .as_ref()
                    .and_then(|item| item.provider_generation.clone());

                if let Some(child_provider) = &child_provider {
                    let existing_gen = provider_generation.clone().unwrap_or_default();
                    let new_gen = match recon_output.child_invalidation {
                        Some(ChildInvalidation::Destructive) => {
                            // Inside the open precommit WTxn — use the
                            // in-txn variant to avoid nesting another
                            // batched WTxn on LMDB (would deadlock).
                            let new_id = app_store
                                .reserve_id_range_in_txn(wtxn, &TARGET_ID_KEY, 1)
                                .await?;
                            TargetStateProviderGeneration {
                                provider_id: new_id,
                                provider_schema_version: 0,
                            }
                        }
                        Some(ChildInvalidation::Lossy) => TargetStateProviderGeneration {
                            provider_id: existing_gen.provider_id,
                            provider_schema_version: existing_gen.provider_schema_version + 1,
                        },
                        None => existing_gen,
                    };
                    provider_generation = Some(new_gen.clone());
                    deferred_provider_generations.push((child_provider.clone(), new_gen));
                }

                actions_by_sinks
                    .entry(recon_output.sink)
                    .or_default()
                    .add_action(recon_output.action, child_provider);

                let new_state_bytes = recon_output
                    .tracking_record
                    .map(|s| s.to_bytes())
                    .transpose()?;

                if let Some(item) = &mut prev_item {
                    // Update existing item.
                    item.provider_generation = provider_generation;
                    item.states.push((
                        curr_version,
                        match new_state_bytes {
                            Some(s) => {
                                db_schema::TargetStateInfoItemState::Existing(Cow::Owned(s.into()))
                            }
                            None => db_schema::TargetStateInfoItemState::Deleted,
                        },
                    ));
                } else if let Some(new_state) = new_state_bytes {
                    // Insert new item.
                    prev_item = Some(db_schema::TargetStateInfoItem {
                        key: Cow::Owned(target_state_key_bytes.into()),
                        states: vec![
                            (0, db_schema::TargetStateInfoItemState::Deleted),
                            (
                                curr_version,
                                db_schema::TargetStateInfoItemState::Existing(Cow::Owned(
                                    new_state.into(),
                                )),
                            ),
                        ],
                        provider_schema_version: 0,
                        provider_generation,
                    });
                }
            } else if let Some(item) = &mut prev_item {
                // No change — bump version on existing item.
                for (version, _) in item.states.iter_mut() {
                    *version = curr_version;
                }
            }

            // Collect item for re-insertion after Phase 2. The
            // `__target` claim for `is_new_to_component` paths was
            // already handed off to `precommit_claim_targets` via the
            // pre-flight `paths_to_claim` filter in `submit()`.
            if let Some(item) = prev_item {
                items_to_insert.push((lookup_key, item));
            }
        }

        // Phase 2: Delete + Contained — iterate remaining tracked entries not matched above.
        for (target_state_path_with_pid, item) in tracking_info.target_state_items.iter_mut() {
            // Skip stale entries — commit() will prune them via version retention.
            let parent_provider_gen = target_states_providers
                .get(target_state_path_with_pid.target_state_path.provider_path())
                .and_then(|p| p.provider_generation());
            if target_state_path_with_pid.provider_id.unwrap_or(0)
                != parent_provider_gen.map(|pg| pg.provider_id).unwrap_or(0)
            {
                continue;
            }

            // Contained entries: still referenced by a parent, just bump version.
            if contained_target_state_paths.contains(&target_state_path_with_pid.target_state_path)
            {
                for (version, _) in item.states.iter_mut() {
                    *version = curr_version;
                }
                continue;
            }

            // Delete: target state is no longer declared.
            let Some(target_states_provider) = target_states_providers
                .get(target_state_path_with_pid.target_state_path.provider_path())
            else {
                trace!(
                    "skip deleting target states with path {target_state_path_with_pid} in {} because target states provider not found",
                    stable_path
                );
                continue;
            };
            let target_state_key: StableKey = storekey::decode(item.key.as_ref())?;
            let schema_version_mismatch = match parent_provider_gen {
                Some(pg) => item.provider_schema_version != pg.provider_schema_version,
                None => false,
            };
            let prev_may_be_missing = if full_reprocess || schema_version_mismatch {
                true
            } else {
                item.states.iter().any(|(_, s)| s.is_deleted())
            };
            let prev_states = item
                .states
                .iter()
                .filter_map(|(_, s)| s.as_ref())
                .map(|s_bytes| Prof::TargetStateTrackingRecord::from_bytes(s_bytes))
                .collect::<Result<Vec<_>>>()?;

            let recon_output = target_states_provider
                .handler()
                .ok_or_else(|| {
                    internal_error!(
                        "provider not ready for target state with key {target_state_key:?}"
                    )
                })?
                .reconcile(target_state_key, None, &prev_states, prev_may_be_missing)?;
            if let Some(recon_output) = recon_output {
                actions_by_sinks
                    .entry(recon_output.sink)
                    .or_default()
                    .add_action(recon_output.action, None);
                item.states.push((
                    curr_version,
                    match recon_output
                        .tracking_record
                        .map(|s| s.to_bytes())
                        .transpose()?
                    {
                        Some(s) => {
                            db_schema::TargetStateInfoItemState::Existing(Cow::Owned(s.into()))
                        }
                        None => db_schema::TargetStateInfoItemState::Deleted,
                    },
                ));
            } else {
                for (version, _) in item.states.iter_mut() {
                    *version = curr_version;
                }
            }
        }

        // Insert/re-insert items collected during Phase 1.
        for (path_with_pid, item) in items_to_insert {
            tracking_info.target_state_items.insert(path_with_pid, item);
        }

        // Mark the component as in-flight if we queued any sink action; else
        // clear the slot (no-op if it was already None, but also wipes a stale
        // token from a prior crashed lifecycle now that the current pre_commit
        // has rewritten the items). On success this is cleared by
        // `commit_in_txn`; on sink/commit failure, `rollback_pending_tokens`.
        tracking_info.pending_process_token = if actions_by_sinks.is_empty() {
            None
        } else {
            Some(process_token)
        };

        let data_bytes = rmp_serde::to_vec_named(&tracking_info)?;
        drop(tracking_info); // Release borrow before further mutation.
        (Some(curr_version), Some(data_bytes))
    } else {
        (None, None)
    };
    let (curr_version, new_tracking_info_bytes) = curr_version;

    // Collect modified preempted-owner blobs from `old_tracking_cache`
    // into the write plan. The backend writes them alongside the self
    // tracking_info in the same precommit txn (the apply step inside
    // `precommit(callback)`), collapsing N preempts of one owner into
    // one upsert.
    let mut preempted_owner_updates: BTreeMap<StablePath, Vec<u8>> = BTreeMap::new();
    for path in modified_old_owners {
        let encoded = old_tracking_cache
            .remove(&path)
            .ok_or_else(|| internal_error!("modified old owner missing from cache: {}", path))?;
        preempted_owner_updates.insert(path, encoded);
    }

    // Provider-generation updates: buffered into the output, applied
    // by `submit()` after the precommit txn commits — so a retry of
    // precommit (a fresh precommit_read on PendingRetry) doesn't trip
    // the `OnceLock::set` "already set" guard.
    Ok(PreCommitOutcome::Done {
        output: PreCommitOutput {
            curr_version,
            previously_exists,
            actions_by_sinks,
            processor_name_for_del,
            deferred_provider_generations,
        },
        write_plan: PrecommitWritePlan {
            self_path: stable_path.clone(),
            new_tracking_info: new_tracking_info_bytes,
            preempted_owner_updates,
        },
    })
}

pub(crate) struct SubmitOutput<Prof: EngineProfile> {
    pub built_target_states_providers: Option<TargetStateProviderRegistry<Prof>>,
    pub touched_previous_states: bool,
}

#[instrument(name = "submit", skip_all)]
pub(crate) async fn submit<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    processor: Option<&Prof::ComponentProc>,
    collect_processor_name_name_for_del: impl FnOnce(&str) -> (),
) -> Result<SubmitOutput<Prof>> {
    let processor_name = processor.map(|p| p.processor_info().name.as_str());

    let mut built_target_states_providers: Option<TargetStateProviderRegistry<Prof>> = None;
    let (
        target_states_providers,
        declared_target_states,
        child_path_set,
        fn_memos,
        user_states,
        contained_target_state_paths,
    ) = match comp_ctx.processing_state() {
        ComponentProcessingAction::Build(build_ctx) => {
            // Extract from MutexGuard in a block so the guard is dropped before `.await`.
            let building_state = {
                let mut guard = build_ctx.state.lock().unwrap();
                let Some(state) = guard.take() else {
                    internal_bail!(
                        "Processing for the component at {} is already finished",
                        comp_ctx.stable_path()
                    );
                };
                state
            };

            let child_path_set = building_state.child_path_set;
            let fn_memos = building_state.fn_memos;
            let user_states = building_state.user_states;
            let contained_target_state_paths = finalize_fn_call_memoization(comp_ctx, &fn_memos)?;
            (
                &built_target_states_providers
                    .get_or_insert(building_state.target_states.provider_registry)
                    .providers,
                building_state.target_states.declared_target_states,
                Some(child_path_set),
                fn_memos,
                user_states,
                contained_target_state_paths,
            )
        }
        ComponentProcessingAction::Delete(delete_context) => (
            &delete_context.providers,
            Default::default(),
            None,
            FnMemoCache::default(),
            UserStateCache::new(),
            HashSet::new(),
        ),
    };

    let comp_mode = comp_ctx.mode();
    let full_reprocess = comp_ctx.full_reprocess();
    let process_token = comp_ctx.app_ctx().env().process_token();

    let mut pending_fulfillments: Vec<(TargetStateProvider<Prof>, Prof::TargetHdl)> = Vec::new();

    // Reconcile and pre-commit target states.
    //
    // Retry loop: on `PendingRetry` (concurrent pre_commit elsewhere in
    // this process holds a live token on a preempt-target path) we
    // back off and re-run pre_commit. `pre_commit` borrows the map and
    // only borrows individual `TargetStateValue`s into `reconcile` —
    // abortive paths pay zero clones; the host-specific reconcile impl
    // decides whether to clone into its action.
    //
    // `contained_target_state_paths` is wrapped in `Arc` to avoid full
    // HashSet rehash per retry (its size is unbounded — one entry per
    // fn-memo target). The other captures are O(1) clones (Arc-internal
    // or persistent data structures).
    let app_store = comp_ctx.app_ctx().app_store().clone();
    let stable_path = comp_ctx.stable_path().clone();
    let processor_name_owned: Option<Arc<str>> = processor_name.map(Arc::from);

    if comp_ctx.preview() {
        // Mirror normal precommit Phase 2 planning, but always return
        // `Ok(None)` from the callback so AppStore applies/commits no
        // tracking writes. Actions are collected in-memory only.
        let collector = comp_ctx
            .preview_collector()
            .cloned()
            .ok_or_else(|| internal_error!("preview mode requires a preview collector"))?;
        let preview_result: Arc<Mutex<Option<(bool, Option<String>)>>> = Arc::new(Mutex::new(None));

        let contained_target_state_paths = Arc::new(contained_target_state_paths);
        let declared_target_states = Arc::new(tokio::sync::Mutex::new(declared_target_states));

        let mut pending_backoff = std::time::Duration::from_millis(5);
        const MAX_PENDING_RETRIES: u32 = 8;
        let mut pending_attempt: u32 = 0;
        loop {
            let preview_result_capture = preview_result.clone();
            let collector = collector.clone();
            let captures: Arc<PreCommitCaptures<Prof>> = Arc::new(PreCommitCaptures {
                app_store: app_store.clone(),
                stable_path: stable_path.clone(),
                processor_name: processor_name_owned.clone(),
                contained_target_state_paths: Arc::clone(&contained_target_state_paths),
                target_states_providers: target_states_providers.clone(),
                declared_target_states: Arc::clone(&declared_target_states),
            });

            app_store
                .precommit(&stable_path, move |wtxn, session| {
                    let c = Arc::clone(&captures);
                    let preview_result_capture = preview_result_capture.clone();
                    let collector = collector.clone();
                    Box::pin(async move {
                        let declared_paths_all: Vec<TargetStatePath> = {
                            let guard = c.declared_target_states.lock().await;
                            guard.keys().cloned().collect()
                        };
                        let reads = session
                            .precommit_read(
                                wtxn,
                                PrecommitReadPlan {
                                    self_path: c.stable_path.clone(),
                                    self_token: process_token,
                                },
                            )
                            .await?;

                        let existing_tracking_info_bytes = reads.existing_tracking_info;
                        let tracking_info: Option<db_schema::StablePathEntryTrackingInfo<'_>> =
                            existing_tracking_info_bytes
                                .as_deref()
                                .map(from_msgpack_slice)
                                .transpose()?;
                        let existing_paths: std::collections::HashSet<TargetStatePath> =
                            tracking_info
                                .as_ref()
                                .map(|info| {
                                    info.target_state_items
                                        .keys()
                                        .map(|k| k.target_state_path.clone())
                                        .collect()
                                })
                                .unwrap_or_default();
                        let paths_to_claim: Vec<TargetStatePath> = declared_paths_all
                            .iter()
                            .filter(|p| !existing_paths.contains(*p))
                            .cloned()
                            .collect();

                        let claim = session
                            .precommit_claim_targets(
                                wtxn,
                                PrecommitClaimTargetsPlan {
                                    self_path: c.stable_path.clone(),
                                    paths_to_claim,
                                },
                            )
                            .await?;

                        let outcome = pre_commit(
                            &c.app_store,
                            wtxn,
                            process_token,
                            &c.stable_path,
                            full_reprocess,
                            c.processor_name.as_deref(),
                            &c.contained_target_state_paths,
                            &c.target_states_providers,
                            Arc::clone(&c.declared_target_states),
                            declared_paths_all,
                            tracking_info,
                            claim.prior_owners,
                            claim.preempted_owner_states,
                        )
                        .await?;

                        Ok(match outcome {
                            PreCommitOutcome::Done { output, write_plan: _ } => {
                                for input in output.actions_by_sinks.values() {
                                    if input.child_providers.is_some() {
                                        client_bail!(
                                            "preview currently supports flat/leaf target actions only; \
                                             target actions requiring child target providers are not supported yet"
                                        );
                                    }
                                }
                                let previously_exists = output.previously_exists;
                                let processor_name_for_del = output.processor_name_for_del;
                                let mut guard = collector.lock().unwrap();
                                for (_sink, input) in output.actions_by_sinks {
                                    guard.extend(input.actions);
                                }
                                *preview_result_capture.lock().unwrap() =
                                    Some((previously_exists, processor_name_for_del));
                                None::<(PrecommitWritePlan, PreCommitOutput<Prof>)>
                            }
                            PreCommitOutcome::PendingRetry => None,
                        })
                    })
                })
                .await?;

            if preview_result.lock().unwrap().is_some() {
                break;
            }
            pending_attempt += 1;
            if pending_attempt >= MAX_PENDING_RETRIES {
                client_bail!(
                    "preview pre_commit gave up after {} retries waiting for concurrent ownership transfer at {}",
                    MAX_PENDING_RETRIES,
                    comp_ctx.stable_path(),
                );
            }
            tokio::time::sleep(pending_backoff).await;
            pending_backoff =
                std::cmp::min(pending_backoff * 2, std::time::Duration::from_millis(200));
        }

        let (previously_exists, processor_name_for_del) = preview_result
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| internal_error!("preview pre_commit produced no output"))?;
        if let Some(ref name) = processor_name_for_del {
            collect_processor_name_name_for_del(name);
        }
        return Ok(SubmitOutput {
            built_target_states_providers,
            touched_previous_states: previously_exists,
        });
    }

    // Delete-mode preflight (was in `pre_commit` body pre-Session).
    // The early-return / `demote_component_only` decision needs to
    // happen before opening the submit session so the early-return
    // case doesn't write a stage marker.
    let mut demote_component_only = false;
    if comp_mode == ComponentProcessingMode::Delete {
        app_store.delete_component_memo(&stable_path).await?;
        if let Some((parent_path, key)) = stable_path.as_ref().split_parent() {
            match app_store.read_path_node_type(parent_path, key).await? {
                Some(db_schema::StablePathNodeType::Component) => {
                    return Ok(SubmitOutput {
                        built_target_states_providers: None,
                        touched_previous_states: false,
                    });
                }
                Some(db_schema::StablePathNodeType::Directory) => {
                    demote_component_only = true;
                }
                None => {}
            }
        }
    }

    let contained_target_state_paths = Arc::new(contained_target_state_paths);
    // `declared_target_states` is shared across retries via
    // `Arc<tokio::sync::Mutex<…>>`. The mutex is necessary (not just an
    // `Arc<BTreeMap<…>>`) because for some profiles `TargetStateValue`
    // is `!Sync` (e.g. Python's `Py<PyAny>`); `tokio::sync::Mutex<T>:
    // Sync` holds whenever `T: Send`. There's no contention — only the
    // outer submit task ever locks — so the mutex is purely a `Sync`
    // marker.
    let declared_target_states = Arc::new(tokio::sync::Mutex::new(declared_target_states));

    // Open the precommit txn via `AppStore::precommit` and drive
    // Phase 2 inside the callback: precommit_read + engine reconcile +
    // precommit_claim_targets, returning either `Some((plan, output))`
    // (AppStore applies + commits) or `None` (PendingRetry → AppStore
    // contributes no writes).
    //
    // Retry condition: `PendingRetry` — application-layer signal that
    // another in-process pre_commit holds a live token on a contested
    // target path. Bounded by `MAX_PENDING_RETRIES` (the other side
    // either commits or aborts in finite time).
    //
    // Each retry re-enters `precommit` from scratch; LMDB's
    // read-snapshot precommit_read has nothing to roll back on retry.
    let pre_commit_out: PreCommitOutput<Prof> = {
        let mut pending_backoff = std::time::Duration::from_millis(5);
        const MAX_PENDING_RETRIES: u32 = 8;
        let mut pending_attempt: u32 = 0;
        loop {
            // Per-attempt captures bundle. Every field is `O(1)` to
            // clone (Arc-internal or persistent data structure), so
            // the body's per-call `Arc::clone(&captures)` is cheap.
            let captures: Arc<PreCommitCaptures<Prof>> = Arc::new(PreCommitCaptures {
                app_store: app_store.clone(),
                stable_path: stable_path.clone(),
                processor_name: processor_name_owned.clone(),
                contained_target_state_paths: Arc::clone(&contained_target_state_paths),
                target_states_providers: target_states_providers.clone(),
                declared_target_states: Arc::clone(&declared_target_states),
            });

            // The eager `__cex` upsert (Phase 1) ran earlier from
            // `component.rs` via `eager_existence_upsert`, so this
            // drops straight into Phase 2.
            let output: Option<PreCommitOutput<Prof>> = app_store
                .precommit(&stable_path, move |wtxn, session| {
                    let c = Arc::clone(&captures);
                    Box::pin(async move {
                        let declared_paths_all: Vec<TargetStatePath> = {
                            let guard = c.declared_target_states.lock().await;
                            guard.keys().cloned().collect()
                        };
                        let reads = session
                            .precommit_read(
                                wtxn,
                                PrecommitReadPlan {
                                    self_path: c.stable_path.clone(),
                                    self_token: process_token,
                                },
                            )
                            .await?;

                        // Deserialize the existing tracking record once; the
                        // bytes stay alive in `existing_tracking_info_bytes`
                        // for the remainder of this attempt. Engine-side
                        // filter: only paths not already in self's tracking
                        // need a `__target` touch (per spec §4.1
                        // per-component exclusivity), so warm reprocess
                        // collapses to zero `__target` round-trips.
                        let existing_tracking_info_bytes = reads.existing_tracking_info;
                        let tracking_info: Option<db_schema::StablePathEntryTrackingInfo<'_>> =
                            existing_tracking_info_bytes
                                .as_deref()
                                .map(from_msgpack_slice)
                                .transpose()?;
                        let existing_paths: std::collections::HashSet<TargetStatePath> =
                            tracking_info
                                .as_ref()
                                .map(|info| {
                                    info.target_state_items
                                        .keys()
                                        .map(|k| k.target_state_path.clone())
                                        .collect()
                                })
                                .unwrap_or_default();
                        let paths_to_claim: Vec<TargetStatePath> = declared_paths_all
                            .iter()
                            .filter(|p| !existing_paths.contains(*p))
                            .cloned()
                            .collect();

                        let claim = session
                            .precommit_claim_targets(
                                wtxn,
                                PrecommitClaimTargetsPlan {
                                    self_path: c.stable_path.clone(),
                                    paths_to_claim,
                                },
                            )
                            .await?;

                        let outcome = pre_commit(
                            &c.app_store,
                            wtxn,
                            process_token,
                            &c.stable_path,
                            full_reprocess,
                            c.processor_name.as_deref(),
                            &c.contained_target_state_paths,
                            &c.target_states_providers,
                            Arc::clone(&c.declared_target_states),
                            declared_paths_all,
                            tracking_info,
                            claim.prior_owners,
                            claim.preempted_owner_states,
                        )
                        .await?;

                        Ok(match outcome {
                            PreCommitOutcome::Done { output, write_plan } => {
                                Some((write_plan, output))
                            }
                            PreCommitOutcome::PendingRetry => None,
                        })
                    })
                })
                .await?;

            match output {
                Some(output) => break output,
                None => {
                    // PendingRetry: AppStore contributed no writes. Back
                    // off, retry.
                    pending_attempt += 1;
                    if pending_attempt >= MAX_PENDING_RETRIES {
                        client_bail!(
                            "pre_commit gave up after {} retries waiting for concurrent ownership transfer at {}",
                            MAX_PENDING_RETRIES,
                            comp_ctx.stable_path(),
                        );
                    }
                    tokio::time::sleep(pending_backoff).await;
                    pending_backoff =
                        std::cmp::min(pending_backoff * 2, std::time::Duration::from_millis(200));
                }
            }
        }
    };

    if let Some(ref name) = pre_commit_out.processor_name_for_del {
        collect_processor_name_name_for_del(name);
    }
    let curr_version = pre_commit_out.curr_version;
    let touched_previous_states = pre_commit_out.previously_exists;
    let actions_by_sinks = pre_commit_out.actions_by_sinks;

    // Apply deferred provider-generation updates now that precommit
    // has committed — past this point no retry can roll back.
    // `set_provider_generation` is `OnceLock::set`, so calling it at
    // most once per successful submit is the invariant we preserve.
    for (child_provider, new_gen) in pre_commit_out.deferred_provider_generations {
        child_provider.set_provider_generation(new_gen)?;
    }

    // Sink apply. On failure we clear the stage marker so a
    // subsequent precommit doesn't see a stale token from this
    // attempt.
    let sink_result: Result<()> = async {
        let host_runtime_ctx = comp_ctx.app_ctx().env().host_runtime_ctx();
        for (sink, input) in actions_by_sinks {
            let handlers = sink
                .apply(host_runtime_ctx, Arc::clone(comp_ctx.host_ctx()), input.actions)
                .await?;
            if let Some(child_providers) = input.child_providers {
                let Some(handlers) = handlers else {
                    client_bail!("expect child providers returned by Sink");
                };
                if handlers.len() != child_providers.len() {
                    client_bail!(
                        "expect child providers returned by Sink to be the same length as the actions ({}), got {}",
                        child_providers.len(),
                        handlers.len(),
                    );
                }
                for (child_target_state_def, child_provider) in
                    std::iter::zip(handlers, child_providers)
                {
                    if let Some(child_provider) = child_provider {
                        if let Some(child_target_state_def) = child_target_state_def {
                            pending_fulfillments
                                .push((child_provider, child_target_state_def.handler));
                        } else {
                            client_bail!(
                                "expect child provider returned by Sink to be fulfilled"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
    .await;

    if let Err(e) = sink_result {
        cleanup_pending_token(comp_ctx, process_token).await;
        return Err(e);
    }

    // Commit. `AppStore::commit` is a normal trait method — no
    // session handoff needed.
    let committer = Committer::new(comp_ctx, &target_states_providers, demote_component_only)?;
    if let Err(e) = committer
        .commit(child_path_set, fn_memos, user_states, curr_version)
        .await
    {
        // The commit txn either committed or rolled back before
        // returning Err — either way the stage marker may still be
        // live, so clear it via the retry helper.
        cleanup_pending_token(comp_ctx, process_token).await;
        return Err(e);
    }

    // Fulfill child handlers and register their attachment providers.
    // Done after commit so the immutable borrow on providers is released.
    if let Some(ref mut registry) = built_target_states_providers {
        for (child_provider, handler) in pending_fulfillments {
            child_provider.fulfill_handler(handler, registry)?;
        }
    }

    Ok(SubmitOutput {
        built_target_states_providers,
        touched_previous_states,
    })
}

/// Clear this component's `pending_process_token` field in the
/// tracking-info blob. Called when sink_apply or commit failed
/// between the successful precommit txn and a successful
/// `app_store.commit`.
///
/// Without this, the token the precommit wrote would deadlock any
/// future pre_commit in this process that touches an overlapping
/// path (live-token branch in the detection sub-pass).
///
/// Items the failed pre_commit modified retain their multi-state
/// shape on disk; the next pre_commit's main pass picks them up via
/// `prev_item.is_pending()` → force `prev_may_be_missing = true`, so
/// the sink-tracking divergence the failure may have caused gets
/// re-reconciled.
///
/// Each iteration calls
/// [`AppStoreTrait::clear_stage_marker`](crate::state_store::AppStoreTrait::clear_stage_marker).
/// Retried indefinitely with exponential backoff — every failure is
/// logged but the function does not return until the cleanup
/// succeeds. If the process exits while this is still retrying, the
/// next process picks up the leftover state via the same
/// `is_pending()` check.
async fn cleanup_pending_token<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    process_token: u128,
) {
    let app_store = comp_ctx.app_ctx().app_store().clone();
    let path = comp_ctx.stable_path().clone();
    let mut backoff = std::time::Duration::from_millis(10);
    loop {
        match app_store.clear_stage_marker(&path, process_token).await {
            Ok(()) => return,
            Err(e) => {
                error!(
                    "Failed to clean up pending stage token for {}: {:?}; will retry",
                    comp_ctx.stable_path(),
                    e
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, std::time::Duration::from_secs(5));
            }
        }
    }
}

#[instrument(name = "post_submit_after_ready", skip_all)]
pub(crate) async fn post_submit_for_build<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    comp_memo: Option<(
        Fingerprint,
        &'_ Prof::FunctionData,
        &'_ MemoStatesPayload<Prof>,
    )>,
) -> Result<()> {
    let Some((fp, ret, memo_states)) = comp_memo else {
        return Ok(());
    };

    let ret_bytes = ret.to_bytes()?;
    let memo_states_serialized = serialize_memo_values::<Prof>(&memo_states.positional)?;
    let context_memo_states_serialized =
        serialize_context_memo_states::<Prof>(&memo_states.by_context_fp)?;
    let memo_info = db_schema::ComponentMemoizationInfo {
        processor_fp: fp,
        return_value: db_schema::MemoizedValue::Inlined(Cow::Borrowed(ret_bytes.as_ref())),
        logic_deps: comp_ctx.take_logic_deps(),
        memo_states: memo_states_serialized,
        context_memo_states: context_memo_states_serialized,
    };
    let encoded = rmp_serde::to_vec_named(&memo_info)?;

    // Routes through the single-writer batcher so concurrent callers
    // coalesce into one underlying write txn.
    comp_ctx
        .app_ctx()
        .app_store()
        .finalize_memoization(comp_ctx.stable_path(), &encoded)
        .await
}

pub(crate) async fn cleanup_tombstone<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
) -> Result<()> {
    let Some(parent) = comp_ctx.component().parent() else {
        return Ok(());
    };
    let owner_path: StablePath = parent.stable_path().clone();
    let relative_path: StablePath = comp_ctx
        .stable_path()
        .as_ref()
        .strip_parent(owner_path.as_ref())?
        .into();
    // Routes through the single-writer batcher. Per-component
    // exclusivity rules out races on the same tombstone; GC's
    // eventual-consistency tolerates a missed sweep.
    comp_ctx
        .app_ctx()
        .app_store()
        .cleanup_tombstone(&owner_path, &relative_path)
        .await
}

pub(crate) async fn ensure_path_node_type(
    app_store: &AppStore,
    wtxn: &mut WriteTxn<'_>,
    parent_path: StablePathRef<'_>,
    key: &StableKey,
    target_node_type: db_schema::StablePathNodeType,
) -> Result<()> {
    app_store
        .ensure_path_node_type(wtxn, parent_path, key, target_node_type)
        .await
}

/// Eager existence upsert at the start of Build. Writes the component's own
/// `ChildExistence(self)` row into its parent and recursively ensures every
/// ancestor existence bit up to the root, in its own write transaction
/// (separate from submit/commit). Called once per Build invocation before
/// the user processor runs.
///
/// Maintains the invariant: a component's existence bit (and the full
/// ancestor chain) must exist in DB before any of its (or its descendants')
/// tracked state. See `internal_states.md` §3.1 / §3.3.
///
/// Routes through the single-writer batcher so concurrent
/// eager-upserts coalesce — opening our own `env.write_txn()` would
/// bypass the batcher and serialize every eager-upsert through heed's
/// writer mutex.
pub(crate) async fn eager_existence_upsert<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
) -> Result<()> {
    let path = comp_ctx.stable_path();
    if path.is_empty() {
        return Ok(());
    }
    // The in-process parent has already had its own `ensure_existence_chain`
    // called before this child was mounted (per-component mount ordering),
    // so its `__cex` chain is in place and we can skip ancestor rows
    // for any prefix of the parent's path. The root app component has
    // no parent — treat its empty stable_path as the known parent.
    let known_parent_path = comp_ctx
        .component()
        .parent()
        .map(|p| p.stable_path().clone())
        .unwrap_or_else(StablePath::root);
    comp_ctx
        .app_ctx()
        .app_store()
        .ensure_existence_chain(path, &known_parent_path)
        .await
}

/// Walk every entry in the function-memo cache and produce the set of
/// target-state paths protected from GC because they are referenced
/// (directly or transitively) by an already-stored memo.
///
/// All reads are in-memory; the cache was eagerly prefetched at the start
/// of build mode. Untouched entries remain in `Stored(_)` state and get
/// deleted at flush time; entries that are reachable as transitive deps
/// of an already-stored memo are decoded in place so flush keeps them.
fn finalize_fn_call_memoization<Prof: EngineProfile>(
    comp_ctx: &ComponentProcessorContext<Prof>,
    cache: &FnMemoCache<Prof>,
) -> Result<HashSet<TargetStatePath>> {
    let env = comp_ctx.app_ctx().env();
    let mut contained_target_state_paths: HashSet<TargetStatePath> = HashSet::new();
    let mut visited: HashSet<Fingerprint> = HashSet::new();
    let mut deps_to_walk: VecDeque<Fingerprint> = VecDeque::new();

    // First pass: every Ready(Some) entry with `already_stored=true`
    // contributes its target states and seeds the dep walk. `already_stored=false`
    // entries were just executed this run; their target states are in the
    // regular declared_target_states pipeline, not "contained".
    for (fp, lock) in cache.iter() {
        let guard = lock
            .try_read()
            .map_err(|_| internal_error!("fn call memo entry is locked during finalize"))?;
        if let FnCallMemoEntry::Ready(Some(memo)) = &*guard {
            if memo.already_stored {
                visited.insert(*fp);
                contained_target_state_paths.extend(memo.target_state_paths.iter().cloned());
                for dep_fp in memo.dependency_memo_entries.iter() {
                    if visited.insert(*dep_fp) {
                        deps_to_walk.push_back(*dep_fp);
                    }
                }
            }
        }
    }

    // Transitive dep walk: decode-on-access `Stored` entries so flush keeps
    // them, and collect their target states. Entries already `Ready` skip
    // straight to the field read.
    while let Some(fp) = deps_to_walk.pop_front() {
        let Some(lock) = cache.get(fp) else {
            continue;
        };
        let mut guard = lock
            .try_write()
            .map_err(|_| internal_error!("fn call memo entry is locked during finalize"))?;
        if matches!(&*guard, FnCallMemoEntry::Stored(_)) {
            decode_stored_entry::<Prof>(&mut guard, env)?;
        }
        if let FnCallMemoEntry::Ready(Some(memo)) = &*guard {
            contained_target_state_paths.extend(memo.target_state_paths.iter().cloned());
            for dep_fp in memo.dependency_memo_entries.iter() {
                if visited.insert(*dep_fp) {
                    deps_to_walk.push_back(*dep_fp);
                }
            }
        }
    }
    Ok(contained_target_state_paths)
}
