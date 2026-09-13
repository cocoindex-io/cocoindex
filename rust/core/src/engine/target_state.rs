use crate::prelude::*;

use crate::{
    engine::{context::ComponentProcessorContext, profile::EngineProfile},
    state::{
        stable_path::StableKey,
        target_state_path::{TargetStatePath, TargetStateProviderGeneration},
    },
};

use cocoindex_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};
use std::{
    collections::HashMap,
    future::Future,
    hash::{Hash, Hasher},
    ops::Range,
};

enum ChildTargetSlotState<H> {
    Pending,
    Fulfilled(H),
    Consumed,
}

/// Fulfillment handle for the child target provider of one action.
///
/// The engine mints one slot per action whose target state was declared with
/// `declare_target_state_with_child`, hands it to the sink alongside the action,
/// and reads the handler back (via [`Self::take`]) once the sink call returns.
/// A slot is fulfilled at most once per sink call; fulfilling it after the
/// engine has read it back is an error too, so a sink that stashes a slot for
/// later is caught. When a failed batch is re-applied in smaller batches, the
/// slots of the re-applied actions are [reset](Self::reset) first.
///
/// Clones share one state: the engine keeps one clone, the sink gets the other.
pub struct ChildTargetSlot<Prof: EngineProfile> {
    state: Arc<Mutex<ChildTargetSlotState<Prof::TargetHdl>>>,
}

impl<Prof: EngineProfile> Clone for ChildTargetSlot<Prof> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<Prof: EngineProfile> ChildTargetSlot<Prof> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ChildTargetSlotState::Pending)),
        }
    }

    /// Provide the handler for the child target states under this action.
    pub fn fulfill(&self, handler: Prof::TargetHdl) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        match &*state {
            ChildTargetSlotState::Pending => {
                *state = ChildTargetSlotState::Fulfilled(handler);
                Ok(())
            }
            ChildTargetSlotState::Fulfilled(_) => {
                client_bail!("child target slot fulfilled more than once")
            }
            ChildTargetSlotState::Consumed => {
                client_bail!("child target slot fulfilled after its sink call returned")
            }
        }
    }

    /// Forget a fulfillment from a failed sink call, so the action can be
    /// applied again. Only the runner calls this, between attempts of one
    /// batch; the engine reads the slot back only after the last attempt.
    fn reset(&self) {
        *self.state.lock().unwrap() = ChildTargetSlotState::Pending;
    }

    /// Take the handler out; `None` if the slot was never fulfilled. Consumes
    /// the slot: a later `fulfill` fails, and a second `take` is an internal
    /// error.
    pub fn take(&self) -> Result<Option<Prof::TargetHdl>> {
        let mut state = self.state.lock().unwrap();
        match std::mem::replace(&mut *state, ChildTargetSlotState::Consumed) {
            ChildTargetSlotState::Pending => Ok(None),
            ChildTargetSlotState::Fulfilled(handler) => Ok(Some(handler)),
            ChildTargetSlotState::Consumed => {
                Err(internal_error!("child target slot taken more than once"))
            }
        }
    }
}

/// One action for a sink call, with the slot for its child target provider
/// when the declaring call was `declare_target_state_with_child`.
pub type TargetActionWithChildSlot<Prof> = (
    <Prof as EngineProfile>::TargetAction,
    Option<ChildTargetSlot<Prof>>,
);

#[async_trait]
pub trait TargetActionSink<Prof: EngineProfile>: Send + Sync + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Apply `actions` to the external system as one unit. Every child slot
    /// handed over must be fulfilled before this returns; the engine reads
    /// them back afterwards.
    ///
    /// One call may carry the actions of several processing components: the
    /// engine merges the actions of components that finish while an earlier
    /// call is in flight. When a call fails, the engine re-applies subsets of
    /// the same actions (with their child slots reset) to isolate the failure,
    /// so an implementation must tolerate seeing actions of a failed call
    /// again — which idempotent actions do by construction. The actions are
    /// borrowed for that reason: the engine keeps them for the retry.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    async fn apply(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        host_ctx: Arc<Prof::HostCtx>,
        actions: &[TargetActionWithChildSlot<Prof>],
    ) -> Result<()>;
}

/// Cloneable handle to a target action sink and its per-sink batcher.
#[derive(Clone)]
pub struct TargetActionSinkKeeper<Prof: EngineProfile> {
    inner: Arc<TargetActionSinkKeeperInner<Prof>>,
}

struct TargetActionSinkKeeperInner<Prof: EngineProfile> {
    batcher: Batcher<TargetActionRunner<Prof>>,
}

impl<Prof: EngineProfile> TargetActionSinkKeeper<Prof> {
    pub fn new(sink: Prof::TargetActionSink) -> Self {
        let sink = Arc::new(sink);
        Self {
            inner: Arc::new(TargetActionSinkKeeperInner {
                batcher: Batcher::new(
                    TargetActionRunner { sink },
                    Arc::new(BatchQueue::new()),
                    BatchingOptions::default(),
                ),
            }),
        }
    }

    pub async fn apply(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        host_ctx: Arc<Prof::HostCtx>,
        actions: Vec<TargetActionWithChildSlot<Prof>>,
    ) -> Result<()> {
        if actions.is_empty() {
            return Ok(());
        }
        // The outer `Result` is the batcher's own failure (e.g. cancellation);
        // the inner one is this input's outcome from the sink.
        self.inner
            .batcher
            .run(TargetActionRunnerInput {
                host_runtime_ctx: host_runtime_ctx.clone(),
                host_ctx,
                actions,
            })
            .await?
    }

    pub fn downgrade(&self) -> WeakTargetActionSinkKeeper<Prof> {
        WeakTargetActionSinkKeeper {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

/// Weak counterpart of [`TargetActionSinkKeeper`], for registries that intern
/// keepers without keeping an otherwise-idle sink (and its batcher) alive.
pub struct WeakTargetActionSinkKeeper<Prof: EngineProfile> {
    inner: std::sync::Weak<TargetActionSinkKeeperInner<Prof>>,
}

impl<Prof: EngineProfile> WeakTargetActionSinkKeeper<Prof> {
    pub fn upgrade(&self) -> Option<TargetActionSinkKeeper<Prof>> {
        self.inner
            .upgrade()
            .map(|inner| TargetActionSinkKeeper { inner })
    }
}

impl<Prof: EngineProfile> PartialEq for TargetActionSinkKeeper<Prof> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<Prof: EngineProfile> Eq for TargetActionSinkKeeper<Prof> {}

impl<Prof: EngineProfile> Hash for TargetActionSinkKeeper<Prof> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.inner).hash(state);
    }
}

struct TargetActionRunnerInput<Prof: EngineProfile> {
    host_runtime_ctx: Prof::HostRuntimeCtx,
    host_ctx: Arc<Prof::HostCtx>,
    actions: Vec<TargetActionWithChildSlot<Prof>>,
}

struct TargetActionRunnerContext<Prof: EngineProfile> {
    host_runtime_ctx: Prof::HostRuntimeCtx,
    host_ctx: Arc<Prof::HostCtx>,
}

impl<Prof: EngineProfile> PartialEq for TargetActionRunnerContext<Prof> {
    fn eq(&self, other: &Self) -> bool {
        self.host_runtime_ctx == other.host_runtime_ctx
            && Arc::ptr_eq(&self.host_ctx, &other.host_ctx)
    }
}

impl<Prof: EngineProfile> Eq for TargetActionRunnerContext<Prof> {}

impl<Prof: EngineProfile> Hash for TargetActionRunnerContext<Prof> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host_runtime_ctx.hash(state);
        Arc::as_ptr(&self.host_ctx).hash(state);
    }
}

/// Runs a sink's batches. Each input is one processing component's reconciled
/// actions for the sink; the batcher merges the inputs of components that
/// finish while an earlier batch is in flight.
struct TargetActionRunner<Prof: EngineProfile> {
    sink: Arc<Prof::TargetActionSink>,
}

#[async_trait]
impl<Prof: EngineProfile> Runner for TargetActionRunner<Prof> {
    type Input = TargetActionRunnerInput<Prof>;
    /// Per-input outcome. Merging inputs into one sink call is an
    /// optimization that must not couple their fates, so a failure is
    /// reported per input rather than as a batch-level `Err`, which the
    /// batcher would fan out to every input of the batch.
    type Output = Result<()>;

    async fn run(
        &self,
        inputs: Vec<Self::Input>,
    ) -> Result<impl ExactSizeIterator<Item = Self::Output>> {
        let num_inputs = inputs.len();
        let mut groups = HashMap::<
            TargetActionRunnerContext<Prof>,
            Vec<(usize, Vec<TargetActionWithChildSlot<Prof>>)>,
        >::new();
        for (input_idx, input) in inputs.into_iter().enumerate() {
            let context = TargetActionRunnerContext {
                host_runtime_ctx: input.host_runtime_ctx,
                host_ctx: input.host_ctx,
            };
            groups
                .entry(context)
                .or_default()
                .push((input_idx, input.actions));
        }

        let mut outputs: Vec<Option<Self::Output>> =
            std::iter::repeat_with(|| None).take(num_inputs).collect();
        for (context, inputs) in groups {
            // The sink wants one flat action list per compatible host
            // context. Remember each input's range within it so a failed
            // call can be retried along input boundaries.
            let mut actions = Vec::new();
            let mut input_indexes = Vec::with_capacity(inputs.len());
            let mut spans = Vec::with_capacity(inputs.len());
            for (input_idx, mut input_actions) in inputs {
                let start = actions.len();
                actions.append(&mut input_actions);
                input_indexes.push(input_idx);
                spans.push(start..actions.len());
            }

            let results = apply_isolating_failures(&spans, |range| {
                // A retry re-applies actions whose slots the failed call may
                // have fulfilled already; the last attempt's fulfillment is
                // the one the engine reads back.
                for (_, slot) in &actions[range.clone()] {
                    if let Some(slot) = slot {
                        slot.reset();
                    }
                }
                self.sink.apply(
                    &context.host_runtime_ctx,
                    Arc::clone(&context.host_ctx),
                    &actions[range],
                )
            })
            .await;
            for (input_idx, result) in std::iter::zip(input_indexes, results) {
                outputs[input_idx] = Some(result);
            }
        }

        Ok(outputs
            .into_iter()
            .map(|output| output.expect("every input is assigned an outcome")))
    }
}

/// Apply a batch merged from several components through `apply`, confining a
/// failure to the components it belongs to.
///
/// `spans` locates each component's actions within one flat action list:
/// contiguous, in component order. `apply(range)` makes one sink call over
/// the actions in `range`.
///
/// The whole batch is applied first, so the happy path costs the single sink
/// call it always did. When that call fails and the batch spans more than one
/// component, the batch is bisected along component boundaries — never inside
/// a component, whose actions stay one atomic unit — and each half is
/// re-applied, recursively. A sub-batch's error thus reaches only the
/// components in it, and a component that still fails on its own gets its own
/// error. Worst case (every component failing) costs `2n - 1` sink calls for
/// `n` components; a single bad component costs about `2·log2(n)`.
///
/// Cancellation and deadline errors are not caused by any particular
/// component, so they are reported to every component of the failed batch
/// without retrying.
async fn apply_isolating_failures<Fut>(
    spans: &[Range<usize>],
    apply: impl Fn(Range<usize>) -> Fut,
) -> Vec<Result<()>>
where
    Fut: Future<Output = Result<()>>,
{
    let mut results = Vec::with_capacity(spans.len());
    // Sub-batches still to apply, as index ranges over `spans`. The second
    // half of a split is pushed first, so sub-batches complete in component
    // order and each one's results are simply appended.
    let mut pending = vec![0..spans.len()];
    while let Some(sub) = pending.pop() {
        debug_assert_eq!(results.len(), sub.start);
        let sub_spans = &spans[sub.clone()];
        let Some(action_range) = sub_spans
            .first()
            .zip(sub_spans.last())
            .map(|(first, last)| first.start..last.end)
        else {
            continue;
        };
        let err = match apply(action_range).await {
            Ok(()) => {
                results.extend(sub_spans.iter().map(|_| Ok(())));
                continue;
            }
            Err(err) => {
                if sub.len() > 1 && !err.is_cancelled() && !err.is_deadline_exceeded() {
                    if sub.len() == spans.len() {
                        warn!(
                            "Target action sink failed on a batch merged from {} components; \
                             retrying in smaller batches to isolate the failure: {err}",
                            sub.len()
                        );
                    } else {
                        debug!(
                            "Retrying {} of the merged components in smaller batches: {err}",
                            sub.len()
                        );
                    }
                    let mid = sub.start + sub.len() / 2;
                    pending.push(mid..sub.end);
                    pending.push(sub.start..mid);
                    continue;
                }
                err
            }
        };
        // Every component of this sub-batch gets the error: a replica for
        // all but one, the original for the last.
        results.extend((1..sub_spans.len()).map(|_| Err(err.replica())));
        results.push(Err(err));
    }
    results
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildInvalidation {
    Destructive,
    Lossy,
}

pub struct TargetReconcileOutput<Prof: EngineProfile> {
    pub action: Prof::TargetAction,
    pub sink: TargetActionSinkKeeper<Prof>,
    pub tracking_record: Option<Prof::TargetStateTrackingRecord>,
    pub child_invalidation: Option<ChildInvalidation>,
}

pub trait TargetHandler<Prof: EngineProfile>: Send + Sync + Sized + 'static {
    /// Reconcile the desired target state against the previously-tracked
    /// records, returning the action to take.
    ///
    /// `desired_target_state` is borrowed (not owned) because the engine
    /// holds it under a short-lived `tokio::sync::MutexGuard` for the
    /// duration of this call — see the lock-scoped call site in
    /// `submit()`'s `pre_commit`. Borrowing here lets the host-specific
    /// implementation decide whether (and how) to clone:
    ///
    /// * Native Rust profile (`Value: Clone`): typically `value.clone()`
    ///   when constructing the `Action`.
    /// * Python profile (`Py<PyAny>: !Clone`): `value.clone_ref(py)`
    ///   under the GIL.
    ///
    /// Avoids forcing every call site to round-trip through an
    /// engine-level `clone_target_state_value` even when the impl
    /// might not need an owned copy.
    fn reconcile(
        &self,
        key: StableKey,
        desired_target_state: Option<&Prof::TargetStateValue>,
        prev_possible_records: &[Prof::TargetStateTrackingRecord],
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Prof>>>;

    /// Return all attachment types this handler supports, keyed by type name.
    /// The engine eagerly registers these as providers so that orphaned
    /// attachments can be cleaned up even when not declared in the current run.
    fn attachments(&self) -> Result<Vec<(Arc<str>, Prof::TargetHdl)>> {
        Ok(vec![])
    }
}

pub(crate) struct TargetStateProviderInner<Prof: EngineProfile> {
    parent_provider: Option<TargetStateProvider<Prof>>,
    stable_key: StableKey,
    target_state_path: TargetStatePath,
    /// Whether this provider was created for a declared target state (child
    /// providers from `register_lazy`), as opposed to provider-only segments
    /// (root providers, attachments). Target-state-backed segments resolve
    /// via the declaring component's owner-index/tracking records, so they
    /// need no persisted segment-name entry.
    backed_by_target_state: bool,
    handler: OnceLock<Prof::TargetHdl>,
    orphaned: OnceLock<()>,
    provider_generation: OnceLock<TargetStateProviderGeneration>,
    attachments: Mutex<HashMap<Arc<str>, TargetStateProvider<Prof>>>,
}

#[derive(Clone)]
pub struct TargetStateProvider<Prof: EngineProfile> {
    pub(crate) inner: Arc<TargetStateProviderInner<Prof>>,
}

impl<Prof: EngineProfile> TargetStateProvider<Prof> {
    pub fn target_state_path(&self) -> &TargetStatePath {
        &self.inner.target_state_path
    }

    pub fn handler(&self) -> Option<&Prof::TargetHdl> {
        self.inner.handler.get()
    }

    /// Fulfill the handler and eagerly register all its attachment providers
    /// into the given registry so that `pre_commit` Phase 2 can clean up
    /// orphaned attachments.
    pub fn fulfill_handler(
        &self,
        handler: Prof::TargetHdl,
        registry: &mut TargetStateProviderRegistry<Prof>,
    ) -> Result<()> {
        self.inner
            .handler
            .set(handler)
            .map_err(|_| internal_error!("Handler is already fulfilled"))?;
        self.register_all_attachment_providers(registry)
    }

    pub fn stable_key(&self) -> &StableKey {
        &self.inner.stable_key
    }

    pub fn stable_key_chain(&self) -> Vec<StableKey> {
        let mut chain = vec![self.inner.stable_key.clone()];
        let mut current = self;
        while let Some(parent) = &current.inner.parent_provider {
            chain.push(parent.inner.stable_key.clone());
            current = parent;
        }
        chain.reverse();
        chain
    }

    /// Collect segment-name entries (lone segment fingerprint → stable key)
    /// for this provider and its ancestors, stopping at the first ancestor
    /// backed by a declared target state: that segment resolves via its
    /// declaring component's owner-index/tracking records, and that
    /// component's own pre-commit covers the segments above it. `out` dedups
    /// across calls; an already-present fingerprint is skipped but the walk
    /// continues, since providers at different depths can share a segment
    /// (e.g. the same attachment type on two tables).
    pub(crate) fn collect_provider_only_segment_names(
        &self,
        out: &mut HashMap<utils::fingerprint::Fingerprint, StableKey>,
    ) {
        let mut current = self;
        loop {
            if current.inner.backed_by_target_state {
                return;
            }
            let fp = *current
                .inner
                .target_state_path
                .as_slice()
                .last()
                .expect("target state path is never empty");
            out.entry(fp)
                .or_insert_with(|| current.inner.stable_key.clone());
            match &current.inner.parent_provider {
                Some(parent) => current = parent,
                None => return,
            }
        }
    }

    pub fn is_orphaned(&self) -> bool {
        self.inner.orphaned.get().is_some()
    }

    pub fn provider_generation(&self) -> Option<&TargetStateProviderGeneration> {
        self.inner.provider_generation.get()
    }

    pub fn set_provider_generation(&self, generation: TargetStateProviderGeneration) -> Result<()> {
        self.inner
            .provider_generation
            .set(generation)
            .map_err(|_| internal_error!("Provider generation already set"))
    }

    fn register_all_attachment_providers(
        &self,
        registry: &mut TargetStateProviderRegistry<Prof>,
    ) -> Result<()> {
        let handler = match self.handler() {
            Some(h) => h,
            None => return Ok(()),
        };
        let att_entries = handler.attachments()?;
        if att_entries.is_empty() {
            return Ok(());
        }

        let mut attachments = self.inner.attachments.lock().unwrap();
        let provider_generation = self.provider_generation().cloned().unwrap_or_default();

        for (att_type, att_handler) in att_entries {
            if attachments.contains_key(&*att_type) {
                continue;
            }
            let symbol_key = StableKey::Symbol(att_type.clone());
            let target_state_path = self.target_state_path().concat(&symbol_key);

            let provider = TargetStateProvider {
                inner: Arc::new(TargetStateProviderInner {
                    parent_provider: Some(self.clone()),
                    stable_key: symbol_key,
                    target_state_path: target_state_path.clone(),
                    backed_by_target_state: false,
                    handler: OnceLock::from(att_handler),
                    orphaned: OnceLock::new(),
                    provider_generation: OnceLock::from(provider_generation.clone()),
                    attachments: Mutex::new(HashMap::new()),
                }),
            };

            registry.add(target_state_path, provider.clone())?;
            attachments.insert(att_type, provider);
        }
        Ok(())
    }

    /// Get or create an attachment provider for the given type.
    /// Called from Python when an attachment is declared (e.g. `declare_vector_index`).
    /// Returns the cached provider if already registered (by eager or prior lazy call).
    pub fn register_attachment_provider(
        &self,
        comp_ctx: &ComponentProcessorContext<Prof>,
        att_type: &str,
    ) -> Result<TargetStateProvider<Prof>> {
        // Fast path: already registered (eagerly or by a previous call).
        let attachments = self.inner.attachments.lock().unwrap();
        if let Some(existing) = attachments.get(att_type) {
            return Ok(existing.clone());
        }
        drop(attachments);

        // Slow path: not yet registered. This can happen if the handler doesn't
        // include this type in attachments(), or during the first run before
        // eager registration has occurred. Build it from the handler.
        let handler = self
            .handler()
            .ok_or_else(|| client_error!("Cannot register attachment on unfulfilled provider"))?;
        let att_entries = handler.attachments()?;
        let att_handler = att_entries
            .into_iter()
            .find(|(k, _)| &**k == att_type)
            .map(|(_, h)| h)
            .ok_or_else(|| {
                client_error!("Handler does not support attachment type: {att_type:?}")
            })?;

        let symbol_key = StableKey::Symbol(att_type.into());
        let target_state_path = self.target_state_path().concat(&symbol_key);

        let provider_generation = self.provider_generation().cloned().unwrap_or_default();

        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: Some(self.clone()),
                stable_key: symbol_key,
                target_state_path: target_state_path.clone(),
                backed_by_target_state: false,
                handler: OnceLock::from(att_handler),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::from(provider_generation),
                attachments: Mutex::new(HashMap::new()),
            }),
        };

        comp_ctx.update_building_state(|building_state| {
            building_state
                .target_states
                .provider_registry
                .add(target_state_path, provider.clone())
        })?;

        let mut attachments = self.inner.attachments.lock().unwrap();
        attachments.insert(att_type.into(), provider.clone());
        Ok(provider)
    }
}

#[derive(Default)]
pub struct TargetStateProviderRegistry<Prof: EngineProfile> {
    pub(crate) providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    pub(crate) curr_target_state_paths: Vec<TargetStatePath>,
}

impl<Prof: EngineProfile> TargetStateProviderRegistry<Prof> {
    pub fn new(
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    ) -> Self {
        Self {
            providers,
            curr_target_state_paths: Vec::new(),
        }
    }

    pub fn add(
        &mut self,
        target_state_path: TargetStatePath,
        provider: TargetStateProvider<Prof>,
    ) -> Result<()> {
        if self.providers.contains_key(&target_state_path) {
            client_bail!(
                "Target state provider already registered for path: {:?}",
                target_state_path
            );
        }
        self.curr_target_state_paths.push(target_state_path.clone());
        self.providers.insert_mut(target_state_path, provider);
        Ok(())
    }

    pub fn register_root(
        &mut self,
        name: String,
        handler: Prof::TargetHdl,
    ) -> Result<TargetStateProvider<Prof>> {
        let target_state_path =
            TargetStatePath::new(utils::fingerprint::Fingerprint::from(&name)?, None);
        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: None,
                stable_key: StableKey::Symbol(name.into()),
                target_state_path: target_state_path.clone(),
                backed_by_target_state: false,
                handler: OnceLock::from(handler),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::new(),
                attachments: Mutex::new(HashMap::new()),
            }),
        };
        self.add(target_state_path, provider.clone())?;
        provider.register_all_attachment_providers(self)?;
        Ok(provider)
    }

    pub fn register_lazy(
        &mut self,
        parent_provider: &TargetStateProvider<Prof>,
        stable_key: StableKey,
    ) -> Result<TargetStateProvider<Prof>> {
        let target_state_path = parent_provider.target_state_path().concat(&stable_key);
        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: Some(parent_provider.clone()),
                stable_key,
                target_state_path: target_state_path.clone(),
                backed_by_target_state: true,
                handler: OnceLock::new(),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::new(),
                attachments: Mutex::new(HashMap::new()),
            }),
        };
        self.add(target_state_path, provider.clone())?;
        Ok(provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sink stand-in over actions `0..n`: records each call's action range
    /// and fails a call that includes a poisoned action.
    struct FakeSink {
        poisoned: Vec<usize>,
        calls: Mutex<Vec<Range<usize>>>,
    }

    impl FakeSink {
        fn new(poisoned: &[usize]) -> Self {
            Self {
                poisoned: poisoned.to_vec(),
                calls: Mutex::new(Vec::new()),
            }
        }

        async fn apply(&self, range: Range<usize>) -> Result<()> {
            self.calls.lock().unwrap().push(range.clone());
            if let Some(bad) = range.clone().find(|idx| self.poisoned.contains(idx)) {
                return Err(client_error!("poisoned action {bad}"));
            }
            Ok(())
        }

        fn calls(&self) -> Vec<Range<usize>> {
            self.calls.lock().unwrap().clone()
        }
    }

    /// Spans of consecutive components with the given action counts.
    fn spans(sizes: &[usize]) -> Vec<Range<usize>> {
        let mut start = 0;
        sizes
            .iter()
            .map(|size| {
                let span = start..start + size;
                start += size;
                span
            })
            .collect()
    }

    /// Per-component outcomes with errors rendered as their message.
    fn summarize(results: Vec<Result<()>>) -> Vec<std::result::Result<(), String>> {
        results
            .into_iter()
            .map(|result| result.map_err(|err| err.to_string()))
            .collect()
    }

    fn poisoned(idx: usize) -> std::result::Result<(), String> {
        Err(format!("Invalid Request: poisoned action {idx}"))
    }

    #[tokio::test]
    async fn success_costs_a_single_call() {
        let sink = FakeSink::new(&[]);
        let results = apply_isolating_failures(&spans(&[2, 1, 3]), |r| sink.apply(r)).await;
        assert_eq!(sink.calls(), vec![0..6]);
        assert_eq!(summarize(results), vec![Ok(()), Ok(()), Ok(())]);
    }

    #[tokio::test]
    async fn failure_is_confined_to_the_poisoned_component() {
        let sink = FakeSink::new(&[2]);
        let results = apply_isolating_failures(&spans(&[1, 1, 1, 1]), |r| sink.apply(r)).await;
        // Whole batch, then bisection down to the poisoned component; the
        // clean half and the clean sibling are applied once each.
        assert_eq!(sink.calls(), vec![0..4, 0..2, 2..4, 2..3, 3..4]);
        assert_eq!(
            summarize(results),
            vec![Ok(()), Ok(()), poisoned(2), Ok(())]
        );
    }

    #[tokio::test]
    async fn never_splits_inside_a_component() {
        let sink = FakeSink::new(&[1]);
        let results = apply_isolating_failures(&spans(&[3, 2]), |r| sink.apply(r)).await;
        // The poisoned component's three actions stay one unit: it is retried
        // whole and fails whole; its sibling still lands.
        assert_eq!(sink.calls(), vec![0..5, 0..3, 3..5]);
        assert_eq!(summarize(results), vec![poisoned(1), Ok(())]);
    }

    #[tokio::test]
    async fn isolates_poisoned_components_in_both_halves() {
        let sink = FakeSink::new(&[0, 3]);
        let results = apply_isolating_failures(&spans(&[1, 1, 1, 1]), |r| sink.apply(r)).await;
        assert_eq!(sink.calls(), vec![0..4, 0..2, 0..1, 1..2, 2..4, 2..3, 3..4]);
        assert_eq!(
            summarize(results),
            vec![poisoned(0), Ok(()), Ok(()), poisoned(3)]
        );
    }

    #[tokio::test]
    async fn every_component_failing_costs_at_most_two_n_minus_one_calls() {
        let sink = FakeSink::new(&[0, 1, 2, 3]);
        let results = apply_isolating_failures(&spans(&[1, 1, 1, 1]), |r| sink.apply(r)).await;
        assert_eq!(sink.calls().len(), 7);
        assert_eq!(
            summarize(results),
            vec![poisoned(0), poisoned(1), poisoned(2), poisoned(3)]
        );
    }

    #[tokio::test]
    async fn single_component_batch_fails_without_retry() {
        let sink = FakeSink::new(&[1]);
        let results = apply_isolating_failures(&spans(&[2]), |r| sink.apply(r)).await;
        assert_eq!(sink.calls(), vec![0..2]);
        assert_eq!(summarize(results), vec![poisoned(1)]);
    }

    #[tokio::test]
    async fn cancellation_and_deadline_are_not_retried() {
        for make_error in [
            utils::error::Error::cancelled as fn() -> utils::error::Error,
            utils::error::Error::deadline_exceeded,
        ] {
            let calls = Mutex::new(Vec::new());
            let results = apply_isolating_failures(&spans(&[1, 2, 1]), |r| {
                calls.lock().unwrap().push(r);
                let err = make_error();
                async move { Err::<(), _>(err) }
            })
            .await;
            assert_eq!(*calls.lock().unwrap(), vec![0..4]);
            assert_eq!(results.len(), 3);
            let probe = make_error();
            for result in &results {
                let err = result.as_ref().unwrap_err();
                assert_eq!(err.is_cancelled(), probe.is_cancelled());
                assert_eq!(err.is_deadline_exceeded(), probe.is_deadline_exceeded());
            }
        }
    }
}
