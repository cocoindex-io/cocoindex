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
    hash::{Hash, Hasher},
};

pub struct ChildTargetDef<Prof: EngineProfile> {
    pub handler: Prof::TargetHdl,
}

#[async_trait]
pub trait TargetActionSink<Prof: EngineProfile>: Send + Sync + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to apply the action.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    async fn apply(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        host_ctx: Arc<Prof::HostCtx>,
        actions: Vec<Prof::TargetAction>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<Prof>>>>>;
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
        actions: Vec<Prof::TargetAction>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<Prof>>>>> {
        if actions.is_empty() {
            return Ok(None);
        }
        self.inner
            .batcher
            .run(TargetActionRunnerInput {
                host_runtime_ctx: host_runtime_ctx.clone(),
                host_ctx,
                actions,
            })
            .await
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
    actions: Vec<Prof::TargetAction>,
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

struct TargetActionRunner<Prof: EngineProfile> {
    sink: Arc<Prof::TargetActionSink>,
}

#[async_trait]
impl<Prof: EngineProfile> Runner for TargetActionRunner<Prof> {
    type Input = TargetActionRunnerInput<Prof>;
    type Output = Option<Vec<Option<ChildTargetDef<Prof>>>>;

    async fn run(
        &self,
        inputs: Vec<Self::Input>,
    ) -> Result<impl ExactSizeIterator<Item = Self::Output>> {
        let num_inputs = inputs.len();
        if num_inputs == 0 {
            return Ok(Vec::new().into_iter());
        }

        let mut groups =
            HashMap::<TargetActionRunnerContext<Prof>, Vec<(usize, Vec<Prof::TargetAction>)>>::new(
            );
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

        let mut outputs: Vec<Option<Vec<Option<ChildTargetDef<Prof>>>>> =
            std::iter::repeat_with(|| None).take(num_inputs).collect();
        for (context, inputs) in groups {
            let mut actions = Vec::new();
            let mut action_counts = Vec::with_capacity(inputs.len());
            let mut input_indexes = Vec::with_capacity(inputs.len());

            // Each input is one component's reconciled actions; the sink wants
            // one flat action list per compatible host context.
            for (input_idx, mut input_actions) in inputs {
                input_indexes.push(input_idx);
                action_counts.push(input_actions.len());
                actions.append(&mut input_actions);
            }

            let actions_len = actions.len();
            let Some(handlers) = self
                .sink
                .apply(&context.host_runtime_ctx, context.host_ctx, actions)
                .await?
            else {
                continue;
            };
            if handlers.len() != actions_len {
                client_bail!(
                    "expect child providers returned by Sink to be the same length as the actions ({}), got {}",
                    actions_len,
                    handlers.len(),
                );
            }
            let mut handlers = handlers.into_iter();
            for (input_idx, count) in std::iter::zip(input_indexes, action_counts) {
                outputs[input_idx] = Some(handlers.by_ref().take(count).collect());
            }
        }

        Ok(outputs.into_iter())
    }
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
