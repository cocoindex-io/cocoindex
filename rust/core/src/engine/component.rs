use crate::engine::runtime::get_runtime;
use crate::prelude::*;

use crate::engine::context::FnCallContext;
use crate::engine::context::{AppContext, ComponentProcessingMode, ComponentProcessorContext};
use crate::engine::execution::{
    cleanup_tombstone, post_submit_for_build, submit, use_or_invalidate_component_memoization,
};
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::{TargetStateProvider, TargetStateProviderRegistry};
use crate::state::stable_path::{StablePath, StablePathRef};
use crate::state::stable_path_set::StablePathSet;
use crate::state::target_state_path::TargetStatePath;
use cocoindex_utils::error::{SharedError, SharedResult, SharedResultExt};
use cocoindex_utils::fingerprint::Fingerprint;

#[derive(Debug, Clone)]
pub struct ComponentProcessorInfo {
    pub name: String,
}

impl ComponentProcessorInfo {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

pub trait ComponentProcessor<Prof: EngineProfile>: Send + Sync + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to build the component.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    fn process(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        comp_ctx: &ComponentProcessorContext<Prof>,
    ) -> Result<impl Future<Output = Result<Prof::FunctionData>> + Send + 'static>;

    /// Fingerprint of the memoization key. When matching, re-processing can be skipped.
    /// When None, memoization is not enabled for the component.
    fn memo_key_fingerprint(&self) -> Option<Fingerprint>;

    fn processor_info(&self) -> &ComponentProcessorInfo;
}

struct ComponentInner<Prof: EngineProfile> {
    app_ctx: AppContext<Prof>,
    stable_path: StablePath,

    // For check existence / dedup
    //   live_sub_components: HashMap<StablePath, std::rc::Weak<ComponentInner<Prof>>>,
    /// Semaphore to ensure `process()` and `commit_effects()` calls cannot happen in parallel.
    build_semaphore: tokio::sync::Semaphore,
    last_memo_fp: Mutex<Option<Fingerprint>>,
}

#[derive(Clone)]
pub struct Component<Prof: EngineProfile> {
    inner: Arc<ComponentInner<Prof>>,
}

struct ComponentBgChildReadinessState {
    remaining_count: usize,
    build_done: bool,
    is_readiness_set: bool,
    outcome: ComponentRunOutcome,
}

impl ComponentBgChildReadinessState {
    fn maybe_set_readiness(
        &mut self,
        result: Option<Result<ComponentRunOutcome, SharedError>>,
        readiness: &tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>>,
    ) {
        if self.is_readiness_set {
            return;
        }
        if let Some(result) = result {
            if let Ok(outcome) = result {
                self.outcome.merge(outcome);
            } else {
                self.is_readiness_set = true;
                readiness.set(result).expect("readiness set more than once");
                return;
            }
        }
        if self.remaining_count == 0 && self.build_done {
            self.is_readiness_set = true;
            readiness
                .set(Ok(std::mem::take(&mut self.outcome)))
                .expect("readiness set more than once");
        }
    }
}

#[derive(Debug, Default, Clone)]
struct ComponentRunOutcome {
    has_exception: bool,
}

impl ComponentRunOutcome {
    fn merge(&mut self, other: Self) {
        self.has_exception |= other.has_exception;
    }
}

struct ComponentBgChildReadinessInner {
    state: Mutex<ComponentBgChildReadinessState>,
    readiness: tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>>,
}

#[derive(Clone)]
pub(crate) struct ComponentBgChildReadiness {
    inner: Arc<ComponentBgChildReadinessInner>,
}

struct ComponentBgChildReadinessChildGuard {
    readiness: ComponentBgChildReadiness,
    resolved: bool,
}

impl Drop for ComponentBgChildReadinessChildGuard {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let mut state = self.readiness.state().lock().unwrap();
        state.remaining_count -= 1;
        state.maybe_set_readiness(
            Some(Err(SharedError::new(internal_error!(
                "Child component build cancelled"
            )))),
            self.readiness.readiness(),
        );
    }
}

impl ComponentBgChildReadinessChildGuard {
    fn resolve(mut self, outcome: ComponentRunOutcome) {
        {
            let mut state = self.readiness.state().lock().unwrap();
            state.remaining_count -= 1;
            state.maybe_set_readiness(Some(Ok(outcome)), self.readiness.readiness());
        }
        self.resolved = true;
    }
}

impl Default for ComponentBgChildReadiness {
    fn default() -> Self {
        Self {
            inner: Arc::new(ComponentBgChildReadinessInner {
                state: Mutex::new(ComponentBgChildReadinessState {
                    remaining_count: 0,
                    is_readiness_set: false,
                    build_done: false,
                    outcome: Default::default(),
                }),
                readiness: tokio::sync::SetOnce::new(),
            }),
        }
    }
}

impl ComponentBgChildReadiness {
    fn state(&self) -> &Mutex<ComponentBgChildReadinessState> {
        &self.inner.state
    }

    fn readiness(&self) -> &tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>> {
        &self.inner.readiness
    }

    fn add_child(self) -> ComponentBgChildReadinessChildGuard {
        self.state().lock().unwrap().remaining_count += 1;
        ComponentBgChildReadinessChildGuard {
            readiness: self,
            resolved: false,
        }
    }

    fn set_build_done(&self) {
        let mut state = self.state().lock().unwrap();
        state.build_done = true;
        state.maybe_set_readiness(None, self.readiness());
    }
}

pub struct ComponentMountRunHandle<Prof: EngineProfile> {
    join_handle: tokio::task::JoinHandle<Result<ComponentBuildOutput<Prof>>>,
}

impl<Prof: EngineProfile> ComponentMountRunHandle<Prof> {
    pub async fn result(
        self,
        parent_context: Option<&ComponentProcessorContext<Prof>>,
    ) -> Result<Prof::FunctionData> {
        let output = self.join_handle.await??;
        if let Some(parent_context) = parent_context {
            parent_context.update_building_state(|building_state| {
                for target_state_path in
                    output.built_target_states_providers.curr_target_state_paths
                {
                    let Some(provider) = output
                        .built_target_states_providers
                        .providers
                        .get(&target_state_path)
                    else {
                        error!(
                            "target states provider not found for path {}",
                            target_state_path
                        );
                        continue;
                    };
                    if !provider.is_orphaned() {
                        building_state
                            .target_states
                            .provider_registry
                            .add(target_state_path, provider.clone())?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(output.ret)
    }
}

pub struct ComponentExecutionHandle {
    join_handle: tokio::task::JoinHandle<SharedResult<()>>,
}

impl ComponentExecutionHandle {
    pub async fn ready(self) -> Result<()> {
        self.join_handle.await?.into_result()
    }
}

struct ComponentBuildOutput<Prof: EngineProfile> {
    ret: Prof::FunctionData,
    built_target_states_providers: TargetStateProviderRegistry<Prof>,
}

impl<Prof: EngineProfile> Component<Prof> {
    pub(crate) fn new(app_ctx: AppContext<Prof>, stable_path: StablePath) -> Self {
        Self {
            inner: Arc::new(ComponentInner {
                app_ctx,
                stable_path,
                build_semaphore: tokio::sync::Semaphore::const_new(1),
                last_memo_fp: Mutex::new(None),
            }),
        }
    }

    pub fn mount_child(&self, fn_ctx: &FnCallContext, stable_path: StablePath) -> Result<Self> {
        let relative_path: StablePath = stable_path
            .as_ref()
            .strip_parent(self.stable_path().as_ref())?
            .into();
        fn_ctx.update(|inner| inner.child_components.push(relative_path));
        Ok(self.get_child(stable_path))
    }

    pub fn get_child(&self, stable_path: StablePath) -> Self {
        // TODO: Get the child component directly if it already exists.
        Self::new(self.app_ctx().clone(), stable_path)
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        &self.inner.app_ctx
    }

    pub fn stable_path(&self) -> &StablePath {
        &self.inner.stable_path
    }

    pub(crate) fn relative_path(
        &self,
        context: &ComponentProcessorContext<Prof>,
    ) -> Result<StablePathRef<'_>> {
        if let Some(parent_ctx) = context.parent_context() {
            self.stable_path()
                .as_ref()
                .strip_parent(parent_ctx.stable_path().as_ref())
        } else {
            Ok(self.stable_path().as_ref())
        }
    }

    pub fn run(
        self,
        processor: Prof::ComponentProc,
        context: ComponentProcessorContext<Prof>,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        let relative_path = self.relative_path(&context)?;
        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_child());
        let span = info_span!("component.run", component_path = %relative_path);
        let join_handle = get_runtime().spawn(
            async move {
                let result = self.execute_once(&context, Some(&processor)).await;
                let (outcome, output) = match result {
                    Ok((outcome, output)) => (outcome, Ok(output)),
                    Err(err) => (
                        ComponentRunOutcome {
                            has_exception: true,
                        },
                        Err(err),
                    ),
                };
                child_readiness_guard.map(|guard| guard.resolve(outcome));
                output?
                    .ok_or_else(|| internal_error!("component deletion can only run in background"))
            }
            .instrument(span),
        );
        Ok(ComponentMountRunHandle { join_handle })
    }

    pub fn run_in_background(
        self,
        processor: Prof::ComponentProc,
        context: ComponentProcessorContext<Prof>,
    ) -> Result<ComponentExecutionHandle> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.

        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_child());
        let join_handle = get_runtime().spawn(async move {
            let result = self.execute_once(&context, Some(&processor)).await;
            // For background child component, only log the error. Never propagate the error back to the parent.
            if let Err(err) = &result {
                error!("component build failed:\n{err:?}");
            }
            child_readiness_guard.map(|guard| {
                guard.resolve(result.map(|(outcome, _)| outcome).unwrap_or_else(|_| {
                    ComponentRunOutcome {
                        has_exception: true,
                    }
                }))
            });
            Ok(())
        });
        Ok(ComponentExecutionHandle { join_handle })
    }

    pub fn delete(
        self,
        context: ComponentProcessorContext<Prof>,
    ) -> Result<ComponentExecutionHandle> {
        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_child());
        let join_handle = get_runtime().spawn(async move {
            trace!("deleting component at {}", self.stable_path());
            let result = self.execute_once(&context, None).await;
            let outcome = match &result {
                Ok((outcome, _)) => outcome.clone(),
                Err(err) => {
                    error!("component delete failed:\n{err}");
                    ComponentRunOutcome {
                        has_exception: true,
                    }
                }
            };
            if let Some(guard) = child_readiness_guard {
                guard.resolve(outcome);
            }
            result.map(|_| ()).map_err(Into::into)
        });
        Ok(ComponentExecutionHandle { join_handle })
    }

    async fn execute_once(
        &self,
        processor_context: &ComponentProcessorContext<Prof>,
        processor: Option<&Prof::ComponentProc>,
    ) -> Result<(ComponentRunOutcome, Option<ComponentBuildOutput<Prof>>)> {
        let mut reported_processor_name: Option<Cow<'_, str>> = None;
        let mut memo_fp_to_store: Option<Fingerprint> = None;
        let processing_stats = processor_context.processing_stats();

        if let Some(processor) = processor {
            let processor_name = processor.processor_info().name.as_str();
            memo_fp_to_store = processor.memo_key_fingerprint();

            // Fast-path: component memoization check does not require acquiring the build permit.
            // If it hits, we can immediately return without processing/submitting/waiting.

            match use_or_invalidate_component_memoization(processor_context, memo_fp_to_store) {
                Ok(Some(ret)) => {
                    processing_stats.update(processor_name.as_ref(), |stats| {
                        stats.num_execution_starts += 1;
                        stats.num_unchanged += 1;
                    });
                    return Ok((
                        ComponentRunOutcome::default(),
                        Some(ComponentBuildOutput {
                            ret,
                            built_target_states_providers: Default::default(),
                        }),
                    ));
                }
                Err(err) => {
                    error!("component memoization restore failed: {err:?}");
                }
                Ok(None) => {}
            }

            processor_context
                .processing_stats()
                .update(processor_name.as_ref(), |stats| {
                    stats.num_execution_starts += 1;
                });
            reported_processor_name = Some(Cow::Borrowed(processor.processor_info().name.as_str()));
        }

        let result = {
            let reported_processor_name = &mut reported_processor_name;
            async move {
                // Acquire the semaphore to ensure `process()` and `commit_effects()` cannot happen in parallel.
                let ret_n_submit_output = {
                    let _permit = self.inner.build_semaphore.acquire().await?;

                    if memo_fp_to_store.is_some() {
                        *self.inner.last_memo_fp.lock().unwrap() = memo_fp_to_store;
                        // TODO: when matching, it means there're ongoing processing for the same memoization key pending on children.
                        // We can piggyback on the same processing to avoid duplicating the work.
                    }

                    let ret: Result<Option<Prof::FunctionData>> = match &processor {
                        Some(processor) => processor
                            .process(
                                processor_context.app_ctx().env().host_runtime_ctx(),
                                &processor_context,
                            )?
                            .await
                            .map(Some),
                        None => Ok(None),
                    };
                    match ret {
                        Ok(ret) => {
                            let submit_output = submit(processor_context, processor, |name| {
                                if reported_processor_name.is_none() {
                                    processing_stats.update(&name, |stats| {
                                        stats.num_execution_starts += 1;
                                    });
                                    *reported_processor_name = Some(Cow::Owned(name.to_string()));
                                }
                            })
                            .await?;
                            Ok((ret, submit_output))
                        }
                        Err(err) => Err(err),
                    }
                };

                // Wait until children components ready.
                let components_readiness = processor_context.components_readiness();
                components_readiness.set_build_done();
                let children_outcome = components_readiness
                    .readiness()
                    .wait()
                    .await
                    .clone()
                    .into_result()?;

                let (ret, submit_output) = ret_n_submit_output?;
                let build_output = match ret {
                    Some(ret) => {
                        if !children_outcome.has_exception {
                            let comp_memo = if let Some(fp) = memo_fp_to_store
                                && let last_memo_fp = processor_context
                                    .component()
                                    .inner
                                    .last_memo_fp
                                    .lock()
                                    .unwrap()
                                && *last_memo_fp == memo_fp_to_store
                            {
                                Some((fp, &ret))
                            } else {
                                None
                            };
                            post_submit_for_build(
                                processor_context,
                                comp_memo,
                                submit_output.memos_with_mounts_to_store,
                            )
                            .await?;
                        }
                        Some(ComponentBuildOutput {
                            ret,
                            built_target_states_providers: submit_output
                                .built_target_states_providers
                                .ok_or_else(|| {
                                    internal_error!("expect built target states providers")
                                })?,
                        })
                    }
                    None => {
                        cleanup_tombstone(&processor_context)?;
                        None
                    }
                };
                Ok::<_, Error>((
                    children_outcome,
                    build_output,
                    submit_output.touched_previous_states,
                ))
            }
            .await
        };

        let final_processor_name = reported_processor_name
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or(db_schema::UNKNOWN_PROCESSOR_NAME);
        match result {
            Ok((children_outcome, build_output, touched_previous_states)) => {
                processing_stats.update(final_processor_name, |stats| {
                    if reported_processor_name.is_none() {
                        stats.num_execution_starts += 1;
                    }
                    match processor_context.mode() {
                        ComponentProcessingMode::Build => {
                            if touched_previous_states {
                                stats.num_reprocesses += 1;
                            } else {
                                stats.num_adds += 1;
                            }
                        }
                        ComponentProcessingMode::Delete => {
                            stats.num_deletes += 1;
                        }
                    }
                });
                Ok((children_outcome, build_output))
            }
            Err(err) => {
                processing_stats.update(final_processor_name, |stats| {
                    if reported_processor_name.is_none() {
                        stats.num_execution_starts += 1;
                    }
                    stats.num_errors += 1;
                });
                Err(err)
            }
        }
    }

    pub fn new_processor_context_for_build(
        &self,
        parent_ctx: Option<&ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
    ) -> Result<ComponentProcessorContext<Prof>> {
        let providers = if let Some(parent_ctx) = parent_ctx {
            let sub_path = self
                .stable_path()
                .as_ref()
                .strip_parent(parent_ctx.stable_path().as_ref())?;
            parent_ctx.update_building_state(|building_state| {
                building_state
                    .child_path_set
                    .add_child(sub_path, StablePathSet::Component)?;
                Ok(building_state
                    .target_states
                    .provider_registry
                    .providers
                    .clone())
            })?
        } else {
            self.app_ctx()
                .env()
                .target_states_providers()
                .lock()
                .unwrap()
                .providers
                .clone()
        };
        Ok(ComponentProcessorContext::new(
            self.clone(),
            providers,
            parent_ctx.cloned(),
            processing_stats,
            ComponentProcessingMode::Build,
        ))
    }

    pub fn new_processor_context_for_delete(
        &self,
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        parent_ctx: Option<&ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
    ) -> ComponentProcessorContext<Prof> {
        ComponentProcessorContext::new(
            self.clone(),
            providers,
            parent_ctx.cloned(),
            processing_stats,
            ComponentProcessingMode::Delete,
        )
    }
}
