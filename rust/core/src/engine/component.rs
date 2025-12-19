use crate::engine::runtime::get_runtime;
use crate::prelude::*;

use crate::engine::context::{AppContext, ComponentProcessingMode, ComponentProcessorContext};
use crate::engine::effect::{EffectProvider, EffectProviderRegistry};
use crate::engine::execution::{cleanup_tombstone, submit};
use crate::engine::profile::EngineProfile;
use crate::state::effect_path::EffectPath;
use crate::state::stable_path::StablePath;
use crate::state::stable_path_set::StablePathSet;
use cocoindex_utils::error::{SharedError, SharedResult, SharedResultExt, SharedResultExtRef};

pub trait ComponentProcessor<Prof: EngineProfile>: Send + Sync + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to build the component.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    fn process(
        &self,
        context: &ComponentProcessorContext<Prof>,
    ) -> Result<impl Future<Output = Result<Prof::ComponentProcRet, Prof::Error>> + Send + 'static>;
}

struct ComponentInner<Prof: EngineProfile> {
    app_ctx: AppContext<Prof>,
    stable_path: StablePath,
    // For check existence / dedup
    //   live_sub_components: HashMap<StablePath, std::rc::Weak<ComponentInner<Prof>>>,
    /// Semaphore to ensure `process()` and `commit_effects()` calls cannot happen in parallel.
    build_semaphore: tokio::sync::Semaphore,
}

#[derive(Clone)]
pub struct Component<Prof: EngineProfile> {
    inner: Arc<ComponentInner<Prof>>,
}

struct ComponentBgChildReadinessState {
    remaining_count: usize,
    build_done: bool,
    is_readiness_set: bool,
}

impl ComponentBgChildReadinessState {
    fn maybe_set_readiness(
        &mut self,
        result: Result<(), SharedError>,
        readiness: &tokio::sync::SetOnce<SharedResult<()>>,
    ) {
        if self.is_readiness_set {
            return;
        }
        if result.is_err() || self.remaining_count == 0 && self.build_done {
            self.is_readiness_set = true;
            readiness.set(result).expect("readiness set more than once");
        }
    }
}

struct ComponentBgChildReadinessInner {
    state: Mutex<ComponentBgChildReadinessState>,
    readiness: tokio::sync::SetOnce<SharedResult<()>>,
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
            Err(SharedError::new(anyhow::anyhow!(
                "Child component build cancelled"
            ))),
            self.readiness.readiness(),
        );
    }
}

impl ComponentBgChildReadinessChildGuard {
    fn resolve(mut self, result: Result<(), SharedError>) {
        {
            let mut state = self.readiness.state().lock().unwrap();
            state.remaining_count -= 1;
            state.maybe_set_readiness(result, self.readiness.readiness());
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

    pub fn readiness(&self) -> &tokio::sync::SetOnce<SharedResult<()>> {
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
        state.maybe_set_readiness(Ok(()), self.readiness());
    }
}

pub struct ComponentMountRunHandle<Prof: EngineProfile> {
    join_handle: tokio::task::JoinHandle<Result<Result<ComponentBuildOutput<Prof>, Prof::Error>>>,
}

impl<Prof: EngineProfile> ComponentMountRunHandle<Prof> {
    pub async fn result(
        self,
        parent_context: Option<&ComponentProcessorContext<Prof>>,
    ) -> Result<Result<Prof::ComponentProcRet, Prof::Error>> {
        let result = self.join_handle.await??;
        let ret = match result {
            Ok(output) => {
                if let Some(parent_context) = parent_context {
                    parent_context.update_building_state(|building_state| {
                        for effect_path in output.provider_registry.curr_effect_paths {
                            let Some(provider) =
                                output.provider_registry.providers.get(&effect_path)
                            else {
                                error!("effect provider not found for path {}", effect_path);
                                continue;
                            };
                            if !provider.is_orphaned() {
                                building_state
                                    .effect
                                    .provider_registry
                                    .add(effect_path, provider.clone())?;
                            }
                        }
                        Ok(())
                    })?;
                }
                Ok(output.ret)
            }
            Err(err) => Err(err),
        };
        Ok(ret)
    }
}
pub struct ComponentMountHandle {
    join_handle: tokio::task::JoinHandle<SharedResult<()>>,
}

impl ComponentMountHandle {
    pub async fn ready(self) -> Result<()> {
        self.join_handle.await?.anyhow_result()
    }
}

struct ComponentBuildOutput<Prof: EngineProfile> {
    ret: Prof::ComponentProcRet,
    provider_registry: EffectProviderRegistry<Prof>,
}

impl<Prof: EngineProfile> Component<Prof> {
    pub(crate) fn new(app_ctx: AppContext<Prof>, stable_path: StablePath) -> Self {
        Self {
            inner: Arc::new(ComponentInner {
                app_ctx,
                stable_path,
                build_semaphore: tokio::sync::Semaphore::const_new(1),
            }),
        }
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

    pub fn run(
        self,
        processor: Prof::ComponentProc,
        parent_context: Option<ComponentProcessorContext<Prof>>,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        let processor_context = self.new_processor_context_for_build(parent_context)?;
        let join_handle = get_runtime().spawn(async move {
            let output = self
                .execute_once(&processor_context, Some(processor))
                .await?;
            let ret = match output {
                Ok(Some(output)) => Ok(output),
                Ok(None) => {
                    bail!("component deletion can only run in background");
                }
                Err(err) => Err(err),
            };
            Ok(ret)
        });
        Ok(ComponentMountRunHandle { join_handle })
    }

    pub fn run_in_background(
        self,
        processor: Prof::ComponentProc,
        parent_context: Option<ComponentProcessorContext<Prof>>,
    ) -> Result<ComponentMountHandle> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.

        let child_readiness_guard = parent_context
            .as_ref()
            .map(|c| c.components_readiness().clone().add_child());
        let processor_context = self.new_processor_context_for_build(parent_context)?;
        let join_handle = get_runtime().spawn(async move {
            let result = self.execute_once(&processor_context, Some(processor)).await;
            let shared_result = match result {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(err)) => {
                    error!("component build failed:\n{err}");
                    Ok(())
                }
                Err(err) => Err(SharedError::new(err)),
            };
            child_readiness_guard.map(|guard| guard.resolve(shared_result.clone()));
            shared_result
        });
        Ok(ComponentMountHandle { join_handle })
    }

    pub fn delete(
        self,
        parent_context: Option<ComponentProcessorContext<Prof>>,
        providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
    ) -> Result<()> {
        let child_readiness_guard = parent_context
            .as_ref()
            .map(|c| c.components_readiness().clone().add_child());
        let processor_context = ComponentProcessorContext::new(
            self.clone(),
            providers,
            parent_context,
            ComponentProcessingMode::Delete,
        );
        get_runtime().spawn(async move {
            trace!("deleting component at {}", self.stable_path());
            let result = self
                .execute_once(&processor_context, None)
                .await
                .and_then(|ret| {
                    cleanup_tombstone(&processor_context)?;
                    Ok(ret)
                });
            let shared_result = match result {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(err)) => Err(SharedError::new(err.into())),
                Err(err) => Err(SharedError::new(err)),
            };
            child_readiness_guard.map(|guard| guard.resolve(shared_result.clone()));
            shared_result
        });
        Ok(())
    }

    async fn execute_once(
        &self,
        processor_context: &ComponentProcessorContext<Prof>,
        processor: Option<Prof::ComponentProc>,
    ) -> Result<Result<Option<ComponentBuildOutput<Prof>>, Prof::Error>> {
        // Acquire the semaphore to ensure `process()` and `commit_effects()` cannot happen in parallel.
        let output = {
            let _permit = self.inner.build_semaphore.acquire().await?;

            let ret = match processor {
                Some(processor) => processor.process(&processor_context)?.await.map(Some),
                None => Ok(None),
            };
            match ret {
                Ok(ret) => {
                    let provider_registry = submit(&processor_context).await?;
                    if let Some(ret) = ret {
                        Ok(Some(ComponentBuildOutput {
                            ret,
                            provider_registry: provider_registry.ok_or_else(|| {
                                anyhow::anyhow!("expect a provider registry for component build")
                            })?,
                        }))
                    } else {
                        Ok(None)
                    }
                }
                Err(err) => Err(err),
            }
        };

        // Wait until children components ready.
        let components_readiness = processor_context.components_readiness();
        components_readiness.set_build_done();
        components_readiness
            .readiness()
            .wait()
            .await
            .anyhow_result()?;

        Ok(output)
    }

    fn new_processor_context_for_build(
        &self,
        parent_ctx: Option<ComponentProcessorContext<Prof>>,
    ) -> Result<ComponentProcessorContext<Prof>> {
        let providers = if let Some(parent_ctx) = &parent_ctx {
            let sub_path = self
                .stable_path()
                .as_ref()
                .strip_parent(parent_ctx.stable_path().as_ref())?;
            parent_ctx.update_building_state(|building_state| {
                building_state
                    .child_path_set
                    .add_child(sub_path, StablePathSet::Component)?;
                Ok(building_state.effect.provider_registry.providers.clone())
            })?
        } else {
            self.app_ctx()
                .env()
                .effect_providers()
                .lock()
                .unwrap()
                .providers
                .clone()
        };
        Ok(ComponentProcessorContext::new(
            self.clone(),
            providers,
            parent_ctx,
            ComponentProcessingMode::Build,
        ))
    }
}
