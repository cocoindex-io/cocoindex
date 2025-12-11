use crate::engine::runtime::get_runtime;
use crate::prelude::*;

use crate::engine::context::{AppContext, ComponentProcessorContext};
use crate::engine::effect::EffectProvider;
use crate::engine::effect_exec::commit_effects;
use crate::engine::profile::EngineProfile;
use crate::state::effect_path::EffectPath;
use crate::state::state_path::StatePath;
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
    app_ctx: Arc<AppContext<Prof>>,
    state_path: StatePath,
    processor: Prof::ComponentProc,
    providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
    // For check existence / dedup
    //   live_sub_components: HashMap<StatePath, std::rc::Weak<ComponentInner<Prof>>>,
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

pub(crate) struct ComponentBgChildReadiness {
    state: Mutex<ComponentBgChildReadinessState>,
    pub readiness: tokio::sync::SetOnce<SharedResult<()>>,
}

struct ComponentBgChildReadinessChildGuard {
    readiness: Arc<ComponentBgChildReadiness>,
    resolved: bool,
}

impl Drop for ComponentBgChildReadinessChildGuard {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let mut state = self.readiness.state.lock().unwrap();
        state.remaining_count -= 1;
        state.maybe_set_readiness(
            Err(SharedError::new(anyhow::anyhow!(
                "Child component build cancelled"
            ))),
            &self.readiness.readiness,
        );
    }
}

impl ComponentBgChildReadinessChildGuard {
    fn resolve(mut self, result: Result<(), SharedError>) {
        {
            let mut state = self.readiness.state.lock().unwrap();
            state.remaining_count -= 1;
            state.maybe_set_readiness(result, &self.readiness.readiness);
        }
        self.resolved = true;
    }
}

impl Default for ComponentBgChildReadiness {
    fn default() -> Self {
        Self {
            state: Mutex::new(ComponentBgChildReadinessState {
                remaining_count: 0,
                is_readiness_set: false,
                build_done: false,
            }),
            readiness: tokio::sync::SetOnce::new(),
        }
    }
}

impl ComponentBgChildReadiness {
    fn add_child(self: Arc<Self>) -> ComponentBgChildReadinessChildGuard {
        self.state.lock().unwrap().remaining_count += 1;
        ComponentBgChildReadinessChildGuard {
            readiness: self,
            resolved: false,
        }
    }

    fn set_build_done(&self) {
        let mut state = self.state.lock().unwrap();
        state.build_done = true;
        state.maybe_set_readiness(Ok(()), &self.readiness);
    }
}

pub struct ComponentMountRunHandle<Prof: EngineProfile> {
    join_handle: tokio::task::JoinHandle<Result<Result<Prof::ComponentProcRet, Prof::Error>>>,
}

impl<Prof: EngineProfile> ComponentMountRunHandle<Prof> {
    pub async fn result(self) -> Result<Result<Prof::ComponentProcRet, Prof::Error>> {
        self.join_handle.await?
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

impl<Prof: EngineProfile> Component<Prof> {
    pub(crate) fn new(
        app_ctx: Arc<AppContext<Prof>>,
        state_path: StatePath,
        processor: Prof::ComponentProc,
        providers: rpds::HashTrieMapSync<EffectPath, EffectProvider<Prof>>,
    ) -> Self {
        Self {
            inner: Arc::new(ComponentInner {
                app_ctx,
                state_path,
                processor,
                providers,
                build_semaphore: tokio::sync::Semaphore::const_new(1),
            }),
        }
    }

    pub fn from_parent_context(
        state_path: StatePath,
        parent_ctx: &ComponentProcessorContext<Prof>,
        processor: Prof::ComponentProc,
    ) -> Self {
        let effect = parent_ctx.effect().lock().unwrap();
        Self::new(
            parent_ctx.app_ctx().clone(),
            state_path,
            processor,
            effect.provider_registry.providers.clone(),
        )
    }

    pub fn app_ctx(&self) -> &Arc<AppContext<Prof>> {
        &self.inner.app_ctx
    }

    pub fn state_path(&self) -> &StatePath {
        &self.inner.state_path
    }

    pub fn run(
        self,
        parent_context: Option<ComponentProcessorContext<Prof>>,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        let join_handle = get_runtime().spawn(async move { self.build_once(parent_context).await });
        Ok(ComponentMountRunHandle { join_handle })
    }

    pub fn run_in_background(
        self,
        parent_context: Option<ComponentProcessorContext<Prof>>,
    ) -> Result<ComponentMountHandle> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.

        let child_readiness_guard = parent_context
            .as_ref()
            .map(|c| c.components_readiness().clone().add_child());
        let join_handle = get_runtime().spawn(async move {
            let result = self.build_once(parent_context).await;
            let shared_result = match result {
                Ok(_) => Ok(()),
                Err(err) => Err(SharedError::new(err)),
            };
            child_readiness_guard.map(|guard| guard.resolve(shared_result.clone()));
            shared_result
        });
        Ok(ComponentMountHandle { join_handle })
    }

    async fn build_once(
        &self,
        parent_context: Option<ComponentProcessorContext<Prof>>,
    ) -> Result<Result<Prof::ComponentProcRet, Prof::Error>> {
        let processor_context = ComponentProcessorContext::new(
            self.inner.app_ctx.clone(),
            self.inner.state_path.clone(),
            self.inner.providers.clone(),
            parent_context,
        );

        // Acquire the semaphore to ensure `process()` and `commit_effects()` cannot happen in parallel.
        let ret = {
            let _permit = self.inner.build_semaphore.acquire().await?;

            let ret = self.inner.processor.process(&processor_context)?.await;
            let Ok(ret) = ret else {
                return Ok(ret);
            };
            commit_effects(&processor_context).await?;
            ret
        };

        // Wait until children components ready.
        let components_readiness = processor_context.components_readiness();
        components_readiness.set_build_done();
        components_readiness
            .readiness
            .wait()
            .await
            .anyhow_result()?;

        Ok(Ok(ret))
    }
}
