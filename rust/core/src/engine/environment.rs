use crate::{
    engine::profile::EngineProfile,
    engine::target_state::TargetStateProviderRegistry,
    prelude::*,
    state_store::{AppStore, ReadTxn, Storage, StorageSettings, WriteTxn},
};

use cocoindex_utils::fingerprint::Fingerprint;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::RwLock;

/// User-facing environment settings. Currently just storage configuration;
/// re-exported as a type alias so existing call sites continue to work.
pub type EnvironmentSettings = StorageSettings;

struct EnvironmentInner<Prof: EngineProfile> {
    storage: Storage,
    app_names: Mutex<BTreeSet<String>>,
    target_states_providers: Arc<Mutex<TargetStateProviderRegistry<Prof>>>,
    host_runtime_ctx: Prof::HostRuntimeCtx,
    logic_set: RwLock<HashSet<Fingerprint>>,
    /// Eager initial memo states for tracked context values, keyed by the
    /// value's fingerprint. Populated at `provide()` time from Python, read
    /// on cache miss to populate a new memo entry's `context_memo_states`.
    /// See `specs/memo_validation/plan.md` → "Extension: state validation
    /// for tracked context values".
    context_initial_states: RwLock<HashMap<Fingerprint, Vec<Prof::FunctionData>>>,
}

#[derive(Clone)]
pub struct Environment<Prof: EngineProfile> {
    inner: Arc<EnvironmentInner<Prof>>,
}

impl<Prof: EngineProfile> Environment<Prof> {
    pub fn new(
        settings: EnvironmentSettings,
        target_states_providers: Arc<Mutex<TargetStateProviderRegistry<Prof>>>,
        host_runtime_ctx: Prof::HostRuntimeCtx,
    ) -> Result<Self> {
        let storage = Storage::new(&settings)?;
        let state = Arc::new(EnvironmentInner {
            storage,
            app_names: Mutex::new(BTreeSet::new()),
            target_states_providers,
            host_runtime_ctx,
            logic_set: RwLock::new(HashSet::new()),
            context_initial_states: RwLock::new(HashMap::new()),
        });
        Ok(Self { inner: state })
    }

    pub fn storage(&self) -> &Storage {
        &self.inner.storage
    }

    /// Open a read transaction. Delegates to [`Storage::read_txn`] which
    /// handles transient and stale-reader retry.
    pub async fn read_txn(&self) -> Result<ReadTxn<'_>> {
        self.inner.storage.read_txn().await
    }

    /// Run a batched write transaction. Delegates to [`Storage::run_txn`].
    pub async fn run_txn<T, F>(&self, body: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'txn> FnOnce(&mut WriteTxn<'txn>) -> Result<T> + Send + 'static,
    {
        self.inner.storage.run_txn(body).await
    }

    /// Create the per-app sub-store. Delegates to [`Storage::create_app_store`].
    pub fn create_app_store(&self, app_name: &str) -> Result<AppStore> {
        self.inner.storage.create_app_store(app_name)
    }

    pub fn target_states_providers(&self) -> &Arc<Mutex<TargetStateProviderRegistry<Prof>>> {
        &self.inner.target_states_providers
    }

    pub fn host_runtime_ctx(&self) -> &Prof::HostRuntimeCtx {
        &self.inner.host_runtime_ctx
    }

    pub fn register_logic(&self, fp: Fingerprint) {
        self.inner.logic_set.write().unwrap().insert(fp);
    }

    pub fn unregister_logic(&self, fp: &Fingerprint) {
        self.inner.logic_set.write().unwrap().remove(fp);
    }

    pub fn logic_set_contains(&self, fp: &Fingerprint) -> bool {
        self.inner.logic_set.read().unwrap().contains(fp)
    }

    /// Register the eager initial memo states for a tracked context value.
    /// Called at `provide()` time (from the Python context provider) after
    /// the value's canonicalization and state-function collection.
    pub fn register_context_initial_states(
        &self,
        fp: Fingerprint,
        states: Vec<Prof::FunctionData>,
    ) {
        self.inner
            .context_initial_states
            .write()
            .unwrap()
            .insert(fp, states);
    }

    /// Remove the initial states for a tracked context fingerprint.
    /// Called on re-provide (when a context key is provided with a new value
    /// whose fingerprint differs).
    pub fn unregister_context_initial_states(&self, fp: &Fingerprint) {
        self.inner
            .context_initial_states
            .write()
            .unwrap()
            .remove(fp);
    }

    /// Collect initial memo states for the given tracked context fingerprints.
    ///
    /// Fingerprints with no entry in the registry (i.e. the tracked value
    /// had no `__coco_memo_state__`) are silently skipped. Returns the list
    /// of `(fp, states)` pairs for fps that were found.
    pub fn collect_context_initial_states<'a, I>(
        &self,
        fps: I,
    ) -> Vec<(Fingerprint, Vec<Prof::FunctionData>)>
    where
        I: IntoIterator<Item = &'a Fingerprint>,
    {
        let map = self.inner.context_initial_states.read().unwrap();
        fps.into_iter()
            .filter_map(|fp| map.get(fp).map(|v| (*fp, v.clone())))
            .collect()
    }
}

pub struct AppRegistration<Prof: EngineProfile> {
    name: String,
    env: Environment<Prof>,
}

impl<Prof: EngineProfile> AppRegistration<Prof> {
    pub fn new(name: &str, env: &Environment<Prof>) -> Result<Self> {
        let mut app_names = env.inner.app_names.lock().unwrap();
        if !app_names.insert(name.to_string()) {
            client_bail!("App name already registered: {}", name);
        }
        Ok(Self {
            name: name.to_string(),
            env: env.clone(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<Prof: EngineProfile> Drop for AppRegistration<Prof> {
    fn drop(&mut self) {
        let mut app_names = self.env.inner.app_names.lock().unwrap();
        app_names.remove(&self.name);
    }
}
