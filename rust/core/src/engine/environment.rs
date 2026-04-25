use crate::{
    engine::profile::EngineProfile, engine::target_state::TargetStateProviderRegistry,
    engine::txn_batcher::TxnBatcher, prelude::*,
};

use cocoindex_utils::fingerprint::Fingerprint;
use cocoindex_utils::retryable;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Duration;

const DEFAULT_MAX_DBS: u32 = 1024;
const DEFAULT_LMDB_MAP_SIZE: usize = 0x1_0000_0000; // 4GiB

/// Phase 1: short timeout to handle transient concurrency.
static LMDB_READ_TXN_RETRY_PHASE1: retryable::RetryOptions = retryable::RetryOptions {
    retry_timeout: Some(Duration::from_secs(3)),
    initial_backoff: Duration::from_millis(10),
    max_backoff: Duration::from_secs(1),
};

/// Phase 2: after clearing stale readers, retry indefinitely.
static LMDB_READ_TXN_RETRY_PHASE2: retryable::RetryOptions = retryable::RetryOptions {
    retry_timeout: None,
    initial_backoff: Duration::from_millis(10),
    max_backoff: Duration::from_secs(1),
};

fn default_max_dbs() -> u32 {
    DEFAULT_MAX_DBS
}
fn default_lmdb_map_size() -> usize {
    DEFAULT_LMDB_MAP_SIZE
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EnvironmentSettings {
    pub db_path: PathBuf,
    #[serde(default = "default_max_dbs")]
    pub lmdb_max_dbs: u32,
    #[serde(default = "default_lmdb_map_size")]
    pub lmdb_map_size: usize,
}

struct EnvironmentInner<Prof: EngineProfile> {
    db_env: heed::Env<heed::WithoutTls>,
    txn_batcher: TxnBatcher,
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
        let db_path = settings.db_path.join("mdb");
        // Create the directory if not exists.
        std::fs::create_dir_all(&db_path)?;
        // Backward compatibility: migrate LMDB files from old layout into mdb/.
        Self::migrate_legacy_db_files(&settings.db_path, &db_path)?;
        if settings.lmdb_max_dbs < 1 {
            client_bail!("lmdb_max_dbs must be >= 1, got {}", settings.lmdb_max_dbs);
        }
        if settings.lmdb_map_size == 0 {
            client_bail!("lmdb_map_size must be > 0, got {}", settings.lmdb_map_size);
        }
        let db_env = unsafe {
            heed::EnvOpenOptions::new()
                .read_txn_without_tls()
                .max_dbs(settings.lmdb_max_dbs)
                .map_size(settings.lmdb_map_size)
                .open(db_path)
        }?;
        let cleared_count = db_env.clear_stale_readers()?;
        if cleared_count > 0 {
            info!("Cleared {cleared_count} stale readers");
        }

        let txn_batcher = TxnBatcher::new(db_env.clone());
        let state = Arc::new(EnvironmentInner {
            db_env,
            txn_batcher,
            app_names: Mutex::new(BTreeSet::new()),
            target_states_providers,
            host_runtime_ctx,
            logic_set: RwLock::new(HashSet::new()),
            context_initial_states: RwLock::new(HashMap::new()),
        });
        Ok(Self { inner: state })
    }

    /// Migrate legacy LMDB files from the old layout (directly in `base_path`)
    /// into the new `db_path` subdirectory.
    fn migrate_legacy_db_files(base_path: &PathBuf, db_path: &PathBuf) -> Result<()> {
        let legacy_files: Vec<PathBuf> = ["data.mdb", "lock.mdb"]
            .iter()
            .map(|name| base_path.join(name))
            .filter(|path| path.exists())
            .collect();
        if legacy_files.is_empty() {
            return Ok(());
        }
        info!(
            "Migrating legacy LMDB files from {} to {}",
            base_path.display(),
            db_path.display()
        );
        for src in legacy_files {
            let dst = db_path.join(src.file_name().unwrap());
            std::fs::rename(&src, &dst)?;
        }
        Ok(())
    }

    pub fn db_env(&self) -> &heed::Env<heed::WithoutTls> {
        &self.inner.db_env
    }

    /// Open an LMDB read transaction with automatic retry on `MDB_READERS_FULL`.
    ///
    /// Two-phase strategy:
    /// 1. Retry with a short timeout — handles transient reader slot contention.
    /// 2. If phase 1 times out, call `clear_stale_readers()` to reclaim slots
    ///    from dead processes, then retry indefinitely.
    pub async fn read_txn(&self) -> Result<heed::RoTxn<'_, heed::WithoutTls>> {
        let db_env = self.db_env();
        let try_read_txn = || async {
            match db_env.read_txn() {
                Ok(txn) => retryable::Ok(txn),
                Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                    warn!("LMDB readers full, retrying");
                    Err(retryable::Error::retryable(internal_error!(
                        "LMDB readers full"
                    )))
                }
                Err(e) => Err(retryable::Error::not_retryable(e)),
            }
        };

        // Phase 1: short timeout for transient concurrency.
        match retryable::run(&try_read_txn, &LMDB_READ_TXN_RETRY_PHASE1).await {
            Ok(txn) => return Ok(txn),
            Err(e) if !e.is_retryable => return Err(e.into()),
            Err(_) => {}
        }

        // Phase 2: clear stale readers, then retry indefinitely.
        let cleared = db_env.clear_stale_readers()?;
        if cleared > 0 {
            warn!("Cleared {cleared} stale LMDB readers");
        }
        retryable::run(&try_read_txn, &LMDB_READ_TXN_RETRY_PHASE2)
            .await
            .map_err(Into::into)
    }

    pub fn txn_batcher(&self) -> &TxnBatcher {
        &self.inner.txn_batcher
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
