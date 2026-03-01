use crate::{
    engine::profile::EngineProfile, engine::target_state::TargetStateProviderRegistry, prelude::*,
};

use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

const DEFAULT_MAX_DBS: u32 = 1024;
const DEFAULT_LMDB_MAP_SIZE: usize = 0x1_0000_0000; // 4GiB

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
    db_env: heed::Env,
    app_names: Mutex<BTreeSet<String>>,
    target_states_providers: Arc<Mutex<TargetStateProviderRegistry<Prof>>>,
    host_runtime_ctx: Prof::HostRuntimeCtx,
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
                .max_dbs(settings.lmdb_max_dbs)
                .map_size(settings.lmdb_map_size)
                .open(db_path)
        }?;
        let cleared_count = db_env.clear_stale_readers()?;
        if cleared_count > 0 {
            info!("Cleared {cleared_count} stale readers");
        }

        let state = Arc::new(EnvironmentInner {
            db_env,
            app_names: Mutex::new(BTreeSet::new()),
            target_states_providers,
            host_runtime_ctx,
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

    pub fn db_env(&self) -> &heed::Env {
        &self.inner.db_env
    }

    pub fn target_states_providers(&self) -> &Arc<Mutex<TargetStateProviderRegistry<Prof>>> {
        &self.inner.target_states_providers
    }

    pub fn host_runtime_ctx(&self) -> &Prof::HostRuntimeCtx {
        &self.inner.host_runtime_ctx
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
