use crate::{
    engine::profile::EngineProfile, engine::target_state::TargetStateProviderRegistry, prelude::*,
};

use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf, u32};

// TODO: Expose these as settings.
const MAX_DBS: u32 = 1024;
const LMDB_MAP_SIZE: usize = 0x1_0000_0000; // 4GiB

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EnvironmentSettings {
    pub db_path: PathBuf,
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
        // Create the directory if not exists.
        std::fs::create_dir_all(&settings.db_path)?;
        let db_env = unsafe {
            heed::EnvOpenOptions::new()
                .max_dbs(MAX_DBS)
                .map_size(LMDB_MAP_SIZE)
                .open(settings.db_path.clone())
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
