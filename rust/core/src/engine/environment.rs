use crate::{
    engine::{
        effect::{EffectReconciler, RootEffectProviderRegistry},
        profile::EngineProfile,
    },
    prelude::*,
};

use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EnvironmentSettings {
    pub db_path: PathBuf,
}

struct EnvironmentInner<Prof: EngineProfile> {
    db_env: heed::Env<heed::WithoutTls>,
    app_names: Mutex<BTreeSet<String>>,
    effect_providers: RootEffectProviderRegistry<Prof>,
}

#[derive(Clone)]
pub struct Environment<Prof: EngineProfile> {
    inner: Arc<EnvironmentInner<Prof>>,
}

impl<Prof: EngineProfile> Environment<Prof> {
    pub fn new(
        settings: EnvironmentSettings,
        effect_providers: RootEffectProviderRegistry<Prof>,
    ) -> Result<Self> {
        // Create the directory if not exists.
        std::fs::create_dir_all(&settings.db_path)?;

        let state = Arc::new(EnvironmentInner {
            db_env: unsafe {
                heed::EnvOpenOptions::new()
                    .read_txn_without_tls()
                    .open(settings.db_path.clone())
            }?,
            app_names: Mutex::new(BTreeSet::new()),
            effect_providers,
        });
        Ok(Self { inner: state })
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
            bail!("App name already registered: {}", name);
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
