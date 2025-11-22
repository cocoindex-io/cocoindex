use crate::prelude::*;

use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EnvironmentSettings {
    pub db_path: PathBuf,
}

struct EnvironmentState {
    db_env: heed::Env<heed::WithoutTls>,
    app_names: Mutex<BTreeSet<String>>,
}

#[derive(Clone)]
pub struct Environment {
    state: Arc<EnvironmentState>,
}

impl Environment {
    pub fn new(settings: EnvironmentSettings) -> Result<Self> {
        // Create the directory if not exists.
        std::fs::create_dir_all(&settings.db_path)?;

        let state = Arc::new(EnvironmentState {
            db_env: unsafe {
                heed::EnvOpenOptions::new()
                    .read_txn_without_tls()
                    .open(settings.db_path.clone())
            }?,
            app_names: Mutex::new(BTreeSet::new()),
        });
        Ok(Self { state })
    }
}

pub struct AppRegistration {
    name: String,
    env: Environment,
}

impl AppRegistration {
    pub fn new(name: &str, env: &Environment) -> Result<Self> {
        let mut app_names = env.state.app_names.lock().unwrap();
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

impl Drop for AppRegistration {
    fn drop(&mut self) {
        let mut app_names = self.env.state.app_names.lock().unwrap();
        app_names.remove(&self.name);
    }
}
