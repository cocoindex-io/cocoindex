use crate::prelude::*;

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug)]
pub struct PlanetSettings {
    db_path: PathBuf,
}
pub struct Planet {
    db_env: heed::Env<heed::WithoutTls>,
}

impl Planet {
    pub fn new(settings: PlanetSettings) -> Result<Self> {
        let db_env = unsafe {
            heed::EnvOpenOptions::new()
                .read_txn_without_tls()
                .open(settings.db_path)
        }?;
        Ok(Self { db_env })
    }
}

static PLANET: LazyLock<Mutex<Option<Planet>>> = LazyLock::new(|| Mutex::new(None));

pub fn init_planet(settings: PlanetSettings) -> Result<()> {
    let planet = Planet::new(settings)?;
    *PLANET.lock().unwrap() = Some(planet);
    Ok(())
}

pub fn close_planet() -> Result<()> {
    let mut planet = PLANET.lock().unwrap();
    *planet = None;
    Ok(())
}
