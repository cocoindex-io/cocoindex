use std::collections::hash_map;

use crate::prelude::*;

pub struct AuthRegistry {
    entries: RwLock<HashMap<String, serde_json::Value>>,
}

impl Default for AuthRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthRegistry {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    pub fn add(&self, key: String, value: serde_json::Value) -> Result<()> {
        let mut entries = self.entries.write().unwrap();
        match entries.entry(key) {
            hash_map::Entry::Occupied(entry) => {
                api_bail!("Auth entry already exists: {}", entry.key());
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
        Ok(())
    }

    pub fn get<T: DeserializeOwned>(&self, entry_ref: &spec::AuthEntryReference<T>) -> Result<T> {
        let entries = self.entries.read().unwrap();
        match entries.get(&entry_ref.key) {
            Some(value) => Ok(serde_json::from_value(value.clone())?),
            None => api_bail!("Auth entry not found: {}", entry_ref.key),
        }
    }
}
