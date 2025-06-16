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
            None => api_bail!(
                "Auth entry `{key}` not found.\n\
                Hint: If you're not referencing `{key}` in your flow, it will likely be caused by a previously persisted target using it. \
                You need to bring back the definition for the auth entry `{key}`, so that CocoIndex will be able to do a cleanup in the next `setup` run. \
                See https://cocoindex.io/docs/core/flow_def#auth-registry for more details.",
                key = entry_ref.key
            ),
        }
    }
}
