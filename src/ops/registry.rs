use super::interface::ExecutorFactory;
use anyhow::Result;
use std::collections::HashMap;

pub struct ExecutorFactoryRegistry {
    factories: HashMap<String, ExecutorFactory>,
}

impl Default for ExecutorFactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutorFactoryRegistry {
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, factory: ExecutorFactory) -> Result<()> {
        match self.factories.entry(name) {
            std::collections::hash_map::Entry::Occupied(entry) => Err(anyhow::anyhow!(
                "Factory with name already exists: {}",
                entry.key()
            )),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(factory);
                Ok(())
            }
        }
    }

    pub fn get(&self, name: &str) -> Option<&ExecutorFactory> {
        self.factories.get(name)
    }
}
