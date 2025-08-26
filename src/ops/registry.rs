use super::interface::ExecutorFactory;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ExecutorFactoryRegistry {
    source_factories: HashMap<String, Arc<dyn super::interface::SourceFactory + Send + Sync>>,
    function_factories:
        HashMap<String, Arc<dyn super::interface::SimpleFunctionFactory + Send + Sync>>,
    target_factories: HashMap<String, Arc<dyn super::interface::TargetFactory + Send + Sync>>,
}

impl Default for ExecutorFactoryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutorFactoryRegistry {
    pub fn new() -> Self {
        Self {
            source_factories: HashMap::new(),
            function_factories: HashMap::new(),
            target_factories: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, factory: ExecutorFactory) -> Result<()> {
        match factory {
            ExecutorFactory::Source(source_factory) => match self.source_factories.entry(name) {
                std::collections::hash_map::Entry::Occupied(entry) => Err(anyhow::anyhow!(
                    "Source factory with name already exists: {}",
                    entry.key()
                )),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(source_factory);
                    Ok(())
                }
            },
            ExecutorFactory::SimpleFunction(function_factory) => {
                match self.function_factories.entry(name) {
                    std::collections::hash_map::Entry::Occupied(entry) => Err(anyhow::anyhow!(
                        "Function factory with name already exists: {}",
                        entry.key()
                    )),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(function_factory);
                        Ok(())
                    }
                }
            }
            ExecutorFactory::ExportTarget(target_factory) => {
                match self.target_factories.entry(name) {
                    std::collections::hash_map::Entry::Occupied(entry) => Err(anyhow::anyhow!(
                        "Target factory with name already exists: {}",
                        entry.key()
                    )),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(target_factory);
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn get_source(
        &self,
        name: &str,
    ) -> Option<&Arc<dyn super::interface::SourceFactory + Send + Sync>> {
        self.source_factories.get(name)
    }

    pub fn get_function(
        &self,
        name: &str,
    ) -> Option<&Arc<dyn super::interface::SimpleFunctionFactory + Send + Sync>> {
        self.function_factories.get(name)
    }

    pub fn get_target(
        &self,
        name: &str,
    ) -> Option<&Arc<dyn super::interface::TargetFactory + Send + Sync>> {
        self.target_factories.get(name)
    }
}
