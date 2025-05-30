use super::{
    factory_bases::*, functions, registry::ExecutorFactoryRegistry, sdk::ExecutorFactory, sources,
    storages,
};
use anyhow::Result;
use std::sync::{LazyLock, RwLock, RwLockReadGuard};

fn register_executor_factories(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    let reqwest_client = reqwest::Client::new();

    sources::local_file::Factory.register(registry)?;
    sources::google_drive::Factory.register(registry)?;
    sources::amazon_s3::Factory.register(registry)?;

    functions::parse_json::Factory.register(registry)?;
    functions::split_recursively::Factory.register(registry)?;
    functions::extract_by_llm::Factory.register(registry)?;

    storages::postgres::Factory::default().register(registry)?;
    storages::qdrant::register(registry)?;
    storages::kuzu::register(registry, reqwest_client)?;

    storages::neo4j::Factory::new().register(registry)?;

    Ok(())
}

static EXECUTOR_FACTORY_REGISTRY: LazyLock<RwLock<ExecutorFactoryRegistry>> = LazyLock::new(|| {
    let mut registry = ExecutorFactoryRegistry::new();
    register_executor_factories(&mut registry).expect("Failed to register executor factories");
    RwLock::new(registry)
});

pub fn executor_factory_registry() -> RwLockReadGuard<'static, ExecutorFactoryRegistry> {
    EXECUTOR_FACTORY_REGISTRY.read().unwrap()
}

pub fn register_factory(name: String, factory: ExecutorFactory) -> Result<()> {
    let mut registry = EXECUTOR_FACTORY_REGISTRY.write().unwrap();
    registry.register(name, factory)
}
