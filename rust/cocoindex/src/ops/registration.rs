use super::{
    factory_bases::*, functions, registry::ExecutorFactoryRegistry, sdk::ExecutorFactory, sources,
    targets,
};
use crate::prelude::*;
use cocoindex_utils::client_error;
use std::sync::{LazyLock, RwLock};

fn register_executor_factories(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    let reqwest_client = reqwest::Client::new();

    sources::local_file::Factory.register(registry)?;
    sources::google_drive::Factory.register(registry)?;
    sources::amazon_s3::Factory.register(registry)?;
    sources::azure_blob::Factory.register(registry)?;
    sources::postgres::Factory.register(registry)?;

    functions::detect_program_lang::register(registry)?;
    functions::embed_text::register(registry)?;
    functions::extract_by_llm::Factory.register(registry)?;
    functions::parse_json::Factory.register(registry)?;
    functions::split_by_separators::register(registry)?;
    functions::split_recursively::register(registry)?;

    targets::postgres::register(registry)?;
    targets::qdrant::register(registry)?;
    targets::kuzu::register(registry, reqwest_client)?;

    targets::neo4j::Factory::new().register(registry)?;

    Ok(())
}

static EXECUTOR_FACTORY_REGISTRY: LazyLock<RwLock<ExecutorFactoryRegistry>> = LazyLock::new(|| {
    let mut registry = ExecutorFactoryRegistry::new();
    register_executor_factories(&mut registry).expect("Failed to register executor factories");
    RwLock::new(registry)
});

pub fn get_optional_source_factory(
    kind: &str,
) -> Option<std::sync::Arc<dyn super::interface::SourceFactory + Send + Sync>> {
    let registry = EXECUTOR_FACTORY_REGISTRY.read().unwrap();
    registry.get_source(kind).cloned()
}

pub fn get_optional_function_factory(
    kind: &str,
) -> Option<std::sync::Arc<dyn super::interface::SimpleFunctionFactory + Send + Sync>> {
    let registry = EXECUTOR_FACTORY_REGISTRY.read().unwrap();
    registry.get_function(kind).cloned()
}

pub fn get_optional_target_factory(
    kind: &str,
) -> Option<std::sync::Arc<dyn super::interface::TargetFactory + Send + Sync>> {
    let registry = EXECUTOR_FACTORY_REGISTRY.read().unwrap();
    registry.get_target(kind).cloned()
}

pub fn get_optional_attachment_factory(
    kind: &str,
) -> Option<std::sync::Arc<dyn super::interface::TargetAttachmentFactory + Send + Sync>> {
    let registry = EXECUTOR_FACTORY_REGISTRY.read().unwrap();
    registry.get_target_attachment(kind).cloned()
}

pub fn get_source_factory(
    kind: &str,
) -> Result<std::sync::Arc<dyn super::interface::SourceFactory + Send + Sync>> {
    get_optional_source_factory(kind)
        .ok_or_else(|| client_error!("Source factory not found for op kind: {}", kind))
}

pub fn get_function_factory(
    kind: &str,
) -> Result<std::sync::Arc<dyn super::interface::SimpleFunctionFactory + Send + Sync>> {
    get_optional_function_factory(kind)
        .ok_or_else(|| client_error!("Function factory not found for op kind: {}", kind))
}

pub fn get_target_factory(
    kind: &str,
) -> Result<std::sync::Arc<dyn super::interface::TargetFactory + Send + Sync>> {
    get_optional_target_factory(kind)
        .ok_or_else(|| client_error!("Target factory not found for op kind: {}", kind))
}

pub fn get_attachment_factory(
    kind: &str,
) -> Result<std::sync::Arc<dyn super::interface::TargetAttachmentFactory + Send + Sync>> {
    get_optional_attachment_factory(kind)
        .ok_or_else(|| client_error!("Attachment factory not found for op kind: {}", kind))
}

pub fn register_factory(name: String, factory: ExecutorFactory) -> Result<()> {
    let mut registry = EXECUTOR_FACTORY_REGISTRY.write().unwrap();
    registry.register(name, factory)
}
