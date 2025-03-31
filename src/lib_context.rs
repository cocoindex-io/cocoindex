use crate::prelude::*;

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::execution::source_indexer::SourceIndexingContext;
use crate::service::error::ApiError;
use crate::settings;
use crate::setup;
use crate::{builder::AnalyzedFlow, execution::query::SimpleSemanticsQueryHandler};
use async_lock::OnceCell;
use axum::http::StatusCode;
use sqlx::PgPool;
use tokio::runtime::Runtime;

pub struct FlowContext {
    pub flow: Arc<AnalyzedFlow>,
    pub source_indexing_contexts: Vec<OnceCell<Arc<SourceIndexingContext>>>,
    pub query_handlers: Mutex<BTreeMap<String, Arc<SimpleSemanticsQueryHandler>>>,
}

impl FlowContext {
    pub fn new(flow: Arc<AnalyzedFlow>) -> Self {
        let mut source_indexing_contexts = Vec::new();
        source_indexing_contexts
            .resize_with(flow.flow_instance.import_ops.len(), || OnceCell::new());
        Self {
            flow,
            source_indexing_contexts,
            query_handlers: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn get_source_indexing_context(
        &self,
        source_idx: usize,
        pool: &PgPool,
    ) -> Result<&Arc<SourceIndexingContext>> {
        self.source_indexing_contexts[source_idx]
            .get_or_try_init(|| async move {
                Ok(Arc::new(
                    SourceIndexingContext::load(self.flow.clone(), source_idx, pool).await?,
                ))
            })
            .await
    }

    pub fn get_query_handler(&self, name: &str) -> Result<Arc<SimpleSemanticsQueryHandler>> {
        let query_handlers = self.query_handlers.lock().unwrap();
        let query_handler = query_handlers
            .get(name)
            .ok_or_else(|| {
                ApiError::new(
                    &format!("Query handler not found: {name}"),
                    StatusCode::NOT_FOUND,
                )
            })?
            .clone();
        Ok(query_handler)
    }
}

pub struct LibContext {
    pub runtime: Runtime,
    pub pool: PgPool,
    pub flows: Mutex<BTreeMap<String, Arc<FlowContext>>>,
    pub combined_setup_states: RwLock<setup::AllSetupState<setup::ExistingMode>>,
}

impl LibContext {
    pub fn get_flow_context(&self, flow_name: &str) -> Result<Arc<FlowContext>> {
        let flows = self.flows.lock().unwrap();
        let flow_ctx = flows
            .get(flow_name)
            .ok_or_else(|| {
                ApiError::new(
                    &format!("Flow instance not found: {flow_name}"),
                    StatusCode::NOT_FOUND,
                )
            })?
            .clone();
        Ok(flow_ctx)
    }
}

pub fn create_lib_context(settings: settings::Settings) -> Result<LibContext> {
    console_subscriber::init();
    env_logger::init();

    let runtime = Runtime::new()?;
    let (pool, all_css) = runtime.block_on(async {
        let pool = PgPool::connect(&settings.database_url).await?;
        let existing_ss = setup::get_existing_setup_state(&pool).await?;
        anyhow::Ok((pool, existing_ss))
    })?;
    Ok(LibContext {
        runtime,
        pool,
        combined_setup_states: RwLock::new(all_css),
        flows: Mutex::new(BTreeMap::new()),
    })
}
