use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::service::error::ApiError;
use crate::settings;
use crate::setup;
use crate::{builder::AnalyzedFlow, execution::query::SimpleSemanticsQueryHandler};
use anyhow::Result;
use axum::http::StatusCode;
use sqlx::PgPool;
use tokio::runtime::Runtime;

pub struct FlowContext {
    pub flow: Arc<AnalyzedFlow>,
    pub query_handlers: BTreeMap<String, Arc<SimpleSemanticsQueryHandler>>,
}

impl FlowContext {
    pub fn new(flow: Arc<AnalyzedFlow>) -> Self {
        Self {
            flow,
            query_handlers: BTreeMap::new(),
        }
    }
}

pub struct LibContext {
    pub runtime: Runtime,
    pub pool: PgPool,
    pub flows: RwLock<BTreeMap<String, FlowContext>>,
    pub combined_setup_states: RwLock<setup::AllSetupState<setup::ExistingMode>>,
}

impl LibContext {
    pub fn with_flow_context<R>(
        &self,
        flow_name: &str,
        f: impl FnOnce(&FlowContext) -> R,
    ) -> Result<R, ApiError> {
        let flows = self.flows.read().unwrap();
        let flow_context = flows.get(flow_name).ok_or_else(|| {
            ApiError::new(
                &format!("Flow instance not found: {flow_name}"),
                StatusCode::NOT_FOUND,
            )
        })?;
        Ok(f(flow_context))
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
        flows: RwLock::new(BTreeMap::new()),
    })
}
