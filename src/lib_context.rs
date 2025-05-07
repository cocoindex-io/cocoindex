use crate::prelude::*;

use crate::execution::source_indexer::SourceIndexingContext;
use crate::service::error::ApiError;
use crate::settings;
use crate::setup;
use crate::{builder::AnalyzedFlow, execution::query::SimpleSemanticsQueryHandler};
use axum::http::StatusCode;
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use std::collections::BTreeMap;
use tokio::runtime::Runtime;

pub struct FlowContext {
    pub flow: Arc<AnalyzedFlow>,
    pub source_indexing_contexts: Vec<tokio::sync::OnceCell<Arc<SourceIndexingContext>>>,
    pub query_handlers: Mutex<BTreeMap<String, Arc<SimpleSemanticsQueryHandler>>>,
}

impl FlowContext {
    pub fn new(flow: Arc<AnalyzedFlow>) -> Self {
        let mut source_indexing_contexts = Vec::new();
        source_indexing_contexts.resize_with(flow.flow_instance.import_ops.len(), || {
            tokio::sync::OnceCell::new()
        });
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

static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());
static AUTH_REGISTRY: LazyLock<Arc<AuthRegistry>> = LazyLock::new(|| Arc::new(AuthRegistry::new()));

#[derive(Default)]
pub struct DbPools {
    pub pools: Mutex<HashMap<(String, Option<String>), Arc<tokio::sync::OnceCell<PgPool>>>>,
}

impl DbPools {
    pub async fn get_pool(&self, conn_spec: &settings::DatabaseConnectionSpec) -> Result<PgPool> {
        let db_pool_cell = {
            let key = (conn_spec.url.clone(), conn_spec.user.clone());
            let mut db_pools = self.pools.lock().unwrap();
            db_pools.entry(key).or_default().clone()
        };
        let pool = db_pool_cell
            .get_or_try_init(|| async move {
                let mut pg_options: PgConnectOptions = conn_spec.url.parse()?;
                if let Some(user) = &conn_spec.user {
                    pg_options = pg_options.username(user);
                }
                if let Some(password) = &conn_spec.password {
                    pg_options = pg_options.password(password);
                }
                let pool = PgPool::connect_with(pg_options)
                    .await
                    .context("Failed to connect to database")?;
                anyhow::Ok(pool)
            })
            .await?;
        Ok(pool.clone())
    }
}

pub struct LibContext {
    pub db_pools: DbPools,
    pub builtin_db_pool: PgPool,
    pub flows: Mutex<BTreeMap<String, Arc<FlowContext>>>,
    pub all_setup_states: RwLock<setup::AllSetupState<setup::ExistingMode>>,
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

pub fn get_runtime() -> &'static Runtime {
    &TOKIO_RUNTIME
}

pub fn get_auth_registry() -> &'static Arc<AuthRegistry> {
    &AUTH_REGISTRY
}

static LIB_INIT: OnceLock<()> = OnceLock::new();
pub fn create_lib_context(settings: settings::Settings) -> Result<LibContext> {
    LIB_INIT.get_or_init(|| {
        let _ = env_logger::try_init();

        pyo3_async_runtimes::tokio::init_with_runtime(get_runtime()).unwrap();
    });

    let db_pools = DbPools::default();
    let (pool, all_setup_states) = get_runtime().block_on(async {
        let pool = db_pools.get_pool(&settings.database).await?;
        let existing_ss = setup::get_existing_setup_state(&pool).await?;
        anyhow::Ok((pool, existing_ss))
    })?;
    Ok(LibContext {
        db_pools,
        builtin_db_pool: pool,
        all_setup_states: RwLock::new(all_setup_states),
        flows: Mutex::new(BTreeMap::new()),
    })
}

static LIB_CONTEXT: RwLock<Option<Arc<LibContext>>> = RwLock::new(None);

pub(crate) fn init_lib_context(settings: settings::Settings) -> Result<()> {
    let mut lib_context_locked = LIB_CONTEXT.write().unwrap();
    *lib_context_locked = Some(Arc::new(create_lib_context(settings)?));
    Ok(())
}

pub(crate) fn get_lib_context() -> Result<Arc<LibContext>> {
    let lib_context_locked = LIB_CONTEXT.read().unwrap();
    lib_context_locked
        .as_ref()
        .cloned()
        .ok_or_else(|| anyhow!("CocoIndex library is not initialized or already stopped"))
}

pub(crate) fn clear_lib_context() {
    let mut lib_context_locked = LIB_CONTEXT.write().unwrap();
    *lib_context_locked = None;
}
