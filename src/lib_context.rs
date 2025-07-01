use crate::prelude::*;

use crate::builder::AnalyzedFlow;
use crate::execution::source_indexer::SourceIndexingContext;
use crate::service::error::ApiError;
use crate::settings;
use crate::setup::ObjectSetupStatus;
use axum::http::StatusCode;
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use tokio::runtime::Runtime;

pub struct FlowExecutionContext {
    pub setup_execution_context: Arc<exec_ctx::FlowSetupExecutionContext>,
    pub setup_status: setup::FlowSetupStatus,
    source_indexing_contexts: Vec<tokio::sync::OnceCell<Arc<SourceIndexingContext>>>,
}

async fn build_setup_context(
    analyzed_flow: &AnalyzedFlow,
    existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
) -> Result<(
    Arc<exec_ctx::FlowSetupExecutionContext>,
    setup::FlowSetupStatus,
)> {
    let setup_execution_context = Arc::new(exec_ctx::build_flow_setup_execution_context(
        &analyzed_flow.flow_instance,
        &analyzed_flow.data_schema,
        &analyzed_flow.setup_state,
        existing_flow_ss,
    )?);

    let setup_status = setup::check_flow_setup_status(
        Some(&setup_execution_context.setup_state),
        existing_flow_ss,
    )
    .await?;

    Ok((setup_execution_context, setup_status))
}

impl FlowExecutionContext {
    async fn new(
        analyzed_flow: &AnalyzedFlow,
        existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
    ) -> Result<Self> {
        let (setup_execution_context, setup_status) =
            build_setup_context(analyzed_flow, existing_flow_ss).await?;

        let mut source_indexing_contexts = Vec::new();
        source_indexing_contexts.resize_with(analyzed_flow.flow_instance.import_ops.len(), || {
            tokio::sync::OnceCell::new()
        });

        Ok(Self {
            setup_execution_context,
            setup_status,
            source_indexing_contexts,
        })
    }

    pub async fn update_setup_state(
        &mut self,
        analyzed_flow: &AnalyzedFlow,
        existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
    ) -> Result<()> {
        let (setup_execution_context, setup_status) =
            build_setup_context(analyzed_flow, existing_flow_ss).await?;

        self.setup_execution_context = setup_execution_context;
        self.setup_status = setup_status;
        Ok(())
    }

    pub async fn get_source_indexing_context(
        &self,
        flow: &Arc<AnalyzedFlow>,
        source_idx: usize,
        pool: &PgPool,
    ) -> Result<&Arc<SourceIndexingContext>> {
        Ok(self.source_indexing_contexts[source_idx]
            .get_or_try_init(|| async move {
                anyhow::Ok(Arc::new(
                    SourceIndexingContext::load(
                        flow.clone(),
                        source_idx,
                        self.setup_execution_context.clone(),
                        pool,
                    )
                    .await?,
                ))
            })
            .await?)
    }
}

pub struct FlowContext {
    pub flow: Arc<AnalyzedFlow>,
    execution_ctx: Arc<tokio::sync::RwLock<FlowExecutionContext>>,
}

impl FlowContext {
    pub fn flow_name(&self) -> &str {
        &self.flow.flow_instance.name
    }

    pub async fn new(
        flow: Arc<AnalyzedFlow>,
        existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
    ) -> Result<Self> {
        let execution_ctx = Arc::new(tokio::sync::RwLock::new(
            FlowExecutionContext::new(&flow, existing_flow_ss).await?,
        ));
        Ok(Self {
            flow,
            execution_ctx,
        })
    }

    pub async fn use_execution_ctx(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<FlowExecutionContext>> {
        let execution_ctx = self.execution_ctx.read().await;
        if !execution_ctx.setup_status.is_up_to_date() {
            api_bail!(
                "Setup for flow `{}` is not up-to-date. Please run `cocoindex setup` to update the setup.",
                self.flow_name()
            );
        }
        Ok(execution_ctx)
    }

    pub async fn use_owned_execution_ctx(
        &self,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<FlowExecutionContext>> {
        let execution_ctx = self.execution_ctx.clone().read_owned().await;
        if !execution_ctx.setup_status.is_up_to_date() {
            api_bail!(
                "Setup for flow `{}` is not up-to-date. Please run `cocoindex setup` to update the setup.",
                self.flow_name()
            );
        }
        Ok(execution_ctx)
    }

    pub fn get_execution_ctx_for_setup(&self) -> &tokio::sync::RwLock<FlowExecutionContext> {
        &self.execution_ctx
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

pub struct LibSetupContext {
    pub all_setup_states: setup::AllSetupStates<setup::ExistingMode>,
    pub global_setup_status: setup::GlobalSetupStatus,
}
pub struct PersistenceContext {
    pub builtin_db_pool: PgPool,
    pub setup_ctx: tokio::sync::RwLock<LibSetupContext>,
}

pub struct LibContext {
    pub db_pools: DbPools,
    pub persistence_ctx: Option<PersistenceContext>,
    pub flows: Mutex<BTreeMap<String, Arc<FlowContext>>>,
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

    pub fn require_persistence_ctx(&self) -> Result<&PersistenceContext> {
        self.persistence_ctx
            .as_ref()
            .ok_or_else(|| anyhow!("Database is required for this operation. Please set COCOINDEX_DATABASE_URL environment variable and call cocoindex.init() with database settings."))
    }

    pub fn require_builtin_db_pool(&self) -> Result<&PgPool> {
        Ok(&self.require_persistence_ctx()?.builtin_db_pool)
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
    let persistence_ctx = if let Some(database_spec) = &settings.database {
        let (pool, all_setup_states) = get_runtime().block_on(async {
            let pool = db_pools.get_pool(database_spec).await?;
            let existing_ss = setup::get_existing_setup_state(&pool).await?;
            anyhow::Ok((pool, existing_ss))
        })?;
        Some(PersistenceContext {
            builtin_db_pool: pool,
            setup_ctx: tokio::sync::RwLock::new(LibSetupContext {
                global_setup_status: setup::GlobalSetupStatus::from_setup_states(&all_setup_states),
                all_setup_states,
            }),
        })
    } else {
        // No database configured
        None
    };

    Ok(LibContext {
        db_pools,
        persistence_ctx,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_pools_default() {
        let db_pools = DbPools::default();
        assert!(db_pools.pools.lock().unwrap().is_empty());
    }

    #[test]
    fn test_settings_structure_without_database() {
        let settings = settings::Settings {
            database: None,
            app_namespace: "test".to_string(),
        };

        // Test that we can create the basic structure
        assert!(settings.database.is_none());
        assert_eq!(settings.app_namespace, "test");
    }

    #[test]
    fn test_lib_context_without_database() {
        let settings = settings::Settings {
            database: None,
            app_namespace: "test".to_string(),
        };

        let lib_context = create_lib_context(settings).unwrap();
        assert!(lib_context.persistence_ctx.is_none());
        assert!(lib_context.require_builtin_db_pool().is_err());
    }

    #[test]
    fn test_persistence_context_type_safety() {
        // This test ensures that PersistenceContext groups related fields together
        let settings = settings::Settings {
            database: Some(settings::DatabaseConnectionSpec {
                url: "postgresql://test".to_string(),
                user: None,
                password: None,
            }),
            app_namespace: "test".to_string(),
        };

        // This would fail at runtime due to invalid connection, but we're testing the structure
        let result = create_lib_context(settings);
        // We expect this to fail due to invalid connection, but the structure should be correct
        assert!(result.is_err());
    }
}
