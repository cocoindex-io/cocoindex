use std::time::Duration;

use crate::prelude::*;

use crate::builder::AnalyzedFlow;
use crate::execution::source_indexer::SourceIndexingContext;
use crate::service::error::ApiError;
use crate::service::query_handler::{QueryHandler, QueryHandlerSpec};
use crate::settings;
use crate::setup::ObjectSetupChange;
use axum::http::StatusCode;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::runtime::Runtime;

pub struct FlowExecutionContext {
    pub setup_execution_context: Arc<exec_ctx::FlowSetupExecutionContext>,
    pub setup_change: setup::FlowSetupChange,
    source_indexing_contexts: Vec<tokio::sync::OnceCell<Arc<SourceIndexingContext>>>,
}

async fn build_setup_context(
    analyzed_flow: &AnalyzedFlow,
    existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
) -> Result<(
    Arc<exec_ctx::FlowSetupExecutionContext>,
    setup::FlowSetupChange,
)> {
    let setup_execution_context = Arc::new(exec_ctx::build_flow_setup_execution_context(
        &analyzed_flow.flow_instance,
        &analyzed_flow.data_schema,
        &analyzed_flow.setup_state,
        existing_flow_ss,
    )?);

    let setup_change = setup::diff_flow_setup_states(
        Some(&setup_execution_context.setup_state),
        existing_flow_ss,
        &analyzed_flow.flow_instance_ctx,
    )
    .await?;

    Ok((setup_execution_context, setup_change))
}

impl FlowExecutionContext {
    async fn new(
        analyzed_flow: &AnalyzedFlow,
        existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
    ) -> Result<Self> {
        let (setup_execution_context, setup_change) =
            build_setup_context(analyzed_flow, existing_flow_ss).await?;

        let mut source_indexing_contexts = Vec::new();
        source_indexing_contexts.resize_with(analyzed_flow.flow_instance.import_ops.len(), || {
            tokio::sync::OnceCell::new()
        });

        Ok(Self {
            setup_execution_context,
            setup_change,
            source_indexing_contexts,
        })
    }

    pub async fn update_setup_state(
        &mut self,
        analyzed_flow: &AnalyzedFlow,
        existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
    ) -> Result<()> {
        let (setup_execution_context, setup_change) =
            build_setup_context(analyzed_flow, existing_flow_ss).await?;

        self.setup_execution_context = setup_execution_context;
        self.setup_change = setup_change;
        Ok(())
    }

    pub async fn get_source_indexing_context(
        &self,
        flow: &Arc<AnalyzedFlow>,
        source_idx: usize,
        pool: &PgPool,
    ) -> Result<&Arc<SourceIndexingContext>> {
        self.source_indexing_contexts[source_idx]
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
            .await
    }
}

pub struct QueryHandlerContext {
    pub info: Arc<QueryHandlerSpec>,
    pub handler: Arc<dyn QueryHandler>,
}

pub struct FlowContext {
    pub flow: Arc<AnalyzedFlow>,
    execution_ctx: Arc<tokio::sync::RwLock<FlowExecutionContext>>,
    pub query_handlers: RwLock<HashMap<String, QueryHandlerContext>>,
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
            query_handlers: RwLock::new(HashMap::new()),
        })
    }

    pub async fn use_execution_ctx(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, FlowExecutionContext>> {
        let execution_ctx = self.execution_ctx.read().await;
        if !execution_ctx.setup_change.is_up_to_date() {
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
        if !execution_ctx.setup_change.is_up_to_date() {
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

type PoolKey = (String, Option<String>);
type PoolValue = Arc<tokio::sync::OnceCell<PgPool>>;

#[derive(Default)]
pub struct DbPools {
    pub pools: Mutex<HashMap<PoolKey, PoolValue>>,
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

                // Try to connect to the database with a low timeout first.
                {
                    let pool_options = PgPoolOptions::new()
                        .max_connections(1)
                        .min_connections(1)
                        .acquire_timeout(Duration::from_secs(30));
                    let pool = pool_options
                        .connect_with(pg_options.clone())
                        .await
                        .context(format!("Failed to connect to database {}", conn_spec.url))?;
                    let _ = pool.acquire().await?;
                }

                // Now create the actual pool.
                let pool_options = PgPoolOptions::new()
                    .max_connections(conn_spec.max_connections)
                    .min_connections(conn_spec.min_connections)
                    .acquire_slow_level(log::LevelFilter::Info)
                    .acquire_slow_threshold(Duration::from_secs(10))
                    .acquire_timeout(Duration::from_secs(5 * 60));
                let pool = pool_options
                    .connect_with(pg_options)
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
    pub global_setup_change: setup::GlobalSetupChange,
}
pub struct PersistenceContext {
    pub builtin_db_pool: PgPool,
    pub setup_ctx: tokio::sync::RwLock<LibSetupContext>,
}

pub struct LibContext {
    pub db_pools: DbPools,
    pub persistence_ctx: Option<PersistenceContext>,
    pub flows: Mutex<BTreeMap<String, Arc<FlowContext>>>,
    pub app_namespace: String,

    pub global_concurrency_controller: Arc<concur_control::ConcurrencyController>,
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

    pub fn remove_flow_context(&self, flow_name: &str) {
        let mut flows = self.flows.lock().unwrap();
        flows.remove(flow_name);
    }

    pub fn require_persistence_ctx(&self) -> Result<&PersistenceContext> {
        self.persistence_ctx.as_ref().ok_or_else(|| {
            anyhow!(
                "Database is required for this operation. \
                         The easiest way is to set COCOINDEX_DATABASE_URL environment variable. \
                         Please see https://cocoindex.io/docs/core/settings for more details."
            )
        })
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
pub async fn create_lib_context(settings: settings::Settings) -> Result<LibContext> {
    LIB_INIT.get_or_init(|| {
        let _ = env_logger::try_init();

        pyo3_async_runtimes::tokio::init_with_runtime(get_runtime()).unwrap();

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });

    let db_pools = DbPools::default();
    let persistence_ctx = if let Some(database_spec) = &settings.database {
        let pool = db_pools.get_pool(database_spec).await?;
        let all_setup_states = setup::get_existing_setup_state(&pool).await?;
        Some(PersistenceContext {
            builtin_db_pool: pool,
            setup_ctx: tokio::sync::RwLock::new(LibSetupContext {
                global_setup_change: setup::GlobalSetupChange::from_setup_states(&all_setup_states),
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
        app_namespace: settings.app_namespace,
        global_concurrency_controller: Arc::new(concur_control::ConcurrencyController::new(
            &concur_control::Options {
                max_inflight_rows: settings.global_execution_options.source_max_inflight_rows,
                max_inflight_bytes: settings.global_execution_options.source_max_inflight_bytes,
            },
        )),
    })
}

static GET_SETTINGS_FN: Mutex<Option<Box<dyn Fn() -> Result<settings::Settings> + Send + Sync>>> =
    Mutex::new(None);
fn get_settings() -> Result<settings::Settings> {
    let get_settings_fn = GET_SETTINGS_FN.lock().unwrap();
    let settings = if let Some(get_settings_fn) = &*get_settings_fn {
        get_settings_fn()?
    } else {
        bail!("CocoIndex setting function is not provided");
    };
    Ok(settings)
}

pub(crate) fn set_settings_fn(
    get_settings_fn: Box<dyn Fn() -> Result<settings::Settings> + Send + Sync>,
) {
    let mut get_settings_fn_locked = GET_SETTINGS_FN.lock().unwrap();
    *get_settings_fn_locked = Some(get_settings_fn);
}

static LIB_CONTEXT: LazyLock<tokio::sync::Mutex<Option<Arc<LibContext>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(None));

pub(crate) async fn init_lib_context(settings: Option<settings::Settings>) -> Result<()> {
    error!("Init lib context");
    let settings = match settings {
        Some(settings) => settings,
        None => get_settings()?,
    };
    error!("Init lib context with settings: {:?}", settings);
    let mut lib_context_locked = LIB_CONTEXT.lock().await;
    *lib_context_locked = Some(Arc::new(create_lib_context(settings).await?));
    Ok(())
}

pub(crate) async fn get_lib_context() -> Result<Arc<LibContext>> {
    let mut lib_context_locked = LIB_CONTEXT.lock().await;
    let lib_context = if let Some(lib_context) = &*lib_context_locked {
        lib_context.clone()
    } else {
        let setting = get_settings()?;
        let lib_context = Arc::new(create_lib_context(setting).await?);
        *lib_context_locked = Some(lib_context.clone());
        lib_context
    };
    Ok(lib_context)
}

pub(crate) async fn clear_lib_context() {
    let mut lib_context_locked = LIB_CONTEXT.lock().await;
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

    #[tokio::test]
    async fn test_lib_context_without_database() {
        let lib_context = create_lib_context(settings::Settings::default())
            .await
            .unwrap();
        assert!(lib_context.persistence_ctx.is_none());
        assert!(lib_context.require_builtin_db_pool().is_err());
    }

    #[tokio::test]
    async fn test_persistence_context_type_safety() {
        // This test ensures that PersistenceContext groups related fields together
        let settings = settings::Settings {
            database: Some(settings::DatabaseConnectionSpec {
                url: "postgresql://test".to_string(),
                user: None,
                password: None,
                max_connections: 10,
                min_connections: 1,
            }),
            ..Default::default()
        };

        // This would fail at runtime due to invalid connection, but we're testing the structure
        let result = create_lib_context(settings).await;
        // We expect this to fail due to invalid connection, but the structure should be correct
        assert!(result.is_err());
    }
}
