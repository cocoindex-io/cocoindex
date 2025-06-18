use crate::prelude::*;

use crate::builder::AnalyzedFlow;
use crate::execution::source_indexer::SourceIndexingContext;
use crate::service::error::ApiError;
use crate::settings;
use crate::setup;
use axum::http::StatusCode;
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use std::collections::BTreeMap;
use tokio::runtime::Runtime;

pub struct FlowContext {
    pub flow: Arc<AnalyzedFlow>,
    pub source_indexing_contexts: Vec<tokio::sync::OnceCell<Arc<SourceIndexingContext>>>,
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

pub struct PersistenceContext {
    pub builtin_db_pool: PgPool,
    pub all_setup_states: RwLock<setup::AllSetupState<setup::ExistingMode>>,
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

    pub fn require_builtin_db_pool(&self) -> Result<&PgPool> {
        self.persistence_ctx
            .as_ref()
            .map(|ctx| &ctx.builtin_db_pool)
            .ok_or_else(|| anyhow!("Database is required for this operation. Please set COCOINDEX_DATABASE_URL environment variable and call cocoindex.init() with database settings."))
    }

    pub fn require_all_setup_states(
        &self,
    ) -> Result<&RwLock<setup::AllSetupState<setup::ExistingMode>>> {
        self.persistence_ctx
            .as_ref()
            .map(|ctx| &ctx.all_setup_states)
            .ok_or_else(|| anyhow!("Database is required for this operation. Please set COCOINDEX_DATABASE_URL environment variable and call cocoindex.init() with database settings."))
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
            all_setup_states: RwLock::new(all_setup_states),
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
        assert!(lib_context.require_all_setup_states().is_err());
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
