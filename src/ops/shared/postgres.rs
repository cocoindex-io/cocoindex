use crate::prelude::*;

use crate::settings::DatabaseConnectionSpec;
use sqlx::PgPool;

pub async fn get_db_pool(
    db_ref: Option<&spec::AuthEntryReference<DatabaseConnectionSpec>>,
    auth_registry: &AuthRegistry,
) -> Result<PgPool> {
    let lib_context = get_lib_context()?;
    let db_conn_spec = db_ref
        .as_ref()
        .map(|db_ref| auth_registry.get(db_ref))
        .transpose()?;
    let db_pool = match db_conn_spec {
        Some(db_conn_spec) => lib_context.db_pools.get_pool(&db_conn_spec).await?,
        None => lib_context.require_builtin_db_pool()?.clone(),
    };
    Ok(db_pool)
}
