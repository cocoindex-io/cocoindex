use crate::prelude::*;

use crate::ops::sdk::*;
use crate::settings::DatabaseConnectionSpec;
use sqlx::PgPool;
use sqlx::postgres::types::PgRange;
use std::ops::Bound;

pub async fn get_db_pool(
    db_ref: Option<&spec::AuthEntryReference<DatabaseConnectionSpec>>,
    auth_registry: &AuthRegistry,
) -> Result<PgPool> {
    let lib_context = get_lib_context().await?;
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

pub fn bind_key_field<'arg>(
    builder: &mut sqlx::QueryBuilder<'arg, sqlx::Postgres>,
    key_value: &'arg KeyPart,
) -> Result<()> {
    match key_value {
        KeyPart::Bytes(v) => {
            builder.push_bind(&**v);
        }
        KeyPart::Str(v) => {
            builder.push_bind(&**v);
        }
        KeyPart::Bool(v) => {
            builder.push_bind(v);
        }
        KeyPart::Int64(v) => {
            builder.push_bind(v);
        }
        KeyPart::Range(v) => {
            builder.push_bind(PgRange {
                start: Bound::Included(v.start as i64),
                end: Bound::Excluded(v.end as i64),
            });
        }
        KeyPart::Uuid(v) => {
            builder.push_bind(v);
        }
        KeyPart::Date(v) => {
            builder.push_bind(v);
        }
        KeyPart::Struct(fields) => {
            builder.push_bind(sqlx::types::Json(fields));
        }
    }
    Ok(())
}
