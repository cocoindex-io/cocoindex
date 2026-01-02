use crate::prelude::*;
use sqlx::PgPool;
pub enum WriteAction {
    Insert,
    Update,
}

pub async fn ensure_schema_from_search_path(pool: &PgPool) -> Result<()> {
    let search_path: String = sqlx::query_scalar("SHOW search_path")
        .fetch_one(pool)
        .await?;
    // Parse the first schema that is not pg_catalog or information_schema
    let schema_opt = search_path
        .split(',')
        .map(|s| s.trim().trim_matches('"'))
        .find(|s| *s != "pg_catalog" && *s != "information_schema");
    if let Some(schema) = schema_opt {
        let query = format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            schema.replace('"', "\"\"")
        );
        sqlx::query(&query)
            .execute(pool)
            .await
            .with_context(|| format!("Failed to create schema `{}`", schema))?;
    }
    Ok(())
}

pub fn sanitize_identifier(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        if c.is_alphanumeric() || c == '_' {
            result.push(c);
        } else {
            result.push_str("__");
        }
    }
    result
}
