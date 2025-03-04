use super::{db_tracking_setup::TrackingTableSetupState, memoization::MemoizationInfo};
use crate::utils::{db::WriteAction, fingerprint::Fingerprint};
use anyhow::Result;
use sqlx::PgPool;

pub type TrackedTargetKey = (serde_json::Value, i64, Option<Fingerprint>);
pub type TrackedTargetKeyForSource = Vec<(i32, Vec<TrackedTargetKey>)>;

#[derive(sqlx::FromRow, Debug)]
pub struct SourceTrackingInfo {
    pub max_process_ordinal: i64,
    pub staging_target_keys: sqlx::types::Json<TrackedTargetKeyForSource>,
    pub memoization_info: Option<sqlx::types::Json<Option<MemoizationInfo>>>,

    pub processed_source_ordinal: Option<i64>,
    pub process_logic_fingerprint: Option<Vec<u8>>,
    pub process_ordinal: Option<i64>,
    pub process_time_micros: Option<i64>,
    pub target_keys: Option<sqlx::types::Json<TrackedTargetKeyForSource>>,
}

pub async fn read_source_tracking_info(
    source_id: i32,
    source_key_json: &serde_json::Value,
    db_setup: &TrackingTableSetupState,
    pool: &PgPool,
) -> Result<Option<SourceTrackingInfo>> {
    let query_str = format!(
        "SELECT max_process_ordinal, staging_target_keys, memoization_info, processed_source_ordinal, process_logic_fingerprint, process_ordinal, process_time_micros, target_keys FROM {} WHERE source_id = $1 AND source_key = $2",
        db_setup.table_name
    );
    let tracking_info = sqlx::query_as(&query_str)
        .bind(source_id)
        .bind(source_key_json)
        .fetch_optional(pool)
        .await?;

    Ok(tracking_info)
}

#[derive(sqlx::FromRow, Debug)]
pub struct SourceTrackingInfoForPrecommit {
    pub max_process_ordinal: i64,
    pub staging_target_keys: sqlx::types::Json<TrackedTargetKeyForSource>,

    pub processed_source_ordinal: Option<i64>,
    pub process_ordinal: Option<i64>,
    pub target_keys: Option<sqlx::types::Json<TrackedTargetKeyForSource>>,
}

pub async fn read_source_tracking_info_for_precommit(
    source_id: i32,
    source_key_json: &serde_json::Value,
    db_setup: &TrackingTableSetupState,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<Option<SourceTrackingInfoForPrecommit>> {
    let query_str = format!(
        "SELECT max_process_ordinal, staging_target_keys, processed_source_ordinal, process_ordinal, target_keys FROM {} WHERE source_id = $1 AND source_key = $2",
        db_setup.table_name
    );
    let precommit_tracking_info = sqlx::query_as(&query_str)
        .bind(source_id)
        .bind(source_key_json)
        .fetch_optional(db_executor)
        .await?;

    Ok(precommit_tracking_info)
}

pub async fn precommit_source_tracking_info(
    source_id: i32,
    source_key_json: &serde_json::Value,
    max_process_ordinal: i64,
    staging_target_keys: TrackedTargetKeyForSource,
    memoization_info: Option<&MemoizationInfo>,
    db_setup: &TrackingTableSetupState,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    action: WriteAction,
) -> Result<()> {
    let query_str = match action {
        WriteAction::Insert => format!(
            "INSERT INTO {} (source_id, source_key, max_process_ordinal, staging_target_keys, memoization_info) VALUES ($1, $2, $3, $4, $5)",
            db_setup.table_name),
        WriteAction::Update => format!(
            "UPDATE {} SET max_process_ordinal = $3, staging_target_keys = $4, memoization_info = $5 WHERE source_id = $1 AND source_key = $2",
            db_setup.table_name
        ),
    };
    sqlx::query(&query_str)
        .bind(source_id) // $1
        .bind(source_key_json) // $2
        .bind(max_process_ordinal) // $3
        .bind(sqlx::types::Json(staging_target_keys)) // $4
        .bind(memoization_info.map(|m| sqlx::types::Json(m))) // $5
        .execute(db_executor)
        .await?;
    Ok(())
}

#[derive(sqlx::FromRow, Debug)]
pub struct SourceTrackingInfoForCommit {
    pub staging_target_keys: sqlx::types::Json<TrackedTargetKeyForSource>,
    pub process_ordinal: Option<i64>,
}

pub async fn read_source_tracking_info_for_commit(
    source_id: i32,
    source_key_json: &serde_json::Value,
    db_setup: &TrackingTableSetupState,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<Option<SourceTrackingInfoForCommit>> {
    let query_str = format!(
        "SELECT staging_target_keys, process_ordinal FROM {} WHERE source_id = $1 AND source_key = $2",
        db_setup.table_name
    );
    let commit_tracking_info = sqlx::query_as(&query_str)
        .bind(source_id)
        .bind(source_key_json)
        .fetch_optional(db_executor)
        .await?;
    Ok(commit_tracking_info)
}

pub async fn commit_source_tracking_info(
    source_id: i32,
    source_key_json: &serde_json::Value,
    staging_target_keys: TrackedTargetKeyForSource,
    processed_source_ordinal: Option<i64>,
    logic_fingerprint: &[u8],
    process_ordinal: i64,
    process_time_micros: i64,
    target_keys: TrackedTargetKeyForSource,
    db_setup: &TrackingTableSetupState,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    action: WriteAction,
) -> Result<()> {
    let query_str = match action {
        WriteAction::Insert => format!(
            "INSERT INTO {} ( \
               source_id, source_key, \
               max_process_ordinal, staging_target_keys, \
               processed_source_ordinal, process_logic_fingerprint, process_ordinal, process_time_micros, target_keys) \
            VALUES ($1, $2, $6 + 1, $3, $4, $5, $6, $7, $8)",
            db_setup.table_name
        ),
        WriteAction::Update => format!(
            "UPDATE {} SET staging_target_keys = $3, processed_source_ordinal = $4, process_logic_fingerprint = $5, process_ordinal = $6, process_time_micros = $7, target_keys = $8 WHERE source_id = $1 AND source_key = $2",
            db_setup.table_name
        ),
    };
    sqlx::query(&query_str)
        .bind(source_id) // $1
        .bind(source_key_json) // $2
        .bind(sqlx::types::Json(staging_target_keys)) // $3
        .bind(processed_source_ordinal) // $4
        .bind(logic_fingerprint) // $5
        .bind(process_ordinal) // $6
        .bind(process_time_micros) // $7
        .bind(sqlx::types::Json(target_keys)) // $8
        .execute(db_executor)
        .await?;
    Ok(())
}

pub async fn delete_source_tracking_info(
    source_id: i32,
    source_key_json: &serde_json::Value,
    db_setup: &TrackingTableSetupState,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<()> {
    let query_str = format!(
        "DELETE FROM {} WHERE source_id = $1 AND source_key = $2",
        db_setup.table_name
    );
    sqlx::query(&query_str)
        .bind(source_id)
        .bind(source_key_json)
        .execute(db_executor)
        .await?;
    Ok(())
}

#[derive(sqlx::FromRow, Debug)]
pub struct SourceTrackingKey {
    pub source_key: serde_json::Value,
}

pub async fn list_source_tracking_keys(
    source_id: i32,
    db_setup: &TrackingTableSetupState,
    pool: &PgPool,
) -> Result<Vec<SourceTrackingKey>> {
    let query_str = format!(
        "SELECT source_key FROM {} WHERE source_id = $1",
        db_setup.table_name
    );
    let keys: Vec<SourceTrackingKey> = sqlx::query_as(&query_str)
        .bind(source_id)
        .fetch_all(pool)
        .await?;
    Ok(keys)
}
