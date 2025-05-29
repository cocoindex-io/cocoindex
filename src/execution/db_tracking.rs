use crate::prelude::*;

use super::{db_tracking_setup::TrackingTableSetupState, memoization::StoredMemoizationInfo};
use crate::utils::{db::WriteAction, fingerprint::Fingerprint};
use futures::Stream;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use sqlx::PgPool;
use std::fmt;

#[derive(Debug, Clone)]
pub struct TrackedTargetKeyInfo {
    pub key: serde_json::Value,
    pub additional_key: serde_json::Value,
    pub process_ordinal: i64,
    // None means deletion.
    pub fingerprint: Option<Fingerprint>,
}

impl Serialize for TrackedTargetKeyInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.key)?;
        seq.serialize_element(&self.process_ordinal)?;
        seq.serialize_element(&self.fingerprint)?;
        if !self.additional_key.is_null() {
            seq.serialize_element(&self.additional_key)?;
        }
        seq.end()
    }
}

impl<'de> serde::Deserialize<'de> for TrackedTargetKeyInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TrackedTargetKeyVisitor;

        impl<'de> Visitor<'de> for TrackedTargetKeyVisitor {
            type Value = TrackedTargetKeyInfo;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a sequence of 3 or 4 elements for TrackedTargetKey")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<TrackedTargetKeyInfo, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let target_key: serde_json::Value = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let process_ordinal: i64 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let fingerprint: Option<Fingerprint> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let additional_key: Option<serde_json::Value> = seq.next_element()?;

                Ok(TrackedTargetKeyInfo {
                    key: target_key,
                    process_ordinal,
                    fingerprint,
                    additional_key: additional_key.unwrap_or(serde_json::Value::Null),
                })
            }
        }

        deserializer.deserialize_seq(TrackedTargetKeyVisitor)
    }
}

/// (source_id, target_key)
pub type TrackedTargetKeyForSource = Vec<(i32, Vec<TrackedTargetKeyInfo>)>;

#[derive(sqlx::FromRow, Debug)]
pub struct SourceTrackingInfoForProcessing {
    pub memoization_info: Option<sqlx::types::Json<Option<StoredMemoizationInfo>>>,

    pub processed_source_ordinal: Option<i64>,
    pub process_logic_fingerprint: Option<Vec<u8>>,
}

pub async fn read_source_tracking_info_for_processing(
    source_id: i32,
    source_key_json: &serde_json::Value,
    db_setup: &TrackingTableSetupState,
    pool: &PgPool,
) -> Result<Option<SourceTrackingInfoForProcessing>> {
    let query_str = format!(
        "SELECT memoization_info, processed_source_ordinal, process_logic_fingerprint FROM {} WHERE source_id = $1 AND source_key = $2",
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
    pub process_logic_fingerprint: Option<Vec<u8>>,
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
        "SELECT max_process_ordinal, staging_target_keys, processed_source_ordinal, process_logic_fingerprint, process_ordinal, target_keys FROM {} WHERE source_id = $1 AND source_key = $2",
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
    memoization_info: Option<&StoredMemoizationInfo>,
    db_setup: &TrackingTableSetupState,
    db_executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    action: WriteAction,
) -> Result<()> {
    let query_str = match action {
        WriteAction::Insert => format!(
            "INSERT INTO {} (source_id, source_key, max_process_ordinal, staging_target_keys, memoization_info) VALUES ($1, $2, $3, $4, $5)",
            db_setup.table_name
        ),
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
        .bind(memoization_info.map(sqlx::types::Json)) // $5
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
pub struct TrackedSourceKeyMetadata {
    pub source_key: serde_json::Value,
    pub processed_source_ordinal: Option<i64>,
    pub process_logic_fingerprint: Option<Vec<u8>>,
}

pub struct ListTrackedSourceKeyMetadataState {
    query_str: String,
}

impl ListTrackedSourceKeyMetadataState {
    pub fn new() -> Self {
        Self {
            query_str: String::new(),
        }
    }

    pub fn list<'a>(
        &'a mut self,
        source_id: i32,
        db_setup: &'a TrackingTableSetupState,
        pool: &'a PgPool,
    ) -> impl Stream<Item = Result<TrackedSourceKeyMetadata, sqlx::Error>> + 'a {
        self.query_str = format!(
            "SELECT source_key, processed_source_ordinal, process_logic_fingerprint FROM {} WHERE source_id = $1",
            db_setup.table_name
        );
        sqlx::query_as(&self.query_str).bind(source_id).fetch(pool)
    }
}

#[derive(sqlx::FromRow, Debug)]
pub struct SourceLastProcessedInfo {
    pub processed_source_ordinal: Option<i64>,
    pub process_logic_fingerprint: Option<Vec<u8>>,
    pub process_time_micros: Option<i64>,
}

pub async fn read_source_last_processed_info(
    source_id: i32,
    source_key_json: &serde_json::Value,
    db_setup: &TrackingTableSetupState,
    pool: &PgPool,
) -> Result<Option<SourceLastProcessedInfo>> {
    let query_str = format!(
        "SELECT processed_source_ordinal, process_logic_fingerprint, process_time_micros FROM {} WHERE source_id = $1 AND source_key = $2",
        db_setup.table_name
    );
    let last_processed_info = sqlx::query_as(&query_str)
        .bind(source_id)
        .bind(source_key_json)
        .fetch_optional(pool)
        .await?;
    Ok(last_processed_info)
}
