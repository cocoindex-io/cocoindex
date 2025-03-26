use anyhow::Result;
use futures::future::{join, join_all, try_join, try_join_all};
use itertools::Itertools;
use log::error;
use serde::Serialize;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use super::db_tracking::{self, read_source_tracking_info, TrackedTargetKey};
use super::db_tracking_setup;
use super::memoization::{EvaluationMemory, EvaluationMemoryOptions, StoredMemoizationInfo};
use crate::base::schema;
use crate::base::value::{self, FieldValues, KeyValue};
use crate::builder::plan::*;
use crate::ops::interface::{ExportTargetMutation, ExportTargetUpsertEntry};
use crate::utils::db::WriteAction;
use crate::utils::fingerprint::{Fingerprint, Fingerprinter};

use super::evaluator::{evaluate_source_entry, ScopeValueBuilder};

#[derive(Debug, Serialize, Default)]
pub struct UpdateStats {
    pub num_insertions: AtomicUsize,
    pub num_deletions: AtomicUsize,
    pub num_already_exists: AtomicUsize,
    pub num_errors: AtomicUsize,
}

impl std::fmt::Display for UpdateStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_source_rows = self.num_insertions.load(Relaxed)
            + self.num_deletions.load(Relaxed)
            + self.num_already_exists.load(Relaxed);
        write!(f, "{num_source_rows} source rows processed",)?;
        if self.num_errors.load(Relaxed) > 0 {
            write!(f, " with {} ERRORS", self.num_errors.load(Relaxed))?;
        }
        write!(
            f,
            ": {} added, {} removed, {} already exists",
            self.num_insertions.load(Relaxed),
            self.num_deletions.load(Relaxed),
            self.num_already_exists.load(Relaxed)
        )?;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct SourceUpdateInfo {
    pub source_name: String,
    pub stats: UpdateStats,
}

impl std::fmt::Display for SourceUpdateInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.source_name, self.stats)
    }
}

#[derive(Debug, Serialize)]
pub struct IndexUpdateInfo {
    pub sources: Vec<SourceUpdateInfo>,
}

impl std::fmt::Display for IndexUpdateInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for source in self.sources.iter() {
            writeln!(f, "{}", source)?;
        }
        Ok(())
    }
}

pub fn extract_primary_key(
    primary_key_def: &AnalyzedPrimaryKeyDef,
    record: &FieldValues,
) -> Result<KeyValue> {
    let key = match primary_key_def {
        AnalyzedPrimaryKeyDef::Fields(fields) => {
            if fields.len() == 1 {
                record.fields[fields[0] as usize].as_key()?
            } else {
                let mut key_values = Vec::with_capacity(fields.len());
                for field in fields.iter() {
                    key_values.push(record.fields[*field as usize].as_key()?);
                }
                KeyValue::Struct(key_values)
            }
        }
    };
    Ok(key)
}

enum WithApplyStatus<T = ()> {
    Normal(T),
    Collapsed,
}

#[derive(Default)]
struct TrackingInfoForTarget<'a> {
    export_op: Option<&'a AnalyzedExportOp>,

    // Existing keys info. Keyed by target key.
    // Will be removed after new rows for the same key are added into `new_staging_keys_info` and `mutation.upserts`,
    // hence all remaining ones are to be deleted.
    existing_staging_keys_info: HashMap<serde_json::Value, Vec<(i64, Option<Fingerprint>)>>,
    existing_keys_info: HashMap<serde_json::Value, Vec<(i64, Option<Fingerprint>)>>,

    // New keys info for staging.
    new_staging_keys_info: Vec<TrackedTargetKey>,

    // Mutation to apply to the target storage.
    mutation: ExportTargetMutation,
}

#[derive(Debug)]
struct PrecommitData<'a> {
    scope_value: &'a ScopeValueBuilder,
    memoization_info: &'a StoredMemoizationInfo,
}
struct PrecommitMetadata {
    source_entry_exists: bool,
    process_ordinal: i64,
    existing_process_ordinal: Option<i64>,
    new_target_keys: db_tracking::TrackedTargetKeyForSource,
}
struct PrecommitOutput {
    metadata: PrecommitMetadata,
    target_mutations: HashMap<i32, ExportTargetMutation>,
}

async fn precommit_source_tracking_info(
    source_id: i32,
    source_key_json: &serde_json::Value,
    source_ordinal: Option<i64>,
    data: Option<PrecommitData<'_>>,
    process_timestamp: &chrono::DateTime<chrono::Utc>,
    db_setup: &db_tracking_setup::TrackingTableSetupState,
    export_ops: &[AnalyzedExportOp],
    pool: &PgPool,
) -> Result<WithApplyStatus<PrecommitOutput>> {
    let mut txn = pool.begin().await?;

    let tracking_info = db_tracking::read_source_tracking_info_for_precommit(
        source_id,
        source_key_json,
        db_setup,
        &mut *txn,
    )
    .await?;
    let tracking_info_exists = tracking_info.is_some();
    if source_ordinal.is_some()
        && tracking_info
            .as_ref()
            .and_then(|info| info.processed_source_ordinal)
            > source_ordinal
    {
        return Ok(WithApplyStatus::Collapsed);
    }
    let process_ordinal = (tracking_info
        .as_ref()
        .map(|info| info.max_process_ordinal)
        .unwrap_or(0)
        + 1)
    .max(process_timestamp.timestamp_millis());
    let existing_process_ordinal = tracking_info.as_ref().and_then(|info| info.process_ordinal);

    let mut tracking_info_for_targets = HashMap::<i32, TrackingInfoForTarget>::new();
    for export_op in export_ops.iter() {
        tracking_info_for_targets
            .entry(export_op.target_id)
            .or_default()
            .export_op = Some(export_op);
    }

    // Collect `tracking_info_for_targets` from existing tracking info.
    if let Some(info) = tracking_info {
        let sqlx::types::Json(staging_target_keys) = info.staging_target_keys;
        for (target_id, keys_info) in staging_target_keys.into_iter() {
            let target_info = tracking_info_for_targets.entry(target_id).or_default();
            for key_info in keys_info.into_iter() {
                target_info
                    .existing_staging_keys_info
                    .entry(key_info.0)
                    .or_default()
                    .push((key_info.1, key_info.2));
            }
        }

        if let Some(sqlx::types::Json(target_keys)) = info.target_keys {
            for (target_id, keys_info) in target_keys.into_iter() {
                let target_info = tracking_info_for_targets.entry(target_id).or_default();
                for key_info in keys_info.into_iter() {
                    target_info
                        .existing_keys_info
                        .entry(key_info.0)
                        .or_default()
                        .push((key_info.1, key_info.2));
                }
            }
        }
    }

    let mut new_target_keys_info = db_tracking::TrackedTargetKeyForSource::default();
    if let Some(data) = &data {
        for export_op in export_ops.iter() {
            let collected_values = data.scope_value.collected_values
                [export_op.input.collector_idx as usize]
                .lock()
                .unwrap();
            let target_info = tracking_info_for_targets
                .entry(export_op.target_id)
                .or_default();
            let mut keys_info = Vec::new();
            for value in collected_values.iter() {
                let primary_key = extract_primary_key(&export_op.primary_key_def, value)?;
                let primary_key_json = serde_json::to_value(&primary_key)?;

                let mut field_values = FieldValues {
                    fields: Vec::with_capacity(export_op.value_fields.len()),
                };
                for field in export_op.value_fields.iter() {
                    field_values
                        .fields
                        .push(value.fields[*field as usize].clone());
                }
                let curr_fp = Some(
                    Fingerprinter::default()
                        .with(&field_values)?
                        .into_fingerprint(),
                );

                let existing_target_keys = target_info.existing_keys_info.remove(&primary_key_json);
                let existing_staging_target_keys = target_info
                    .existing_staging_keys_info
                    .remove(&primary_key_json);

                if existing_target_keys
                    .as_ref()
                    .map(|keys| !keys.is_empty() && keys.iter().all(|(_, fp)| fp == &curr_fp))
                    .unwrap_or(false)
                    && existing_staging_target_keys
                        .map(|keys| keys.iter().all(|(_, fp)| fp == &curr_fp))
                        .unwrap_or(true)
                {
                    // Already exists, with exactly the same value fingerprint.
                    // Nothing need to be changed, except carrying over the existing target keys info.
                    let (existing_ordinal, existing_fp) =
                        existing_target_keys.unwrap().into_iter().next().unwrap();
                    keys_info.push((primary_key_json, existing_ordinal, existing_fp));
                } else {
                    // Entry with new value. Needs to be upserted.
                    target_info.mutation.upserts.push(ExportTargetUpsertEntry {
                        key: primary_key,
                        value: field_values,
                    });
                    target_info.new_staging_keys_info.push((
                        primary_key_json.clone(),
                        process_ordinal,
                        curr_fp,
                    ));
                    keys_info.push((primary_key_json, process_ordinal, curr_fp));
                }
            }
            new_target_keys_info.push((export_op.target_id, keys_info));
        }
    }

    let mut new_staging_target_keys = db_tracking::TrackedTargetKeyForSource::default();
    let mut target_mutations = HashMap::with_capacity(export_ops.len());
    for (target_id, target_tracking_info) in tracking_info_for_targets.into_iter() {
        let legacy_keys: HashSet<serde_json::Value> = target_tracking_info
            .existing_keys_info
            .into_keys()
            .chain(target_tracking_info.existing_staging_keys_info.into_keys())
            .collect();

        let mut new_staging_keys_info = target_tracking_info.new_staging_keys_info;
        // Add tracking info for deletions.
        new_staging_keys_info.extend(
            legacy_keys
                .iter()
                .map(|key| ((*key).clone(), process_ordinal, None)),
        );
        new_staging_target_keys.push((target_id, new_staging_keys_info));

        if let Some(export_op) = target_tracking_info.export_op {
            let mut mutation = target_tracking_info.mutation;
            mutation.delete_keys.reserve(legacy_keys.len());
            for legacy_key in legacy_keys.into_iter() {
                mutation.delete_keys.push(
                    value::Value::<value::ScopeValue>::from_json(
                        legacy_key,
                        &export_op.primary_key_type,
                    )?
                    .as_key()?,
                );
            }
            target_mutations.insert(target_id, mutation);
        }
    }

    db_tracking::precommit_source_tracking_info(
        source_id,
        source_key_json,
        process_ordinal,
        new_staging_target_keys,
        data.as_ref().map(|data| data.memoization_info),
        db_setup,
        &mut *txn,
        if tracking_info_exists {
            WriteAction::Update
        } else {
            WriteAction::Insert
        },
    )
    .await?;

    txn.commit().await?;

    Ok(WithApplyStatus::Normal(PrecommitOutput {
        metadata: PrecommitMetadata {
            source_entry_exists: data.is_some(),
            process_ordinal,
            existing_process_ordinal,
            new_target_keys: new_target_keys_info,
        },
        target_mutations,
    }))
}

async fn commit_source_tracking_info(
    source_id: i32,
    source_key_json: &serde_json::Value,
    source_ordinal: Option<i64>,
    logic_fingerprint: &[u8],
    precommit_metadata: PrecommitMetadata,
    process_timestamp: &chrono::DateTime<chrono::Utc>,
    db_setup: &db_tracking_setup::TrackingTableSetupState,
    pool: &PgPool,
) -> Result<WithApplyStatus<()>> {
    let mut txn = pool.begin().await?;

    let tracking_info = db_tracking::read_source_tracking_info_for_commit(
        source_id,
        source_key_json,
        db_setup,
        &mut *txn,
    )
    .await?;
    let tracking_info_exists = tracking_info.is_some();
    if tracking_info.as_ref().and_then(|info| info.process_ordinal)
        >= Some(precommit_metadata.process_ordinal)
    {
        return Ok(WithApplyStatus::Collapsed);
    }

    let cleaned_staging_target_keys = tracking_info
        .map(|info| {
            let sqlx::types::Json(staging_target_keys) = info.staging_target_keys;
            staging_target_keys
                .into_iter()
                .filter_map(|(target_id, target_keys)| {
                    let cleaned_target_keys: Vec<_> = target_keys
                        .into_iter()
                        .filter(|(_, ordinal, _)| {
                            Some(*ordinal) > precommit_metadata.existing_process_ordinal
                                && *ordinal != precommit_metadata.process_ordinal
                        })
                        .collect();
                    if !cleaned_target_keys.is_empty() {
                        Some((target_id, cleaned_target_keys))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !precommit_metadata.source_entry_exists && cleaned_staging_target_keys.is_empty() {
        // TODO: When we support distributed execution in the future, we'll need to leave a tombstone for a while
        // to prevent an earlier update causing the record reappear because of out-of-order processing.
        if tracking_info_exists {
            db_tracking::delete_source_tracking_info(
                source_id,
                source_key_json,
                db_setup,
                &mut *txn,
            )
            .await?;
        }
    } else {
        db_tracking::commit_source_tracking_info(
            source_id,
            source_key_json,
            cleaned_staging_target_keys,
            source_ordinal,
            logic_fingerprint,
            precommit_metadata.process_ordinal,
            process_timestamp.timestamp_micros(),
            precommit_metadata.new_target_keys,
            db_setup,
            &mut *txn,
            if tracking_info_exists {
                WriteAction::Update
            } else {
                WriteAction::Insert
            },
        )
        .await?;
    }

    txn.commit().await?;

    Ok(WithApplyStatus::Normal(()))
}

pub async fn evaluate_source_entry_with_memory(
    plan: &ExecutionPlan,
    source_op: &AnalyzedSourceOp,
    schema: &schema::DataSchema,
    key: &value::KeyValue,
    options: EvaluationMemoryOptions,
    pool: &PgPool,
) -> Result<Option<ScopeValueBuilder>> {
    let stored_info = if options.enable_cache || !options.evaluation_only {
        let source_key_json = serde_json::to_value(key)?;
        let existing_tracking_info = read_source_tracking_info(
            source_op.source_id,
            &source_key_json,
            &plan.tracking_table_setup,
            pool,
        )
        .await?;
        existing_tracking_info
            .and_then(|info| info.memoization_info.map(|info| info.0))
            .flatten()
    } else {
        None
    };
    let memory = EvaluationMemory::new(chrono::Utc::now(), stored_info, options);
    let data_builder = evaluate_source_entry(plan, source_op, schema, key, &memory).await?;
    Ok(data_builder)
}

pub async fn update_source_entry(
    plan: &ExecutionPlan,
    source_op: &AnalyzedSourceOp,
    schema: &schema::DataSchema,
    key: &value::KeyValue,
    only_for_deletion: bool,
    pool: &PgPool,
    stats: &UpdateStats,
) -> Result<()> {
    let source_key_json = serde_json::to_value(key)?;
    let process_timestamp = chrono::Utc::now();

    // Phase 1: Evaluate with memoization info.

    // TODO: Skip if the source is not newer and the processing logic is not changed.
    let existing_tracking_info = read_source_tracking_info(
        source_op.source_id,
        &source_key_json,
        &plan.tracking_table_setup,
        pool,
    )
    .await?;
    let already_exists = existing_tracking_info.is_some();
    let memoization_info = existing_tracking_info
        .and_then(|info| info.memoization_info.map(|info| info.0))
        .flatten();
    let evaluation_memory = EvaluationMemory::new(
        process_timestamp,
        memoization_info,
        EvaluationMemoryOptions {
            enable_cache: true,
            evaluation_only: false,
        },
    );
    let value_builder = if !only_for_deletion {
        evaluate_source_entry(plan, source_op, schema, key, &evaluation_memory).await?
    } else {
        None
    };
    let exists = value_builder.is_some();

    if already_exists {
        if exists {
            stats.num_already_exists.fetch_add(1, Relaxed);
        } else {
            stats.num_deletions.fetch_add(1, Relaxed);
        }
    } else if exists {
        stats.num_insertions.fetch_add(1, Relaxed);
    } else {
        return Ok(());
    }

    let memoization_info = evaluation_memory.into_stored()?;
    let (source_ordinal, precommit_data) = match &value_builder {
        Some(scope_value) => {
            (
                // TODO: Generate the actual source ordinal.
                Some(1),
                Some(PrecommitData {
                    scope_value,
                    memoization_info: &memoization_info,
                }),
            )
        }
        None => (None, None),
    };

    // Phase 2 (precommit): Update with the memoization info and stage target keys.
    let precommit_output = precommit_source_tracking_info(
        source_op.source_id,
        &source_key_json,
        source_ordinal,
        precommit_data,
        &process_timestamp,
        &plan.tracking_table_setup,
        &plan.export_ops,
        pool,
    )
    .await?;
    let precommit_output = match precommit_output {
        WithApplyStatus::Normal(output) => output,
        WithApplyStatus::Collapsed => return Ok(()),
    };

    // Phase 3: Apply changes to the target storage, including upserting new target records and removing existing ones.
    let mut target_mutations = precommit_output.target_mutations;
    let apply_futs = plan.export_ops.iter().filter_map(|export_op| {
        target_mutations
            .remove(&export_op.target_id)
            .and_then(|mutation| {
                if !mutation.is_empty() {
                    Some(export_op.executor.apply_mutation(mutation))
                } else {
                    None
                }
            })
    });

    // TODO: Handle errors.
    try_join_all(apply_futs).await?;

    // Phase 4: Update the tracking record.
    commit_source_tracking_info(
        source_op.source_id,
        &source_key_json,
        source_ordinal,
        &plan.logic_fingerprint,
        precommit_output.metadata,
        &process_timestamp,
        &plan.tracking_table_setup,
        pool,
    )
    .await?;

    Ok(())
}

async fn update_source_entry_with_err_handling(
    plan: &ExecutionPlan,
    source_op: &AnalyzedSourceOp,
    schema: &schema::DataSchema,
    key: &value::KeyValue,
    only_for_deletion: bool,
    pool: &PgPool,
    stats: &UpdateStats,
) {
    let r = update_source_entry(plan, source_op, schema, key, only_for_deletion, pool, stats).await;
    if let Err(e) = r {
        stats.num_errors.fetch_add(1, Relaxed);
        error!("{:?}", e.context("Error in indexing a source row"));
    }
}

async fn update_source(
    source_name: &str,
    plan: &ExecutionPlan,
    source_op: &AnalyzedSourceOp,
    schema: &schema::DataSchema,
    pool: &PgPool,
) -> Result<SourceUpdateInfo> {
    let (keys, existing_keys_json) = try_join(
        source_op.executor.list_keys(),
        db_tracking::list_source_tracking_keys(
            source_op.source_id,
            &plan.tracking_table_setup,
            pool,
        ),
    )
    .await?;

    let stats = UpdateStats::default();
    let upsert_futs = join_all(keys.iter().map(|key| {
        update_source_entry_with_err_handling(plan, source_op, schema, key, false, pool, &stats)
    }));
    let deleted_keys = existing_keys_json
        .into_iter()
        .map(|existing_key_json| {
            value::Value::<value::ScopeValue>::from_json(
                existing_key_json.source_key,
                &source_op.primary_key_type,
            )?
            .as_key()
        })
        .filter_ok(|existing_key| !keys.contains(existing_key))
        .collect::<Result<Vec<_>>>()?;
    let delete_futs = join_all(deleted_keys.iter().map(|key| {
        update_source_entry_with_err_handling(plan, source_op, schema, key, true, pool, &stats)
    }));
    join(upsert_futs, delete_futs).await;

    Ok(SourceUpdateInfo {
        source_name: source_name.to_string(),
        stats,
    })
}

pub async fn update(
    plan: &ExecutionPlan,
    schema: &schema::DataSchema,
    pool: &PgPool,
) -> Result<IndexUpdateInfo> {
    let source_update_stats = try_join_all(
        plan.source_ops
            .iter()
            .map(|source_op| async move {
                update_source(source_op.name.as_str(), plan, source_op, schema, pool).await
            })
            .collect::<Vec<_>>(),
    )
    .await?;
    Ok(IndexUpdateInfo {
        sources: source_update_stats,
    })
}
