use crate::prelude::*;

use futures::future::try_join_all;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};

use super::db_tracking::{self, read_source_tracking_info_for_processing, TrackedTargetKey};
use super::db_tracking_setup;
use super::evaluator::{evaluate_source_entry, ScopeValueBuilder};
use super::memoization::{EvaluationMemory, EvaluationMemoryOptions, StoredMemoizationInfo};
use super::stats;

use crate::base::schema;
use crate::base::value::{self, FieldValues, KeyValue};
use crate::builder::plan::*;
use crate::ops::interface::{ExportTargetMutation, ExportTargetUpsertEntry, Ordinal};
use crate::utils::db::WriteAction;
use crate::utils::fingerprint::{Fingerprint, Fingerprinter};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum SourceVersionKind {
    #[default]
    NonExistent,
    DifferentLogic,
    CurrentLogic,
    Deleted,
}

#[derive(Debug, Clone, Default)]
pub struct SourceVersion {
    pub ordinal: Option<Ordinal>,
    pub kind: SourceVersionKind,
}

impl SourceVersion {
    pub fn from_stored(
        stored_ordinal: Option<i64>,
        stored_fp: &Option<Vec<u8>>,
        curr_fp: Fingerprint,
    ) -> Self {
        Self {
            ordinal: stored_ordinal.map(Ordinal),
            kind: match &stored_fp {
                Some(stored_fp) => {
                    if stored_fp.as_slice() == curr_fp.0.as_slice() {
                        SourceVersionKind::CurrentLogic
                    } else {
                        SourceVersionKind::DifferentLogic
                    }
                }
                None => SourceVersionKind::NonExistent,
            },
        }
    }

    pub fn from_current(ordinal: Option<Ordinal>) -> Self {
        Self {
            ordinal,
            kind: SourceVersionKind::CurrentLogic,
        }
    }

    pub fn for_deletion(&self) -> Self {
        Self {
            ordinal: self.ordinal,
            kind: SourceVersionKind::Deleted,
        }
    }

    pub fn should_skip(
        &self,
        target: &SourceVersion,
        update_stats: Option<&stats::UpdateStats>,
    ) -> bool {
        let should_skip = match (self.ordinal, target.ordinal) {
            (Some(orginal), Some(target_ordinal)) => {
                orginal > target_ordinal || (orginal == target_ordinal && self.kind >= target.kind)
            }
            _ => false,
        };
        if should_skip {
            if let Some(update_stats) = update_stats {
                update_stats.num_skipped.inc(1);
            }
        }
        should_skip
    }
}

pub enum SkippedOr<T> {
    Normal(T),
    Skipped(SourceVersion),
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
    source_version: &SourceVersion,
    logic_fp: Fingerprint,
    data: Option<PrecommitData<'_>>,
    process_timestamp: &chrono::DateTime<chrono::Utc>,
    db_setup: &db_tracking_setup::TrackingTableSetupState,
    export_ops: &[AnalyzedExportOp],
    update_stats: &stats::UpdateStats,
    pool: &PgPool,
) -> Result<SkippedOr<PrecommitOutput>> {
    let mut txn = pool.begin().await?;

    let tracking_info = db_tracking::read_source_tracking_info_for_precommit(
        source_id,
        source_key_json,
        db_setup,
        &mut *txn,
    )
    .await?;
    if let Some(tracking_info) = &tracking_info {
        let existing_source_version = SourceVersion::from_stored(
            tracking_info.processed_source_ordinal,
            &tracking_info.process_logic_fingerprint,
            logic_fp,
        );
        if existing_source_version.should_skip(source_version, Some(update_stats)) {
            return Ok(SkippedOr::Skipped(existing_source_version));
        }
    }
    let tracking_info_exists = tracking_info.is_some();
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
                let existing_target_keys = target_info.existing_keys_info.remove(&primary_key_json);
                let existing_staging_target_keys = target_info
                    .existing_staging_keys_info
                    .remove(&primary_key_json);

                let curr_fp = if !export_op.value_stable {
                    Some(
                        Fingerprinter::default()
                            .with(&field_values)?
                            .into_fingerprint(),
                    )
                } else {
                    None
                };
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

    Ok(SkippedOr::Normal(PrecommitOutput {
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
    source_version: &SourceVersion,
    logic_fingerprint: &[u8],
    precommit_metadata: PrecommitMetadata,
    process_timestamp: &chrono::DateTime<chrono::Utc>,
    db_setup: &db_tracking_setup::TrackingTableSetupState,
    pool: &PgPool,
) -> Result<()> {
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
        return Ok(());
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
            source_version.ordinal.map(|o| o.into()),
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

    Ok(())
}

pub async fn evaluate_source_entry_with_memory(
    plan: &ExecutionPlan,
    import_op: &AnalyzedImportOp,
    schema: &schema::DataSchema,
    key: &value::KeyValue,
    options: EvaluationMemoryOptions,
    pool: &PgPool,
) -> Result<Option<ScopeValueBuilder>> {
    let stored_info = if options.enable_cache || !options.evaluation_only {
        let source_key_json = serde_json::to_value(key)?;
        let existing_tracking_info = read_source_tracking_info_for_processing(
            import_op.source_id,
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
    let source_value = match import_op.executor.get_value(key).await? {
        Some(d) => d,
        None => return Ok(None),
    };
    let output = evaluate_source_entry(plan, import_op, schema, key, source_value, &memory).await?;
    Ok(Some(output))
}

pub async fn update_source_row(
    plan: &ExecutionPlan,
    import_op: &AnalyzedImportOp,
    schema: &schema::DataSchema,
    key: &value::KeyValue,
    source_value: Option<FieldValues>,
    source_version: &SourceVersion,
    pool: &PgPool,
    update_stats: &stats::UpdateStats,
) -> Result<SkippedOr<()>> {
    let source_key_json = serde_json::to_value(key)?;
    let process_timestamp = chrono::Utc::now();

    // Phase 1: Evaluate with memoization info.
    let existing_tracking_info = read_source_tracking_info_for_processing(
        import_op.source_id,
        &source_key_json,
        &plan.tracking_table_setup,
        pool,
    )
    .await?;
    let already_exists = existing_tracking_info.is_some();
    let memoization_info = match existing_tracking_info {
        Some(info) => {
            let existing_version = SourceVersion::from_stored(
                info.processed_source_ordinal,
                &info.process_logic_fingerprint,
                plan.logic_fingerprint,
            );
            if existing_version.should_skip(source_version, Some(update_stats)) {
                return Ok(SkippedOr::Skipped(existing_version));
            }
            info.memoization_info.map(|info| info.0).flatten()
        }
        None => Default::default(),
    };
    let (output, stored_mem_info) = match source_value {
        Some(source_value) => {
            let evaluation_memory = EvaluationMemory::new(
                process_timestamp,
                memoization_info,
                EvaluationMemoryOptions {
                    enable_cache: true,
                    evaluation_only: false,
                },
            );
            let output = evaluate_source_entry(
                plan,
                import_op,
                schema,
                key,
                source_value,
                &evaluation_memory,
            )
            .await?;
            (Some(output), evaluation_memory.into_stored()?)
        }
        None => Default::default(),
    };

    // Phase 2 (precommit): Update with the memoization info and stage target keys.
    let precommit_output = precommit_source_tracking_info(
        import_op.source_id,
        &source_key_json,
        source_version,
        plan.logic_fingerprint,
        output.as_ref().map(|scope_value| PrecommitData {
            scope_value,
            memoization_info: &stored_mem_info,
        }),
        &process_timestamp,
        &plan.tracking_table_setup,
        &plan.export_ops,
        update_stats,
        pool,
    )
    .await?;
    let precommit_output = match precommit_output {
        SkippedOr::Normal(output) => output,
        SkippedOr::Skipped(source_version) => return Ok(SkippedOr::Skipped(source_version)),
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
        import_op.source_id,
        &source_key_json,
        source_version,
        &plan.logic_fingerprint.0,
        precommit_output.metadata,
        &process_timestamp,
        &plan.tracking_table_setup,
        pool,
    )
    .await?;

    if already_exists {
        if output.is_some() {
            update_stats.num_repreocesses.inc(1);
        } else {
            update_stats.num_deletions.inc(1);
        }
    } else if output.is_some() {
        update_stats.num_insertions.inc(1);
    }

    Ok(SkippedOr::Normal(()))
}
