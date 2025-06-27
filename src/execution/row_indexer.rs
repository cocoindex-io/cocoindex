use crate::prelude::*;

use futures::future::try_join_all;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};

use super::db_tracking::{self, TrackedTargetKeyInfo, read_source_tracking_info_for_processing};
use super::db_tracking_setup;
use super::evaluator::{
    EvaluateSourceEntryOutput, SourceRowEvaluationContext, evaluate_source_entry,
};
use super::memoization::{EvaluationMemory, EvaluationMemoryOptions, StoredMemoizationInfo};
use super::stats;

use crate::base::value::{self, FieldValues, KeyValue};
use crate::builder::plan::*;
use crate::ops::interface::{
    ExportTargetMutation, ExportTargetUpsertEntry, Ordinal, SourceExecutorGetOptions,
};
use crate::utils::db::WriteAction;
use crate::utils::fingerprint::{Fingerprint, Fingerprinter};

pub fn extract_primary_key(
    primary_key_def: &AnalyzedPrimaryKeyDef,
    record: &FieldValues,
) -> Result<KeyValue> {
    match primary_key_def {
        AnalyzedPrimaryKeyDef::Fields(fields) => {
            KeyValue::from_values(fields.iter().map(|field| &record.fields[*field as usize]))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum SourceVersionKind {
    #[default]
    UnknownLogic,
    DifferentLogic,
    CurrentLogic,
    NonExistence,
}

#[derive(Debug, Clone, Default)]
pub struct SourceVersion {
    pub ordinal: Ordinal,
    pub kind: SourceVersionKind,
}

impl SourceVersion {
    pub fn from_stored(
        stored_ordinal: Option<i64>,
        stored_fp: &Option<Vec<u8>>,
        curr_fp: Fingerprint,
    ) -> Self {
        Self {
            ordinal: Ordinal(stored_ordinal),
            kind: match &stored_fp {
                Some(stored_fp) => {
                    if stored_fp.as_slice() == curr_fp.0.as_slice() {
                        SourceVersionKind::CurrentLogic
                    } else {
                        SourceVersionKind::DifferentLogic
                    }
                }
                None => SourceVersionKind::UnknownLogic,
            },
        }
    }

    pub fn from_stored_processing_info(
        info: &db_tracking::SourceTrackingInfoForProcessing,
        curr_fp: Fingerprint,
    ) -> Self {
        Self::from_stored(
            info.processed_source_ordinal,
            &info.process_logic_fingerprint,
            curr_fp,
        )
    }

    pub fn from_stored_precommit_info(
        info: &db_tracking::SourceTrackingInfoForPrecommit,
        curr_fp: Fingerprint,
    ) -> Self {
        Self::from_stored(
            info.processed_source_ordinal,
            &info.process_logic_fingerprint,
            curr_fp,
        )
    }

    pub fn from_current_with_ordinal(ordinal: Ordinal) -> Self {
        Self {
            ordinal,
            kind: SourceVersionKind::CurrentLogic,
        }
    }

    pub fn from_current_data(data: &interface::SourceData) -> Self {
        let kind = match &data.value {
            interface::SourceValue::Existence(_) => SourceVersionKind::CurrentLogic,
            interface::SourceValue::NonExistence => SourceVersionKind::NonExistence,
        };
        Self {
            ordinal: data.ordinal,
            kind,
        }
    }

    pub fn should_skip(
        &self,
        target: &SourceVersion,
        update_stats: Option<&stats::UpdateStats>,
    ) -> bool {
        // Ordinal indicates monotonic invariance - always respect ordinal order
        // Never process older ordinals to maintain consistency
        let should_skip = match (self.ordinal.0, target.ordinal.0) {
            (Some(existing_ordinal), Some(target_ordinal)) => {
                // Skip if target ordinal is older, or same ordinal with same/older logic version
                existing_ordinal > target_ordinal
                    || (existing_ordinal == target_ordinal && self.kind >= target.kind)
            }
            _ => false,
        };
        if should_skip {
            if let Some(update_stats) = update_stats {
                update_stats.num_no_change.inc(1);
            }
        }
        should_skip
    }
}

pub enum SkippedOr<T> {
    Normal(T),
    Skipped(SourceVersion),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TargetKeyPair {
    pub key: serde_json::Value,
    pub additional_key: serde_json::Value,
}

#[derive(Default)]
struct TrackingInfoForTarget<'a> {
    export_op: Option<&'a AnalyzedExportOp>,

    // Existing keys info. Keyed by target key.
    // Will be removed after new rows for the same key are added into `new_staging_keys_info` and `mutation.upserts`,
    // hence all remaining ones are to be deleted.
    existing_staging_keys_info: HashMap<TargetKeyPair, Vec<(i64, Option<Fingerprint>)>>,
    existing_keys_info: HashMap<TargetKeyPair, Vec<(i64, Option<Fingerprint>)>>,

    // New keys info for staging.
    new_staging_keys_info: Vec<TrackedTargetKeyInfo>,

    // Mutation to apply to the target storage.
    mutation: ExportTargetMutation,
}

#[derive(Debug)]
struct PrecommitData<'a> {
    evaluate_output: &'a EvaluateSourceEntryOutput,
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
    export_ops_exec_ctx: &[exec_ctx::ExportOpExecutionContext],
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
        let existing_source_version =
            SourceVersion::from_stored_precommit_info(tracking_info, logic_fp);
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
    for (export_op, export_op_exec_ctx) in
        std::iter::zip(export_ops.iter(), export_ops_exec_ctx.iter())
    {
        tracking_info_for_targets
            .entry(export_op_exec_ctx.target_id)
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
                    .entry(TargetKeyPair {
                        key: key_info.key,
                        additional_key: key_info.additional_key,
                    })
                    .or_default()
                    .push((key_info.process_ordinal, key_info.fingerprint));
            }
        }

        if let Some(sqlx::types::Json(target_keys)) = info.target_keys {
            for (target_id, keys_info) in target_keys.into_iter() {
                let target_info = tracking_info_for_targets.entry(target_id).or_default();
                for key_info in keys_info.into_iter() {
                    target_info
                        .existing_keys_info
                        .entry(TargetKeyPair {
                            key: key_info.key,
                            additional_key: key_info.additional_key,
                        })
                        .or_default()
                        .push((key_info.process_ordinal, key_info.fingerprint));
                }
            }
        }
    }

    let mut new_target_keys_info = db_tracking::TrackedTargetKeyForSource::default();
    if let Some(data) = &data {
        for (export_op, export_op_exec_ctx) in
            std::iter::zip(export_ops.iter(), export_ops_exec_ctx.iter())
        {
            let target_info = tracking_info_for_targets
                .entry(export_op_exec_ctx.target_id)
                .or_default();
            let mut keys_info = Vec::new();
            let collected_values =
                &data.evaluate_output.collected_values[export_op.input.collector_idx as usize];
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
                let additional_key = export_op.export_target_factory.extract_additional_key(
                    &primary_key,
                    &field_values,
                    export_op.export_context.as_ref(),
                )?;
                let target_key_pair = TargetKeyPair {
                    key: primary_key_json,
                    additional_key,
                };
                let existing_target_keys = target_info.existing_keys_info.remove(&target_key_pair);
                let existing_staging_target_keys = target_info
                    .existing_staging_keys_info
                    .remove(&target_key_pair);

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
                    let (existing_ordinal, existing_fp) = existing_target_keys
                        .ok_or_else(invariance_violation)?
                        .into_iter()
                        .next()
                        .ok_or_else(invariance_violation)?;
                    keys_info.push(TrackedTargetKeyInfo {
                        key: target_key_pair.key,
                        additional_key: target_key_pair.additional_key,
                        process_ordinal: existing_ordinal,
                        fingerprint: existing_fp,
                    });
                } else {
                    // Entry with new value. Needs to be upserted.
                    let tracked_target_key = TrackedTargetKeyInfo {
                        key: target_key_pair.key.clone(),
                        additional_key: target_key_pair.additional_key.clone(),
                        process_ordinal,
                        fingerprint: curr_fp,
                    };
                    target_info.mutation.upserts.push(ExportTargetUpsertEntry {
                        key: primary_key,
                        additional_key: target_key_pair.additional_key,
                        value: field_values,
                    });
                    target_info
                        .new_staging_keys_info
                        .push(tracked_target_key.clone());
                    keys_info.push(tracked_target_key);
                }
            }
            new_target_keys_info.push((export_op_exec_ctx.target_id, keys_info));
        }
    }

    let mut new_staging_target_keys = db_tracking::TrackedTargetKeyForSource::default();
    let mut target_mutations = HashMap::with_capacity(export_ops.len());
    for (target_id, target_tracking_info) in tracking_info_for_targets.into_iter() {
        let legacy_keys: HashSet<TargetKeyPair> = target_tracking_info
            .existing_keys_info
            .into_keys()
            .chain(target_tracking_info.existing_staging_keys_info.into_keys())
            .collect();

        let mut new_staging_keys_info = target_tracking_info.new_staging_keys_info;
        // Add tracking info for deletions.
        new_staging_keys_info.extend(legacy_keys.iter().map(|key| TrackedTargetKeyInfo {
            key: key.key.clone(),
            additional_key: key.additional_key.clone(),
            process_ordinal,
            fingerprint: None,
        }));
        new_staging_target_keys.push((target_id, new_staging_keys_info));

        if let Some(export_op) = target_tracking_info.export_op {
            let mut mutation = target_tracking_info.mutation;
            mutation.deletes.reserve(legacy_keys.len());
            for legacy_key in legacy_keys.into_iter() {
                let key = value::Value::<value::ScopeValue>::from_json(
                    legacy_key.key,
                    &export_op.primary_key_type,
                )?
                .as_key()?;
                mutation.deletes.push(interface::ExportTargetDeleteEntry {
                    key,
                    additional_key: legacy_key.additional_key,
                });
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
                        .filter(|key_info| {
                            Some(key_info.process_ordinal)
                                > precommit_metadata.existing_process_ordinal
                                && key_info.process_ordinal != precommit_metadata.process_ordinal
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
            source_version.ordinal.into(),
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

async fn try_content_hash_optimization(
    source_id: i32,
    src_eval_ctx: &SourceRowEvaluationContext<'_>,
    source_key_json: &serde_json::Value,
    source_version: &SourceVersion,
    current_hash: &crate::utils::fingerprint::Fingerprint,
    tracking_info: &db_tracking::SourceTrackingInfoForProcessing,
    existing_version: &Option<SourceVersion>,
    db_setup: &db_tracking_setup::TrackingTableSetupState,
    update_stats: &stats::UpdateStats,
    pool: &PgPool,
) -> Result<Option<SkippedOr<()>>> {
    // Check if we can use content hash optimization
    if !existing_version
        .as_ref()
        .map_or(false, |v| v.kind == SourceVersionKind::CurrentLogic)
    {
        return Ok(None);
    }

    if !tracking_info
        .max_process_ordinal
        .zip(tracking_info.process_ordinal)
        .map_or(false, |(max_ord, proc_ord)| max_ord == proc_ord)
    {
        return Ok(None);
    }

    let existing_hash = tracking_info
        .memoization_info
        .as_ref()
        .and_then(|info| info.0.as_ref())
        .and_then(|stored_info| stored_info.content_hash.as_ref());

    if existing_hash != Some(current_hash) {
        return Ok(None);
    }

    // Content hash matches - try optimization
    let mut txn = pool.begin().await?;

    let current_tracking_info = db_tracking::read_source_tracking_info_for_precommit(
        source_id,
        source_key_json,
        db_setup,
        &mut *txn,
    )
    .await?;

    let Some(current_tracking_info) = current_tracking_info else {
        return Ok(None);
    };

    // Check 1: Same check as precommit - verify no newer version exists
    let current_source_version = SourceVersion::from_stored_precommit_info(
        &current_tracking_info,
        src_eval_ctx.plan.logic_fingerprint,
    );
    if current_source_version.should_skip(source_version, Some(update_stats)) {
        return Ok(Some(SkippedOr::Skipped(current_source_version)));
    }

    // Check 2: Verify process_ordinal hasn't changed (no concurrent processing)
    let original_process_ordinal = tracking_info.process_ordinal;
    if current_tracking_info.process_ordinal != original_process_ordinal {
        return Ok(None);
    }

    // Safe to apply optimization - just update tracking table
    db_tracking::update_source_tracking_ordinal(
        source_id,
        source_key_json,
        source_version.ordinal.0,
        db_setup,
        &mut *txn,
    )
    .await?;

    txn.commit().await?;
    update_stats.num_no_change.inc(1);
    Ok(Some(SkippedOr::Normal(())))
}

pub async fn evaluate_source_entry_with_memory(
    src_eval_ctx: &SourceRowEvaluationContext<'_>,
    setup_execution_ctx: &exec_ctx::FlowSetupExecutionContext,
    options: EvaluationMemoryOptions,
    pool: &PgPool,
) -> Result<Option<EvaluateSourceEntryOutput>> {
    let stored_info = if options.enable_cache || !options.evaluation_only {
        let source_key_json = serde_json::to_value(src_eval_ctx.key)?;
        let source_id = setup_execution_ctx.import_ops[src_eval_ctx.import_op_idx].source_id;
        let existing_tracking_info = read_source_tracking_info_for_processing(
            source_id,
            &source_key_json,
            &setup_execution_ctx.setup_state.tracking_table,
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
    let source_value = src_eval_ctx
        .import_op
        .executor
        .get_value(
            src_eval_ctx.key,
            &SourceExecutorGetOptions {
                include_value: true,
                include_ordinal: false,
            },
        )
        .await?
        .value
        .ok_or_else(|| anyhow::anyhow!("value not returned"))?;
    let output = match source_value {
        interface::SourceValue::Existence(source_value) => {
            Some(evaluate_source_entry(src_eval_ctx, source_value, &memory).await?)
        }
        interface::SourceValue::NonExistence => None,
    };
    Ok(output)
}

pub async fn update_source_row(
    src_eval_ctx: &SourceRowEvaluationContext<'_>,
    setup_execution_ctx: &exec_ctx::FlowSetupExecutionContext,
    source_value: interface::SourceValue,
    source_version: &SourceVersion,
    pool: &PgPool,
    update_stats: &stats::UpdateStats,
) -> Result<SkippedOr<()>> {
    let source_key_json = serde_json::to_value(src_eval_ctx.key)?;
    let process_time = chrono::Utc::now();
    let source_id = setup_execution_ctx.import_ops[src_eval_ctx.import_op_idx].source_id;

    // Phase 1: Check existing tracking info and apply optimizations
    let existing_tracking_info = read_source_tracking_info_for_processing(
        source_id,
        &source_key_json,
        &setup_execution_ctx.setup_state.tracking_table,
        pool,
    )
    .await?;

    let existing_version = match &existing_tracking_info {
        Some(info) => {
            let existing_version = SourceVersion::from_stored_processing_info(
                info,
                src_eval_ctx.plan.logic_fingerprint,
            );

            // First check ordinal-based skipping
            if existing_version.should_skip(source_version, Some(update_stats)) {
                return Ok(SkippedOr::Skipped(existing_version));
            }

            Some(existing_version)
        }
        None => None,
    };

    // Compute content hash once if needed for both optimization and evaluation
    let current_content_hash = match &source_value {
        interface::SourceValue::Existence(source_value) => Some(
            Fingerprinter::default()
                .with(source_value)?
                .into_fingerprint(),
        ),
        interface::SourceValue::NonExistence => None,
    };

    if let (Some(current_hash), Some(existing_tracking_info)) =
        (&current_content_hash, &existing_tracking_info)
    {
        if let Some(optimization_result) = try_content_hash_optimization(
            source_id,
            src_eval_ctx,
            &source_key_json,
            source_version,
            current_hash,
            existing_tracking_info,
            &existing_version,
            &setup_execution_ctx.setup_state.tracking_table,
            update_stats,
            pool,
        )
        .await?
        {
            return Ok(optimization_result);
        }
    }

    let (output, stored_mem_info) = {
        let extracted_memoization_info = existing_tracking_info
            .and_then(|info| info.memoization_info)
            .and_then(|info| info.0);

        match source_value {
            interface::SourceValue::Existence(source_value) => {
                let evaluation_memory = EvaluationMemory::new(
                    process_time,
                    extracted_memoization_info,
                    EvaluationMemoryOptions {
                        enable_cache: true,
                        evaluation_only: false,
                    },
                );

                let output =
                    evaluate_source_entry(src_eval_ctx, source_value, &evaluation_memory).await?;
                let mut stored_info = evaluation_memory.into_stored()?;
                stored_info.content_hash = current_content_hash;

                (Some(output), stored_info)
            }
            interface::SourceValue::NonExistence => (None, Default::default()),
        }
    };

    // Phase 2 (precommit): Update with the memoization info and stage target keys.
    let precommit_output = precommit_source_tracking_info(
        source_id,
        &source_key_json,
        source_version,
        src_eval_ctx.plan.logic_fingerprint,
        output.as_ref().map(|scope_value| PrecommitData {
            evaluate_output: scope_value,
            memoization_info: &stored_mem_info,
        }),
        &process_time,
        &setup_execution_ctx.setup_state.tracking_table,
        &src_eval_ctx.plan.export_ops,
        &setup_execution_ctx.export_ops,
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
    let apply_futs = src_eval_ctx
        .plan
        .export_op_groups
        .iter()
        .filter_map(|export_op_group| {
            let mutations_w_ctx: Vec<_> = export_op_group
                .op_idx
                .iter()
                .filter_map(|export_op_idx| {
                    let export_op = &src_eval_ctx.plan.export_ops[*export_op_idx];
                    target_mutations
                        .remove(&setup_execution_ctx.export_ops[*export_op_idx].target_id)
                        .filter(|m| !m.is_empty())
                        .map(|mutation| interface::ExportTargetMutationWithContext {
                            mutation,
                            export_context: export_op.export_context.as_ref(),
                        })
                })
                .collect();
            (!mutations_w_ctx.is_empty()).then(|| {
                export_op_group
                    .target_factory
                    .apply_mutation(mutations_w_ctx)
            })
        });

    // TODO: Handle errors.
    try_join_all(apply_futs).await?;

    // Phase 4: Update the tracking record.
    commit_source_tracking_info(
        source_id,
        &source_key_json,
        source_version,
        &src_eval_ctx.plan.logic_fingerprint.0,
        precommit_output.metadata,
        &process_time,
        &setup_execution_ctx.setup_state.tracking_table,
        pool,
    )
    .await?;

    if let Some(existing_version) = existing_version {
        if output.is_some() {
            if !source_version.ordinal.is_available()
                || source_version.ordinal != existing_version.ordinal
            {
                update_stats.num_updates.inc(1);
            } else {
                update_stats.num_reprocesses.inc(1);
            }
        } else {
            update_stats.num_deletions.inc(1);
        }
    } else if output.is_some() {
        update_stats.num_insertions.inc(1);
    }

    Ok(SkippedOr::Normal(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_github_actions_scenario_ordinal_behavior() {
        // Test ordinal-based behavior - should_skip only cares about ordinal monotonic invariance
        // Content hash optimization is handled at update_source_row level

        let processed_version = SourceVersion {
            ordinal: Ordinal(Some(1000)), // Original timestamp
            kind: SourceVersionKind::CurrentLogic,
        };

        // GitHub Actions checkout: timestamp changes but content same
        let after_checkout_version = SourceVersion {
            ordinal: Ordinal(Some(2000)), // New timestamp after checkout
            kind: SourceVersionKind::CurrentLogic,
        };

        // Should NOT skip at should_skip level (ordinal is newer - monotonic invariance)
        // Content hash optimization happens at update_source_row level to update only tracking
        assert!(!processed_version.should_skip(&after_checkout_version, None));

        // Reverse case: if we somehow get an older ordinal, always skip
        assert!(after_checkout_version.should_skip(&processed_version, None));

        // Now simulate actual content change
        let content_changed_version = SourceVersion {
            ordinal: Ordinal(Some(3000)), // Even newer timestamp
            kind: SourceVersionKind::CurrentLogic,
        };

        // Should NOT skip processing (ordinal is newer)
        assert!(!processed_version.should_skip(&content_changed_version, None));
    }

    #[test]
    fn test_content_hash_computation() {
        use crate::base::value::{BasicValue, FieldValues, Value};
        use crate::utils::fingerprint::Fingerprinter;

        // Test that content hash is computed correctly from source data
        let source_data1 = FieldValues {
            fields: vec![
                Value::Basic(BasicValue::Str("Hello".into())),
                Value::Basic(BasicValue::Int64(42)),
            ],
        };

        let source_data2 = FieldValues {
            fields: vec![
                Value::Basic(BasicValue::Str("Hello".into())),
                Value::Basic(BasicValue::Int64(42)),
            ],
        };

        let source_data3 = FieldValues {
            fields: vec![
                Value::Basic(BasicValue::Str("World".into())), // Different content
                Value::Basic(BasicValue::Int64(42)),
            ],
        };

        let hash1 = Fingerprinter::default()
            .with(&source_data1)
            .unwrap()
            .into_fingerprint();

        let hash2 = Fingerprinter::default()
            .with(&source_data2)
            .unwrap()
            .into_fingerprint();

        let hash3 = Fingerprinter::default()
            .with(&source_data3)
            .unwrap()
            .into_fingerprint();

        // Same content should produce same hash
        assert_eq!(hash1, hash2);

        // Different content should produce different hash
        assert_ne!(hash1, hash3);
        assert_ne!(hash2, hash3);
    }

    #[test]
    fn test_github_actions_content_hash_optimization_requirements() {
        // This test documents the exact requirements for GitHub Actions scenario
        // where file modification times change but content remains the same

        use crate::utils::fingerprint::Fingerprinter;

        // Simulate file content that remains the same across GitHub Actions checkout
        let file_content = "const hello = 'world';\nexport default hello;";

        // Hash before checkout (original file)
        let hash_before_checkout = Fingerprinter::default()
            .with(&file_content)
            .unwrap()
            .into_fingerprint();

        // Hash after checkout (same content, different timestamp)
        let hash_after_checkout = Fingerprinter::default()
            .with(&file_content)
            .unwrap()
            .into_fingerprint();

        // Content hashes must be identical for optimization to work
        assert_eq!(
            hash_before_checkout, hash_after_checkout,
            "Content hash optimization requires identical hashes for same content"
        );

        // Test with slightly different content (should produce different hashes)
        let modified_content = "const hello = 'world!';\nexport default hello;"; // Added !
        let hash_modified = Fingerprinter::default()
            .with(&modified_content)
            .unwrap()
            .into_fingerprint();

        assert_ne!(
            hash_before_checkout, hash_modified,
            "Different content should produce different hashes"
        );
    }

    #[test]
    fn test_github_actions_ordinal_behavior_with_content_optimization() {
        // Test the complete GitHub Actions scenario:
        // 1. File processed with ordinal=1000, content_hash=ABC
        // 2. GitHub Actions checkout: ordinal=2000, content_hash=ABC (same content)
        // 3. Should use content hash optimization (update only tracking, skip evaluation)

        let original_processing = SourceVersion {
            ordinal: Ordinal(Some(1000)), // Original file timestamp
            kind: SourceVersionKind::CurrentLogic,
        };

        let after_github_checkout = SourceVersion {
            ordinal: Ordinal(Some(2000)), // New timestamp after checkout
            kind: SourceVersionKind::CurrentLogic,
        };

        // Step 1: Ordinal check should NOT skip (newer ordinal means potential processing needed)
        assert!(
            !original_processing.should_skip(&after_github_checkout, None),
            "GitHub Actions: newer ordinal should not be skipped at ordinal level"
        );

        // Step 2: Content hash optimization should trigger when content is same
        // This is tested in the integration level - the optimization path should:
        // - Compare content hashes
        // - If same: update only tracking info (process_ordinal, process_time)
        // - Skip expensive evaluation and target storage updates

        // Step 3: After optimization, tracking shows the new ordinal
        let after_optimization = SourceVersion {
            ordinal: Ordinal(Some(2000)), // Updated to new ordinal
            kind: SourceVersionKind::CurrentLogic,
        };

        // Future requests with same ordinal should be skipped
        assert!(
            after_optimization.should_skip(&after_github_checkout, None),
            "After optimization, same ordinal should be skipped"
        );
    }
}
