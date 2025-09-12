use crate::prelude::*;

use crate::setup::{CombinedState, ResourceSetupChange, ResourceSetupInfo, SetupChangeType};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

pub fn default_tracking_table_name(flow_name: &str) -> String {
    format!(
        "{}__cocoindex_tracking",
        utils::db::sanitize_identifier(flow_name)
    )
}

pub fn default_source_state_table_name(flow_name: &str) -> String {
    format!(
        "{}__cocoindex_srcstate",
        utils::db::sanitize_identifier(flow_name)
    )
}

pub const CURRENT_TRACKING_TABLE_VERSION: i32 = 1;

async fn upgrade_tracking_table(
    pool: &PgPool,
    desired_state: &TrackingTableSetupState,
    existing_version_id: i32,
) -> Result<()> {
    if existing_version_id < 1 && desired_state.version_id >= 1 {
        let table_name = &desired_state.table_name;
        let opt_fast_fingerprint_column = desired_state
            .has_fast_fingerprint_column
            .then(|| "processed_source_fp BYTEA,")
            .unwrap_or("");
        let query =  format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
                source_id INTEGER NOT NULL,
                source_key JSONB NOT NULL,

                -- Update in the precommit phase: after evaluation done, before really applying the changes to the target storage.
                max_process_ordinal BIGINT NOT NULL,
                staging_target_keys JSONB NOT NULL,
                memoization_info JSONB,

                -- Update after applying the changes to the target storage.
                processed_source_ordinal BIGINT,
                {opt_fast_fingerprint_column}
                process_logic_fingerprint BYTEA,
                process_ordinal BIGINT,
                process_time_micros BIGINT,
                target_keys JSONB,

                PRIMARY KEY (source_id, source_key)
            );",
        );
        sqlx::query(&query).execute(pool).await?;
    }

    Ok(())
}

async fn create_source_state_table(pool: &PgPool, table_name: &str) -> Result<()> {
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {table_name} (
            source_id INTEGER NOT NULL,
            key JSONB NOT NULL,
            value JSONB NOT NULL,

            PRIMARY KEY (source_id, key)
        )"
    );
    sqlx::query(&query).execute(pool).await?;
    Ok(())
}

async fn delete_source_states_for_sources(
    pool: &PgPool,
    table_name: &str,
    source_ids: &Vec<i32>,
) -> Result<()> {
    let query = format!("DELETE FROM {} WHERE source_id = ANY($1)", table_name,);
    sqlx::query(&query).bind(source_ids).execute(pool).await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TrackingTableSetupState {
    pub table_name: String,
    pub version_id: i32,
    #[serde(default)]
    pub source_state_table_name: Option<String>,
    #[serde(default)]
    pub has_fast_fingerprint_column: bool,
}

#[derive(Debug)]
pub struct TrackingTableSetupChange {
    pub desired_state: Option<TrackingTableSetupState>,

    pub min_existing_version_id: Option<i32>,
    pub legacy_tracking_table_names: BTreeSet<String>,

    pub source_state_table_always_exists: bool,
    pub legacy_source_state_table_names: BTreeSet<String>,

    pub source_names_need_state_cleanup: BTreeMap<i32, BTreeSet<String>>,
}

impl TrackingTableSetupChange {
    pub fn new(
        desired: Option<&TrackingTableSetupState>,
        existing: &CombinedState<TrackingTableSetupState>,
        source_names_need_state_cleanup: BTreeMap<i32, BTreeSet<String>>,
    ) -> Option<Self> {
        let legacy_tracking_table_names = existing
            .legacy_values(desired, |v| &v.table_name)
            .into_iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let legacy_source_state_table_names = existing
            .legacy_values(desired, |v| &v.source_state_table_name)
            .into_iter()
            .filter_map(|v| v.clone())
            .collect::<BTreeSet<_>>();
        let min_existing_version_id = existing
            .always_exists()
            .then(|| existing.possible_versions().map(|v| v.version_id).min())
            .flatten();
        if desired.is_some() || min_existing_version_id.is_some() {
            Some(Self {
                desired_state: desired.cloned(),
                legacy_tracking_table_names,
                source_state_table_always_exists: existing.always_exists()
                    && existing
                        .possible_versions()
                        .all(|v| v.source_state_table_name.is_some()),
                legacy_source_state_table_names,
                min_existing_version_id,
                source_names_need_state_cleanup,
            })
        } else {
            None
        }
    }

    pub fn into_setup_info(
        self,
    ) -> ResourceSetupInfo<(), TrackingTableSetupState, TrackingTableSetupChange> {
        ResourceSetupInfo {
            key: (),
            state: self.desired_state.clone(),
            description: "Internal Storage for Tracking".to_string(),
            setup_change: Some(self),
            legacy_key: None,
        }
    }
}

impl ResourceSetupChange for TrackingTableSetupChange {
    fn describe_changes(&self) -> Vec<setup::ChangeDescription> {
        let mut changes: Vec<setup::ChangeDescription> = vec![];
        if self.desired_state.is_some() && !self.legacy_tracking_table_names.is_empty() {
            changes.push(setup::ChangeDescription::Action(format!(
                "Rename legacy tracking tables: {}. ",
                self.legacy_tracking_table_names.iter().join(", ")
            )));
        }
        match (self.min_existing_version_id, &self.desired_state) {
            (None, Some(state)) => {
                changes.push(setup::ChangeDescription::Action(format!(
                    "Create the tracking table: {}. ",
                    state.table_name
                )));
            }
            (Some(min_version_id), Some(desired)) => {
                if min_version_id < desired.version_id {
                    changes.push(setup::ChangeDescription::Action(
                        "Update the tracking table. ".into(),
                    ));
                }
            }
            (Some(_), None) => changes.push(setup::ChangeDescription::Action(format!(
                "Drop existing tracking table: {}. ",
                self.legacy_tracking_table_names.iter().join(", ")
            ))),
            (None, None) => (),
        }

        let source_state_table_name = self
            .desired_state
            .as_ref()
            .and_then(|v| v.source_state_table_name.as_ref());
        if let Some(source_state_table_name) = source_state_table_name {
            if !self.legacy_source_state_table_names.is_empty() {
                changes.push(setup::ChangeDescription::Action(format!(
                    "Rename legacy source state tables: {}. ",
                    self.legacy_source_state_table_names.iter().join(", ")
                )));
            }
            if !self.source_state_table_always_exists {
                changes.push(setup::ChangeDescription::Action(format!(
                    "Create the source state table: {}. ",
                    source_state_table_name
                )));
            }
        } else if !self.source_state_table_always_exists
            && !self.legacy_source_state_table_names.is_empty()
        {
            changes.push(setup::ChangeDescription::Action(format!(
                "Drop existing source state table: {}. ",
                self.legacy_source_state_table_names.iter().join(", ")
            )));
        }

        if !self.source_names_need_state_cleanup.is_empty() {
            changes.push(setup::ChangeDescription::Action(format!(
                "Clean up legacy source states: {}. ",
                self.source_names_need_state_cleanup
                    .values()
                    .flatten()
                    .dedup()
                    .join(", ")
            )));
        }
        changes
    }

    fn change_type(&self) -> SetupChangeType {
        match (self.min_existing_version_id, &self.desired_state) {
            (None, Some(_)) => SetupChangeType::Create,
            (Some(min_version_id), Some(desired)) => {
                let source_state_table_up_to_date = self.legacy_source_state_table_names.is_empty()
                    && self.source_names_need_state_cleanup.is_empty()
                    && (self.source_state_table_always_exists
                        || desired.source_state_table_name.is_none());

                if min_version_id == desired.version_id
                    && self.legacy_tracking_table_names.is_empty()
                    && source_state_table_up_to_date
                {
                    SetupChangeType::NoChange
                } else if min_version_id < desired.version_id || !source_state_table_up_to_date {
                    SetupChangeType::Update
                } else {
                    SetupChangeType::Invalid
                }
            }
            (Some(_), None) => SetupChangeType::Delete,
            (None, None) => SetupChangeType::NoChange,
        }
    }
}

impl TrackingTableSetupChange {
    pub async fn apply_change(&self) -> Result<()> {
        let lib_context = get_lib_context().await?;
        let pool = lib_context.require_builtin_db_pool()?;
        if let Some(desired) = &self.desired_state {
            for lagacy_name in self.legacy_tracking_table_names.iter() {
                let query = format!(
                    "ALTER TABLE IF EXISTS {} RENAME TO {}",
                    lagacy_name, desired.table_name
                );
                sqlx::query(&query).execute(pool).await?;
            }

            if self.min_existing_version_id != Some(desired.version_id) {
                upgrade_tracking_table(pool, desired, self.min_existing_version_id.unwrap_or(0))
                    .await?;
            }
        } else {
            for lagacy_name in self.legacy_tracking_table_names.iter() {
                let query = format!("DROP TABLE IF EXISTS {lagacy_name}");
                sqlx::query(&query).execute(pool).await?;
            }
        }

        let source_state_table_name = self
            .desired_state
            .as_ref()
            .and_then(|v| v.source_state_table_name.as_ref());
        if let Some(source_state_table_name) = source_state_table_name {
            for lagacy_name in self.legacy_source_state_table_names.iter() {
                let query = format!(
                    "ALTER TABLE IF EXISTS {lagacy_name} RENAME TO {source_state_table_name}"
                );
                sqlx::query(&query).execute(pool).await?;
            }
            if !self.source_state_table_always_exists {
                create_source_state_table(pool, source_state_table_name).await?;
            }
            if !self.source_names_need_state_cleanup.is_empty() {
                delete_source_states_for_sources(
                    pool,
                    source_state_table_name,
                    &self
                        .source_names_need_state_cleanup
                        .keys()
                        .map(|v| *v)
                        .collect::<Vec<_>>(),
                )
                .await?;
            }
        } else {
            for lagacy_name in self.legacy_source_state_table_names.iter() {
                let query = format!("DROP TABLE IF EXISTS {lagacy_name}");
                sqlx::query(&query).execute(pool).await?;
            }
        }
        Ok(())
    }
}
