use crate::{
    lib_context::{FlowContext, FlowExecutionContext, LibSetupContext},
    ops::{
        get_optional_target_factory,
        interface::{FlowInstanceContext, TargetFactory},
    },
    prelude::*,
};

use sqlx::PgPool;
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use super::{AllSetupStates, GlobalSetupChange};
use super::{
    CombinedState, DesiredMode, ExistingMode, FlowSetupChange, FlowSetupState, ObjectSetupChange,
    ObjectStatus, ResourceIdentifier, ResourceSetupChange, ResourceSetupInfo, SetupChangeType,
    StateChange, TargetSetupState, db_metadata,
};
use crate::execution::db_tracking_setup;
use std::fmt::Write;

enum MetadataRecordType {
    FlowVersion,
    FlowMetadata,
    TrackingTable,
    Target(String),
}

impl Display for MetadataRecordType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataRecordType::FlowVersion => f.write_str(db_metadata::FLOW_VERSION_RESOURCE_TYPE),
            MetadataRecordType::FlowMetadata => write!(f, "FlowMetadata"),
            MetadataRecordType::TrackingTable => write!(f, "TrackingTable"),
            MetadataRecordType::Target(target_id) => write!(f, "Target:{target_id}"),
        }
    }
}

impl std::str::FromStr for MetadataRecordType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == db_metadata::FLOW_VERSION_RESOURCE_TYPE {
            Ok(Self::FlowVersion)
        } else if s == "FlowMetadata" {
            Ok(Self::FlowMetadata)
        } else if s == "TrackingTable" {
            Ok(Self::TrackingTable)
        } else if let Some(target_id) = s.strip_prefix("Target:") {
            Ok(Self::Target(target_id.to_string()))
        } else {
            anyhow::bail!("Invalid MetadataRecordType string: {}", s)
        }
    }
}

fn from_metadata_record<S: DeserializeOwned + Debug + Clone>(
    state: Option<serde_json::Value>,
    staging_changes: sqlx::types::Json<Vec<StateChange<serde_json::Value>>>,
    legacy_state_key: Option<serde_json::Value>,
) -> Result<CombinedState<S>> {
    let current: Option<S> = state.map(utils::deser::from_json_value).transpose()?;
    let staging: Vec<StateChange<S>> = (staging_changes.0.into_iter())
        .map(|sc| -> Result<_> {
            Ok(match sc {
                StateChange::Upsert(v) => StateChange::Upsert(utils::deser::from_json_value(v)?),
                StateChange::Delete => StateChange::Delete,
            })
        })
        .collect::<Result<_>>()?;
    Ok(CombinedState {
        current,
        staging,
        legacy_state_key,
    })
}

fn get_export_target_factory(target_type: &str) -> Option<Arc<dyn TargetFactory + Send + Sync>> {
    get_optional_target_factory(target_type)
}

pub async fn get_existing_setup_state(pool: &PgPool) -> Result<AllSetupStates<ExistingMode>> {
    let setup_metadata_records = db_metadata::read_setup_metadata(pool).await?;

    let setup_metadata_records = if let Some(records) = setup_metadata_records {
        records
    } else {
        return Ok(AllSetupStates::default());
    };

    // Group setup metadata records by flow name
    let setup_metadata_records = setup_metadata_records.into_iter().fold(
        BTreeMap::<String, Vec<_>>::new(),
        |mut acc, record| {
            acc.entry(record.flow_name.clone())
                .or_default()
                .push(record);
            acc
        },
    );

    let flows = setup_metadata_records
        .into_iter()
        .map(|(flow_name, metadata_records)| -> anyhow::Result<_> {
            let mut flow_ss = FlowSetupState::default();
            for metadata_record in metadata_records {
                let state = metadata_record.state;
                let staging_changes = metadata_record.staging_changes;
                match MetadataRecordType::from_str(&metadata_record.resource_type)? {
                    MetadataRecordType::FlowVersion => {
                        flow_ss.seen_flow_metadata_version =
                            db_metadata::parse_flow_version(&state);
                    }
                    MetadataRecordType::FlowMetadata => {
                        flow_ss.metadata = from_metadata_record(state, staging_changes, None)?;
                    }
                    MetadataRecordType::TrackingTable => {
                        flow_ss.tracking_table =
                            from_metadata_record(state, staging_changes, None)?;
                    }
                    MetadataRecordType::Target(target_type) => {
                        let normalized_key = {
                            if let Some(factory) = get_export_target_factory(&target_type) {
                                factory.normalize_setup_key(&metadata_record.key)?
                            } else {
                                metadata_record.key.clone()
                            }
                        };
                        let combined_state = from_metadata_record(
                            state,
                            staging_changes,
                            (normalized_key != metadata_record.key).then_some(metadata_record.key),
                        )?;
                        flow_ss.targets.insert(
                            super::ResourceIdentifier {
                                key: normalized_key,
                                target_kind: target_type,
                            },
                            combined_state,
                        );
                    }
                }
            }
            Ok((flow_name, flow_ss))
        })
        .collect::<Result<_>>()?;

    Ok(AllSetupStates {
        has_metadata_table: true,
        flows,
    })
}

fn diff_state<E, D, T>(
    existing_state: Option<&E>,
    desired_state: Option<&D>,
    diff: impl Fn(Option<&E>, &D) -> Option<StateChange<T>>,
) -> Option<StateChange<T>>
where
    E: PartialEq<D>,
{
    match (existing_state, desired_state) {
        (None, None) => None,
        (Some(_), None) => Some(StateChange::Delete),
        (existing_state, Some(desired_state)) => {
            if existing_state.map(|e| e == desired_state).unwrap_or(false) {
                None
            } else {
                diff(existing_state, desired_state)
            }
        }
    }
}

fn to_object_status<A, B>(existing: Option<A>, desired: Option<B>) -> Option<ObjectStatus> {
    Some(match (&existing, &desired) {
        (Some(_), None) => ObjectStatus::Deleted,
        (None, Some(_)) => ObjectStatus::New,
        (Some(_), Some(_)) => ObjectStatus::Existing,
        (None, None) => return None,
    })
}

#[derive(Debug, Default)]
struct GroupedResourceStates {
    desired: Option<TargetSetupState>,
    existing: CombinedState<TargetSetupState>,
}

fn group_resource_states<'a>(
    desired: impl Iterator<Item = (&'a ResourceIdentifier, &'a TargetSetupState)>,
    existing: impl Iterator<Item = (&'a ResourceIdentifier, &'a CombinedState<TargetSetupState>)>,
) -> Result<IndexMap<&'a ResourceIdentifier, GroupedResourceStates>> {
    let mut grouped: IndexMap<&'a ResourceIdentifier, GroupedResourceStates> = desired
        .into_iter()
        .map(|(key, state)| {
            (
                key,
                GroupedResourceStates {
                    desired: Some(state.clone()),
                    existing: CombinedState::default(),
                },
            )
        })
        .collect();
    for (key, state) in existing {
        let entry = grouped.entry(key);
        if state.current.is_some() {
            if let indexmap::map::Entry::Occupied(entry) = &entry {
                if entry.get().existing.current.is_some() {
                    bail!("Duplicate existing state for key: {}", entry.key());
                }
            }
        }
        let entry = entry.or_default();
        if let Some(current) = &state.current {
            entry.existing.current = Some(current.clone());
        }
        if let Some(legacy_state_key) = &state.legacy_state_key {
            if entry
                .existing
                .legacy_state_key
                .as_ref()
                .is_some_and(|v| v != legacy_state_key)
            {
                warn!(
                    "inconsistent legacy key: {:?}, {:?}",
                    key, entry.existing.legacy_state_key
                );
            }
            entry.existing.legacy_state_key = Some(legacy_state_key.clone());
        }
        for s in state.staging.iter() {
            match s {
                StateChange::Upsert(v) => {
                    entry.existing.staging.push(StateChange::Upsert(v.clone()))
                }
                StateChange::Delete => entry.existing.staging.push(StateChange::Delete),
            }
        }
    }
    Ok(grouped)
}

pub async fn diff_flow_setup_states(
    desired_state: Option<&FlowSetupState<DesiredMode>>,
    existing_state: Option<&FlowSetupState<ExistingMode>>,
    flow_instance_ctx: &Arc<FlowInstanceContext>,
) -> Result<FlowSetupChange> {
    let metadata_change = diff_state(
        existing_state.map(|e| &e.metadata),
        desired_state.map(|d| &d.metadata),
        |_, desired_state| Some(StateChange::Upsert(desired_state.clone())),
    );

    // If the source kind has changed, we need to clean the source states.
    let source_names_needs_states_cleanup: BTreeMap<i32, BTreeSet<String>> =
        if let Some(desired_state) = desired_state
            && let Some(existing_state) = existing_state
        {
            let new_source_id_to_kind = desired_state
                .metadata
                .sources
                .values()
                .map(|v| (v.source_id, &v.source_kind))
                .collect::<HashMap<i32, &String>>();

            let mut existing_source_id_to_name_kind =
                BTreeMap::<i32, Vec<(&String, &String)>>::new();
            for (name, setup_state) in existing_state
                .metadata
                .possible_versions()
                .flat_map(|v| v.sources.iter())
            {
                // For backward compatibility, we only process source states for non-empty source kinds.
                if !setup_state.source_kind.is_empty() {
                    existing_source_id_to_name_kind
                        .entry(setup_state.source_id)
                        .or_default()
                        .push((&name, &setup_state.source_kind));
                }
            }

            (existing_source_id_to_name_kind.into_iter())
                .map(|(id, name_kinds)| {
                    let new_kind = new_source_id_to_kind.get(&id).map(|v| *v);
                    let source_names_for_legacy_states = name_kinds
                        .into_iter()
                        .filter_map(|(name, kind)| {
                            if Some(kind) != new_kind {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .collect::<BTreeSet<_>>();
                    (id, source_names_for_legacy_states)
                })
                .filter(|(_, v)| !v.is_empty())
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };

    let tracking_table_change = db_tracking_setup::TrackingTableSetupChange::new(
        desired_state.map(|d| &d.tracking_table),
        &existing_state
            .map(|e| Cow::Borrowed(&e.tracking_table))
            .unwrap_or_default(),
        source_names_needs_states_cleanup,
    );

    let mut target_resources = Vec::new();
    let mut unknown_resources = Vec::new();

    let grouped_target_resources = group_resource_states(
        desired_state.iter().flat_map(|d| d.targets.iter()),
        existing_state.iter().flat_map(|e| e.targets.iter()),
    )?;
    for (resource_id, v) in grouped_target_resources.into_iter() {
        let factory = match get_export_target_factory(&resource_id.target_kind) {
            Some(factory) => factory,
            None => {
                unknown_resources.push(resource_id.clone());
                continue;
            }
        };
        let state = v.desired.clone();
        let target_state = v
            .desired
            .and_then(|state| (!state.common.setup_by_user).then_some(state.state));
        let existing_without_setup_by_user = CombinedState {
            current: v
                .existing
                .current
                .and_then(|s| s.state_unless_setup_by_user()),
            staging: v
                .existing
                .staging
                .into_iter()
                .filter_map(|s| match s {
                    StateChange::Upsert(s) => {
                        s.state_unless_setup_by_user().map(StateChange::Upsert)
                    }
                    StateChange::Delete => Some(StateChange::Delete),
                })
                .collect(),
            legacy_state_key: v.existing.legacy_state_key.clone(),
        };
        let never_setup_by_sys = target_state.is_none()
            && existing_without_setup_by_user.current.is_none()
            && existing_without_setup_by_user.staging.is_empty();
        let setup_change = if never_setup_by_sys {
            None
        } else {
            Some(
                factory
                    .diff_setup_states(
                        &resource_id.key,
                        target_state,
                        existing_without_setup_by_user,
                        flow_instance_ctx.clone(),
                    )
                    .await?,
            )
        };
        target_resources.push(ResourceSetupInfo {
            key: resource_id.clone(),
            state,
            description: factory.describe_resource(&resource_id.key)?,
            setup_change,
            legacy_key: v
                .existing
                .legacy_state_key
                .map(|legacy_state_key| ResourceIdentifier {
                    target_kind: resource_id.target_kind.clone(),
                    key: legacy_state_key,
                }),
        });
    }
    Ok(FlowSetupChange {
        status: to_object_status(existing_state, desired_state),
        seen_flow_metadata_version: existing_state.and_then(|s| s.seen_flow_metadata_version),
        metadata_change,
        tracking_table: tracking_table_change.map(|c| c.into_setup_info()),
        target_resources,
        unknown_resources,
    })
}

struct ResourceSetupChangeItem<'a, K: 'a, C: ResourceSetupChange> {
    key: &'a K,
    setup_change: &'a C,
}

async fn maybe_update_resource_setup<
    'a,
    K: 'a,
    S: 'a,
    C: ResourceSetupChange,
    ChangeApplierResultFut: Future<Output = Result<()>>,
>(
    resource_kind: &str,
    write: &mut (dyn std::io::Write + Send),
    resources: impl Iterator<Item = &'a ResourceSetupInfo<K, S, C>>,
    apply_change: impl FnOnce(Vec<ResourceSetupChangeItem<'a, K, C>>) -> ChangeApplierResultFut,
) -> Result<()> {
    let mut changes = Vec::new();
    for resource in resources {
        if let Some(setup_change) = &resource.setup_change {
            if setup_change.change_type() != SetupChangeType::NoChange {
                changes.push(ResourceSetupChangeItem {
                    key: &resource.key,
                    setup_change,
                });
                writeln!(write, "{}:", resource.description)?;
                for change in setup_change.describe_changes() {
                    match change {
                        setup::ChangeDescription::Action(action) => {
                            writeln!(write, "  - {action}")?;
                        }
                        setup::ChangeDescription::Note(_) => {}
                    }
                }
            }
        }
    }
    if !changes.is_empty() {
        write!(write, "Pushing change for {resource_kind}...")?;
        apply_change(changes).await?;
        writeln!(write, "DONE")?;
    }
    Ok(())
}

async fn apply_changes_for_flow(
    write: &mut (dyn std::io::Write + Send),
    flow_ctx: &FlowContext,
    flow_setup_change: &FlowSetupChange,
    existing_setup_state: &mut Option<setup::FlowSetupState<setup::ExistingMode>>,
    pool: &PgPool,
) -> Result<()> {
    let Some(status) = flow_setup_change.status else {
        return Ok(());
    };
    let verb = match status {
        ObjectStatus::New => "Creating",
        ObjectStatus::Deleted => "Deleting",
        ObjectStatus::Existing => "Updating resources for ",
        _ => bail!("invalid flow status"),
    };
    write!(write, "\n{verb} flow {}:\n", flow_ctx.flow_name())?;

    let mut update_info =
        HashMap::<db_metadata::ResourceTypeKey, db_metadata::StateUpdateInfo>::new();

    if let Some(metadata_change) = &flow_setup_change.metadata_change {
        update_info.insert(
            db_metadata::ResourceTypeKey::new(
                MetadataRecordType::FlowMetadata.to_string(),
                serde_json::Value::Null,
            ),
            db_metadata::StateUpdateInfo::new(metadata_change.desired_state(), None)?,
        );
    }
    if let Some(tracking_table) = &flow_setup_change.tracking_table {
        if tracking_table
            .setup_change
            .as_ref()
            .map(|c| c.change_type() != SetupChangeType::NoChange)
            .unwrap_or_default()
        {
            update_info.insert(
                db_metadata::ResourceTypeKey::new(
                    MetadataRecordType::TrackingTable.to_string(),
                    serde_json::Value::Null,
                ),
                db_metadata::StateUpdateInfo::new(tracking_table.state.as_ref(), None)?,
            );
        }
    }

    for target_resource in &flow_setup_change.target_resources {
        update_info.insert(
            db_metadata::ResourceTypeKey::new(
                MetadataRecordType::Target(target_resource.key.target_kind.clone()).to_string(),
                target_resource.key.key.clone(),
            ),
            db_metadata::StateUpdateInfo::new(
                target_resource.state.as_ref(),
                target_resource.legacy_key.as_ref().map(|k| {
                    db_metadata::ResourceTypeKey::new(
                        MetadataRecordType::Target(k.target_kind.clone()).to_string(),
                        k.key.clone(),
                    )
                }),
            )?,
        );
    }

    let new_version_id = db_metadata::stage_changes_for_flow(
        flow_ctx.flow_name(),
        flow_setup_change.seen_flow_metadata_version,
        &update_info,
        pool,
    )
    .await?;

    if let Some(tracking_table) = &flow_setup_change.tracking_table {
        maybe_update_resource_setup(
            "tracking table",
            write,
            std::iter::once(tracking_table),
            |setup_change| setup_change[0].setup_change.apply_change(),
        )
        .await?;
    }

    let mut setup_change_by_target_kind = IndexMap::<&str, Vec<_>>::new();
    for target_resource in &flow_setup_change.target_resources {
        setup_change_by_target_kind
            .entry(target_resource.key.target_kind.as_str())
            .or_default()
            .push(target_resource);
    }
    for (target_kind, resources) in setup_change_by_target_kind.into_iter() {
        maybe_update_resource_setup(
            target_kind,
            write,
            resources.into_iter(),
            |setup_change| async move {
                let factory = get_export_target_factory(target_kind).ok_or_else(|| {
                    anyhow::anyhow!("No factory found for target kind: {}", target_kind)
                })?;
                factory
                    .apply_setup_changes(
                        setup_change
                            .into_iter()
                            .map(|s| interface::ResourceSetupChangeItem {
                                key: &s.key.key,
                                setup_change: s.setup_change.as_ref(),
                            })
                            .collect(),
                        flow_ctx.flow.flow_instance_ctx.clone(),
                    )
                    .await?;
                Ok(())
            },
        )
        .await?;
    }

    let is_deletion = status == ObjectStatus::Deleted;
    db_metadata::commit_changes_for_flow(
        flow_ctx.flow_name(),
        new_version_id,
        &update_info,
        is_deletion,
        pool,
    )
    .await?;
    if is_deletion {
        *existing_setup_state = None;
    } else {
        let (existing_metadata, existing_tracking_table, existing_targets) =
            match std::mem::take(existing_setup_state) {
                Some(s) => (Some(s.metadata), Some(s.tracking_table), s.targets),
                None => Default::default(),
            };
        let metadata = CombinedState::from_change(
            existing_metadata,
            flow_setup_change
                .metadata_change
                .as_ref()
                .map(|v| v.desired_state()),
        );
        let tracking_table = CombinedState::from_change(
            existing_tracking_table,
            flow_setup_change.tracking_table.as_ref().map(|c| {
                c.setup_change
                    .as_ref()
                    .and_then(|c| c.desired_state.as_ref())
            }),
        );
        let mut targets = existing_targets;
        for target_resource in &flow_setup_change.target_resources {
            match &target_resource.state {
                Some(state) => {
                    targets.insert(
                        target_resource.key.clone(),
                        CombinedState::from_desired(state.clone()),
                    );
                }
                None => {
                    targets.shift_remove(&target_resource.key);
                }
            }
        }
        *existing_setup_state = Some(setup::FlowSetupState {
            metadata,
            tracking_table,
            seen_flow_metadata_version: Some(new_version_id),
            targets,
        });
    }

    writeln!(write, "Done for flow {}", flow_ctx.flow_name())?;
    Ok(())
}

async fn apply_global_changes(
    write: &mut (dyn std::io::Write + Send),
    setup_change: &GlobalSetupChange,
    all_setup_states: &mut AllSetupStates<ExistingMode>,
) -> Result<()> {
    maybe_update_resource_setup(
        "metadata table",
        write,
        std::iter::once(&setup_change.metadata_table),
        |setup_change| setup_change[0].setup_change.apply_change(),
    )
    .await?;

    if setup_change
        .metadata_table
        .setup_change
        .as_ref()
        .is_some_and(|c| c.change_type() == SetupChangeType::Create)
    {
        all_setup_states.has_metadata_table = true;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowSetupChangeAction {
    Setup,
    Drop,
}
pub struct SetupChangeBundle {
    pub action: FlowSetupChangeAction,
    pub flow_names: Vec<String>,
}

impl SetupChangeBundle {
    pub async fn describe(&self, lib_context: &LibContext) -> Result<(String, bool)> {
        let mut text = String::new();
        let mut is_up_to_date = true;

        let setup_ctx = lib_context
            .require_persistence_ctx()?
            .setup_ctx
            .read()
            .await;
        let setup_ctx = &*setup_ctx;

        if self.action == FlowSetupChangeAction::Setup {
            is_up_to_date = is_up_to_date && setup_ctx.global_setup_change.is_up_to_date();
            write!(&mut text, "{}", setup_ctx.global_setup_change)?;
        }

        for flow_name in &self.flow_names {
            let flow_ctx = {
                let flows = lib_context.flows.lock().unwrap();
                flows
                    .get(flow_name)
                    .ok_or_else(|| anyhow::anyhow!("Flow instance not found: {flow_name}"))?
                    .clone()
            };
            let flow_exec_ctx = flow_ctx.get_execution_ctx_for_setup().read().await;

            let mut setup_change_buffer = None;
            let setup_change = get_flow_setup_change(
                setup_ctx,
                &flow_ctx,
                &flow_exec_ctx,
                &self.action,
                &mut setup_change_buffer,
            )
            .await?;

            is_up_to_date = is_up_to_date && setup_change.is_up_to_date();
            write!(
                &mut text,
                "{}",
                setup::FormattedFlowSetupChange(flow_name, setup_change)
            )?;
        }
        Ok((text, is_up_to_date))
    }

    pub async fn apply(
        &self,
        lib_context: &LibContext,
        write: &mut (dyn std::io::Write + Send),
    ) -> Result<()> {
        let persistence_ctx = lib_context.require_persistence_ctx()?;
        let mut setup_ctx = persistence_ctx.setup_ctx.write().await;
        let setup_ctx = &mut *setup_ctx;

        if self.action == FlowSetupChangeAction::Setup
            && !setup_ctx.global_setup_change.is_up_to_date()
        {
            apply_global_changes(
                write,
                &setup_ctx.global_setup_change,
                &mut setup_ctx.all_setup_states,
            )
            .await?;
            setup_ctx.global_setup_change =
                GlobalSetupChange::from_setup_states(&setup_ctx.all_setup_states);
        }

        for flow_name in &self.flow_names {
            let flow_ctx = {
                let flows = lib_context.flows.lock().unwrap();
                flows
                    .get(flow_name)
                    .ok_or_else(|| anyhow::anyhow!("Flow instance not found: {flow_name}"))?
                    .clone()
            };
            let mut flow_exec_ctx = flow_ctx.get_execution_ctx_for_setup().write().await;
            apply_changes_for_flow_ctx(
                self.action,
                &flow_ctx,
                &mut flow_exec_ctx,
                setup_ctx,
                &persistence_ctx.builtin_db_pool,
                write,
            )
            .await?;
        }
        Ok(())
    }
}

async fn get_flow_setup_change<'a>(
    setup_ctx: &LibSetupContext,
    flow_ctx: &'a FlowContext,
    flow_exec_ctx: &'a FlowExecutionContext,
    action: &FlowSetupChangeAction,
    buffer: &'a mut Option<FlowSetupChange>,
) -> Result<&'a FlowSetupChange> {
    let result = match action {
        FlowSetupChangeAction::Setup => &flow_exec_ctx.setup_change,
        FlowSetupChangeAction::Drop => {
            let existing_state = setup_ctx.all_setup_states.flows.get(flow_ctx.flow_name());
            buffer.insert(
                diff_flow_setup_states(None, existing_state, &flow_ctx.flow.flow_instance_ctx)
                    .await?,
            )
        }
    };
    Ok(result)
}

pub(crate) async fn apply_changes_for_flow_ctx(
    action: FlowSetupChangeAction,
    flow_ctx: &FlowContext,
    flow_exec_ctx: &mut FlowExecutionContext,
    setup_ctx: &mut LibSetupContext,
    db_pool: &PgPool,
    write: &mut (dyn std::io::Write + Send),
) -> Result<()> {
    let mut setup_change_buffer = None;
    let setup_change = get_flow_setup_change(
        setup_ctx,
        &flow_ctx,
        &flow_exec_ctx,
        &action,
        &mut setup_change_buffer,
    )
    .await?;
    if setup_change.is_up_to_date() {
        return Ok(());
    }

    let mut flow_states = setup_ctx
        .all_setup_states
        .flows
        .remove(flow_ctx.flow_name());
    apply_changes_for_flow(write, &flow_ctx, setup_change, &mut flow_states, db_pool).await?;

    flow_exec_ctx
        .update_setup_state(&flow_ctx.flow, flow_states.as_ref())
        .await?;
    if let Some(flow_states) = flow_states {
        setup_ctx
            .all_setup_states
            .flows
            .insert(flow_ctx.flow_name().to_string(), flow_states);
    }
    Ok(())
}
