use crate::prelude::*;

use crate::execution::db_tracking_setup;
use crate::ops::get_target_factory;
use crate::ops::interface::SetupStateCompatibility;

pub struct ImportOpExecutionContext {
    pub source_id: i32,
}

pub struct ExportOpExecutionContext {
    pub target_id: i32,
}

pub struct FlowSetupExecutionContext {
    pub setup_state: setup::FlowSetupState<setup::DesiredMode>,
    pub import_ops: Vec<ImportOpExecutionContext>,
    pub export_ops: Vec<ExportOpExecutionContext>,
}

pub struct AnalyzedTargetSetupState {
    pub target_kind: String,
    pub setup_key: serde_json::Value,
    pub desired_setup_state: serde_json::Value,
    pub setup_by_user: bool,
    /// None for declarations.
    pub key_type: Option<Box<[schema::ValueType]>>,
}

pub struct AnalyzedSetupState {
    pub targets: Vec<AnalyzedTargetSetupState>,
    pub declarations: Vec<AnalyzedTargetSetupState>,
}

fn build_import_op_exec_ctx(
    import_op: &spec::NamedSpec<spec::ImportOpSpec>,
    import_op_output_type: &schema::EnrichedValueType,
    existing_source_states: Option<&Vec<&setup::SourceSetupState>>,
    metadata: &mut setup::FlowSetupMetadata,
) -> Result<ImportOpExecutionContext> {
    let keys_schema_no_attrs = import_op_output_type
        .typ
        .key_schema()
        .iter()
        .map(|field| field.value_type.typ.without_attrs())
        .collect::<Box<[_]>>();

    let existing_source_ids = existing_source_states
        .iter()
        .flat_map(|v| v.iter())
        .filter_map(|state| {
            let existing_keys_schema: &[schema::ValueType] =
                if let Some(keys_schema) = &state.keys_schema {
                    keys_schema
                } else if let Some(key_schema) = &state.key_schema {
                    std::slice::from_ref(key_schema)
                } else {
                    &[]
                };
            if existing_keys_schema == keys_schema_no_attrs.as_ref() {
                Some(state.source_id)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();
    let source_id = if existing_source_ids.len() == 1 {
        existing_source_ids.into_iter().next().unwrap()
    } else {
        if existing_source_ids.len() > 1 {
            warn!("Multiple source states with the same key schema found");
        }
        metadata.last_source_id += 1;
        metadata.last_source_id
    };
    metadata.sources.insert(
        import_op.name.clone(),
        setup::SourceSetupState {
            source_id,

            // Keep this field for backward compatibility,
            // so users can still swap back to older version if needed.
            key_schema: Some(if keys_schema_no_attrs.len() == 1 {
                keys_schema_no_attrs[0].clone()
            } else {
                schema::ValueType::Struct(schema::StructSchema {
                    fields: Arc::new(
                        import_op_output_type
                            .typ
                            .key_schema()
                            .iter()
                            .map(|field| {
                                schema::FieldSchema::new(
                                    field.name.clone(),
                                    field.value_type.clone(),
                                )
                            })
                            .collect(),
                    ),
                    description: None,
                })
            }),
            keys_schema: Some(keys_schema_no_attrs),
            source_kind: import_op.spec.source.kind.clone(),
        },
    );
    Ok(ImportOpExecutionContext { source_id })
}

fn build_target_id(
    analyzed_target_ss: &AnalyzedTargetSetupState,
    existing_target_states: &HashMap<&setup::ResourceIdentifier, Vec<&setup::TargetSetupState>>,
    metadata: &mut setup::FlowSetupMetadata,
    target_states: &mut IndexMap<setup::ResourceIdentifier, setup::TargetSetupState>,
) -> Result<i32> {
    let target_factory = get_target_factory(&analyzed_target_ss.target_kind)?;

    let resource_id = setup::ResourceIdentifier {
        key: analyzed_target_ss.setup_key.clone(),
        target_kind: analyzed_target_ss.target_kind.clone(),
    };
    let existing_target_states = existing_target_states.get(&resource_id);
    let mut compatible_target_ids = HashSet::<Option<i32>>::new();
    let mut reusable_schema_version_ids = HashSet::<Option<i32>>::new();
    for existing_state in existing_target_states.iter().flat_map(|v| v.iter()) {
        let compatibility = if let Some(key_type) = &analyzed_target_ss.key_type
            && let Some(existing_key_type) = &existing_state.common.key_type
            && key_type != existing_key_type
        {
            SetupStateCompatibility::NotCompatible
        } else if analyzed_target_ss.setup_by_user != existing_state.common.setup_by_user {
            SetupStateCompatibility::NotCompatible
        } else {
            target_factory.check_state_compatibility(
                &analyzed_target_ss.desired_setup_state,
                &existing_state.state,
            )?
        };
        let compatible_target_id = if compatibility != SetupStateCompatibility::NotCompatible {
            reusable_schema_version_ids.insert(
                (compatibility == SetupStateCompatibility::Compatible)
                    .then_some(existing_state.common.schema_version_id),
            );
            Some(existing_state.common.target_id)
        } else {
            None
        };
        compatible_target_ids.insert(compatible_target_id);
    }

    let target_id = if compatible_target_ids.len() == 1 {
        compatible_target_ids.into_iter().next().flatten()
    } else {
        if compatible_target_ids.len() > 1 {
            warn!("Multiple target states with the same key schema found");
        }
        None
    };
    let target_id = target_id.unwrap_or_else(|| {
        metadata.last_target_id += 1;
        metadata.last_target_id
    });
    let max_schema_version_id = existing_target_states
        .iter()
        .flat_map(|v| v.iter())
        .map(|s| s.common.max_schema_version_id)
        .max()
        .unwrap_or(0);
    let schema_version_id = if reusable_schema_version_ids.len() == 1 {
        reusable_schema_version_ids
            .into_iter()
            .next()
            .unwrap()
            .unwrap_or(max_schema_version_id + 1)
    } else {
        max_schema_version_id + 1
    };
    match target_states.entry(resource_id) {
        indexmap::map::Entry::Occupied(entry) => {
            api_bail!(
                "Target resource already exists: kind = {}, key = {}",
                entry.key().target_kind,
                entry.key().key
            );
        }
        indexmap::map::Entry::Vacant(entry) => {
            entry.insert(setup::TargetSetupState {
                common: setup::TargetSetupStateCommon {
                    target_id,
                    schema_version_id,
                    max_schema_version_id: max_schema_version_id.max(schema_version_id),
                    setup_by_user: analyzed_target_ss.setup_by_user,
                    key_type: analyzed_target_ss.key_type.clone(),
                },
                state: analyzed_target_ss.desired_setup_state.clone(),
            });
        }
    }
    Ok(target_id)
}

pub fn build_flow_setup_execution_context(
    flow_inst: &spec::FlowInstanceSpec,
    data_schema: &schema::FlowSchema,
    analyzed_ss: &AnalyzedSetupState,
    existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
) -> Result<FlowSetupExecutionContext> {
    let existing_metadata_versions = || {
        existing_flow_ss
            .iter()
            .flat_map(|flow_ss| flow_ss.metadata.possible_versions())
    };

    let mut source_states_by_name = HashMap::<&str, Vec<&setup::SourceSetupState>>::new();
    for metadata_version in existing_metadata_versions() {
        for (source_name, state) in metadata_version.sources.iter() {
            source_states_by_name
                .entry(source_name.as_str())
                .or_default()
                .push(state);
        }
    }

    let mut target_states_by_name_type =
        HashMap::<&setup::ResourceIdentifier, Vec<&setup::TargetSetupState>>::new();
    for metadata_version in existing_flow_ss.iter() {
        for (resource_id, target) in metadata_version.targets.iter() {
            target_states_by_name_type
                .entry(resource_id)
                .or_default()
                .extend(target.possible_versions());
        }
    }

    let mut metadata = setup::FlowSetupMetadata {
        last_source_id: existing_metadata_versions()
            .map(|metadata| metadata.last_source_id)
            .max()
            .unwrap_or(0),
        last_target_id: existing_metadata_versions()
            .map(|metadata| metadata.last_target_id)
            .max()
            .unwrap_or(0),
        sources: BTreeMap::new(),
        features: existing_flow_ss
            .map(|m| {
                m.metadata
                    .possible_versions()
                    .flat_map(|v| v.features.iter())
                    .cloned()
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_else(setup::flow_features::default_features),
    };
    let mut target_states = IndexMap::new();

    let import_op_exec_ctx = flow_inst
        .import_ops
        .iter()
        .map(|import_op| {
            let output_type = data_schema
                .root_op_scope
                .op_output_types
                .get(&import_op.name)
                .ok_or_else(invariance_violation)?;
            build_import_op_exec_ctx(
                &import_op,
                output_type,
                source_states_by_name.get(&import_op.name.as_str()),
                &mut metadata,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let export_op_exec_ctx = analyzed_ss
        .targets
        .iter()
        .map(|analyzed_target_ss| {
            let target_id = build_target_id(
                analyzed_target_ss,
                &target_states_by_name_type,
                &mut metadata,
                &mut target_states,
            )?;
            Ok(ExportOpExecutionContext { target_id })
        })
        .collect::<Result<Vec<_>>>()?;

    for analyzed_target_ss in analyzed_ss.declarations.iter() {
        build_target_id(
            analyzed_target_ss,
            &target_states_by_name_type,
            &mut metadata,
            &mut target_states,
        )?;
    }

    let setup_state = setup::FlowSetupState::<setup::DesiredMode> {
        seen_flow_metadata_version: existing_flow_ss
            .and_then(|flow_ss| flow_ss.seen_flow_metadata_version),
        tracking_table: db_tracking_setup::TrackingTableSetupState {
            table_name: existing_flow_ss
                .and_then(|flow_ss| {
                    flow_ss
                        .tracking_table
                        .current
                        .as_ref()
                        .map(|v| v.table_name.clone())
                })
                .unwrap_or_else(|| db_tracking_setup::default_tracking_table_name(&flow_inst.name)),
            version_id: db_tracking_setup::CURRENT_TRACKING_TABLE_VERSION,
            source_state_table_name: metadata
                .features
                .contains(setup::flow_features::SOURCE_STATE_TABLE)
                .then(|| {
                    existing_flow_ss
                        .and_then(|flow_ss| flow_ss.tracking_table.current.as_ref())
                        .and_then(|v| v.source_state_table_name.clone())
                        .unwrap_or_else(|| {
                            db_tracking_setup::default_source_state_table_name(&flow_inst.name)
                        })
                }),
            has_fast_fingerprint_column: metadata
                .features
                .contains(setup::flow_features::FAST_FINGERPRINT),
        },
        targets: target_states,
        metadata,
    };
    Ok(FlowSetupExecutionContext {
        setup_state,
        import_ops: import_op_exec_ctx,
        export_ops: export_op_exec_ctx,
    })
}
