/// Concepts:
/// - Resource: some setup that needs to be tracked and maintained.
/// - Setup State: current state of a resource.
/// - Staging Change: states changes that may not be really applied yet.
/// - Combined Setup State: Setup State + Staging Change.
/// - Status Check: information about changes that are being applied / need to be applied.
///
/// Resource hierarchy:
/// - [resource: setup metadata table] /// - Flow
///   - [resource: metadata]
///   - [resource: tracking table]
///   - Target
///     - [resource: target-specific stuff]
use crate::prelude::*;

use indenter::indented;
use owo_colors::{AnsiColors, OwoColorize};
use std::any::Any;
use std::fmt::Debug;
use std::fmt::{Display, Write};
use std::hash::Hash;

use super::db_metadata;
use crate::execution::db_tracking_setup::{
    self, TrackingTableSetupState, TrackingTableSetupStatus,
};

const INDENT: &str = "    ";

pub trait StateMode: Clone + Copy {
    type State<T: Debug + Clone>: Debug + Clone;
    type DefaultState<T: Debug + Clone + Default>: Debug + Clone + Default;
}

#[derive(Debug, Clone, Copy)]
pub struct DesiredMode;
impl StateMode for DesiredMode {
    type State<T: Debug + Clone> = T;
    type DefaultState<T: Debug + Clone + Default> = T;
}

#[derive(Debug, Clone)]
pub struct CombinedState<T> {
    pub current: Option<T>,
    pub staging: Vec<StateChange<T>>,
    pub legacy_state_key: Option<serde_json::Value>,
}

impl<T> CombinedState<T> {
    pub fn possible_versions(&self) -> impl Iterator<Item = &T> {
        self.current
            .iter()
            .chain(self.staging.iter().flat_map(|s| s.state().into_iter()))
    }

    pub fn always_exists(&self) -> bool {
        self.current.is_some() && self.staging.iter().all(|s| !s.is_delete())
    }

    pub fn legacy_values<V: Ord + Eq, F: Fn(&T) -> &V>(
        &self,
        desired: Option<&T>,
        f: F,
    ) -> BTreeSet<&V> {
        let desired_value = desired.map(&f);
        self.possible_versions()
            .map(f)
            .filter(|v| Some(*v) != desired_value)
            .collect()
    }
}

impl<T: Debug + Clone> Default for CombinedState<T> {
    fn default() -> Self {
        Self {
            current: None,
            staging: vec![],
            legacy_state_key: None,
        }
    }
}

impl<T: PartialEq + Debug + Clone> PartialEq<T> for CombinedState<T> {
    fn eq(&self, other: &T) -> bool {
        self.staging.is_empty() && self.current.as_ref() == Some(other)
    }
}

#[derive(Clone, Copy)]
pub struct ExistingMode;
impl StateMode for ExistingMode {
    type State<T: Debug + Clone> = CombinedState<T>;
    type DefaultState<T: Debug + Clone + Default> = CombinedState<T>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateChange<State> {
    Upsert(State),
    Delete,
}

impl<State> StateChange<State> {
    pub fn is_delete(&self) -> bool {
        matches!(self, StateChange::Delete)
    }

    pub fn desired_state(&self) -> Option<&State> {
        match self {
            StateChange::Upsert(state) => Some(state),
            StateChange::Delete => None,
        }
    }

    pub fn state(&self) -> Option<&State> {
        match self {
            StateChange::Upsert(state) => Some(state),
            StateChange::Delete => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceSetupState {
    pub source_id: i32,
    pub key_schema: schema::ValueType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ResourceIdentifier {
    pub key: serde_json::Value,
    pub target_kind: String,
}

impl Display for ResourceIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.target_kind, self.key)
    }
}

/// Common state (i.e. not specific to a target kind) for a target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TargetSetupStateCommon {
    pub target_id: i32,
    pub schema_version_id: i32,
    pub max_schema_version_id: i32,
    #[serde(default)]
    pub setup_by_user: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TargetSetupState {
    pub common: TargetSetupStateCommon,

    pub state: serde_json::Value,
}

impl TargetSetupState {
    pub fn state_unless_setup_by_user(self) -> Option<serde_json::Value> {
        (!self.common.setup_by_user).then_some(self.state)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct FlowSetupMetadata {
    pub last_source_id: i32,
    pub last_target_id: i32,
    pub sources: BTreeMap<String, SourceSetupState>,
}

#[derive(Debug, Clone)]
pub struct FlowSetupState<Mode: StateMode> {
    // The version number for the flow, last seen in the metadata table.
    pub seen_flow_metadata_version: Option<u64>,
    pub metadata: Mode::DefaultState<FlowSetupMetadata>,
    pub tracking_table: Mode::State<db_tracking_setup::TrackingTableSetupState>,
    pub targets: IndexMap<ResourceIdentifier, Mode::State<TargetSetupState>>,
}

impl Default for FlowSetupState<ExistingMode> {
    fn default() -> Self {
        Self {
            seen_flow_metadata_version: None,
            metadata: Default::default(),
            tracking_table: Default::default(),
            targets: IndexMap::new(),
        }
    }
}

impl PartialEq for FlowSetupState<DesiredMode> {
    fn eq(&self, other: &Self) -> bool {
        self.metadata == other.metadata
            && self.tracking_table == other.tracking_table
            && self.targets == other.targets
    }
}

#[derive(Debug, Clone)]
pub struct AllSetupState<Mode: StateMode> {
    pub has_metadata_table: bool,
    pub flows: BTreeMap<String, FlowSetupState<Mode>>,
}

impl<Mode: StateMode> Default for AllSetupState<Mode> {
    fn default() -> Self {
        Self {
            has_metadata_table: false,
            flows: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SetupChangeType {
    NoChange,
    Create,
    Update,
    Delete,
    Invalid,
}

pub trait ResourceSetupStatus: Send + Sync + Debug + Any + 'static {
    fn describe_changes(&self) -> Vec<String>;

    fn change_type(&self) -> SetupChangeType;
}

impl ResourceSetupStatus for Box<dyn ResourceSetupStatus> {
    fn describe_changes(&self) -> Vec<String> {
        self.as_ref().describe_changes()
    }

    fn change_type(&self) -> SetupChangeType {
        self.as_ref().change_type()
    }
}

impl ResourceSetupStatus for std::convert::Infallible {
    fn describe_changes(&self) -> Vec<String> {
        unreachable!()
    }

    fn change_type(&self) -> SetupChangeType {
        unreachable!()
    }
}

#[derive(Debug)]
pub struct ResourceSetupInfo<K, S, C: ResourceSetupStatus> {
    pub key: K,
    pub state: Option<S>,
    pub description: String,

    /// If `None`, the resource is managed by users.
    pub setup_status: Option<C>,

    pub legacy_key: Option<K>,
}

impl<K, S, C: ResourceSetupStatus> std::fmt::Display for ResourceSetupInfo<K, S, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status_code = match self.setup_status.as_ref().map(|c| c.change_type()) {
            Some(SetupChangeType::NoChange) => "READY",
            Some(SetupChangeType::Create) => "TO CREATE",
            Some(SetupChangeType::Update) => "TO UPDATE",
            Some(SetupChangeType::Delete) => "TO DELETE",
            Some(SetupChangeType::Invalid) => "INVALID",
            None => "USER MANAGED",
        };
        let status_str = format!("[ {:^9} ]", status_code);
        let status_full = status_str.color(AnsiColors::Cyan);
        let desc_colored = &self.description;
        writeln!(f, "{} {}", status_full, desc_colored)?;
        if let Some(setup_status) = &self.setup_status {
            let changes = setup_status.describe_changes();
            if !changes.is_empty() {
                let mut f = indented(f).with_str(INDENT);
                writeln!(f, "{}", "TODO:".color(AnsiColors::BrightBlack))?;
                for change in changes {
                    writeln!(f, "  - {}", change.color(AnsiColors::BrightBlack))?;
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

impl<K, S, C: ResourceSetupStatus> ResourceSetupInfo<K, S, C> {
    pub fn is_up_to_date(&self) -> bool {
        self.setup_status
            .as_ref()
            .is_none_or(|c| c.change_type() == SetupChangeType::NoChange)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectStatus {
    Invalid,
    New,
    Existing,
    Deleted,
}

pub trait ObjectSetupStatus {
    fn status(&self) -> ObjectStatus;
    fn is_up_to_date(&self) -> bool;
}

#[derive(Debug)]
pub struct FlowSetupStatus {
    pub status: ObjectStatus,
    pub seen_flow_metadata_version: Option<u64>,

    pub metadata_change: Option<StateChange<FlowSetupMetadata>>,

    pub tracking_table:
        Option<ResourceSetupInfo<(), TrackingTableSetupState, TrackingTableSetupStatus>>,
    pub target_resources:
        Vec<ResourceSetupInfo<ResourceIdentifier, TargetSetupState, Box<dyn ResourceSetupStatus>>>,

    pub unknown_resources: Vec<ResourceIdentifier>,
}

impl ObjectSetupStatus for FlowSetupStatus {
    fn status(&self) -> ObjectStatus {
        self.status
    }

    fn is_up_to_date(&self) -> bool {
        self.metadata_change.is_none()
            && self
                .tracking_table
                .as_ref()
                .is_none_or(|t| t.is_up_to_date())
            && self
                .target_resources
                .iter()
                .all(|target| target.is_up_to_date())
    }
}

#[derive(Debug)]
pub struct AllSetupStatus {
    pub metadata_table: ResourceSetupInfo<(), (), db_metadata::MetadataTableSetup>,

    pub flows: BTreeMap<String, FlowSetupStatus>,
}

impl AllSetupStatus {
    pub fn is_up_to_date(&self) -> bool {
        self.metadata_table.is_up_to_date()
            && self.flows.iter().all(|(_, flow)| flow.is_up_to_date())
    }
}

pub struct ObjectSetupStatusCode<'a, Status: ObjectSetupStatus>(&'a Status);
impl<Status: ObjectSetupStatus> std::fmt::Display for ObjectSetupStatusCode<'_, Status> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[ {:^9} ]",
            match self.0.status() {
                ObjectStatus::New => "TO CREATE",
                ObjectStatus::Existing =>
                    if self.0.is_up_to_date() {
                        "READY"
                    } else {
                        "TO UPDATE"
                    },
                ObjectStatus::Deleted => "TO DELETE",
                ObjectStatus::Invalid => "INVALID",
            }
        )
    }
}

pub struct FormattedFlowSetupStatus<'a>(&'a str, &'a FlowSetupStatus);

impl std::fmt::Display for FormattedFlowSetupStatus<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let flow_ssc = self.1;

        write!(
            f,
            "{} {}\n",
            ObjectSetupStatusCode(flow_ssc)
                .to_string()
                .color(AnsiColors::Cyan),
            format!("Flow: {}", self.0)
        )?;

        let mut f = indented(f).with_str(INDENT);
        if let Some(tracking_table) = &flow_ssc.tracking_table {
            write!(f, "{}", tracking_table)?;
        }
        for target_resource in &flow_ssc.target_resources {
            write!(f, "{}", target_resource)?;
        }
        for resource in &flow_ssc.unknown_resources {
            writeln!(f, "[  UNKNOWN  ] {resource}")?;
        }

        Ok(())
    }
}

impl std::fmt::Display for AllSetupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.metadata_table)?;
        for (flow_name, flow_status) in &self.flows {
            writeln!(f, "{}", FormattedFlowSetupStatus(flow_name, flow_status))?;
        }
        Ok(())
    }
}
