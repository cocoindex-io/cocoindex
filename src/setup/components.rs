use super::{CombinedState, ResourceSetupStatus, SetupChangeType, StateChange};
use crate::prelude::*;
use std::fmt::Debug;

pub trait State<Key>: Debug + Send + Sync {
    fn key(&self) -> Key;
}

#[async_trait]
pub trait SetupOperator: 'static + Send + Sync {
    type Key: Debug + Hash + Eq + Clone + Send + Sync;
    type State: State<Self::Key>;
    type SetupState: Send + Sync + IntoIterator<Item = Self::State>;
    type Context: Sync;

    fn describe_key(&self, key: &Self::Key) -> String;

    fn describe_state(&self, state: &Self::State) -> String;

    fn is_up_to_date(&self, current: &Self::State, desired: &Self::State) -> bool;

    async fn create(&self, state: &Self::State, context: &Self::Context) -> Result<()>;

    async fn delete(&self, key: &Self::Key, context: &Self::Context) -> Result<()>;

    async fn update(&self, state: &Self::State, context: &Self::Context) -> Result<()> {
        self.delete(&state.key(), context).await?;
        self.create(state, context).await
    }
}

#[derive(Debug)]
struct CompositeStateUpsert<S> {
    state: S,
    already_exists: bool,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SetupStatus<D: SetupOperator> {
    #[derivative(Debug = "ignore")]
    desc: D,
    keys_to_delete: IndexSet<D::Key>,
    states_to_upsert: Vec<CompositeStateUpsert<D::State>>,
}

impl<D: SetupOperator> SetupStatus<D> {
    pub fn create(
        desc: D,
        desired: Option<D::SetupState>,
        existing: CombinedState<D::SetupState>,
    ) -> Result<Self> {
        let existing_component_states = CombinedState {
            current: existing.current.map(|s| {
                s.into_iter()
                    .map(|s| (s.key(), s))
                    .collect::<IndexMap<_, _>>()
            }),
            staging: existing
                .staging
                .into_iter()
                .map(|s| match s {
                    StateChange::Delete => StateChange::Delete,
                    StateChange::Upsert(s) => {
                        StateChange::Upsert(s.into_iter().map(|s| (s.key(), s)).collect())
                    }
                })
                .collect(),
            legacy_state_key: existing.legacy_state_key,
        };
        let mut keys_to_delete = IndexSet::new();
        let mut states_to_upsert = vec![];

        // Collect all existing component keys
        for c in existing_component_states.possible_versions() {
            keys_to_delete.extend(c.keys().cloned());
        }

        if let Some(desired_state) = desired {
            for desired_comp_state in desired_state {
                let key = desired_comp_state.key();

                // Remove keys that should be kept from deletion list
                keys_to_delete.shift_remove(&key);

                // Add components that need to be updated
                let is_up_to_date = existing_component_states.always_exists()
                    && existing_component_states.possible_versions().all(|v| {
                        v.get(&key)
                            .map_or(false, |s| desc.is_up_to_date(s, &desired_comp_state))
                    });
                if !is_up_to_date {
                    let already_exists = existing_component_states
                        .possible_versions()
                        .any(|v| v.contains_key(&key));
                    states_to_upsert.push(CompositeStateUpsert {
                        state: desired_comp_state,
                        already_exists,
                    });
                }
            }
        }

        Ok(Self {
            desc,
            keys_to_delete,
            states_to_upsert,
        })
    }
}

impl<D: SetupOperator + Send + Sync> ResourceSetupStatus for SetupStatus<D> {
    fn describe_changes(&self) -> Vec<String> {
        let mut result = vec![];

        for key in &self.keys_to_delete {
            result.push(format!("Delete {}", self.desc.describe_key(key)));
        }

        for state in &self.states_to_upsert {
            result.push(format!(
                "{} {}",
                if state.already_exists {
                    "Update"
                } else {
                    "Create"
                },
                self.desc.describe_state(&state.state)
            ));
        }

        result
    }

    fn change_type(&self) -> SetupChangeType {
        if self.keys_to_delete.is_empty() && self.states_to_upsert.is_empty() {
            SetupChangeType::NoChange
        } else if self.keys_to_delete.is_empty() {
            SetupChangeType::Create
        } else if self.states_to_upsert.is_empty() {
            SetupChangeType::Delete
        } else {
            SetupChangeType::Update
        }
    }
}

pub async fn apply_component_changes<D: SetupOperator>(
    changes: Vec<&SetupStatus<D>>,
    context: &D::Context,
) -> Result<()> {
    // First delete components that need to be removed
    for change in changes.iter() {
        for key in &change.keys_to_delete {
            change.desc.delete(key, context).await?;
        }
    }

    // Then upsert components that need to be updated
    for change in changes.iter() {
        for state in &change.states_to_upsert {
            if state.already_exists {
                change.desc.update(&state.state, context).await?;
            } else {
                change.desc.create(&state.state, context).await?;
            }
        }
    }

    Ok(())
}

impl<A: ResourceSetupStatus, B: ResourceSetupStatus> ResourceSetupStatus for (A, B) {
    fn describe_changes(&self) -> Vec<String> {
        let mut result = vec![];
        result.extend(self.0.describe_changes());
        result.extend(self.1.describe_changes());
        result
    }

    fn change_type(&self) -> SetupChangeType {
        match (self.0.change_type(), self.1.change_type()) {
            (SetupChangeType::Invalid, _) | (_, SetupChangeType::Invalid) => {
                SetupChangeType::Invalid
            }
            (SetupChangeType::NoChange, b) => b,
            (a, _) => a,
        }
    }
}
