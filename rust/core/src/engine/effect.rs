use crate::{engine::context::ComponentBuilderContext, prelude::*, state::state_path::StatePath};

use std::hash::Hash;

pub trait EffectSink<Action: Send + 'static>: Send + Sync + Eq + Hash + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to apply the action.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    #[allow(async_fn_in_trait)]
    async fn apply(&self, actions: Vec<Action>) -> Result<()>;
}

pub struct EffectReconcileOutput<ERcl: EffectReconciler> {
    pub state: ERcl::State,
    pub action: ERcl::Action,
    pub sink: ERcl::Sink,
    // TODO: Add fields to indicate compatibility, especially for containers (tables)
    // - Whether or not irreversible (e.g. delete a column from a table)
    // - Whether or not destructive (all children effect should be deleted)
}

pub trait EffectReconciler: Send + Sync + Sized + 'static {
    type Key: Clone + Send + Eq + Hash + 'static;
    type State: Clone + Send + 'static;
    type Action: Send + 'static;
    type Sink: EffectSink<Self::Action>;
    type Decl;

    fn reconcile(
        &self,
        key: Self::Key,
        desired_effect: Option<Self::Decl>,
        prev_possible_states: &[Self::State],
        prev_may_be_missing: bool,
    ) -> Result<EffectReconcileOutput<Self>>;
}

pub struct EffectProvider<ERcl: EffectReconciler> {
    pub(crate) effect_state_path: StatePath,
    pub(crate) reconciler: ERcl,
}

pub fn declare_effect<ERcl: EffectReconciler>(
    state_path: &StatePath,
    context: &ComponentBuilderContext,
    provider: &EffectProvider<ERcl>,
    decl: ERcl::Decl,
    key: ERcl::Key,
    child_reconciler: Option<ERcl>,
) -> Result<Option<EffectProvider<ERcl>>> {
    unimplemented!()
}
