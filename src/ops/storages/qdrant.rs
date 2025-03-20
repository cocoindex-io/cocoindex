use std::sync::Arc;

use crate::base::spec::*;
use crate::ops::sdk::*;
use crate::setup;
use crate::utils::db::ValidIdentifier;
use anyhow::Result;
use derivative::Derivative;
use futures::FutureExt;
use serde::Serialize;

#[derive(Debug, Deserialize)]
pub struct Spec {
    qdrant_url: Option<String>,
    collection_name: Option<String>,
}
const BIND_LIMIT: usize = 65535;

pub struct Executor {
    collection_name: ValidIdentifier,
    key_fields_schema: Vec<FieldSchema>,
    value_fields_schema: Vec<FieldSchema>,
}

impl Executor {
    fn new(
        collection_name: String,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
    ) -> Result<Self> {
        let collection_name = ValidIdentifier::try_from(collection_name)?;
        Ok(Self {
            key_fields_schema,
            value_fields_schema,
            collection_name,
        })
    }
}

#[async_trait]
impl ExportTargetExecutor for Executor {
    async fn apply_mutation(&self, mutation: ExportTargetMutation) -> Result<()> {
        let num_parameters = self.key_fields_schema.len() + self.value_fields_schema.len();
        for _upsert_chunk in mutation.upserts.chunks(BIND_LIMIT / num_parameters) {}

        // TODO: Find a way to batch delete.
        for _delete_key in mutation.delete_keys.iter() {}

        Ok(())
    }
}

#[async_trait]
impl QueryTarget for Executor {
    async fn search(&self, _query: VectorMatchQuery) -> Result<QueryResults> {
        Ok(QueryResults {
            fields: vec![],
            results: vec![],
        })
    }
}

#[derive(Default)]
pub struct Factory {}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableId {
    database_url: Option<String>,
    collection_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupState {}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct SetupStatusCheck {
    #[derivative(Debug = "ignore")]
    table_id: TableId,

    desired_state: Option<SetupState>,
}

impl SetupStatusCheck {
    fn new(table_id: TableId, desired_state: Option<SetupState>) -> Self {
        Self {
            table_id,
            desired_state,
        }
    }
}

#[async_trait]
impl setup::ResourceSetupStatusCheck for SetupStatusCheck {
    type Key = TableId;
    type State = SetupState;

    fn describe_resource(&self) -> String {
        format!("Qdrant table {}", "TABLE ID")
    }

    fn key(&self) -> &Self::Key {
        &self.table_id
    }

    fn desired_state(&self) -> Option<&Self::State> {
        self.desired_state.as_ref()
    }

    fn describe_changes(&self) -> Vec<String> {
        vec![]
    }

    fn change_type(&self) -> setup::SetupChangeType {
        setup::SetupChangeType::NoChange
    }

    async fn apply_change(&self) -> Result<()> {
        Ok(())
    }
}

impl StorageFactoryBase for Arc<Factory> {
    type Spec = Spec;
    type SetupState = SetupState;
    type Key = TableId;

    fn name(&self) -> &str {
        "Qdrant"
    }

    fn build(
        self: Arc<Self>,
        name: String,
        target_id: i32,
        spec: Spec,
        key_fields_schema: Vec<FieldSchema>,
        value_fields_schema: Vec<FieldSchema>,
        storage_options: IndexOptions,
        context: Arc<FlowInstanceContext>,
    ) -> Result<(
        (TableId, SetupState),
        ExecutorFuture<'static, (Arc<dyn ExportTargetExecutor>, Option<Arc<dyn QueryTarget>>)>,
    )> {
        let _ = storage_options;
        let table_id = TableId {
            database_url: spec.qdrant_url.clone(),
            collection_name: spec.collection_name.unwrap_or_else(|| {
                format!("{}__{}__{}", context.flow_instance_name, name, target_id)
            }),
        };
        let setup_state = SetupState {};
        let collection_name = table_id.collection_name.clone();
        let executors = async move {
            let executor = Arc::new(Executor::new(
                collection_name,
                key_fields_schema,
                value_fields_schema,
            )?);
            let query_target = executor.clone();
            Ok((
                executor as Arc<dyn ExportTargetExecutor>,
                Some(query_target as Arc<dyn QueryTarget>),
            ))
        };
        Ok(((table_id, setup_state), executors.boxed()))
    }

    fn check_setup_status(
        &self,
        key: TableId,
        desired: Option<SetupState>,
        existing: setup::CombinedState<SetupState>,
    ) -> Result<
        impl setup::ResourceSetupStatusCheck<Key = TableId, State = SetupState> + 'static,
    > {
        let _ = existing;
        Ok(SetupStatusCheck::new(key, desired))
    }

    fn will_keep_all_existing_data(
        &self,
        _name: &str,
        _target_id: i32,
        desired: &SetupState,
        existing: &SetupState,
    ) -> Result<bool> {
        let _ = existing;
        let _ = desired;
        Ok(true)
    }
}
