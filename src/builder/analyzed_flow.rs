use std::{future::Future, pin::Pin, sync::Arc};

use super::{analyzer, plan};
use crate::{
    api_error,
    base::{schema, spec},
    ops::registry::ExecutorFactoryRegistry,
    service::error::{shared_ok, SharedError, SharedResultExt},
    setup::{self, ObjectSetupStatusCheck},
};
use anyhow::Result;
use futures::{future::Shared, FutureExt};

pub struct AnalyzedFlow {
    pub flow_instance: spec::FlowInstanceSpec,
    pub data_schema: schema::DataSchema,
    pub desired_state: setup::FlowSetupState<setup::DesiredMode>,
    /// It's None if the flow is not up to date
    pub execution_plan: Option<
        Shared<Pin<Box<dyn Future<Output = Result<Arc<plan::ExecutionPlan>, SharedError>> + Send>>>,
    >,
}

impl AnalyzedFlow {
    pub async fn from_flow_instance(
        flow_instance: crate::base::spec::FlowInstanceSpec,
        existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
        registry: &ExecutorFactoryRegistry,
    ) -> Result<Self> {
        let ctx = analyzer::build_flow_instance_context(&flow_instance.name);
        let (data_schema, execution_plan_fut, desired_state) =
            analyzer::analyze_flow(&flow_instance, &ctx, existing_flow_ss, registry)?;
        let setup_status_check =
            setup::check_flow_setup_status(Some(&desired_state), existing_flow_ss)?;
        let execution_plan = if setup_status_check.is_up_to_date() {
            Some(
                async move {
                    shared_ok(Arc::new(
                        execution_plan_fut.await.map_err(SharedError::new)?,
                    ))
                }
                .boxed()
                .shared(),
            )
        } else {
            None
        };
        let result = Self {
            flow_instance,
            data_schema,
            desired_state,
            execution_plan,
        };
        Ok(result)
    }

    pub async fn get_execution_plan(&self) -> Result<Arc<plan::ExecutionPlan>> {
        let execution_plan = self
            .execution_plan
            .as_ref()
            .ok_or_else(|| api_error!("Flow setup is not up to date. Please run `cocoindex setup` to update the setup."))?
            .clone()
            .await
            .std_result()?;
        Ok(execution_plan)
    }
}

pub struct AnalyzedTransientFlow {
    pub transient_flow_instance: spec::TransientFlowSpec,
    pub data_schema: schema::DataSchema,
    pub execution_plan: plan::TransientExecutionPlan,
    pub output_type: schema::EnrichedValueType,
}

impl AnalyzedTransientFlow {
    pub async fn from_transient_flow(
        transient_flow: spec::TransientFlowSpec,
        registry: &ExecutorFactoryRegistry,
    ) -> Result<Self> {
        let ctx = analyzer::build_flow_instance_context(&transient_flow.name);
        let (output_type, data_schema, execution_plan_fut) =
            analyzer::analyze_transient_flow(&transient_flow, &ctx, registry)?;
        Ok(Self {
            transient_flow_instance: transient_flow,
            data_schema: data_schema,
            execution_plan: execution_plan_fut.await?,
            output_type: output_type,
        })
    }
}
