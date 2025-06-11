use crate::{ops::interface::FlowInstanceContext, prelude::*};

use super::{analyzer, plan};
use crate::{
    ops::registry::ExecutorFactoryRegistry,
    service::error::{SharedError, SharedResultExt, shared_ok},
    setup::{self, ObjectSetupStatus},
};

pub struct AnalyzedFlow {
    pub flow_instance: spec::FlowInstanceSpec,
    pub data_schema: schema::FlowSchema,
    pub desired_state: setup::FlowSetupState<setup::DesiredMode>,
    /// It's None if the flow is not up to date
    pub execution_plan:
        Option<Shared<BoxFuture<'static, Result<Arc<plan::ExecutionPlan>, SharedError>>>>,
}

impl AnalyzedFlow {
    pub async fn from_flow_instance(
        flow_instance: crate::base::spec::FlowInstanceSpec,
        flow_instance_ctx: Arc<FlowInstanceContext>,
        existing_flow_ss: Option<&setup::FlowSetupState<setup::ExistingMode>>,
        registry: &ExecutorFactoryRegistry,
    ) -> Result<Self> {
        let (data_schema, execution_plan_fut, desired_state) = analyzer::analyze_flow(
            &flow_instance,
            &flow_instance_ctx,
            existing_flow_ss,
            registry,
        )?;
        let setup_status =
            setup::check_flow_setup_status(Some(&desired_state), existing_flow_ss).await?;
        let execution_plan = if setup_status.is_up_to_date() {
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
    pub data_schema: schema::FlowSchema,
    pub execution_plan: plan::TransientExecutionPlan,
    pub output_type: schema::EnrichedValueType,
}

impl AnalyzedTransientFlow {
    pub async fn from_transient_flow(
        transient_flow: spec::TransientFlowSpec,
        py_exec_ctx: Option<crate::py::PythonExecutionContext>,
    ) -> Result<Self> {
        let ctx = analyzer::build_flow_instance_context(&transient_flow.name, py_exec_ctx);
        let (output_type, data_schema, execution_plan_fut) =
            analyzer::analyze_transient_flow(&transient_flow, &ctx)?;
        Ok(Self {
            transient_flow_instance: transient_flow,
            data_schema,
            execution_plan: execution_plan_fut.await?,
            output_type,
        })
    }
}
