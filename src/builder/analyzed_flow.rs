use crate::{ops::interface::FlowInstanceContext, prelude::*};

use super::{analyzer, plan};
use crate::service::error::{SharedError, SharedResultExt, shared_ok};

pub struct AnalyzedFlow {
    pub flow_instance: spec::FlowInstanceSpec,
    pub data_schema: schema::FlowSchema,
    pub setup_state: exec_ctx::AnalyzedSetupState,

    pub flow_instance_ctx: Arc<FlowInstanceContext>,

    /// It's None if the flow is not up to date
    pub execution_plan: Shared<BoxFuture<'static, Result<Arc<plan::ExecutionPlan>, SharedError>>>,
}

impl AnalyzedFlow {
    pub async fn from_flow_instance(
        flow_instance: crate::base::spec::FlowInstanceSpec,
        flow_instance_ctx: Arc<FlowInstanceContext>,
    ) -> Result<Self> {
        let (data_schema, setup_state, execution_plan_fut) =
            analyzer::analyze_flow(&flow_instance, flow_instance_ctx.clone())
                .await
                .with_context(|| format!("analyzing flow `{}`", flow_instance.name))?;
        let execution_plan = async move {
            shared_ok(Arc::new(
                execution_plan_fut.await.map_err(SharedError::new)?,
            ))
        }
        .boxed()
        .shared();
        let result = Self {
            flow_instance,
            data_schema,
            setup_state,
            flow_instance_ctx,
            execution_plan,
        };
        Ok(result)
    }

    pub async fn get_execution_plan(&self) -> Result<Arc<plan::ExecutionPlan>> {
        let execution_plan = self.execution_plan.clone().await.anyhow_result()?;
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
            analyzer::analyze_transient_flow(&transient_flow, ctx).await?;
        Ok(Self {
            transient_flow_instance: transient_flow,
            data_schema,
            execution_plan: execution_plan_fut.await?,
            output_type,
        })
    }
}
