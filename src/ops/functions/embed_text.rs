use crate::{
    llm::{LlmApiType, LlmEmbeddingClient, LlmEmbeddingRequest, new_llm_embedding_client},
    ops::sdk::*,
};

#[derive(Deserialize)]
struct Spec {
    api_type: LlmApiType,
    model: String,
    address: Option<String>,
    output_dimension: Option<u32>,
    task_type: Option<String>,
}

struct Args {
    client: Box<dyn LlmEmbeddingClient>,
    text: ResolvedOpArg,
}

struct Executor {
    spec: Spec,
    args: Args,
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    fn behavior_version(&self) -> Option<u32> {
        Some(1)
    }

    fn enable_cache(&self) -> bool {
        true
    }

    async fn evaluate(&self, input: Vec<Value>) -> Result<Value> {
        let text = self.args.text.value(&input)?.as_str()?;
        let req = LlmEmbeddingRequest {
            model: &self.spec.model,
            text: Cow::Borrowed(text),
            output_dimension: self.spec.output_dimension,
            task_type: self
                .spec
                .task_type
                .as_ref()
                .map(|s| Cow::Borrowed(s.as_str())),
        };
        let embedding = self.args.client.embed_text(req).await?;
        Ok(embedding.embedding.into())
    }
}

struct Factory;

#[async_trait]
impl SimpleFunctionFactoryBase for Factory {
    type Spec = Spec;
    type ResolvedArgs = Args;

    fn name(&self) -> &str {
        "EmbedText"
    }

    fn resolve_schema(
        &self,
        spec: &Spec,
        args_resolver: &mut OpArgsResolver<'_>,
        _context: &FlowInstanceContext,
    ) -> Result<(Self::ResolvedArgs, EnrichedValueType)> {
        let text = args_resolver.next_arg("text")?;
        let client = new_llm_embedding_client(spec.api_type, spec.address.clone())?;
        let output_dimension = match spec.output_dimension {
            Some(output_dimension) => output_dimension,
            None => {
                client.get_default_embedding_dimension(spec.model.as_str())
                    .ok_or_else(|| api_error!("model \"{}\" is unknown for {:?}, needs to specify `output_dimension` explicitly", spec.model, spec.api_type))?
            }
        };
        let output_schema = make_output_type(BasicValueType::Vector(VectorTypeSchema {
            dimension: Some(output_dimension as usize),
            element_type: Box::new(BasicValueType::Float32),
        }));
        Ok((Args { client, text }, output_schema))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        args: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SimpleFunctionExecutor>> {
        Ok(Box::new(Executor { spec, args }))
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    Factory.register(registry)
}
