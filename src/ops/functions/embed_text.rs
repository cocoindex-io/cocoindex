use crate::{
    llm::{
        LlmApiConfig, LlmApiType, LlmEmbeddingClient, LlmEmbeddingRequest, new_llm_embedding_client,
    },
    ops::sdk::*,
};

#[derive(Serialize, Deserialize)]
struct Spec {
    api_type: LlmApiType,
    model: String,
    address: Option<String>,
    api_config: Option<LlmApiConfig>,
    output_dimension: Option<u32>,
    task_type: Option<String>,
}

struct Args {
    client: Box<dyn LlmEmbeddingClient>,
    text: ResolvedOpArg,
    expected_output_dimension: usize,
}

struct Executor {
    spec: Spec,
    args: Args,
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    fn behavior_version(&self) -> Option<u32> {
        self.args.client.behavior_version()
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
        if embedding.embedding.len() != self.args.expected_output_dimension {
            if self.spec.output_dimension.is_some() {
                api_bail!(
                    "Expected output dimension {expected} but got {actual} from the embedding API. \
                     Consider setting `output_dimension` to {actual} or leave it unset to use the default.",
                    expected = self.args.expected_output_dimension,
                    actual = embedding.embedding.len()
                );
            } else {
                bail!(
                    "Expected output dimension {expected} but got {actual} from the embedding API. \
                     Consider setting `output_dimension` to {actual} as a workaround.",
                    expected = self.args.expected_output_dimension,
                    actual = embedding.embedding.len()
                )
            }
        }
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

    async fn resolve_schema<'a>(
        &'a self,
        spec: &'a Spec,
        args_resolver: &mut OpArgsResolver<'a>,
        _context: &FlowInstanceContext,
    ) -> Result<(Self::ResolvedArgs, EnrichedValueType)> {
        let text = args_resolver
            .next_arg("text")?
            .expect_type(&ValueType::Basic(BasicValueType::Str))?
            .required()?;
        let client =
            new_llm_embedding_client(spec.api_type, spec.address.clone(), spec.api_config.clone())
                .await?;
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
        Ok((
            Args {
                client,
                text,
                expected_output_dimension: output_dimension as usize,
            },
            output_schema,
        ))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        args: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<impl SimpleFunctionExecutor> {
        Ok(Executor { spec, args })
    }
}

pub fn register(registry: &mut ExecutorFactoryRegistry) -> Result<()> {
    Factory.register(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::functions::test_utils::{build_arg_schema, test_flow_function};

    #[tokio::test]
    #[ignore = "This test requires OpenAI API key or a configured local LLM and may make network calls."]
    async fn test_embed_text() {
        let spec = Spec {
            api_type: LlmApiType::OpenAi,
            model: "text-embedding-ada-002".to_string(),
            address: None,
            api_config: None,
            output_dimension: None,
            task_type: None,
        };

        let factory = Arc::new(Factory);
        let text_content = "CocoIndex is a performant data transformation framework for AI.";

        let input_args_values = vec![text_content.to_string().into()];

        let input_arg_schemas = &[build_arg_schema("text", BasicValueType::Str)];

        let result =
            test_flow_function(&factory, &spec, input_arg_schemas, input_args_values).await;

        if result.is_err() {
            eprintln!(
                "test_embed_text: test_flow_function returned error (potentially expected for evaluate): {:?}",
                result.as_ref().err()
            );
        }

        assert!(
            result.is_ok(),
            "test_flow_function failed. NOTE: This test may require network access/API keys for OpenAI. Error: {:?}",
            result.err()
        );

        let value = result.unwrap();

        match value {
            Value::Basic(BasicValue::Vector(arc_vec)) => {
                assert_eq!(arc_vec.len(), 1536, "Embedding vector dimension mismatch");
                for item in arc_vec.iter() {
                    match item {
                        BasicValue::Float32(_) => {}
                        _ => panic!("Embedding vector element is not Float32: {item:?}"),
                    }
                }
            }
            _ => panic!("Expected Value::Basic(BasicValue::Vector), got {value:?}"),
        }
    }
}
