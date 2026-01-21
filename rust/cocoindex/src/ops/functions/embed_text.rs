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
    expected_output_dimension: Option<u32>,
    task_type: Option<String>,
    api_key: Option<AuthEntryReference<String>>,
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
impl BatchedFunctionExecutor for Executor {
    fn enable_cache(&self) -> bool {
        true
    }

    fn batching_options(&self) -> batching::BatchingOptions {
        // A safe default for most embeddings providers.
        // May tune it for specific providers later.
        batching::BatchingOptions {
            max_batch_size: Some(64),
        }
    }

    async fn evaluate_batch(&self, args: Vec<Vec<Value>>) -> Result<Vec<Value>> {
        let texts = args
            .iter()
            .map(|arg| {
                Ok(Cow::Borrowed(
                    self.args.text.value(&arg)?.as_str()?.as_ref(),
                ))
            })
            .collect::<Result<_>>()?;
        let req = LlmEmbeddingRequest {
            model: &self.spec.model,
            texts,
            output_dimension: self.spec.output_dimension,
            task_type: self
                .spec
                .task_type
                .as_ref()
                .map(|s| Cow::Borrowed(s.as_str())),
        };
        let resp = self.args.client.embed_text(req).await?;
        if resp.embeddings.len() != args.len() {
            api_bail!(
                "Expected {expected} embeddings but got {actual} from the embedding API.",
                expected = args.len(),
                actual = resp.embeddings.len()
            );
        }
        resp.embeddings
            .into_iter()
            .map(|embedding| {
                if embedding.len() != self.args.expected_output_dimension {
                    if self.spec.output_dimension.is_some() {
                        api_bail!(
                            "Expected output dimension {expected} but got {actual} from the embedding API. \
                             Consider setting `output_dimension` to {actual} or leave it unset to use the default.",
                            expected = self.args.expected_output_dimension,
                            actual = embedding.len(),
                        );
                    } else {
                        client_bail!(
                            "Expected output dimension {expected} but got {actual} from the embedding API. \
                             Consider setting `output_dimension` to {actual} as a workaround.",
                            expected = self.args.expected_output_dimension,
                            actual = embedding.len(),
                        );
                    }
                };
                Ok(embedding.into())
            })
            .collect::<Result<Vec<value::Value>>>()
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

    async fn analyze<'a>(
        &'a self,
        spec: &'a Spec,
        args_resolver: &mut OpArgsResolver<'a>,
        context: &FlowInstanceContext,
    ) -> Result<SimpleFunctionAnalysisOutput<Self::ResolvedArgs>> {
        let text = args_resolver
            .next_arg("text")?
            .expect_type(&ValueType::Basic(BasicValueType::Str))?
            .required()?;

        let api_key = spec
            .api_key
            .as_ref()
            .map(|key_ref| context.auth_registry.get(key_ref))
            .transpose()?;

        let client = new_llm_embedding_client(
            spec.api_type,
            spec.address.clone(),
            api_key,
            spec.api_config.clone(),
        )
        .await?;

        // Warn if both parameters are specified but have different values
        if let (Some(expected), Some(output)) =
            (spec.expected_output_dimension, spec.output_dimension)
        {
            if expected != output {
                warn!(
                    "Both `expected_output_dimension` ({expected}) and `output_dimension` ({output}) are specified but have different values. \
                     `expected_output_dimension` will be used for output schema and validation, while `output_dimension` will be sent to the embedding API."
                );
            }
        }

        let expected_output_dimension = spec.expected_output_dimension
            .or(spec.output_dimension)
            .or_else(|| client.get_default_embedding_dimension(spec.model.as_str()))
            .ok_or_else(|| api_error!("model \"{}\" is unknown for {:?}, needs to specify `expected_output_dimension` (or `output_dimension`) explicitly", spec.model, spec.api_type))? as usize;
        let output_schema = make_output_type(BasicValueType::Vector(VectorTypeSchema {
            dimension: Some(expected_output_dimension),
            element_type: Box::new(BasicValueType::Float32),
        }));
        Ok(SimpleFunctionAnalysisOutput {
            behavior_version: client.behavior_version(),
            resolved_args: Args {
                client,
                text,
                expected_output_dimension,
            },
            output_schema,
        })
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        args: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<impl SimpleFunctionExecutor> {
        Ok(Executor { spec, args }.into_fn_executor())
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
            expected_output_dimension: None,
            task_type: None,
            api_key: None,
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
