use crate::ops::sdk::*;
use cocoindex_extra_text::prog_langs;

pub struct Args {
    filename: ResolvedOpArg,
}

struct Executor {
    args: Args,
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    async fn evaluate(&self, input: Vec<value::Value>) -> Result<value::Value> {
        let filename = self.args.filename.value(&input)?.as_str()?;
        let lang_name = prog_langs::detect_language(&filename)
            .map(|name| value::Value::Basic(value::BasicValue::Str(name.into())));
        Ok(lang_name.unwrap_or(value::Value::Null))
    }
}

struct Factory;

#[async_trait]
impl SimpleFunctionFactoryBase for Factory {
    type Spec = EmptySpec;
    type ResolvedArgs = Args;

    fn name(&self) -> &str {
        "DetectProgrammingLanguage"
    }

    async fn analyze<'a>(
        &'a self,
        _spec: &'a EmptySpec,
        args_resolver: &mut OpArgsResolver<'a>,
        _context: &FlowInstanceContext,
    ) -> Result<SimpleFunctionAnalysisOutput<Args>> {
        let args = Args {
            filename: args_resolver
                .next_arg("filename")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?
                .required()?,
        };

        let output_schema = make_output_type(BasicValueType::Str);
        Ok(SimpleFunctionAnalysisOutput {
            resolved_args: args,
            output_schema,
            behavior_version: None,
        })
    }

    async fn build_executor(
        self: Arc<Self>,
        _spec: EmptySpec,
        args: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<impl SimpleFunctionExecutor> {
        Ok(Executor { args })
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
    async fn test_detect_programming_language() {
        let spec = EmptySpec {};
        let factory = Arc::new(Factory);

        let input_args_values = vec!["test.rs".to_string().into()];
        let input_arg_schemas = &[build_arg_schema("filename", BasicValueType::Str)];

        let result =
            test_flow_function(&factory, &spec, input_arg_schemas, input_args_values).await;

        assert!(
            result.is_ok(),
            "test_flow_function failed: {:?}",
            result.err()
        );
        let value = result.unwrap();

        match value {
            Value::Basic(BasicValue::Str(lang)) => {
                assert_eq!(lang.as_ref(), "rust", "Expected 'rust' for .rs extension");
            }
            _ => panic!("Expected Value::Basic(BasicValue::Str), got {value:?}"),
        }
    }

    #[tokio::test]
    async fn test_detect_programming_language_unknown() {
        let spec = EmptySpec {};
        let factory = Arc::new(Factory);

        let input_args_values = vec!["test.unknown".to_string().into()];
        let input_arg_schemas = &[build_arg_schema("filename", BasicValueType::Str)];

        let result =
            test_flow_function(&factory, &spec, input_arg_schemas, input_args_values).await;

        assert!(
            result.is_ok(),
            "test_flow_function failed: {:?}",
            result.err()
        );
        let value = result.unwrap();

        match value {
            Value::Null => {
                // Expected null for unknown extension
            }
            _ => panic!("Expected Value::Null, got {value:?}"),
        }
    }
}
