use crate::ops::sdk::*;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use unicase::UniCase;

pub struct Args {
    text: ResolvedOpArg,
    language: Option<ResolvedOpArg>,
}

type ParseFn = fn(&str) -> Result<serde_json::Value>;
struct LanguageConfig {
    parse_fn: ParseFn,
}

fn add_language(
    output: &mut HashMap<UniCase<&'static str>, Arc<LanguageConfig>>,
    name: &'static str,
    aliases: impl IntoIterator<Item = &'static str>,
    parse_fn: ParseFn,
) {
    let lang_config = Arc::new(LanguageConfig { parse_fn });
    for name in std::iter::once(name).chain(aliases.into_iter()) {
        if output.insert(name.into(), lang_config.clone()).is_some() {
            panic!("Language `{name}` already exists");
        }
    }
}

fn parse_json(text: &str) -> Result<serde_json::Value> {
    Ok(utils::deser::from_json_str(text)?)
}

static PARSE_FN_BY_LANG: LazyLock<HashMap<UniCase<&'static str>, Arc<LanguageConfig>>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();
        add_language(&mut map, "json", [".json"], parse_json);
        map
    });

struct Executor {
    args: Args,
}

#[async_trait]
impl SimpleFunctionExecutor for Executor {
    async fn evaluate(&self, input: Vec<value::Value>) -> Result<value::Value> {
        let text = self.args.text.value(&input)?.as_str()?;
        let lang_config = {
            let language = self.args.language.value(&input)?;
            language
                .optional()
                .map(|v| anyhow::Ok(v.as_str()?.as_ref()))
                .transpose()?
                .and_then(|lang| PARSE_FN_BY_LANG.get(&UniCase::new(lang)))
        };
        let parse_fn = lang_config.map(|c| c.parse_fn).unwrap_or(parse_json);
        let parsed_value = parse_fn(text)?;
        Ok(value::Value::Basic(value::BasicValue::Json(Arc::new(
            parsed_value,
        ))))
    }
}

pub struct Factory;

#[async_trait]
impl SimpleFunctionFactoryBase for Factory {
    type Spec = EmptySpec;
    type ResolvedArgs = Args;

    fn name(&self) -> &str {
        "ParseJson"
    }

    async fn resolve_schema<'a>(
        &'a self,
        _spec: &'a EmptySpec,
        args_resolver: &mut OpArgsResolver<'a>,
        _context: &FlowInstanceContext,
    ) -> Result<(Args, EnrichedValueType)> {
        let args = Args {
            text: args_resolver
                .next_arg("text")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?
                .required()?,
            language: args_resolver
                .next_arg("language")?
                .expect_nullable_type(&ValueType::Basic(BasicValueType::Str))?
                .optional(),
        };

        let output_schema = make_output_type(BasicValueType::Json);
        Ok((args, output_schema))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::functions::test_utils::{build_arg_schema, test_flow_function};
    use serde_json::json;

    #[tokio::test]
    async fn test_parse_json() {
        let spec = EmptySpec {};

        let factory = Arc::new(Factory);
        let json_string_content = r#"{"city": "Magdeburg"}"#;
        let lang_value: Value = "json".to_string().into();

        let input_args_values = vec![json_string_content.to_string().into(), lang_value.clone()];

        let input_arg_schemas = &[
            build_arg_schema("text", BasicValueType::Str),
            build_arg_schema("language", BasicValueType::Str),
        ];

        let result =
            test_flow_function(&factory, &spec, input_arg_schemas, input_args_values).await;

        assert!(
            result.is_ok(),
            "test_flow_function failed: {:?}",
            result.err()
        );
        let value = result.unwrap();

        match value {
            Value::Basic(BasicValue::Json(arc_json_value)) => {
                let expected_json = json!({"city": "Magdeburg"});
                assert_eq!(
                    *arc_json_value, expected_json,
                    "Parsed JSON value mismatch with specified language"
                );
            }
            _ => panic!("Expected Value::Basic(BasicValue::Json), got {value:?}"),
        }
    }
}
