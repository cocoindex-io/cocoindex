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

fn add_language<'a>(
    output: &'a mut HashMap<UniCase<&'static str>, Arc<LanguageConfig>>,
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
    Ok(serde_json::from_str(text)?)
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

    fn resolve_schema(
        &self,
        _spec: &EmptySpec,
        args_resolver: &mut OpArgsResolver<'_>,
        _context: &FlowInstanceContext,
    ) -> Result<(Args, EnrichedValueType)> {
        let args = Args {
            text: args_resolver
                .next_arg("text")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?,
            language: args_resolver
                .next_optional_arg("language")?
                .expect_type(&ValueType::Basic(BasicValueType::Str))?,
        };

        let output_schema = make_output_type(BasicValueType::Json);
        Ok((args, output_schema))
    }

    async fn build_executor(
        self: Arc<Self>,
        _spec: EmptySpec,
        args: Args,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SimpleFunctionExecutor>> {
        Ok(Box::new(Executor { args }))
    }
}
