use async_stream::try_stream;
use log::warn;
use std::borrow::Cow;
use std::{path::PathBuf, sync::Arc};

use super::shared::pattern_matcher::PatternMatcher;
use crate::base::field_attrs;
use crate::{fields_value, ops::sdk::*};

#[derive(Debug, Deserialize)]
pub struct Spec {
    path: String,
    binary: bool,
    included_patterns: Option<Vec<String>>,
    excluded_patterns: Option<Vec<String>>,
}

struct Executor {
    root_path: PathBuf,
    binary: bool,
    pattern_matcher: PatternMatcher,
}

#[async_trait]
impl SourceExecutor for Executor {
    async fn list(
        &self,
        options: &SourceExecutorReadOptions,
    ) -> Result<BoxStream<'async_trait, Result<Vec<PartialSourceRow>>>> {
        let root_component_size = self.root_path.components().count();
        let mut dirs = Vec::new();
        dirs.push(Cow::Borrowed(&self.root_path));
        let mut new_dirs = Vec::new();
        let stream = try_stream! {
            while let Some(dir) = dirs.pop() {
                let mut entries = tokio::fs::read_dir(dir.as_ref()).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    let mut path_components = path.components();
                    for _ in 0..root_component_size {
                        path_components.next();
                    }
                    let Some(relative_path) = path_components.as_path().to_str() else {
                        warn!("Skipped ill-formed file path: {}", path.display());
                        continue;
                    };
                    if path.is_dir() {
                        if !self.pattern_matcher.is_excluded(relative_path) {
                            new_dirs.push(Cow::Owned(path));
                        }
                    } else if self.pattern_matcher.is_file_included(relative_path) {
                        let ordinal: Option<Ordinal> = if options.include_ordinal {
                            Some(path.metadata()?.modified()?.try_into()?)
                        } else {
                            None
                        };
                        yield vec![PartialSourceRow {
                            key: KeyValue::from_single_part(relative_path.to_string()),
                            key_aux_info: serde_json::Value::Null,
                            data: PartialSourceRowData {
                                ordinal,
                                content_version_fp: None,
                                value: None,
                            },
                        }];
                    }
                }
                dirs.extend(new_dirs.drain(..).rev());
            }
        };
        Ok(stream.boxed())
    }

    async fn get_value(
        &self,
        key: &KeyValue,
        _key_aux_info: &serde_json::Value,
        options: &SourceExecutorReadOptions,
    ) -> Result<PartialSourceRowData> {
        let path = key.single_part()?.str_value()?.as_ref();
        if !self.pattern_matcher.is_file_included(path) {
            return Ok(PartialSourceRowData {
                value: Some(SourceValue::NonExistence),
                ordinal: Some(Ordinal::unavailable()),
                content_version_fp: None,
            });
        }
        let path = self.root_path.join(path);
        let ordinal = if options.include_ordinal {
            Some(path.metadata()?.modified()?.try_into()?)
        } else {
            None
        };
        let value = if options.include_value {
            match std::fs::read(path) {
                Ok(content) => {
                    let content = if self.binary {
                        fields_value!(content)
                    } else {
                        fields_value!(String::from_utf8_lossy(&content).to_string())
                    };
                    Some(SourceValue::Existence(content))
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    Some(SourceValue::NonExistence)
                }
                Err(e) => Err(e)?,
            }
        } else {
            None
        };
        Ok(PartialSourceRowData {
            value,
            ordinal,
            content_version_fp: None,
        })
    }

    fn provides_ordinal(&self) -> bool {
        true
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "LocalFile"
    }

    async fn get_output_schema(
        &self,
        spec: &Spec,
        _context: &FlowInstanceContext,
    ) -> Result<EnrichedValueType> {
        let mut struct_schema = StructSchema::default();
        let mut schema_builder = StructSchemaBuilder::new(&mut struct_schema);
        let filename_field = schema_builder.add_field(FieldSchema::new(
            "filename",
            make_output_type(BasicValueType::Str),
        ));
        schema_builder.add_field(FieldSchema::new(
            "content",
            make_output_type(if spec.binary {
                BasicValueType::Bytes
            } else {
                BasicValueType::Str
            })
            .with_attr(
                field_attrs::CONTENT_FILENAME,
                serde_json::to_value(filename_field.to_field_ref())?,
            ),
        ));

        Ok(make_output_type(TableSchema::new(
            TableKind::KTable(KTableInfo { num_key_parts: 1 }),
            struct_schema,
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        _source_name: &str,
        spec: Spec,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(Executor {
            root_path: PathBuf::from(spec.path),
            binary: spec.binary,
            pattern_matcher: PatternMatcher::new(spec.included_patterns, spec.excluded_patterns)?,
        }))
    }
}
