use async_stream::try_stream;
use globset::{Glob, GlobSet, GlobSetBuilder};
use log::warn;
use std::borrow::Cow;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};

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
    included_glob_set: Option<GlobSet>,
    excluded_glob_set: Option<GlobSet>,
}

impl Executor {
    fn is_excluded(&self, path: impl AsRef<Path> + Copy) -> bool {
        self.excluded_glob_set
            .as_ref()
            .is_some_and(|glob_set| glob_set.is_match(path))
    }

    fn is_file_included(&self, path: impl AsRef<Path> + Copy) -> bool {
        self.included_glob_set
            .as_ref()
            .is_none_or(|glob_set| glob_set.is_match(path))
            && !self.is_excluded(path)
    }
}

#[async_trait]
impl SourceExecutor for Executor {
    fn list<'a>(
        &'a self,
        options: &'a SourceExecutorListOptions,
    ) -> BoxStream<'a, Result<Vec<PartialSourceRowMetadata>>> {
        let root_component_size = self.root_path.components().count();
        let mut dirs = Vec::new();
        dirs.push(Cow::Borrowed(&self.root_path));
        let mut new_dirs = Vec::new();
        try_stream! {
            while let Some(dir) = dirs.pop() {
                let mut entries = tokio::fs::read_dir(dir.as_ref()).await?;
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    let mut path_components = path.components();
                    for _ in 0..root_component_size {
                        path_components.next();
                    }
                    let relative_path = path_components.as_path();
                    if path.is_dir() {
                        if !self.is_excluded(relative_path) {
                            new_dirs.push(Cow::Owned(path));
                        }
                    } else if self.is_file_included(relative_path) {
                        let ordinal: Option<Ordinal> = if options.include_ordinal {
                            Some(path.metadata()?.modified()?.try_into()?)
                        } else {
                            None
                        };
                        if let Some(relative_path) = relative_path.to_str() {
                            yield vec![PartialSourceRowMetadata {
                                key: KeyValue::Str(relative_path.into()),
                                ordinal,
                            }];
                        } else {
                            warn!("Skipped ill-formed file path: {}", path.display());
                        }
                    }
                }
                dirs.extend(new_dirs.drain(..).rev());
            }
        }
        .boxed()
    }

    async fn get_value(
        &self,
        key: &KeyValue,
        options: &SourceExecutorGetOptions,
    ) -> Result<PartialSourceRowData> {
        if !self.is_file_included(key.str_value()?.as_ref()) {
            return Ok(PartialSourceRowData {
                value: Some(SourceValue::NonExistence),
                ordinal: Some(Ordinal::unavailable()),
            });
        }
        let path = self.root_path.join(key.str_value()?.as_ref());
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
        Ok(PartialSourceRowData { value, ordinal })
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "LocalFile"
    }

    fn get_output_schema(
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
            TableKind::KTable,
            struct_schema,
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(Executor {
            root_path: PathBuf::from(spec.path),
            binary: spec.binary,
            included_glob_set: spec.included_patterns.map(build_glob_set).transpose()?,
            excluded_glob_set: spec.excluded_patterns.map(build_glob_set).transpose()?,
        }))
    }
}

fn build_glob_set(patterns: Vec<String>) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern.as_str())?);
    }
    Ok(builder.build()?)
}
