use globset::{Glob, GlobSet, GlobSetBuilder};
use log::warn;
use std::{path::PathBuf, sync::Arc};

use crate::{fields_value, ops::sdk::*};

#[derive(Debug, Deserialize)]
pub struct Spec {
    path: String,
    binary: bool,
    included_patterns: Option<Vec<String>>,
    excluded_patterns: Option<Vec<String>>,
}

struct Executor {
    root_path_str: String,
    root_path: PathBuf,
    binary: bool,
    included_glob_set: Option<GlobSet>,
    excluded_glob_set: Option<GlobSet>,
}

impl Executor {
    fn is_excluded(&self, path: &str) -> bool {
        self.excluded_glob_set
            .as_ref()
            .map_or(false, |glob_set| glob_set.is_match(path))
    }

    fn is_file_included(&self, path: &str) -> bool {
        self.included_glob_set
            .as_ref()
            .map_or(true, |glob_set| glob_set.is_match(path))
            && !self.is_excluded(path)
    }

    async fn traverse_dir(&self, dir_path: &PathBuf, result: &mut Vec<KeyValue>) -> Result<()> {
        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(file_name) = path.to_str() {
                let relative_path = &file_name[self.root_path_str.len() + 1..];
                if path.is_dir() {
                    if !self.is_excluded(relative_path) {
                        Box::pin(self.traverse_dir(&path, result)).await?;
                    }
                } else {
                    if self.is_file_included(relative_path) {
                        result.push(KeyValue::Str(Arc::from(relative_path)));
                    }
                }
            } else {
                warn!("Skipped ill-formed file path: {}", path.display());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SourceExecutor for Executor {
    async fn list_keys(&self) -> Result<Vec<KeyValue>> {
        let mut result = Vec::new();
        self.traverse_dir(&self.root_path, &mut result).await?;
        Ok(result)
    }

    async fn get_value(&self, key: &KeyValue) -> Result<Option<FieldValues>> {
        if !self.is_file_included(key.str_value()?.as_ref()) {
            return Ok(None);
        }
        let path = self.root_path.join(key.str_value()?.as_ref());
        let result = match std::fs::read(path) {
            Ok(content) => {
                let content = if self.binary {
                    fields_value!(content)
                } else {
                    fields_value!(String::from_utf8_lossy(&content).to_string())
                };
                Some(content)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        Ok(result)
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
        Ok(make_output_type(CollectionSchema::new(
            CollectionKind::Table,
            vec![
                FieldSchema::new("filename", make_output_type(BasicValueType::Str)),
                FieldSchema::new(
                    "content",
                    make_output_type(if spec.binary {
                        BasicValueType::Bytes
                    } else {
                        BasicValueType::Str
                    }),
                ),
            ],
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(Executor {
            root_path_str: spec.path.clone(),
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
