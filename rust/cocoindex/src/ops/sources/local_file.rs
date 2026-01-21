use async_stream::try_stream;
use std::borrow::Cow;
use std::fs::Metadata;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};
use tracing::warn;

use super::shared::pattern_matcher::PatternMatcher;
use crate::base::field_attrs;
use crate::{fields_value, ops::sdk::*};

#[derive(Debug, Deserialize)]
pub struct Spec {
    path: String,
    binary: bool,
    included_patterns: Option<Vec<String>>,
    excluded_patterns: Option<Vec<String>>,
    max_file_size: Option<i64>,
}

struct Executor {
    root_path: PathBuf,
    binary: bool,
    pattern_matcher: PatternMatcher,
    max_file_size: Option<i64>,
}

async fn ensure_metadata<'a>(
    path: &Path,
    metadata: &'a mut Option<Metadata>,
) -> std::io::Result<&'a Metadata> {
    if metadata.is_none() {
        // Follow symlinks.
        *metadata = Some(tokio::fs::metadata(path).await?);
    }
    Ok(metadata.as_ref().unwrap())
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
                    // We stat per entry at most once when needed.
                    let mut metadata: Option<Metadata> = None;

                    // For symlinks, if the target doesn't exist, log and skip.
                    let file_type = entry.file_type().await?;
                    if file_type.is_symlink() {
                        if let Err(e) = ensure_metadata(&path, &mut metadata).await {
                            if e.kind() == std::io::ErrorKind::NotFound {
                                warn!("Skipped broken symlink: {}", path.display());
                                continue;
                            }
                            Err(e)?;
                        }
                    }
                    let is_dir = if file_type.is_dir() {
                        true
                    } else if file_type.is_symlink() {
                        // Follow symlinks to classify the target.
                        ensure_metadata(&path, &mut metadata).await?.is_dir()
                    } else {
                        false
                    };
                    if is_dir {
                        if !self.pattern_matcher.is_excluded(relative_path) {
                            new_dirs.push(Cow::Owned(path));
                        }
                    } else if self.pattern_matcher.is_file_included(relative_path) {
                        // Check file size limit
                        if let Some(max_size) = self.max_file_size
                            && let Ok(metadata) = ensure_metadata(&path, &mut metadata).await
                            && metadata.len() > max_size as u64
                        {
                            continue;
                        }
                        let ordinal: Option<Ordinal> = if options.include_ordinal {
                            let metadata = ensure_metadata(&path, &mut metadata).await?;
                            Some(metadata.modified()?.try_into()?)
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
        let mut metadata: Option<Metadata> = None;
        // Check file size limit
        if let Some(max_size) = self.max_file_size {
            if let Ok(metadata) = ensure_metadata(&path, &mut metadata).await {
                if metadata.len() > max_size as u64 {
                    return Ok(PartialSourceRowData {
                        value: Some(SourceValue::NonExistence),
                        ordinal: Some(Ordinal::unavailable()),
                        content_version_fp: None,
                    });
                }
            }
        }
        let ordinal = if options.include_ordinal {
            let metadata = ensure_metadata(&path, &mut metadata).await?;
            Some(metadata.modified()?.try_into()?)
        } else {
            None
        };
        let value = if options.include_value {
            match std::fs::read(path) {
                Ok(content) => {
                    let content = if self.binary {
                        fields_value!(content)
                    } else {
                        let (s, _) = utils::bytes_decode::bytes_to_string(&content);
                        fields_value!(s)
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
            max_file_size: spec.max_file_size,
        }))
    }
}
