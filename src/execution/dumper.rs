use anyhow::Result;
use futures::StreamExt;
use futures::future::try_join_all;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use yaml_rust2::YamlEmitter;

use super::evaluator::SourceRowEvaluationContext;
use super::memoization::EvaluationMemoryOptions;
use super::row_indexer;
use crate::base::{schema, value};
use crate::builder::plan::{AnalyzedImportOp, ExecutionPlan};
use crate::ops::interface::SourceExecutorListOptions;
use crate::utils::yaml_ser::YamlSerializer;

#[derive(Debug, Clone, Deserialize)]
pub struct EvaluateAndDumpOptions {
    pub output_dir: String,
    pub use_cache: bool,
}

const FILENAME_PREFIX_MAX_LENGTH: usize = 128;

struct TargetExportData<'a> {
    schema: &'a Vec<schema::FieldSchema>,
    // The purpose is to make rows sorted by primary key.
    data: BTreeMap<value::KeyValue, &'a value::FieldValues>,
}

impl Serialize for TargetExportData<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.data.len()))?;
        for (_, values) in self.data.iter() {
            seq.serialize_element(&value::TypedFieldsValue {
                schema: self.schema,
                values_iter: values.fields.iter(),
            })?;
        }
        seq.end()
    }
}

#[derive(Serialize)]
struct SourceOutputData<'a> {
    key: value::TypedValue<'a>,

    #[serde(skip_serializing_if = "Option::is_none")]
    exports: Option<IndexMap<&'a str, TargetExportData<'a>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

struct Dumper<'a> {
    plan: &'a ExecutionPlan,
    schema: &'a schema::FlowSchema,
    pool: &'a PgPool,
    options: EvaluateAndDumpOptions,
}

impl<'a> Dumper<'a> {
    async fn evaluate_source_entry<'b>(
        &'a self,
        import_op: &'a AnalyzedImportOp,
        key: &value::KeyValue,
        collected_values_buffer: &'b mut Vec<Vec<value::FieldValues>>,
    ) -> Result<Option<IndexMap<&'b str, TargetExportData<'b>>>>
    where
        'a: 'b,
    {
        let data_builder = row_indexer::evaluate_source_entry_with_memory(
            &SourceRowEvaluationContext {
                plan: self.plan,
                import_op,
                schema: self.schema,
                key,
            },
            EvaluationMemoryOptions {
                enable_cache: self.options.use_cache,
                evaluation_only: true,
            },
            self.pool,
        )
        .await?;

        let data_builder = if let Some(data_builder) = data_builder {
            data_builder
        } else {
            return Ok(None);
        };

        *collected_values_buffer = data_builder.collected_values;
        let exports = self
            .plan
            .export_ops
            .iter()
            .map(|export_op| -> Result<_> {
                let collector_idx = export_op.input.collector_idx as usize;
                let entry = (
                    export_op.name.as_str(),
                    TargetExportData {
                        schema: &self.schema.root_op_scope.collectors[collector_idx]
                            .spec
                            .fields,
                        data: collected_values_buffer[collector_idx]
                            .iter()
                            .map(|v| -> Result<_> {
                                let key = row_indexer::extract_primary_key(
                                    &export_op.primary_key_def,
                                    v,
                                )?;
                                Ok((key, v))
                            })
                            .collect::<Result<_>>()?,
                    },
                );
                Ok(entry)
            })
            .collect::<Result<_>>()?;
        Ok(Some(exports))
    }

    async fn evaluate_and_dump_source_entry(
        &self,
        import_op: &AnalyzedImportOp,
        key: value::KeyValue,
        file_path: PathBuf,
    ) -> Result<()> {
        let mut collected_values_buffer = Vec::new();
        let (exports, error) = match self
            .evaluate_source_entry(import_op, &key, &mut collected_values_buffer)
            .await
        {
            Ok(exports) => (exports, None),
            Err(e) => (None, Some(format!("{e:?}"))),
        };
        let key_value = value::Value::from(key);
        let file_data = SourceOutputData {
            key: value::TypedValue {
                t: &import_op.primary_key_type,
                v: &key_value,
            },
            exports,
            error,
        };

        let yaml_output = {
            let mut yaml_output = String::new();
            let yaml_data = YamlSerializer::serialize(&file_data)?;
            let mut yaml_emitter = YamlEmitter::new(&mut yaml_output);
            yaml_emitter.multiline_strings(true);
            yaml_emitter.compact(true);
            yaml_emitter.dump(&yaml_data)?;
            yaml_output
        };
        tokio::fs::write(file_path, yaml_output).await?;

        Ok(())
    }

    async fn evaluate_and_dump_for_source(&self, import_op: &AnalyzedImportOp) -> Result<()> {
        let mut keys_by_filename_prefix: IndexMap<String, Vec<value::KeyValue>> = IndexMap::new();

        let mut rows_stream = import_op.executor.list(&SourceExecutorListOptions {
            include_ordinal: false,
        });
        while let Some(rows) = rows_stream.next().await {
            for row in rows?.into_iter() {
                let mut s = row
                    .key
                    .to_strs()
                    .into_iter()
                    .map(|s| urlencoding::encode(&s).into_owned())
                    .join(":");
                s.truncate(
                    (0..(FILENAME_PREFIX_MAX_LENGTH - import_op.name.as_str().len()))
                        .rev()
                        .find(|i| s.is_char_boundary(*i))
                        .unwrap_or(0),
                );
                keys_by_filename_prefix.entry(s).or_default().push(row.key);
            }
        }
        let output_dir = Path::new(&self.options.output_dir);
        let evaluate_futs =
            keys_by_filename_prefix
                .into_iter()
                .flat_map(|(filename_prefix, keys)| {
                    let num_keys = keys.len();
                    keys.into_iter().enumerate().map(move |(i, key)| {
                        let extra_id = if num_keys > 1 {
                            Cow::Owned(format!(".{}", i))
                        } else {
                            Cow::Borrowed("")
                        };
                        let file_name =
                            format!("{}@{}{}.yaml", import_op.name, filename_prefix, extra_id);
                        let file_path = output_dir.join(Path::new(&file_name));
                        self.evaluate_and_dump_source_entry(import_op, key, file_path)
                    })
                });
        try_join_all(evaluate_futs).await?;
        Ok(())
    }

    async fn evaluate_and_dump(&self) -> Result<()> {
        try_join_all(
            self.plan
                .import_ops
                .iter()
                .map(|import_op| self.evaluate_and_dump_for_source(import_op)),
        )
        .await?;
        Ok(())
    }
}

pub async fn evaluate_and_dump(
    plan: &ExecutionPlan,
    schema: &schema::FlowSchema,
    options: EvaluateAndDumpOptions,
    pool: &PgPool,
) -> Result<()> {
    let output_dir = Path::new(&options.output_dir);
    if output_dir.exists() {
        if !output_dir.is_dir() {
            return Err(anyhow::anyhow!("The path exists and is not a directory"));
        }
    } else {
        tokio::fs::create_dir(output_dir).await?;
    }

    let dumper = Dumper {
        plan,
        schema,
        pool,
        options,
    };
    dumper.evaluate_and_dump().await
}
