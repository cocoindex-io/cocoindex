use anyhow::Result;
use futures::future::try_join_all;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use yaml_rust2::YamlEmitter;

use super::indexer;
use crate::base::{schema, value};
use crate::builder::plan::{AnalyzedExportOp, AnalyzedSourceOp, ExecutionPlan};
use crate::utils::yaml_ser::YamlSerializer;

#[derive(Debug, Clone, Deserialize)]
pub struct DumpEvaluationOutputOptions {
    pub output_dir: String,
    pub use_cache: bool,
}

const FILENAME_PREFIX_MAX_LENGTH: usize = 128;

struct TargetExportData<'a> {
    schema: &'a Vec<schema::FieldSchema>,
    data: BTreeMap<value::KeyValue, &'a value::FieldValues>,
}

impl<'a> Serialize for TargetExportData<'a> {
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
    exports: Option<IndexMap<&'a str, TargetExportData<'a>>>,
    error: Option<String>,
}

struct Dumper<'a> {
    plan: &'a ExecutionPlan,
    schema: &'a schema::DataSchema,
    pool: &'a PgPool,
    options: DumpEvaluationOutputOptions,
}

impl<'a> Dumper<'a> {
    async fn evaluate_source_entry<'b>(
        &'a self,
        source_op: &'a AnalyzedSourceOp,
        key: &value::KeyValue,
        collected_values_buffer: &'b mut Vec<Vec<value::FieldValues>>,
    ) -> Result<Option<IndexMap<&'b str, TargetExportData<'b>>>>
    where
        'a: 'b,
    {
        let cache_option = if self.options.use_cache {
            indexer::EvaluationCacheOption::UseCache(self.pool)
        } else {
            indexer::EvaluationCacheOption::NoCache
        };

        let data_builder = indexer::evaluate_source_entry_with_cache(
            self.plan,
            source_op,
            self.schema,
            key,
            cache_option,
        )
        .await?;

        let data_builder = if let Some(data_builder) = data_builder {
            data_builder
        } else {
            return Ok(None);
        };

        *collected_values_buffer = data_builder
            .collected_values
            .into_iter()
            .map(|v| v.into_inner().unwrap())
            .collect();
        let exports = self
            .plan
            .export_ops
            .iter()
            .map(|export_op| -> Result<_> {
                let collector_idx = export_op.input.collector_idx as usize;
                let entry = (
                    export_op.name.as_str(),
                    TargetExportData {
                        schema: &self.schema.collectors[collector_idx].spec.fields,
                        data: collected_values_buffer
                            .iter()
                            .map(|v| -> Result<_> {
                                let key = indexer::extract_primary_key(
                                    &export_op.primary_key_def,
                                    &v[collector_idx],
                                )?;
                                Ok((key, &v[collector_idx]))
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
        source_op: &AnalyzedSourceOp,
        key: value::KeyValue,
        file_name: PathBuf,
    ) -> Result<()> {
        let mut collected_values_buffer = Vec::new();
        let (exports, error) = match self
            .evaluate_source_entry(source_op, &key, &mut collected_values_buffer)
            .await
        {
            Ok(exports) => (exports, None),
            Err(e) => (None, Some(format!("{e:?}"))),
        };
        let key_value = value::Value::from(key);
        let file_data = SourceOutputData {
            key: value::TypedValue {
                t: &self.schema.fields[source_op.output.field_idx as usize]
                    .value_type
                    .typ,
                v: &key_value,
            },
            exports,
            error,
        };
        let yaml_output = {
            let mut yaml_output = String::new();
            let yaml_data = YamlSerializer::serialize(&file_data)?;
            let mut yaml_emitter = YamlEmitter::new(&mut yaml_output);
            yaml_emitter.dump(&yaml_data)?;
            yaml_output
        };
        let mut file_path = file_name;
        file_path.push(".yaml");
        tokio::fs::write(file_path, yaml_output).await?;
        Ok(())
    }

    async fn evaluate_and_dump_for_source_op(&self, source_op: &AnalyzedSourceOp) -> Result<()> {
        let all_keys = source_op.executor.list_keys().await?;

        let mut keys_by_filename_prefix: IndexMap<String, Vec<value::KeyValue>> = IndexMap::new();
        for key in all_keys {
            let mut s = key
                .to_strs()
                .into_iter()
                .map(|s| urlencoding::encode(&s).into_owned())
                .join(":");
            s.truncate(
                (0..FILENAME_PREFIX_MAX_LENGTH)
                    .rev()
                    .find(|i| s.is_char_boundary(*i))
                    .unwrap_or(0),
            );
            keys_by_filename_prefix.entry(s).or_default().push(key);
        }

        let evaluate_futs =
            keys_by_filename_prefix
                .into_iter()
                .flat_map(|(filename_prefix, keys)| {
                    let num_keys = keys.len();
                    keys.into_iter().enumerate().map(move |(i, key)| {
                        let mut file_path =
                            Path::new(&self.options.output_dir).join(Path::new(&filename_prefix));
                        if num_keys > 1 {
                            file_path.push(format!(".{}", i));
                        }
                        self.evaluate_and_dump_source_entry(source_op, key, file_path)
                    })
                });
        try_join_all(evaluate_futs).await?;
        Ok(())
    }

    async fn evaluate_and_dump(&self) -> Result<()> {
        try_join_all(
            self.plan
                .source_ops
                .iter()
                .map(|source_op| self.evaluate_and_dump_for_source_op(source_op)),
        )
        .await?;
        Ok(())
    }
}

pub async fn dump_evaluation_output(
    plan: &ExecutionPlan,
    schema: &schema::DataSchema,
    options: DumpEvaluationOutputOptions,
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
