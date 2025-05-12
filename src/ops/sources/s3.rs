use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_config::Region;
use async_stream::try_stream;
use globset::{Glob, GlobSet, GlobSetBuilder};
use log::warn;
use std::sync::Arc;
use crate::fields_value;

use crate::base::field_attrs;
use crate::ops::sdk::*;

#[derive(Debug, Deserialize)]
pub struct Spec {
    bucket_name: String,
    prefix: Option<String>,
    binary: bool,
    included_patterns: Option<Vec<String>>,
    excluded_patterns: Option<Vec<String>>,
}

struct Executor {
    client: Client,
    bucket_name: String,
    prefix: Option<String>,
    binary: bool,
    included_glob_set: Option<GlobSet>,
    excluded_glob_set: Option<GlobSet>,
}

impl Executor {
    fn is_excluded(&self, key: &str) -> bool {
        self.excluded_glob_set
            .as_ref()
            .is_some_and(|glob_set| glob_set.is_match(key))
    }

    fn is_file_included(&self, key: &str) -> bool {
        self.included_glob_set
            .as_ref()
            .is_none_or(|glob_set| glob_set.is_match(key))
            && !self.is_excluded(key)
    }
}

#[async_trait]
impl SourceExecutor for Executor {
    fn list<'a>(
        &'a self,
        _options: &'a SourceExecutorListOptions,
    ) -> BoxStream<'a, Result<Vec<SourceRowMetadata>>> {
        let client = self.client.clone();
        let bucket = self.bucket_name.clone();
        let prefix = self.prefix.clone();
        let included_glob_set = self.included_glob_set.clone();
        let excluded_glob_set = self.excluded_glob_set.clone();
        try_stream! {
            let mut continuation_token = None;
            loop {
                let mut req = client
                    .list_objects_v2()
                    .bucket(&bucket);
                if let Some(ref p) = prefix {
                    req = req.prefix(p);
                }
                if let Some(ref token) = continuation_token {
                    req = req.continuation_token(token);
                }
                let resp = req.send().await?;
                if let Some(contents) = &resp.contents {
                    let mut batch = Vec::new();
                    for obj in contents {
                        if let Some(key) = obj.key() {
                            // Only include files (not folders)
                            if key.ends_with('/') { continue; }
                            let include = included_glob_set
                                .as_ref()
                                .map(|gs| gs.is_match(key))
                                .unwrap_or(true);
                            let exclude = excluded_glob_set
                                .as_ref()
                                .map(|gs| gs.is_match(key))
                                .unwrap_or(false);
                            if include && !exclude {
                                batch.push(SourceRowMetadata {
                                    key: KeyValue::Str(key.to_string().into()),
                                    ordinal: obj.last_modified().map(|dt| Ordinal(dt.secs() as i64)),
                                });
                            }
                        }
                    }
                    if !batch.is_empty() {
                        yield batch;
                    }
                }
                if resp.is_truncated == Some(true) {
                    continuation_token = resp.next_continuation_token.clone().map(|s| s.to_string());
                } else {
                    break;
                }
            }
        }.boxed()
    }

    async fn get_value(
        &self,
        key: &KeyValue,
        options: &SourceExecutorGetOptions,
    ) -> Result<Option<SourceValue>> {
        let key_str = key.str_value()?;
        if !self.is_file_included(key_str) {
            return Ok(None);
        }
        let resp = self.client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key_str.as_ref())
            .send()
            .await;
        let obj = match resp {
            Ok(o) => o,
            Err(e) => {
                warn!("Failed to fetch S3 object {}: {}", key_str, e);
                return Ok(None);
            }
        };
        let bytes = obj.body.collect().await?.into_bytes();
        let value = if options.include_value {
            Some(if self.binary {
                fields_value!(bytes.to_vec())
            } else {
                match String::from_utf8(bytes.to_vec()) {
                    Ok(s) => fields_value!(s),
                    Err(e) => {
                        warn!("Failed to decode S3 object {} as UTF-8: {}", key_str, e);
                        return Ok(None);
                    }
                }
            })
        } else {
            None
        };
        Ok(Some(SourceValue { value, ordinal: None }))
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "S3"
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
        let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
        let config = aws_config::from_env().region(region_provider).load().await;
        let client = Client::new(&config);
        Ok(Box::new(Executor {
            client,
            bucket_name: spec.bucket_name,
            prefix: spec.prefix,
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