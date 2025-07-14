use crate::fields_value;
use async_stream::try_stream;
use azure_core::prelude::NextMarker;
use azure_identity::{DefaultAzureCredential, TokenCredentialOptions};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use futures::StreamExt;
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::sync::Arc;

use crate::base::field_attrs;
use crate::ops::sdk::*;

#[derive(Debug, Deserialize)]
pub struct Spec {
    account_name: String,
    container_name: String,
    prefix: Option<String>,
    binary: bool,
    included_patterns: Option<Vec<String>>,
    excluded_patterns: Option<Vec<String>>,
}

struct Executor {
    client: BlobServiceClient,
    container_name: String,
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

fn datetime_to_ordinal(dt: &time::OffsetDateTime) -> Ordinal {
    Ordinal(Some(dt.unix_timestamp_nanos() as i64 / 1000))
}

#[async_trait]
impl SourceExecutor for Executor {
    fn list<'a>(
        &'a self,
        _options: &'a SourceExecutorListOptions,
    ) -> BoxStream<'a, Result<Vec<PartialSourceRowMetadata>>> {
        try_stream! {
            let mut continuation_token: Option<NextMarker> = None;
            loop {
                let mut list_builder = self.client
                    .container_client(&self.container_name)
                    .list_blobs();

                if let Some(p) = &self.prefix {
                    list_builder = list_builder.prefix(p.clone());
                }

                if let Some(token) = continuation_token.take() {
                    list_builder = list_builder.marker(token);
                }

                let mut page_stream = list_builder.into_stream();
                let Some(page_result) = page_stream.next().await else {
                    break;
                };

                let page = page_result?;
                let mut batch = Vec::new();

                for blob in page.blobs.blobs() {
                    let key = &blob.name;

                    // Only include files (not directories)
                    if key.ends_with('/') { continue; }

                    if self.is_file_included(key) {
                        let ordinal = Some(datetime_to_ordinal(&blob.properties.last_modified));
                        batch.push(PartialSourceRowMetadata {
                            key: KeyValue::Str(key.clone().into()),
                            ordinal,
                        });
                    }
                }

                if !batch.is_empty() {
                    yield batch;
                }

                continuation_token = page.next_marker;
                if continuation_token.is_none() {
                    break;
                }
            }
        }
        .boxed()
    }

    async fn get_value(
        &self,
        key: &KeyValue,
        options: &SourceExecutorGetOptions,
    ) -> Result<PartialSourceRowData> {
        let key_str = key.str_value()?;
        if !self.is_file_included(key_str) {
            return Ok(PartialSourceRowData {
                value: Some(SourceValue::NonExistence),
                ordinal: Some(Ordinal::unavailable()),
            });
        }

        let blob_client = self
            .client
            .container_client(&self.container_name)
            .blob_client(key_str.as_ref());

        let mut stream = blob_client.get().into_stream();
        let result = stream.next().await;

        let blob_response = match result {
            Some(response) => response?,
            None => {
                return Ok(PartialSourceRowData {
                    value: Some(SourceValue::NonExistence),
                    ordinal: Some(Ordinal::unavailable()),
                });
            }
        };

        let ordinal = if options.include_ordinal {
            Some(datetime_to_ordinal(
                &blob_response.blob.properties.last_modified,
            ))
        } else {
            None
        };

        let value = if options.include_value {
            let bytes = blob_response.data.collect().await?;
            Some(SourceValue::Existence(if self.binary {
                fields_value!(bytes)
            } else {
                fields_value!(String::from_utf8_lossy(&bytes).to_string())
            }))
        } else {
            None
        };

        Ok(PartialSourceRowData { value, ordinal })
    }

    async fn change_stream(
        &self,
    ) -> Result<Option<BoxStream<'async_trait, Result<SourceChangeMessage>>>> {
        // Azure Blob Storage doesn't have built-in change notifications like S3+SQS
        Ok(None)
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "AzureBlob"
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
            TableKind::KTable,
            struct_schema,
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        let default_credential = Arc::new(DefaultAzureCredential::create(
            TokenCredentialOptions::default(),
        )?);
        let client = BlobServiceClient::new(
            &spec.account_name,
            StorageCredentials::token_credential(default_credential),
        );
        Ok(Box::new(Executor {
            client,
            container_name: spec.container_name,
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
