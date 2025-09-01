use crate::fields_value;
use async_stream::try_stream;
use azure_core::prelude::NextMarker;
use azure_identity::{DefaultAzureCredential, TokenCredentialOptions};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use futures::StreamExt;
use std::sync::Arc;

use super::shared::pattern_matcher::PatternMatcher;
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

    /// SAS token for authentication. Takes precedence over account_access_key.
    sas_token: Option<AuthEntryReference<String>>,
    /// Account access key for authentication. If not provided, will use default Azure credential.
    account_access_key: Option<AuthEntryReference<String>>,
}

struct Executor {
    client: BlobServiceClient,
    container_name: String,
    prefix: Option<String>,
    binary: bool,
    pattern_matcher: PatternMatcher,
}

fn datetime_to_ordinal(dt: &time::OffsetDateTime) -> Ordinal {
    Ordinal(Some(dt.unix_timestamp_nanos() as i64 / 1000))
}

#[async_trait]
impl SourceExecutor for Executor {
    async fn list(
        &self,
        _options: &SourceExecutorReadOptions,
    ) -> Result<BoxStream<'async_trait, Result<Vec<PartialSourceRow>>>> {
        let stream = try_stream! {
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

                    if self.pattern_matcher.is_file_included(key) {
                        let ordinal = Some(datetime_to_ordinal(&blob.properties.last_modified));
                        batch.push(PartialSourceRow {
                            key: KeyValue::from_single_part(key.clone()),
                            key_aux_info: serde_json::Value::Null,
                            data: PartialSourceRowData {
                                ordinal,
                                content_version_fp: None,
                                value: None,
                            },
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
        };
        Ok(stream.boxed())
    }

    async fn get_value(
        &self,
        key: &KeyValue,
        _key_aux_info: &serde_json::Value,
        options: &SourceExecutorReadOptions,
    ) -> Result<PartialSourceRowData> {
        let key_str = key.single_part()?.str_value()?;
        if !self.pattern_matcher.is_file_included(key_str) {
            return Ok(PartialSourceRowData {
                value: Some(SourceValue::NonExistence),
                ordinal: Some(Ordinal::unavailable()),
                content_version_fp: None,
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
                    content_version_fp: None,
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

        Ok(PartialSourceRowData {
            value,
            ordinal,
            content_version_fp: None,
        })
    }

    async fn change_stream(
        &self,
    ) -> Result<Option<BoxStream<'async_trait, Result<SourceChangeMessage>>>> {
        // Azure Blob Storage doesn't have built-in change notifications like S3+SQS
        Ok(None)
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
            TableKind::KTable(KTableInfo { num_key_parts: 1 }),
            struct_schema,
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        _source_name: &str,
        spec: Spec,
        context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        let credential = if let Some(sas_token) = spec.sas_token {
            let sas_token = context.auth_registry.get(&sas_token)?;
            StorageCredentials::sas_token(sas_token)?
        } else if let Some(account_access_key) = spec.account_access_key {
            let account_access_key = context.auth_registry.get(&account_access_key)?;
            StorageCredentials::access_key(spec.account_name.clone(), account_access_key)
        } else {
            let default_credential = Arc::new(DefaultAzureCredential::create(
                TokenCredentialOptions::default(),
            )?);
            StorageCredentials::token_credential(default_credential)
        };

        let client = BlobServiceClient::new(&spec.account_name, credential);
        Ok(Box::new(Executor {
            client,
            container_name: spec.container_name,
            prefix: spec.prefix,
            binary: spec.binary,
            pattern_matcher: PatternMatcher::new(spec.included_patterns, spec.excluded_patterns)?,
        }))
    }
}
