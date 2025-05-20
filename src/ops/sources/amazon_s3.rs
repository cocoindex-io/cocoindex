use crate::fields_value;
use async_stream::try_stream;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::sync::Arc;

use crate::base::field_attrs;
use crate::ops::sdk::*;

#[derive(Debug, Deserialize)]
pub struct Spec {
    bucket_name: String,
    prefix: Option<String>,
    binary: bool,
    included_patterns: Option<Vec<String>>,
    excluded_patterns: Option<Vec<String>>,
    sqs_queue_url: Option<String>,
}

struct SqsContext {
    client: aws_sdk_sqs::Client,
    queue_url: String,
}

impl SqsContext {
    async fn delete_message(&self, receipt_handle: String) -> Result<()> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await?;
        Ok(())
    }
}

struct Executor {
    client: Client,
    bucket_name: String,
    prefix: Option<String>,
    binary: bool,
    included_glob_set: Option<GlobSet>,
    excluded_glob_set: Option<GlobSet>,
    sqs_context: Option<Arc<SqsContext>>,
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

fn datetime_to_ordinal(dt: &aws_sdk_s3::primitives::DateTime) -> Ordinal {
    Ordinal(Some((dt.as_nanos() / 1000) as i64))
}

#[async_trait]
impl SourceExecutor for Executor {
    fn list<'a>(
        &'a self,
        _options: &'a SourceExecutorListOptions,
    ) -> BoxStream<'a, Result<Vec<PartialSourceRowMetadata>>> {
        try_stream! {
            let mut continuation_token = None;
            loop {
                let mut req = self.client
                    .list_objects_v2()
                    .bucket(&self.bucket_name);
                if let Some(ref p) = self.prefix {
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
                            let include = self.included_glob_set
                                .as_ref()
                                .map(|gs| gs.is_match(key))
                                .unwrap_or(true);
                            let exclude = self.excluded_glob_set
                                .as_ref()
                                .map(|gs| gs.is_match(key))
                                .unwrap_or(false);
                            if include && !exclude {
                                batch.push(PartialSourceRowMetadata {
                                    key: KeyValue::Str(key.to_string().into()),
                                    ordinal: obj.last_modified().map(datetime_to_ordinal),
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
    ) -> Result<PartialSourceRowData> {
        let key_str = key.str_value()?;
        if !self.is_file_included(key_str) {
            return Ok(PartialSourceRowData {
                value: Some(SourceValue::NonExistence),
                ordinal: Some(Ordinal::unavailable()),
            });
        }
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key_str.as_ref())
            .send()
            .await;
        let obj = match resp {
            Err(e) if e.as_service_error().map_or(false, |e| e.is_no_such_key()) => {
                return Ok(PartialSourceRowData {
                    value: Some(SourceValue::NonExistence),
                    ordinal: Some(Ordinal::unavailable()),
                });
            }
            r => r?,
        };
        let ordinal = if options.include_ordinal {
            obj.last_modified().map(datetime_to_ordinal)
        } else {
            None
        };
        let value = if options.include_value {
            let bytes = obj.body.collect().await?.into_bytes();
            Some(SourceValue::Existence(if self.binary {
                fields_value!(bytes.to_vec())
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
        let sqs_context = if let Some(sqs_context) = &self.sqs_context {
            sqs_context
        } else {
            return Ok(None);
        };
        let stream = stream! {
            loop {
                 match self.poll_sqs(&sqs_context).await {
                    Ok(messages) => {
                        for message in messages {
                            yield Ok(message);
                        }
                    }
                    Err(e) => {
                        yield Err(e);
                    }
                };
            }
        };
        Ok(Some(stream.boxed()))
    }
}

#[derive(Debug, Deserialize)]
pub struct S3EventNotification {
    #[serde(default, rename = "Records")]
    pub records: Vec<S3EventRecord>,
}

#[derive(Debug, Deserialize)]
pub struct S3EventRecord {
    #[serde(rename = "eventName")]
    pub event_name: String,
    pub s3: Option<S3Entity>,
}

#[derive(Debug, Deserialize)]
pub struct S3Entity {
    pub bucket: S3Bucket,
    pub object: S3Object,
}

#[derive(Debug, Deserialize)]
pub struct S3Bucket {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct S3Object {
    pub key: String,
}

impl Executor {
    async fn poll_sqs(&self, sqs_context: &Arc<SqsContext>) -> Result<Vec<SourceChangeMessage>> {
        let resp = sqs_context
            .client
            .receive_message()
            .queue_url(&sqs_context.queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(20)
            .send()
            .await?;
        let messages = if let Some(messages) = resp.messages {
            messages
        } else {
            return Ok(Vec::new());
        };
        let mut change_messages = vec![];
        for message in messages.into_iter() {
            if let Some(body) = message.body {
                let notification: S3EventNotification = serde_json::from_str(&body)?;
                let mut changes = vec![];
                for record in notification.records {
                    let s3 = if let Some(s3) = record.s3 {
                        s3
                    } else {
                        continue;
                    };
                    if s3.bucket.name != self.bucket_name {
                        continue;
                    }
                    if !self
                        .prefix
                        .as_ref()
                        .map_or(true, |prefix| s3.object.key.starts_with(prefix))
                    {
                        continue;
                    }
                    if record.event_name.starts_with("ObjectCreated:")
                        || record.event_name.starts_with("ObjectRemoved:")
                    {
                        changes.push(SourceChange {
                            key: KeyValue::Str(s3.object.key.into()),
                            data: None,
                        });
                    }
                }
                if let Some(receipt_handle) = message.receipt_handle {
                    if !changes.is_empty() {
                        let sqs_context = sqs_context.clone();
                        change_messages.push(SourceChangeMessage {
                            changes,
                            ack_fn: Some(Box::new(move || {
                                async move { sqs_context.delete_message(receipt_handle).await }
                                    .boxed()
                            })),
                        });
                    } else {
                        sqs_context.delete_message(receipt_handle).await?;
                    }
                }
            }
        }
        Ok(change_messages)
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "AmazonS3"
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
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        Ok(Box::new(Executor {
            client: Client::new(&config),
            bucket_name: spec.bucket_name,
            prefix: spec.prefix,
            binary: spec.binary,
            included_glob_set: spec.included_patterns.map(build_glob_set).transpose()?,
            excluded_glob_set: spec.excluded_patterns.map(build_glob_set).transpose()?,
            sqs_context: spec.sqs_queue_url.map(|url| {
                Arc::new(SqsContext {
                    client: aws_sdk_sqs::Client::new(&config),
                    queue_url: url,
                })
            }),
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
