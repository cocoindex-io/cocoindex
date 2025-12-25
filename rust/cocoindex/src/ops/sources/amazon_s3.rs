use crate::fields_value;
use async_stream::try_stream;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use futures::StreamExt;
use redis::Client as RedisClient;
use std::sync::Arc;
use urlencoding;

use super::shared::pattern_matcher::PatternMatcher;
use crate::base::field_attrs;
use crate::ops::sdk::*;

/// Decode a form-encoded URL string, treating '+' as spaces
fn decode_form_encoded_url(input: &str) -> Result<Arc<str>> {
    // Replace '+' with spaces (form encoding convention), then decode
    // This handles both cases correctly:
    // - Literal '+' would be encoded as '%2B' and remain unchanged after replacement
    // - Space would be encoded as '+' and become ' ' after replacement
    let with_spaces = input.replace("+", " ");
    Ok(urlencoding::decode(&with_spaces)?.into())
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    redis_url: String,
    redis_channel: String,
}

#[derive(Debug, Deserialize)]
pub struct Spec {
    bucket_name: String,
    prefix: Option<String>,
    binary: bool,
    included_patterns: Option<Vec<String>>,
    excluded_patterns: Option<Vec<String>>,
    max_file_size: Option<i64>,
    sqs_queue_url: Option<String>,
    redis: Option<RedisConfig>,
    force_path_style: Option<bool>,
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

struct RedisContext {
    client: RedisClient,
    channel: String,
}

impl RedisContext {
    async fn new(redis_url: &str, channel: &str) -> Result<Self> {
        let client = RedisClient::open(redis_url)?;
        Ok(Self {
            client,
            channel: channel.to_string(),
        })
    }

    async fn subscribe(&self) -> Result<redis::aio::PubSub> {
        let mut pubsub = self.client.get_async_pubsub().await?;
        pubsub.subscribe(&self.channel).await?;
        Ok(pubsub)
    }
}

struct Executor {
    client: Client,
    bucket_name: String,
    prefix: Option<String>,
    binary: bool,
    pattern_matcher: PatternMatcher,
    max_file_size: Option<i64>,
    sqs_context: Option<Arc<SqsContext>>,
    redis_context: Option<Arc<RedisContext>>,
}

fn datetime_to_ordinal(dt: &aws_sdk_s3::primitives::DateTime) -> Ordinal {
    Ordinal(Some((dt.as_nanos() / 1000) as i64))
}

#[async_trait]
impl SourceExecutor for Executor {
    async fn list(
        &self,
        _options: &SourceExecutorReadOptions,
    ) -> Result<BoxStream<'async_trait, Result<Vec<PartialSourceRow>>>> {
        let stream = try_stream! {
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
                            // Check file size limit
                            if let Some(max_size) = self.max_file_size {
                                if let Some(size) = obj.size() {
                                    if size > max_size {
                                        continue;
                                    }
                                }
                            }
                            if self.pattern_matcher.is_file_included(key) {
                                batch.push(PartialSourceRow {
                                    key: KeyValue::from_single_part(key.to_string()),
                                    key_aux_info: serde_json::Value::Null,
                                    data: PartialSourceRowData {
                                        ordinal: obj.last_modified().map(datetime_to_ordinal),
                                        content_version_fp: None,
                                        value: None,
                                    },
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
        // Check file size limit
        if let Some(max_size) = self.max_file_size {
            let head_result = self
                .client
                .head_object()
                .bucket(&self.bucket_name)
                .key(key_str.as_ref())
                .send()
                .await?;
            if let Some(size) = head_result.content_length() {
                if size > max_size {
                    return Ok(PartialSourceRowData {
                        value: Some(SourceValue::NonExistence),
                        ordinal: Some(Ordinal::unavailable()),
                        content_version_fp: None,
                    });
                }
            }
        }
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key_str.as_ref())
            .send()
            .await;
        let obj = match resp {
            Err(e) if e.as_service_error().is_some_and(|e| e.is_no_such_key()) => {
                return Ok(PartialSourceRowData {
                    value: Some(SourceValue::NonExistence),
                    ordinal: Some(Ordinal::unavailable()),
                    content_version_fp: None,
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
                let (s, _) = utils::bytes_decode::bytes_to_string(&bytes);
                fields_value!(s)
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
        // Prefer Redis if both are configured, otherwise use SQS if available
        if let Some(redis_context) = &self.redis_context {
            let stream = stream! {
                loop {
                    match self.poll_redis(redis_context).await {
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
        } else if let Some(sqs_context) = &self.sqs_context {
            let stream = stream! {
                loop {
                     match self.poll_sqs(sqs_context).await {
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
        } else {
            Ok(None)
        }
    }

    fn provides_ordinal(&self) -> bool {
        true
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
                let notification: S3EventNotification = utils::deser::from_json_str(&body)?;
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
                        .is_none_or(|prefix| s3.object.key.starts_with(prefix))
                    {
                        continue;
                    }
                    if record.event_name.starts_with("ObjectCreated:")
                        || record.event_name.starts_with("ObjectRemoved:")
                    {
                        let decoded_key = decode_form_encoded_url(&s3.object.key)?;
                        changes.push(SourceChange {
                            key: KeyValue::from_single_part(decoded_key),
                            key_aux_info: serde_json::Value::Null,
                            data: PartialSourceRowData::default(),
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

    async fn poll_redis(
        &self,
        redis_context: &Arc<RedisContext>,
    ) -> Result<Vec<SourceChangeMessage>> {
        let mut pubsub = redis_context.subscribe().await?;
        let mut change_messages = vec![];

        // Wait for a message without timeout - long waiting is expected for event notifications
        let message = pubsub.on_message().next().await;

        if let Some(message) = message {
            let payload: String = message.get_payload()?;
            // Parse the Redis message - MinIO sends S3 event notifications in JSON format
            let notification: S3EventNotification = utils::deser::from_json_str(&payload)?;
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
                    .is_none_or(|prefix| s3.object.key.starts_with(prefix))
                {
                    continue;
                }

                if record.event_name.starts_with("ObjectCreated:")
                    || record.event_name.starts_with("ObjectRemoved:")
                {
                    let decoded_key = decode_form_encoded_url(&s3.object.key)?;
                    changes.push(SourceChange {
                        key: KeyValue::from_single_part(decoded_key),
                        key_aux_info: serde_json::Value::Null,
                        data: PartialSourceRowData::default(),
                    });
                }
            }

            if !changes.is_empty() {
                change_messages.push(SourceChangeMessage {
                    changes,
                    ack_fn: None, // Redis pub/sub doesn't require acknowledgment
                });
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
        let base_config = aws_config::load_defaults(BehaviorVersion::latest()).await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&base_config);
        if let Some(force_path_style) = spec.force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(force_path_style);
        }
        let s3_config = s3_config_builder.build();

        let redis_context = if let Some(redis_config) = &spec.redis {
            Some(Arc::new(
                RedisContext::new(&redis_config.redis_url, &redis_config.redis_channel).await?,
            ))
        } else {
            None
        };

        let sqs_context = spec.sqs_queue_url.map(|url| {
            Arc::new(SqsContext {
                client: aws_sdk_sqs::Client::new(&base_config),
                queue_url: url,
            })
        });

        Ok(Box::new(Executor {
            client: Client::from_conf(s3_config),
            bucket_name: spec.bucket_name,
            prefix: spec.prefix,
            binary: spec.binary,
            pattern_matcher: PatternMatcher::new(spec.included_patterns, spec.excluded_patterns)?,
            max_file_size: spec.max_file_size,
            sqs_context,
            redis_context,
        }))
    }
}
