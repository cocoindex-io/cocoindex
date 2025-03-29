use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, LazyLock},
};

use async_stream::try_stream;
use google_drive3::{
    api::{File, Scope},
    yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator},
    DriveHub,
};
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use log::{trace, warn};

use crate::base::field_attrs;
use crate::ops::sdk::*;

struct ExportMimeType {
    text: &'static str,
    binary: &'static str,
}

const FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";
const FILE_MIME_TYPE: &str = "application/vnd.google-apps.file";
static EXPORT_MIME_TYPES: LazyLock<HashMap<&'static str, ExportMimeType>> = LazyLock::new(|| {
    HashMap::from([
        (
            "application/vnd.google-apps.document",
            ExportMimeType {
                text: "text/markdown",
                binary: "application/pdf",
            },
        ),
        (
            "application/vnd.google-apps.spreadsheet",
            ExportMimeType {
                text: "text/csv",
                binary: "application/pdf",
            },
        ),
        (
            "application/vnd.google-apps.presentation",
            ExportMimeType {
                text: "text/plain",
                binary: "application/pdf",
            },
        ),
        (
            "application/vnd.google-apps.drawing",
            ExportMimeType {
                text: "image/svg+xml",
                binary: "image/png",
            },
        ),
        (
            "application/vnd.google-apps.script",
            ExportMimeType {
                text: "application/vnd.google-apps.script+json",
                binary: "application/vnd.google-apps.script+json",
            },
        ),
    ])
});

fn is_supported_file_type(mime_type: &str) -> bool {
    !mime_type.starts_with("application/vnd.google-apps.")
        || EXPORT_MIME_TYPES.contains_key(mime_type)
        || mime_type == FILE_MIME_TYPE
}

#[derive(Debug, Deserialize)]
pub struct Spec {
    service_account_credential_path: String,
    binary: bool,
    root_folder_ids: Vec<String>,
}

struct Executor {
    drive_hub: DriveHub<HttpsConnector<HttpConnector>>,
    binary: bool,
    root_folder_ids: Vec<Arc<str>>,
}

impl Executor {
    async fn new(spec: Spec) -> Result<Self> {
        let service_account_key =
            read_service_account_key(spec.service_account_credential_path).await?;
        let auth = ServiceAccountAuthenticator::builder(service_account_key)
            .build()
            .await?;
        let client =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                .build(
                    hyper_rustls::HttpsConnectorBuilder::new()
                        .with_provider_and_native_roots(
                            rustls::crypto::aws_lc_rs::default_provider(),
                        )?
                        .https_only()
                        .enable_http2()
                        .build(),
                );
        let drive_hub = DriveHub::new(client, auth);
        Ok(Self {
            drive_hub,
            binary: spec.binary,
            root_folder_ids: spec.root_folder_ids.into_iter().map(Arc::from).collect(),
        })
    }
}

fn escape_string(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\'' | '\\' => escaped.push('\\'),
            _ => {}
        }
        escaped.push(c);
    }
    escaped
}

impl Executor {
    fn visit_file(
        &self,
        file: File,
        new_folder_ids: &mut Vec<Arc<str>>,
        seen_ids: &mut HashSet<Arc<str>>,
    ) -> Result<Option<SourceRowMetadata>> {
        if file.trashed == Some(true) {
            return Ok(None);
        }
        let (id, mime_type) = match (file.id, file.mime_type) {
            (Some(id), Some(mime_type)) => (Arc::<str>::from(id), mime_type),
            (id, mime_type) => {
                warn!("Skipping file with incomplete metadata: id={id:?}, mime_type={mime_type:?}",);
                return Ok(None);
            }
        };
        if !seen_ids.insert(id.clone()) {
            return Ok(None);
        }
        let result = if mime_type == FOLDER_MIME_TYPE {
            new_folder_ids.push(id);
            None
        } else if is_supported_file_type(&mime_type) {
            Some(SourceRowMetadata {
                key: KeyValue::Str(Arc::from(id)),
                ordinal: file.modified_time.map(|t| t.try_into()).transpose()?,
            })
        } else {
            trace!("Skipping file with unsupported mime type: id={id}, mime_type={mime_type}, name={:?}", file.name);
            None
        };
        Ok(result)
    }

    async fn list_files(
        &self,
        folder_id: &str,
        fields: &str,
        next_page_token: &mut Option<String>,
    ) -> Result<impl Iterator<Item = File>> {
        let query = format!("'{}' in parents", escape_string(folder_id));
        let mut list_call = self
            .drive_hub
            .files()
            .list()
            .add_scope(Scope::Readonly)
            .q(&query)
            .param("fields", fields);
        if let Some(next_page_token) = &next_page_token {
            list_call = list_call.page_token(next_page_token);
        }
        let (_, files) = list_call.doit().await?;
        let file_iter = files.files.into_iter().flat_map(|file| file.into_iter());
        Ok(file_iter)
    }
}

trait ResultExt<T> {
    type OptResult;
    fn or_not_found(self) -> Self::OptResult;
}

impl<T> ResultExt<T> for google_drive3::Result<T> {
    type OptResult = google_drive3::Result<Option<T>>;

    fn or_not_found(self) -> Self::OptResult {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(google_drive3::Error::BadRequest(err_msg))
                if err_msg
                    .get("error")
                    .and_then(|e| e.get("code"))
                    .and_then(|code| code.as_i64())
                    == Some(404) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl SourceExecutor for Executor {
    fn list<'a>(
        &'a self,
        options: SourceExecutorListOptions,
    ) -> BoxStream<'a, Result<Vec<SourceRowMetadata>>> {
        let mut seen_ids = HashSet::new();
        let mut folder_ids = self.root_folder_ids.clone();
        let fields = format!(
            "files(id,name,mimeType,trashed{})",
            if options.include_ordinal {
                ",modifiedTime"
            } else {
                ""
            }
        );
        let mut new_folder_ids = Vec::new();
        try_stream! {
            while let Some(folder_id) = folder_ids.pop() {
                let mut next_page_token = None;
                loop {
                    let mut curr_rows = Vec::new();
                    let files = self
                        .list_files(&folder_id, &fields, &mut next_page_token)
                        .await?;
                    for file in files {
                        curr_rows.extend(self.visit_file(file, &mut new_folder_ids, &mut seen_ids)?);
                    }
                    if !curr_rows.is_empty() {
                        yield curr_rows;
                    }
                    if next_page_token.is_none() {
                        break;
                    }
                }
                folder_ids.extend(new_folder_ids.drain(..).rev());
            }
        }
        .boxed()
    }

    async fn get_value(&self, key: &KeyValue) -> Result<Option<SourceData<'async_trait>>> {
        let file_id = key.str_value()?;

        let resp = self
            .drive_hub
            .files()
            .get(file_id)
            .add_scope(Scope::Readonly)
            .param("fields", "id,name,mimeType,trashed,modifiedTime")
            .doit()
            .await
            .or_not_found()?;
        let file = match resp {
            Some((_, file)) if file.trashed != Some(true) => file,
            _ => return Ok(None),
        };

        let modified_time = file.modified_time;
        let value = async move {
            let type_n_body = if let Some(export_mime_type) = file
                .mime_type
                .as_ref()
                .and_then(|mime_type| EXPORT_MIME_TYPES.get(mime_type.as_str()))
            {
                let target_mime_type = if self.binary {
                    export_mime_type.binary
                } else {
                    export_mime_type.text
                };
                self.drive_hub
                    .files()
                    .export(file_id, target_mime_type)
                    .add_scope(Scope::Readonly)
                    .doit()
                    .await
                    .or_not_found()?
                    .map(|content| (Some(target_mime_type.to_string()), content.into_body()))
            } else {
                self.drive_hub
                    .files()
                    .get(file_id)
                    .add_scope(Scope::Readonly)
                    .param("alt", "media")
                    .doit()
                    .await
                    .or_not_found()?
                    .map(|(resp, _)| (file.mime_type, resp.into_body()))
            };
            let value = match type_n_body {
                Some((mime_type, resp_body)) => {
                    let content = resp_body.collect().await?;

                    let fields = vec![
                        file.name.unwrap_or_default().into(),
                        mime_type.into(),
                        if self.binary {
                            content.to_bytes().to_vec().into()
                        } else {
                            String::from_utf8_lossy(&content.to_bytes())
                                .to_string()
                                .into()
                        },
                    ];
                    Some(FieldValues { fields })
                }
                None => None,
            };
            Ok(value)
        }
        .boxed();
        Ok(Some(SourceData {
            ordinal: modified_time.map(|t| t.try_into()).transpose()?,
            value,
        }))
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "GoogleDrive"
    }

    fn get_output_schema(
        &self,
        spec: &Spec,
        _context: &FlowInstanceContext,
    ) -> Result<EnrichedValueType> {
        let mut struct_schema = StructSchema::default();
        let mut schema_builder = StructSchemaBuilder::new(&mut struct_schema);
        schema_builder.add_field(FieldSchema::new(
            "file_id",
            make_output_type(BasicValueType::Str),
        ));
        let filename_field = schema_builder.add_field(FieldSchema::new(
            "filename",
            make_output_type(BasicValueType::Str),
        ));
        let mime_type_field = schema_builder.add_field(FieldSchema::new(
            "mime_type",
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
            )
            .with_attr(
                field_attrs::CONTENT_MIME_TYPE,
                serde_json::to_value(mime_type_field.to_field_ref())?,
            ),
        ));
        Ok(make_output_type(CollectionSchema::new(
            CollectionKind::Table,
            struct_schema,
        )))
    }

    async fn build_executor(
        self: Arc<Self>,
        spec: Spec,
        _context: Arc<FlowInstanceContext>,
    ) -> Result<Box<dyn SourceExecutor>> {
        Ok(Box::new(Executor::new(spec).await?))
    }
}
