use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use google_drive3::{
    api::Scope,
    yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator},
    DriveHub,
};
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use indexmap::IndexSet;
use log::warn;

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
    root_folder_ids: Vec<String>,
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
            root_folder_ids: spec.root_folder_ids,
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
    async fn traverse_folder(
        &self,
        folder_id: &str,
        visited_folder_ids: &mut IndexSet<String>,
        result: &mut IndexSet<KeyValue>,
    ) -> Result<()> {
        if !visited_folder_ids.insert(folder_id.to_string()) {
            return Ok(());
        }
        let query = format!("'{}' in parents", escape_string(folder_id));
        let mut next_page_token: Option<String> = None;
        loop {
            let mut list_call = self
                .drive_hub
                .files()
                .list()
                .add_scope(Scope::Readonly)
                .q(&query);
            if let Some(next_page_token) = &next_page_token {
                list_call = list_call.page_token(next_page_token);
            }
            let (_, files) = list_call.doit().await?;
            if let Some(files) = files.files {
                for file in files {
                    match (file.id, file.mime_type) {
                        (Some(id), Some(mime_type)) => {
                            if mime_type == FOLDER_MIME_TYPE {
                                Box::pin(self.traverse_folder(&id, visited_folder_ids, result))
                                    .await?;
                            } else if is_supported_file_type(&mime_type) {
                                result.insert(KeyValue::Str(Arc::from(id)));
                            } else {
                                warn!("Skipping file with unsupported mime type: id={id}, mime_type={mime_type}, name={:?}", file.name);
                            }
                        }
                        (id, mime_type) => {
                            warn!(
                                "Skipping file with incomplete metadata: id={id:?}, mime_type={mime_type:?}",
                            );
                        }
                    }
                }
            }
            next_page_token = files.next_page_token;
            if next_page_token.is_none() {
                break;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SourceExecutor for Executor {
    async fn list_keys(&self) -> Result<Vec<KeyValue>> {
        let mut result = IndexSet::new();
        for root_folder_id in &self.root_folder_ids {
            self.traverse_folder(root_folder_id, &mut IndexSet::new(), &mut result)
                .await?;
        }
        Ok(result.into_iter().collect())
    }

    async fn get_value(&self, key: &KeyValue) -> Result<Option<FieldValues>> {
        let file_id = key.str_value()?;

        let file = match self
            .drive_hub
            .files()
            .get(file_id)
            .add_scope(Scope::Readonly)
            .param("fields", "id,name,mimeType,trashed")
            .doit()
            .await
        {
            Ok((_, file)) => {
                if file.trashed == Some(true) {
                    return Ok(None);
                }
                file
            }
            Err(google_drive3::Error::BadRequest(err_msg))
                if err_msg
                    .get("error")
                    .and_then(|e| e.get("code"))
                    .and_then(|code| code.as_i64())
                    == Some(404) =>
            {
                return Ok(None);
            }
            Err(e) => Err(e)?,
        };

        let (mime_type, resp_body) = if let Some(export_mime_type) = file
            .mime_type
            .as_ref()
            .and_then(|mime_type| EXPORT_MIME_TYPES.get(mime_type.as_str()))
        {
            let target_mime_type = if self.binary {
                export_mime_type.binary
            } else {
                export_mime_type.text
            };
            let content = self
                .drive_hub
                .files()
                .export(file_id, target_mime_type)
                .add_scope(Scope::Readonly)
                .doit()
                .await?
                .into_body();
            (Some(target_mime_type.to_string()), content)
        } else {
            let (resp, _) = self
                .drive_hub
                .files()
                .get(file_id)
                .add_scope(Scope::Readonly)
                .param("alt", "media")
                .doit()
                .await?;
            (file.mime_type, resp.into_body())
        };
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
        Ok(Some(FieldValues { fields }))
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
