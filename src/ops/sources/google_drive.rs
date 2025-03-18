use std::sync::Arc;

use futures::future::try_join;
use google_drive3::{
    api::Scope,
    yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator},
    DriveHub,
};
use http_body_util::BodyExt;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use indexmap::IndexSet;
use log::debug;

use crate::ops::sdk::*;

const FOLDER_MIME_TYPE: &'static str = "application/vnd.google-apps.folder";

#[derive(Debug, Deserialize)]
pub struct Spec {
    service_account_credential_path: String,
    binary: bool,
    root_folder_id: String,
}

struct Executor {
    drive_hub: DriveHub<HttpsConnector<HttpConnector>>,
    binary: bool,
    root_folder_id: String,
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
                        .with_provider_and_native_roots(rustls::crypto::ring::default_provider())?
                        .https_only()
                        .enable_http2()
                        .build(),
                );
        let drive_hub = DriveHub::new(client, auth);
        Ok(Self {
            drive_hub,
            binary: spec.binary,
            root_folder_id: spec.root_folder_id,
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
                    if let Some(id) = file.id {
                        if file.mime_type.as_ref() == Some(&FOLDER_MIME_TYPE.to_string()) {
                            Box::pin(self.traverse_folder(&id, visited_folder_ids, result)).await?;
                        } else {
                            result.insert(KeyValue::Str(Arc::from(id)));
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
        self.traverse_folder(&self.root_folder_id, &mut IndexSet::new(), &mut result)
            .await?;
        Ok(result.into_iter().collect())
    }

    async fn get_value(&self, key: &KeyValue) -> Result<Option<FieldValues>> {
        let file_id = key.str_value()?;

        let filename = async {
            let (_, file) = self
                .drive_hub
                .files()
                .get(file_id)
                .add_scope(Scope::Readonly)
                .doit()
                .await?;
            anyhow::Ok(file.name.unwrap_or_default())
        };
        let body = async {
            let (resp, _) = self
                .drive_hub
                .files()
                .get(file_id)
                .add_scope(Scope::Readonly)
                .param("alt", "media")
                .doit()
                .await?;
            let content = resp.into_body().collect().await?;
            anyhow::Ok(content)
        };
        let (filename, content) = try_join(filename, body).await?;

        let mut fields = Vec::with_capacity(2);
        fields.push(filename.into());
        if self.binary {
            fields.push(content.to_bytes().to_vec().into());
        } else {
            fields.push(
                String::from_utf8_lossy(&content.to_bytes())
                    .to_string()
                    .into(),
            );
        }

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
        Ok(make_output_type(CollectionSchema::new(
            CollectionKind::Table,
            vec![
                FieldSchema::new("file_id", make_output_type(BasicValueType::Str)),
                FieldSchema::new("filename", make_output_type(BasicValueType::Str)),
                FieldSchema::new(
                    "content",
                    make_output_type(if spec.binary {
                        BasicValueType::Bytes
                    } else {
                        BasicValueType::Str
                    }),
                ),
            ],
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
