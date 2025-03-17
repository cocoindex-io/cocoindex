use std::sync::Arc;

use google_drive3::{
    api::Scope,
    yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator},
    DriveHub,
};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;

use crate::ops::sdk::*;

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
        // let user_secret = read_authorized_user_secret(spec.service_account_credential_path).await?;
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
                        .enable_http1()
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

#[async_trait]
impl SourceExecutor for Executor {
    async fn list_keys(&self) -> Result<Vec<KeyValue>> {
        let query = format!("'{}' in parents", escape_string(&self.root_folder_id));
        let mut next_page_token: Option<String> = None;
        let mut result = Vec::new();
        loop {
            let mut list_call = self
                .drive_hub
                .files()
                .list()
                .q(&query)
                .add_scope(Scope::Readonly);
            if let Some(next_page_token) = &next_page_token {
                list_call = list_call.page_token(next_page_token);
            }
            let (resp, files) = list_call.doit().await?;
            if let Some(files) = files.files {
                for file in files {
                    if let Some(name) = file.name {
                        result.push(KeyValue::Str(Arc::from(name)));
                    }
                }
            }
            next_page_token = files.next_page_token;
            if next_page_token.is_none() {
                break;
            }
        }
        Ok(result)
    }

    async fn get_value(&self, key: &KeyValue) -> Result<Option<FieldValues>> {
        unimplemented!()
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
