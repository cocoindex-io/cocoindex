use log::warn;
use std::{path::PathBuf, sync::Arc};
use std::env;
use std::io::{self, Write};
use webbrowser;
use tiny_http::{Server, Response};
use url::Url;
use serde_json::json;
use std::sync::OnceLock;
use async_lock::Mutex;

use crate::{fields_value, ops::sdk::*};

#[derive(Debug, Deserialize)]
pub struct Spec {
    path: String,
    binary: bool,
}

struct Executor {
    root_path_str: String,
    root_path: PathBuf,
    binary: bool,
}

// Global storage for refresh token
static REFRESH_TOKEN: OnceLock<Mutex<Option<String>>> = OnceLock::new();

impl Executor {
    const TOKEN_URL: &'static str = "https://oauth2.googleapis.com/token";
    const DRIVE_API_URL: &'static str = "https://www.googleapis.com/drive/v3/files";
    const AUTH_URL: &'static str = "https://accounts.google.com/o/oauth2/v2/auth";
    const FOLDER_ID: &'static str = "1Yerp-CTs1TQUH52oy7eRqR1WHzRYhtJW";
    const REDIRECT_URI: &'static str = "http://localhost:8080";
    const SCOPE: &'static str = "https://www.googleapis.com/auth/drive.readonly";

    fn get_global_token() -> &'static Mutex<Option<String>> {
        REFRESH_TOKEN.get_or_init(|| Mutex::new(None))
    }

    async fn validate_refresh_token(refresh_token: &str) -> Result<bool> {
        let client = reqwest::Client::new();
        let client_id = env::var("CLIENT_ID").expect("CLIENT_ID not found");
        let client_secret = env::var("CLIENT_SECRET").expect("CLIENT_SECRET not found");

        let params = [
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", refresh_token.to_string()),
            ("grant_type", "refresh_token".to_string()),
        ];

        let response = client
            .post(Self::TOKEN_URL)
            .form(&params)
            .send()
            .await;

        match response {
            Ok(res) => Ok(res.status().is_success()),
            Err(_) => Ok(false)
        }
    }

    async fn get_access_token(&self) -> Result<String> {
        let client_id = env::var("CLIENT_ID").expect("CLIENT_ID not found");
        let client_secret = env::var("CLIENT_SECRET").expect("CLIENT_SECRET not found");
        
        // Try to get token from global storage first
        let token_mutex = Self::get_global_token();
        let mut token_guard = token_mutex.lock().await;
        
        // If no token in global storage, try environment variable
        if token_guard.is_none() {
            if let Ok(token) = env::var("REFRESH_TOKEN") {
                if Self::validate_refresh_token(&token).await? {
                    *token_guard = Some(token);
                }
            }
        }

        // Get new token if none exists or current one is invalid
        if token_guard.is_none() || !Self::validate_refresh_token(token_guard.as_ref().unwrap()).await? {
            let new_token = Self::get_new_refresh_token().await?;
            *token_guard = Some(new_token);
        }

        let refresh_token = token_guard.as_ref().unwrap().clone();
        drop(token_guard); // Release the lock

        // Get access token using refresh token
        let client = reqwest::Client::new();
        let params = [
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", refresh_token),
            ("grant_type", "refresh_token".to_string()),
        ];

        let response = client
            .post(Self::TOKEN_URL)
            .form(&params)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        Ok(response["access_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No access token in response"))?
            .to_string())
    }

    async fn get_new_refresh_token() -> Result<String> {
        // Create a local server to receive the OAuth callback
        let server = Server::http("localhost:8080").map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;
        
        // Generate the authorization URL
        let client_id = env::var("CLIENT_ID").expect("CLIENT_ID not found");
        let auth_url = format!(
            "{}?client_id={}&response_type=code&scope={}&redirect_uri={}&access_type=offline&prompt=consent&include_granted_scopes=true",
            Self::AUTH_URL,
            client_id,
            urlencoding::encode(Self::SCOPE),
            urlencoding::encode(Self::REDIRECT_URI)
        );

        // Open the browser for user authorization
        println!("Opening browser for authorization...");
        println!("Please complete the authorization in your browser.");
        webbrowser::open(&auth_url)?;

        // Wait for the callback with timeout
        println!("Waiting for authorization callback...");
        let request = tokio::time::timeout(
            std::time::Duration::from_secs(120), // 2 minute timeout
            tokio::task::spawn_blocking(move || server.incoming_requests().next())
        ).await??
        .ok_or_else(|| anyhow::anyhow!("No incoming request received"))?;
            
        // Parse the URL and extract the code
        let url = Url::parse(&format!("http://localhost{}", request.url()))
            .map_err(|e| anyhow::anyhow!("Failed to parse callback URL: {}", e))?;
            
        // Check for error in the callback
        if let Some(error) = url.query_pairs()
            .find(|(key, _)| key == "error")
            .map(|(_, value)| value.into_owned()) {
            return Err(anyhow::anyhow!("Authorization failed: {}", error));
        }

        // Get the authorization code
        let code = url.query_pairs()
            .find(|(key, _)| key == "code")
            .map(|(_, value)| value.into_owned())
            .ok_or_else(|| anyhow::anyhow!("No authorization code received"))?;

        // Send a success response to the browser
        let response = Response::from_string("Authorization successful! You can close this window.");
        request.respond(response)?;

        // Exchange the authorization code for tokens
        let client = reqwest::Client::new();
        let client_secret = env::var("CLIENT_SECRET").expect("CLIENT_SECRET not found");
        
        println!("Exchanging authorization code for tokens...");
        let token_response = client
            .post(Self::TOKEN_URL)
            .form(&json!({
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": Self::REDIRECT_URI,
            }))
            .send()
            .await?;

        // Check if the token request was successful
        if !token_response.status().is_success() {
            let error_text = token_response.text().await?;
            return Err(anyhow::anyhow!("Failed to get tokens: {}", error_text));
        }

        let token_data = token_response.json::<serde_json::Value>().await?;
        
        let refresh_token = token_data["refresh_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No refresh token in response"))?;

        println!("\nAuthorization successful!");
        
        Ok(refresh_token.to_string())
    }

    async fn traverse_dir(&self, _dir_path: &PathBuf, result: &mut Vec<KeyValue>) -> Result<()> {
        log::info!("Getting access token for Google Drive API");
        let access_token = self.get_access_token().await?;
        let client = reqwest::Client::new();

        // Query for files in the specified folder
        let query = format!("'{}' in parents", Self::FOLDER_ID);
        log::debug!("Querying Google Drive API with query: {}", query);
        let response = client
            .get(Self::DRIVE_API_URL)
            .query(&[
                ("q", query.as_str()),
                ("fields", "files(id,name)"),
            ])
            .bearer_auth(&access_token)
            .send()
            .await?;

        let json: serde_json::Value = response.json().await?;
        
        if let Some(files) = json["files"].as_array() {
            log::info!("Found {} files in Google Drive folder", files.len());
            for file in files {
                if let (Some(name), Some(_id)) = (file["name"].as_str(), file["id"].as_str()) {
                    log::debug!("Adding file to results: {}", name);
                    result.push(KeyValue::Str(Arc::from(name)));
                }
            }
        } else {
            log::warn!("No files found in Google Drive folder");
        }

        Ok(())
    }

    async fn download_file(&self, file_name: &str) -> Result<Vec<u8>> {
        let access_token = self.get_access_token().await?;
        let client = reqwest::Client::new();

        // First get the file ID by name
        let query = format!("name = '{}' and '{}' in parents", file_name, Self::FOLDER_ID);
        let response = client
            .get(Self::DRIVE_API_URL)
            .query(&[
                ("q", query.as_str()),
                ("fields", "files(id)"),
            ])
            .bearer_auth(&access_token)
            .send()
            .await?;

        let json: serde_json::Value = response.json().await?;
        
        let file_id = json["files"][0]["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("File not found"))?;

        // Then download the file content
        let url = format!("{}/{}/content", Self::DRIVE_API_URL, file_id);
        println!("Downloading file: {}", file_name);
        let response = client
            .get(&url)
            .bearer_auth(&access_token)
            .send()
            .await?;

        Ok(response.bytes().await?.to_vec())
    }
}

#[async_trait]
impl SourceExecutor for Executor {
    async fn list_keys(&self) -> Result<Vec<KeyValue>> {
        let mut result = Vec::new();
        self.traverse_dir(&self.root_path, &mut result).await?;
        Ok(result)
    }

    async fn get_value(&self, key: &KeyValue) -> Result<Option<FieldValues>> {
        let file_name = key.str_value()?;
        
        match self.download_file(file_name).await {
            Ok(content) => {
                let content = if self.binary {
                    fields_value!(content)
                } else {
                    fields_value!(String::from_utf8_lossy(&content).to_string())
                };
                Ok(Some(content))
            }
            Err(e) => {
                // If file not found, return None, otherwise propagate error
                if e.to_string().contains("File not found") {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }
}

pub struct Factory;

#[async_trait]
impl SourceFactoryBase for Factory {
    type Spec = Spec;

    fn name(&self) -> &str {
        "LocalFile"
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
        Ok(Box::new(Executor {
            root_path_str: spec.path.clone(),
            root_path: PathBuf::from(spec.path),
            binary: spec.binary,
        }))
    }
}
