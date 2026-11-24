//! Tests of the Azure Blob client's Entra ID token sources
//! (`Credential::TokenCredential`) against in-process fakes of the token
//! endpoints: the tenant's token endpoint for a client secret, the App Service
//! identity endpoint for a managed identity, and the `Default` chain's choice
//! between them. Each source is reached through environment variables, as the
//! Azure SDK reaches it, so the cases run one after another in one test.
#![cfg(feature = "azure_blob")]

use std::sync::{Arc, Mutex};

use cocoindex::Result;
use cocoindex::connectors::azure_blob::{
    self, ContainerClient, Credential, ListOptions, TokenCredential,
};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const CONTAINER: &str = "docs";

/// A fake Blob service that lists one blob and records the bearer token of
/// every request.
async fn blob_service(tokens: Arc<Mutex<Vec<String>>>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/account/{CONTAINER}")))
        .respond_with(move |request: &Request| {
            let authorization = request
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            tokens.lock().unwrap().push(authorization);
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/xml")
                .set_body_string(
                    r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults><Blobs><Blob><Name>a.md</Name><Properties>
<Last-Modified>Tue, 02 Jun 2026 00:00:00 GMT</Last-Modified><Etag>"1"</Etag>
<Content-Length>3</Content-Length></Properties></Blob></Blobs><NextMarker/></EnumerationResults>"#,
                )
        })
        .mount(&server)
        .await;
    server
}

fn token_client(blob_service: &MockServer, credential: TokenCredential) -> ContainerClient {
    ContainerClient::new(
        &format!("{}/account", blob_service.uri()),
        CONTAINER,
        Credential::TokenCredential(credential),
    )
    .unwrap()
}

async fn list(client: &ContainerClient) -> Result<usize> {
    Ok(azure_blob::list_blobs(client, ListOptions::default())
        .list()
        .await?
        .len())
}

/// Serve `tokens` in turn, each with `expires_in`, from the client-credentials
/// endpoint of tenant `t1`; returns the requests it answered.
async fn token_endpoint(tokens: Vec<(&'static str, u64)>) -> (MockServer, Arc<Mutex<Vec<String>>>) {
    let server = MockServer::start().await;
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path("/t1/oauth2/v2.0/token"))
        .respond_with({
            let bodies = bodies.clone();
            move |request: &Request| {
                bodies
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request.body).into_owned());
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (token, expires_in) = tokens[n.min(tokens.len() - 1)];
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "token_type": "Bearer",
                    "expires_in": expires_in,
                    "access_token": token,
                }))
            }
        })
        .mount(&server)
        .await;
    (server, bodies)
}

fn set_env(pairs: &[(&str, Option<&str>)]) {
    for (name, value) in pairs {
        // The test's own process, one case at a time.
        unsafe {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

#[tokio::test]
async fn entra_id_tokens_are_fetched_renewed_and_resolved() -> Result<()> {
    // --- A client secret: the client-credentials grant, with the storage
    // scope, and the token reused while it lasts.
    let (authority, bodies) = token_endpoint(vec![("tok-1", 3600)]).await;
    set_env(&[("AZURE_AUTHORITY_HOST", Some(&authority.uri()))]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let blobs = blob_service(seen.clone()).await;
    let client = token_client(
        &blobs,
        TokenCredential::ClientSecret {
            tenant_id: "t1".into(),
            client_id: "app".into(),
            client_secret: "s3cret".into(),
        },
    );
    assert_eq!(list(&client).await?, 1);
    assert_eq!(list(&client).await?, 1);
    let bodies = bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1, "one token for two requests: {bodies:?}");
    assert!(
        bodies[0].contains("grant_type=client_credentials"),
        "{}",
        bodies[0]
    );
    assert!(bodies[0].contains("client_id=app") && bodies[0].contains("client_secret=s3cret"));
    assert!(
        bodies[0].contains("scope=https%3A%2F%2Fstorage.azure.com%2F.default"),
        "{}",
        bodies[0]
    );
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec!["Bearer tok-1".to_string(); 2]
    );

    // --- Renewal: a token that expires within the refresh offset is renewed
    // before the next request, which the new token signs.
    let (authority, bodies) = token_endpoint(vec![("short-1", 20), ("short-2", 3600)]).await;
    set_env(&[("AZURE_AUTHORITY_HOST", Some(&authority.uri()))]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let blobs = blob_service(seen.clone()).await;
    let client = token_client(
        &blobs,
        TokenCredential::ClientSecret {
            tenant_id: "t1".into(),
            client_id: "app".into(),
            client_secret: "s3cret".into(),
        },
    );
    list(&client).await?;
    // A 20 s token is renewed at half its life (10 s), not 300 s before: wait
    // past that point.
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    list(&client).await?;
    list(&client).await?;
    assert_eq!(bodies.lock().unwrap().len(), 2, "renewed once");
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec!["Bearer short-1", "Bearer short-2", "Bearer short-2"]
    );

    // --- A managed identity on App Service: the identity endpoint, with its
    // header and api-version, and a user-assigned client id.
    let identity = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/msi/token"))
        .and(wiremock::matchers::header(
            "X-IDENTITY-HEADER",
            "hdr-secret",
        ))
        .and(query_param("api-version", "2019-08-01"))
        .and(query_param("resource", "https://storage.azure.com/"))
        .and(query_param("client_id", "mi-client"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "mi-tok",
            "expires_on": (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() + 3600).to_string(),
            "resource": "https://storage.azure.com/",
            "token_type": "Bearer",
        })))
        .mount(&identity)
        .await;
    set_env(&[
        (
            "IDENTITY_ENDPOINT",
            Some(&format!("{}/msi/token", identity.uri())),
        ),
        ("IDENTITY_HEADER", Some("hdr-secret")),
    ]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let blobs = blob_service(seen.clone()).await;
    let client = token_client(
        &blobs,
        TokenCredential::ManagedIdentity {
            client_id: Some("mi-client".into()),
        },
    );
    assert_eq!(list(&client).await?, 1);
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec!["Bearer mi-tok".to_string()]
    );

    // --- The default chain prefers the AZURE_* variables over the managed
    // identity that is also configured, and keeps that choice.
    let (authority, bodies) = token_endpoint(vec![("env-tok", 3600)]).await;
    set_env(&[
        ("AZURE_AUTHORITY_HOST", Some(&authority.uri())),
        ("AZURE_TENANT_ID", Some("t1")),
        ("AZURE_CLIENT_ID", Some("app")),
        ("AZURE_CLIENT_SECRET", Some("s3cret")),
    ]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let blobs = blob_service(seen.clone()).await;
    let client = token_client(&blobs, TokenCredential::Default);
    assert_eq!(list(&client).await?, 1);
    assert_eq!(bodies.lock().unwrap().len(), 1);
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec!["Bearer env-tok".to_string()]
    );

    // --- Without the variables, the chain takes the managed identity.
    set_env(&[
        ("AZURE_TENANT_ID", None),
        ("AZURE_CLIENT_ID", None),
        ("AZURE_CLIENT_SECRET", None),
    ]);
    Mock::given(method("GET"))
        .and(path("/msi/token"))
        .and(query_param("api-version", "2019-08-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "mi-default",
            "expires_in": "3600",
        })))
        .mount(&identity)
        .await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let blobs = blob_service(seen.clone()).await;
    let client = token_client(&blobs, TokenCredential::Default);
    assert_eq!(list(&client).await?, 1);
    assert_eq!(
        seen.lock().unwrap().clone(),
        vec!["Bearer mi-default".to_string()]
    );

    // --- A refused token request is an error that names the source and status.
    set_env(&[("IDENTITY_ENDPOINT", None), ("IDENTITY_HEADER", None)]);
    let refusing = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/t1/oauth2/v2.0/token"))
        .respond_with(ResponseTemplate::new(401).set_body_string(r#"{"error":"invalid_client"}"#))
        .mount(&refusing)
        .await;
    set_env(&[("AZURE_AUTHORITY_HOST", Some(&refusing.uri()))]);
    let blobs = blob_service(Arc::new(Mutex::new(Vec::new()))).await;
    let client = token_client(
        &blobs,
        TokenCredential::ClientSecret {
            tenant_id: "t1".into(),
            client_id: "app".into(),
            client_secret: "bad".into(),
        },
    );
    let error = list(&client).await.unwrap_err().to_string();
    assert!(
        error.contains("client secret token")
            && error.contains("401")
            && error.contains("invalid_client"),
        "{error}"
    );
    set_env(&[("AZURE_AUTHORITY_HOST", None)]);
    Ok(())
}
