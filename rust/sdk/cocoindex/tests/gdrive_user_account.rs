//! The Google Drive connector's OAuth sign-in for a personal account, against
//! a fake Google: `wiremock` plays the token, revocation and Drive `about`
//! endpoints, a fake browser follows the authorization link to the loopback
//! listener (or the redirect is pasted back), and the token file lives in a
//! temp dir. Mirrors `python/tests/connectors/test_google_drive_oauth.py`, and
//! checks the token file is the one the Python SDK writes.
#![cfg(feature = "google_drive")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use cocoindex::connectors::gdrive::{
    GoogleDriveClient, GoogleDriveSource, GoogleEndpoints, SignInError, SignInOptions, UserAccount,
};
use sha2::Digest;
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const CLIENT_ID: &str = "123-fake.apps.googleusercontent.com";
const CLIENT_SECRET: &str = "fake-client-secret";
const EMAIL: &str = "someone@example.com";
const SCOPE: &str = "https://www.googleapis.com/auth/drive.readonly";

fn query_of(url: &str) -> BTreeMap<String, String> {
    url.split_once('?')
        .map(|(_, q)| q)
        .unwrap_or("")
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.to_string(), percent_decode(v)))
        .collect()
}

fn percent_decode(v: &str) -> String {
    let mut out = Vec::new();
    let bytes = v.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            out.push(u8::from_str_radix(&v[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

fn form_of(request: &Request) -> BTreeMap<String, String> {
    query_of(&format!("?{}", String::from_utf8_lossy(&request.body)))
}

/// Google's side of the flow: the consent page issues a code bound to the
/// PKCE challenge; the token endpoint checks the verifier.
struct FakeGoogle {
    server: MockServer,
    /// code -> challenge the consent issued it for.
    codes: Arc<Mutex<BTreeMap<String, String>>>,
}

impl FakeGoogle {
    async fn start() -> Self {
        let server = MockServer::start().await;
        let codes: Arc<Mutex<BTreeMap<String, String>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let issued = Arc::new(Mutex::new(0u32));
        // The authorization-code grant: the verifier must hash to the challenge.
        let codes_for_token = codes.clone();
        let issued_for_token = issued.clone();
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=authorization_code"))
            .respond_with(move |request: &Request| {
                let form = form_of(request);
                assert_eq!(form["client_id"], CLIENT_ID);
                assert_eq!(form["client_secret"], CLIENT_SECRET);
                let challenge = codes_for_token.lock().unwrap().remove(&form["code"]);
                let expected = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(sha2::Sha256::digest(form["code_verifier"].as_bytes()));
                if challenge.as_deref() != Some(expected.as_str()) {
                    return ResponseTemplate::new(400)
                        .set_body_json(serde_json::json!({"error": "invalid_grant"}));
                }
                let mut issued = issued_for_token.lock().unwrap();
                *issued += 1;
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": format!("access-{issued}"),
                    "refresh_token": format!("refresh-{issued}"),
                    "expires_in": 3599,
                    "scope": SCOPE,
                    "token_type": "Bearer",
                }))
            })
            .mount(&server)
            .await;
        // The refresh grant: any `refresh-*` token works, a new access token.
        let issued_for_refresh = issued.clone();
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(move |request: &Request| {
                let form = form_of(request);
                if !form["refresh_token"].starts_with("refresh-") {
                    return ResponseTemplate::new(400).set_body_json(serde_json::json!({
                        "error": "invalid_grant", "error_description": "Bad Request"
                    }));
                }
                let mut issued = issued_for_refresh.lock().unwrap();
                *issued += 1;
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": format!("access-{issued}"),
                    "expires_in": 3599,
                    "token_type": "Bearer",
                }))
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/revoke"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/about"))
            .respond_with(|request: &Request| {
                let auth = request
                    .headers
                    .get("authorization")
                    .map(|v| v.to_str().unwrap().to_string())
                    .unwrap_or_default();
                assert!(auth.starts_with("Bearer access-"), "{auth}");
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"user": {"emailAddress": EMAIL}}))
            })
            .mount(&server)
            .await;
        Self { server, codes }
    }

    fn endpoints(&self) -> GoogleEndpoints {
        GoogleEndpoints {
            authorization: format!("{}/auth", self.server.uri()),
            token: format!("{}/token", self.server.uri()),
            revocation: format!("{}/revoke", self.server.uri()),
            about: format!("{}/about", self.server.uri()),
        }
    }

    async fn requests(&self) -> Vec<Request> {
        self.server.received_requests().await.unwrap_or_default()
    }
}

struct Flow {
    google: FakeGoogle,
    dir: tempfile::TempDir,
    client_secrets: std::path::PathBuf,
    said: Arc<Mutex<Vec<String>>>,
    opened: Arc<Mutex<Vec<String>>>,
}

impl Flow {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let client_secrets = dir.path().join("client_secret.json");
        std::fs::write(
            &client_secrets,
            serde_json::json!({"installed": {
                "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
                "redirect_uris": ["http://localhost"],
            }})
            .to_string(),
        )
        .unwrap();
        Self {
            google: FakeGoogle::start().await,
            dir,
            client_secrets,
            said: Arc::new(Mutex::new(Vec::new())),
            opened: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn token_path(&self) -> std::path::PathBuf {
        self.dir
            .path()
            .join("tokens")
            .join("google_drive_token.json")
    }

    fn account(&self) -> UserAccount {
        UserAccount::with_token_path(self.token_path()).with_endpoints(self.google.endpoints())
    }

    /// A browser on this host: follows the link, then the redirect to the
    /// listener.
    fn options(&self) -> SignInOptions {
        let opened = self.opened.clone();
        let said = self.said.clone();
        let codes = self.google.codes.clone();
        SignInOptions {
            open_browser: Arc::new(move |url: &str| {
                opened.lock().unwrap().push(url.to_string());
                land_on(consent(&codes, url));
            }),
            read_line: Arc::new(|| None),
            say: Arc::new(move |message: &str| said.lock().unwrap().push(message.to_string())),
            browser_available: Some(true),
            timeout: Duration::from_secs(10),
        }
    }
}

/// The consent page: the person agrees, and Google redirects back with a code
/// bound to the request's PKCE challenge. Returns the landing address.
fn consent(codes: &Arc<Mutex<BTreeMap<String, String>>>, url: &str) -> String {
    let params = query_of(url);
    assert_eq!(params["client_id"], CLIENT_ID);
    assert_eq!(params["scope"], SCOPE);
    assert_eq!(params["code_challenge_method"], "S256");
    assert_eq!(params["access_type"], "offline");
    let mut codes = codes.lock().unwrap();
    let code = format!("code-{}", codes.len() + 1);
    codes.insert(code.clone(), params["code_challenge"].clone());
    format!(
        "{}?code={code}&state={}",
        params["redirect_uri"], params["state"]
    )
}

/// The browser landing on `url`: one plain HTTP GET, on a thread so the flow's
/// listener answers it.
fn land_on(url: String) {
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        let rest = url.trim_start_matches("http://");
        let (host, target) = rest.split_once('/').unwrap();
        let mut stream = std::net::TcpStream::connect(host).unwrap();
        stream
            .write_all(
                format!("GET /{target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .unwrap();
        let mut response = String::new();
        let _ = stream.read_to_string(&mut response);
    });
}

fn token_file(path: &std::path::Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sign_in_round_trip_stores_the_python_token_file() {
    let flow = Flow::start().await;
    let account = flow.account();
    assert!(!account.is_signed_in());

    let email = account
        .sign_in_with(Some(&flow.client_secrets), flow.options())
        .await
        .unwrap();
    assert_eq!(email, EMAIL);
    assert!(account.is_signed_in());
    assert_eq!(account.email().unwrap(), EMAIL);
    assert_eq!(flow.opened.lock().unwrap().len(), 1);
    assert!(flow.said.lock().unwrap()[0].contains("Finish the sign-in in the browser"));

    // The file is the Python SDK's: same keys, version 1, readable by this
    // user only.
    let file = token_file(&flow.token_path());
    assert_eq!(file["version"], 1);
    assert_eq!(file["client_id"], CLIENT_ID);
    assert_eq!(file["client_secret"], CLIENT_SECRET);
    assert_eq!(file["refresh_token"], "refresh-1");
    assert_eq!(file["access_token"], "access-1");
    assert_eq!(file["email"], EMAIL);
    assert!(file["expires_at"].as_f64().unwrap() > 1e9);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(flow.token_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_browser_the_link_is_printed_and_the_redirect_pasted_back() {
    let flow = Flow::start().await;
    let account = flow.account();
    // The person reads the link from what was said and pastes back the address
    // the browser on another machine landed on.
    let said = flow.said.clone();
    let codes = flow.google.codes.clone();
    let pasted = Arc::new(Mutex::new(vec![
        "not an address\n".to_string(),
        "".to_string(), // filled in once the link is known
    ]));
    let read_line: Arc<dyn Fn() -> Option<String> + Send + Sync> = {
        let pasted = pasted.clone();
        Arc::new(move || {
            // Wait until the instructions name the link.
            let link = loop {
                let said = said.lock().unwrap();
                if let Some(message) = said.first() {
                    let link = message
                        .lines()
                        .map(str::trim)
                        .find(|l| l.starts_with("http"))
                        .unwrap()
                        .to_string();
                    break link;
                }
                drop(said);
                std::thread::sleep(Duration::from_millis(20));
            };
            let mut pasted = pasted.lock().unwrap();
            if pasted.is_empty() {
                return None;
            }
            let line = pasted.remove(0);
            if line.is_empty() {
                Some(format!("{}\n", consent(&codes, &link)))
            } else {
                Some(line)
            }
        })
    };
    let options = SignInOptions {
        open_browser: Arc::new(|_: &str| panic!("no browser must open")),
        read_line,
        browser_available: Some(false),
        ..flow.options()
    };
    let email = account
        .sign_in_with(Some(&flow.client_secrets), options)
        .await
        .unwrap();
    assert_eq!(email, EMAIL);
    let said = flow.said.lock().unwrap().clone();
    assert!(said[0].contains("No browser can open on this host"));
    assert!(said[1].contains("not the address the sign-in ended on"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_redirect_with_another_state_is_refused_and_nothing_is_stored() {
    let flow = Flow::start().await;
    let account = flow.account();
    let options = SignInOptions {
        open_browser: Arc::new(|url: &str| {
            let params = query_of(url);
            land_on(format!(
                "{}?code=code-x&state=someone-elses",
                params["redirect_uri"]
            ));
        }),
        ..flow.options()
    };
    let err = account
        .sign_in_with(Some(&flow.client_secrets), options)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("state does not match"), "{err}");
    assert!(!flow.token_path().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_consent_is_reported() {
    let flow = Flow::start().await;
    let options = SignInOptions {
        open_browser: Arc::new(|url: &str| {
            let params = query_of(url);
            land_on(format!("{}?error=access_denied", params["redirect_uri"]));
        }),
        ..flow.options()
    };
    let err = flow
        .account()
        .sign_in_with(Some(&flow.client_secrets), options)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("cancelled"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_web_client_or_a_missing_client_file_is_refused() {
    let flow = Flow::start().await;
    let web = flow.dir.path().join("web.json");
    std::fs::write(
        &web,
        serde_json::json!({"web": {"client_id": "x", "client_secret": "y"}}).to_string(),
    )
    .unwrap();
    let err = flow
        .account()
        .sign_in_with(Some(&web), flow.options())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Web application client"), "{err}");
    let err = flow
        .account()
        .sign_in_with(Some(&flow.dir.path().join("missing.json")), flow.options())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("No OAuth client file"), "{err}");
    assert!(!flow.token_path().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_access_token_is_refreshed_and_the_file_updated() {
    let flow = Flow::start().await;
    let account = flow.account();
    account
        .sign_in_with(Some(&flow.client_secrets), flow.options())
        .await
        .unwrap();
    // Expire the stored access token, as a run the next day would find it.
    let mut file = token_file(&flow.token_path());
    file["expires_at"] = serde_json::json!(1.0);
    std::fs::write(flow.token_path(), file.to_string()).unwrap();

    Mock::given(method("GET"))
        .and(path("/files"))
        .and(header("authorization", "Bearer access-2"))
        .and(query_param("q", "'root' in parents and trashed = false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "files": [{"id": "txt-1", "name": "a.txt", "mimeType": "text/plain",
                       "size": "1", "modifiedTime": "2026-01-02T00:00:00Z"}]
        })))
        .mount(&flow.google.server)
        .await;
    let client = GoogleDriveClient::from_user_account(&account)
        .unwrap()
        .with_base_url(flow.google.server.uri());
    assert_eq!(client.state_id(), format!("user_account:{EMAIL}"));
    let files = GoogleDriveSource::new(client, vec!["root".to_string()])
        .list_files()
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    // The refreshed token was written back for the next process.
    let file = token_file(&flow.token_path());
    assert_eq!(file["access_token"], "access-2");
    assert_eq!(file["refresh_token"], "refresh-1");
    assert!(file["expires_at"].as_f64().unwrap() > 1e9);
    let refreshes = flow
        .google
        .requests()
        .await
        .iter()
        .filter(|r| r.url.path() == "/token" && form_of(r)["grant_type"] == "refresh_token")
        .count();
    assert_eq!(refreshes, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_sign_in_asks_to_sign_in_again_without_leaking_tokens() {
    let flow = Flow::start().await;
    let account = flow.account();
    account
        .sign_in_with(Some(&flow.client_secrets), flow.options())
        .await
        .unwrap();
    let mut file = token_file(&flow.token_path());
    file["expires_at"] = serde_json::json!(1.0);
    file["refresh_token"] = serde_json::json!("revoked-token");
    std::fs::write(flow.token_path(), file.to_string()).unwrap();

    let client = GoogleDriveClient::from_user_account(&account).unwrap();
    let message = match GoogleDriveSource::new(client, vec!["root".to_string()])
        .list_files()
        .await
    {
        Ok(_) => panic!("a revoked sign-in must not list files"),
        Err(err) => err.to_string(),
    };
    assert!(
        message.contains("invalid_grant") && message.contains("Sign in again"),
        "{message}"
    );
    assert!(!message.contains("revoked-token") && !message.contains("access-1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sign_out_revokes_and_deletes() {
    let flow = Flow::start().await;
    let account = flow.account();
    account
        .sign_in_with(Some(&flow.client_secrets), flow.options())
        .await
        .unwrap();
    assert!(account.sign_out().await.unwrap());
    assert!(!flow.token_path().exists());
    assert!(!account.is_signed_in());
    let revoked: Vec<String> = flow
        .google
        .requests()
        .await
        .iter()
        .filter(|r| r.url.path() == "/revoke")
        .map(|r| form_of(r)["token"].clone())
        .collect();
    assert_eq!(revoked, vec!["refresh-1".to_string()]);
    // A second sign-out has nothing to do.
    assert!(!account.sign_out().await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sign_out_deletes_the_file_even_when_google_is_unreachable() {
    let flow = Flow::start().await;
    flow.account()
        .sign_in_with(Some(&flow.client_secrets), flow.options())
        .await
        .unwrap();
    let unreachable =
        UserAccount::with_token_path(flow.token_path()).with_endpoints(GoogleEndpoints {
            revocation: "http://127.0.0.1:9/revoke".to_string(),
            ..flow.google.endpoints()
        });
    assert!(unreachable.sign_out().await.unwrap());
    assert!(!flow.token_path().exists());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_file_readable_by_others_is_refused() {
    use std::os::unix::fs::PermissionsExt;
    let flow = Flow::start().await;
    let account = flow.account();
    account
        .sign_in_with(Some(&flow.client_secrets), flow.options())
        .await
        .unwrap();
    std::fs::set_permissions(flow.token_path(), std::fs::Permissions::from_mode(0o644)).unwrap();
    let err = account.email().unwrap_err();
    assert!(err.to_string().contains("chmod 600"), "{err}");
    assert!(GoogleDriveClient::from_user_account(&account).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_file_of_another_version_means_signing_in_again() {
    let flow = Flow::start().await;
    let account = flow.account();
    account
        .sign_in_with(Some(&flow.client_secrets), flow.options())
        .await
        .unwrap();
    let mut file = token_file(&flow.token_path());
    file["version"] = serde_json::json!(2);
    std::fs::write(flow.token_path(), file.to_string()).unwrap();
    assert!(!account.is_signed_in());
    let err = GoogleDriveClient::from_user_account(&account)
        .err()
        .unwrap();
    assert!(err.to_string().contains("Sign in once"), "{err}");
}

#[tokio::test]
async fn a_run_without_a_sign_in_says_how_to_sign_in() {
    let dir = tempfile::tempdir().unwrap();
    let account = UserAccount::with_token_path(dir.path().join("none.json"));
    let err = GoogleDriveClient::from_user_account(&account)
        .err()
        .unwrap();
    assert!(
        err.to_string()
            .contains("No usable Google sign-in is stored"),
        "{err}"
    );
}

/// Without a client file, the sign-in takes the client built into the crate;
/// this build has none, and says how to sign in instead, as a `SignInError`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_without_a_builtin_client_asks_for_a_client_file() {
    let flow = Flow::start().await;
    let err = flow
        .account()
        .sign_in_with(None, flow.options())
        .await
        .unwrap_err();
    let sign_in = SignInError::of(&err).expect("a typed sign-in error");
    assert!(
        matches!(sign_in, SignInError::Failed(message) if message.contains("no built-in OAuth client")),
        "{err}"
    );
    assert!(
        flow.opened.lock().unwrap().is_empty(),
        "no consent page was opened"
    );
    assert!(!flow.token_path().exists());
}

/// The errors a run meets are typed: no stored sign-in is `SignInRequired`,
/// an unusable client file is `Failed`, and a plain engine error is neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sign_in_errors_are_typed() {
    let dir = tempfile::tempdir().unwrap();
    let account = UserAccount::with_token_path(dir.path().join("none.json"));
    let err = GoogleDriveClient::from_user_account(&account)
        .err()
        .unwrap();
    assert!(
        matches!(SignInError::of(&err), Some(SignInError::SignInRequired(_))),
        "{err}"
    );
    assert_eq!(
        SignInError::of(&err).unwrap().message(),
        err.to_string(),
        "the error displays as its message"
    );

    let flow = Flow::start().await;
    let err = flow
        .account()
        .sign_in_with(Some(&flow.dir.path().join("missing.json")), flow.options())
        .await
        .unwrap_err();
    assert!(
        matches!(SignInError::of(&err), Some(SignInError::Failed(_))),
        "{err}"
    );
    assert!(SignInError::of(&cocoindex::Error::engine("other")).is_none());
}
