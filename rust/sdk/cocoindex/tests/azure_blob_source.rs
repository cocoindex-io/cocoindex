//! Integration tests for the `azure_blob` source.
//!
//! Most tests run against an in-process fake of the Blob service REST API (a
//! `wiremock` server), which checks each request's authentication: it recomputes
//! the Shared Key signature with the signer in this file, written from the
//! specification independently of the connector's.
//!
//! The `*_on_azurite_when_available` test runs against the Azurite emulator and
//! skips unless `AZURITE_BLOB_ENDPOINT` is set:
//!   docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \
//!     azurite-blob --blobHost 0.0.0.0
//!   AZURITE_BLOB_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1 \
//!     cargo test -p cocoindex --features azure_blob --test azure_blob_source
#![cfg(feature = "azure_blob")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use cocoindex::connectors::azure_blob::{
    self, AzureBlobFile, ContainerClient, Credential, ListOptions,
};
use cocoindex::resources::file::{FileLike, PatternFilePathMatcher};
use cocoindex::{Environment, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use wiremock::http::{HeaderMap, Method};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const ACCOUNT: &str = "testaccount";
const CONTAINER: &str = "docs";
/// Azurite's well-known development account and key.
const AZURITE_ACCOUNT: &str = "devstoreaccount1";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

// ---------------------------------------------------------------------------
// Shared Key signing, from the specification ("Authorize with Shared Key",
// Blob service, version 2015-02-21 and later).
// ---------------------------------------------------------------------------

fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
}

fn shared_key_authorization(
    account: &str,
    key_base64: &str,
    method: &str,
    url: &reqwest::Url,
    headers: &HeaderMap,
) -> String {
    let content_length = match header(headers, "content-length") {
        "0" => "",
        length => length,
    };
    let mut string_to_sign = [
        method,
        header(headers, "content-encoding"),
        header(headers, "content-language"),
        content_length,
        header(headers, "content-md5"),
        header(headers, "content-type"),
        header(headers, "date"),
        header(headers, "if-modified-since"),
        header(headers, "if-match"),
        header(headers, "if-none-match"),
        header(headers, "if-unmodified-since"),
        header(headers, "range"),
    ]
    .join("\n");
    string_to_sign.push('\n');

    let mut ms_headers: Vec<(String, &str)> = headers
        .iter()
        .map(|(name, value)| (name.as_str().to_lowercase(), value.to_str().unwrap()))
        .filter(|(name, _)| name.starts_with("x-ms-"))
        .collect();
    ms_headers.sort();
    for (name, value) in ms_headers {
        string_to_sign.push_str(&format!("{name}:{value}\n"));
    }

    string_to_sign.push_str(&format!("/{account}{}", url.path()));
    let mut params: Vec<(String, String)> = url
        .query_pairs()
        .map(|(name, value)| (name.to_lowercase(), value.into_owned()))
        .collect();
    params.sort();
    for (name, value) in params {
        string_to_sign.push_str(&format!("\n{name}:{value}"));
    }

    let mut mac = Hmac::<Sha256>::new_from_slice(&BASE64.decode(key_base64).unwrap()).unwrap();
    mac.update(string_to_sign.as_bytes());
    format!(
        "SharedKey {account}:{}",
        BASE64.encode(mac.finalize().into_bytes())
    )
}

// ---------------------------------------------------------------------------
// The fake Blob service
// ---------------------------------------------------------------------------

/// The authentication the fake service accepts.
#[derive(Clone)]
enum Accepts {
    SharedKey { key_base64: String },
    Sas { query: &'static str },
    Bearer { token: &'static str },
}

struct StoredBlob {
    content: Vec<u8>,
    /// Bumped by every write; the ETag is derived from it.
    version: u64,
}

type BlobStore = Arc<Mutex<BTreeMap<String, StoredBlob>>>;

/// Serves List Blobs, Get Blob Properties and Get Blob for one container at
/// `/{ACCOUNT}/{CONTAINER}`, two blobs per listing page.
struct FakeBlobService {
    blobs: BlobStore,
    accepts: Accepts,
}

const PAGE_SIZE: usize = 2;
/// When a blob's first version was written (Fri, 02 Jan 2026 03:04:05 GMT);
/// each rewrite is one second later, as a real write moves `Last-Modified`.
const LAST_MODIFIED_SECS: i64 = 1_767_323_045;

fn last_modified(version: u64) -> String {
    let secs = LAST_MODIFIED_SECS as u64 + version - 1;
    httpdate::fmt_http_date(UNIX_EPOCH + std::time::Duration::from_secs(secs))
}

fn error(status: u16, code: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).insert_header("x-ms-error-code", code)
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;")
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

impl FakeBlobService {
    fn is_authorized(&self, request: &Request) -> bool {
        let authorization = header(&request.headers, "authorization");
        match &self.accepts {
            Accepts::SharedKey { key_base64 } => {
                authorization
                    == shared_key_authorization(
                        ACCOUNT,
                        key_base64,
                        request.method.as_str(),
                        &request.url,
                        &request.headers,
                    )
            }
            Accepts::Sas { query } => {
                authorization.is_empty()
                    && request
                        .url
                        .query()
                        .is_some_and(|q| q == *query || q.ends_with(&format!("&{query}")))
            }
            Accepts::Bearer { token } => authorization == format!("Bearer {token}"),
        }
    }

    fn list(&self, request: &Request) -> ResponseTemplate {
        let param = |name: &str| {
            request
                .url
                .query_pairs()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.into_owned())
        };
        if param("restype").as_deref() != Some("container")
            || param("comp").as_deref() != Some("list")
        {
            return error(400, "InvalidQueryParameterValue");
        }
        let prefix = param("prefix").unwrap_or_default();
        let marker = param("marker").unwrap_or_default();
        let blobs = self.blobs.lock().unwrap();
        let mut matching = blobs
            .iter()
            .filter(|(name, _)| name.starts_with(&prefix) && name.as_str() >= marker.as_str());
        let mut body = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
             <EnumerationResults ContainerName=\"{CONTAINER}\"><Blobs>"
        );
        for (name, blob) in matching.by_ref().take(PAGE_SIZE) {
            body.push_str(&format!(
                "<Blob><Name>{}</Name><Properties>\
                 <Last-Modified>{}</Last-Modified>\
                 <Etag>0x{:X}</Etag>\
                 <Content-Length>{}</Content-Length>\
                 <BlobType>BlockBlob</BlobType>\
                 </Properties></Blob>",
                xml_escape(name),
                last_modified(blob.version),
                blob.version,
                blob.content.len(),
            ));
        }
        body.push_str("</Blobs>");
        match matching.next() {
            Some((next, _)) => {
                body.push_str(&format!("<NextMarker>{}</NextMarker>", xml_escape(next)))
            }
            None => body.push_str("<NextMarker />"),
        }
        body.push_str("</EnumerationResults>");
        ResponseTemplate::new(200).set_body_raw(body, "application/xml")
    }

    fn blob(&self, request: &Request, name: &str) -> ResponseTemplate {
        let blobs = self.blobs.lock().unwrap();
        let Some(blob) = blobs.get(name) else {
            return error(404, "BlobNotFound");
        };
        let range = header(&request.headers, "x-ms-range");
        let (status, content) = match range.strip_prefix("bytes=0-") {
            Some(_) if blob.content.is_empty() => return error(416, "InvalidRange"),
            Some(last) => {
                let end = (last.parse::<usize>().unwrap() + 1).min(blob.content.len());
                (206, &blob.content[..end])
            }
            None => (200, blob.content.as_slice()),
        };
        // A HEAD response carries the headers of the GET, without the body.
        ResponseTemplate::new(status)
            .insert_header("ETag", format!("\"0x{:X}\"", blob.version))
            .insert_header("Last-Modified", last_modified(blob.version))
            .set_body_raw(content.to_vec(), "application/octet-stream")
    }
}

impl Respond for FakeBlobService {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        if !self.is_authorized(request) {
            return error(403, "AuthenticationFailed");
        }
        if header(&request.headers, "x-ms-version").is_empty()
            || header(&request.headers, "x-ms-date").is_empty()
        {
            return error(400, "MissingRequiredHeader");
        }
        let container_path = format!("/{ACCOUNT}/{CONTAINER}");
        let path = percent_decode(request.url.path());
        let is_read = request.method == Method::GET || request.method == Method::HEAD;
        match path.strip_prefix(&container_path) {
            Some("") if request.method == Method::GET => self.list(request),
            Some(rest) if is_read && rest.starts_with('/') => self.blob(request, &rest[1..]),
            _ => error(400, "UnsupportedRequest"),
        }
    }
}

/// A running fake service and its blob store.
struct Fake {
    server: MockServer,
    blobs: BlobStore,
}

impl Fake {
    async fn start(accepts: Accepts, objects: &[(&str, &[u8])]) -> Self {
        let blobs: BlobStore = Arc::default();
        let server = MockServer::start().await;
        Mock::given(wiremock::matchers::any())
            .respond_with(FakeBlobService {
                blobs: blobs.clone(),
                accepts,
            })
            .mount(&server)
            .await;
        let fake = Self { server, blobs };
        for (name, content) in objects {
            fake.put(name, content);
        }
        fake
    }

    fn account_url(&self) -> String {
        format!("{}/{ACCOUNT}", self.server.uri())
    }

    fn client(&self, credential: Credential) -> ContainerClient {
        ContainerClient::new(&self.account_url(), CONTAINER, credential).unwrap()
    }

    fn put(&self, name: &str, content: &[u8]) {
        let mut blobs = self.blobs.lock().unwrap();
        let version = blobs.get(name).map_or(1, |blob| blob.version + 1);
        blobs.insert(
            name.to_string(),
            StoredBlob {
                content: content.to_vec(),
                version,
            },
        );
    }

    fn delete(&self, name: &str) {
        self.blobs.lock().unwrap().remove(name);
    }
}

fn test_key() -> String {
    BASE64.encode(b"a test account key, 32 bytes long")
}

async fn start_with_shared_key(objects: &[(&str, &[u8])]) -> (Fake, ContainerClient) {
    let fake = Fake::start(
        Accepts::SharedKey {
            key_base64: test_key(),
        },
        objects,
    )
    .await;
    let client = fake.client(Credential::AccountKey(test_key()));
    (fake, client)
}

const SAMPLE_OBJECTS: &[(&str, &[u8])] = &[
    ("docs/a.md", b"# A\nalpha"),
    ("docs/b.md", b"# B\nbeta beta"),
    ("docs/notes.txt", b"plain text"),
    ("docs/skip/c.md", b"# C skipped"),
    ("docs/sub//d e&f.md", b"# D odd name"),
    ("docs/large.md", &[b'x'; 5000]),
    ("docs/empty.md", b""),
    ("docs/", b""), // directory marker: must be skipped
    ("docsa.md", b"# not in the docs folder"),
    ("other/e.md", b"# E outside prefix"),
];

/// Sorted relative-path keys of a list of files.
fn keys(files: &[AzureBlobFile]) -> Vec<String> {
    let mut keys: Vec<String> = files.iter().map(AzureBlobFile::key).collect();
    keys.sort();
    keys
}

fn under(prefix: &str) -> ListOptions {
    ListOptions {
        prefix: prefix.to_string(),
        ..Default::default()
    }
}

/// The listing, filtering and reading behavior every backend must show for
/// [`SAMPLE_OBJECTS`].
async fn check_lists_reads_and_filters(client: &ContainerClient) -> Result<()> {
    // --- whole container: every blob but the directory marker ---
    let all = azure_blob::list_blobs(client, ListOptions::default())
        .list()
        .await?;
    assert_eq!(
        keys(&all),
        vec![
            "docs/a.md",
            "docs/b.md",
            "docs/empty.md",
            "docs/large.md",
            "docs/notes.txt",
            "docs/skip/c.md",
            "docs/sub//d e&f.md",
            "docsa.md",
            "other/e.md",
        ]
    );

    // --- a prefix names a folder, with or without the trailing slash; keys are
    // the rest of the blob name, exactly as stored ---
    for prefix in ["docs", "docs/"] {
        let under_docs = azure_blob::list_blobs(client, under(prefix)).list().await?;
        assert_eq!(
            keys(&under_docs),
            vec![
                "a.md",
                "b.md",
                "empty.md",
                "large.md",
                "notes.txt",
                "skip/c.md",
                "sub//d e&f.md",
            ],
            "prefix {prefix:?}"
        );
    }

    // --- include + exclude pattern matcher (on the relative path) ---
    let matcher = Arc::new(PatternFilePathMatcher::new(["**/*.md"], ["**/skip/**"]).unwrap());
    let md_only = azure_blob::list_blobs(
        client,
        ListOptions {
            prefix: "docs/".to_string(),
            path_matcher: Some(matcher),
            ..Default::default()
        },
    )
    .list()
    .await?;
    assert_eq!(
        keys(&md_only),
        vec!["a.md", "b.md", "empty.md", "large.md", "sub//d e&f.md"]
    );

    // --- max_file_size ---
    let small = azure_blob::list_blobs(
        client,
        ListOptions {
            prefix: "docs/".to_string(),
            max_file_size: Some(1000),
            ..Default::default()
        },
    )
    .list()
    .await?;
    assert!(!keys(&small).contains(&"large.md".to_string()));
    assert!(keys(&small).contains(&"a.md".to_string()));

    // --- items() yields (relative key, file); a listed file reads itself ---
    let items = azure_blob::list_blobs(client, under("docs/"))
        .items()
        .await?;
    let (_, listed_a) = items
        .iter()
        .find(|(key, _)| key == "a.md")
        .expect("a.md is listed");
    assert_eq!(listed_a.size, 9);
    assert_eq!(listed_a.file_path().path(), "a.md");
    assert_eq!(listed_a.file_path().resolve(), "docs/a.md");
    assert_eq!(listed_a.file_path().account_name(), client.account_name());
    assert_eq!(
        listed_a.file_path().container_name(),
        client.container_name()
    );
    assert_eq!(listed_a.read_text().await?, "# A\nalpha");
    let (_, odd) = items
        .iter()
        .find(|(key, _)| key == "sub//d e&f.md")
        .expect("the oddly named blob is listed");
    assert_eq!(odd.read_text().await?, "# D odd name");

    // --- get_blob: metadata that agrees with the listing's, and reads ---
    let a = azure_blob::get_blob(client, "docs/a.md").await?;
    assert_eq!(a.size, 9);
    assert_eq!(a.key(), "docs/a.md"); // no prefix: the full blob name
    assert_eq!(a.file_path().resolve(), "docs/a.md");
    assert!(a.modified_secs.is_some());
    assert_eq!(a.modified_secs, listed_a.modified_secs);
    assert!(a.etag.is_some());
    assert_eq!(a.etag, listed_a.etag, "HEAD and listing give one ETag");
    assert_eq!(
        FileLike::content_fingerprint(&a).await?,
        FileLike::content_fingerprint(listed_a).await?
    );
    assert_eq!(&a.read().await?[..], b"# A\nalpha");
    assert_eq!(&a.read_size(3).await?[..], b"# A");

    // --- read by name: whole, ranged, past the end, zero bytes, empty blob ---
    assert_eq!(
        azure_blob::read(client, "docs/b.md", None).await?,
        b"# B\nbeta beta"
    );
    assert_eq!(
        azure_blob::read(client, "docs/b.md", Some(3)).await?,
        b"# B"
    );
    assert_eq!(
        azure_blob::read(client, "docs/a.md", Some(1000)).await?,
        b"# A\nalpha"
    );
    assert_eq!(azure_blob::read(client, "docs/a.md", Some(0)).await?, b"");
    assert_eq!(azure_blob::read(client, "docs/empty.md", None).await?, b"");
    assert_eq!(
        azure_blob::read(client, "docs/empty.md", Some(10)).await?,
        b""
    );

    // --- a missing blob is an error that names it ---
    let missing = azure_blob::get_blob(client, "docs/missing.md")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        missing.contains("docs/missing.md") && missing.contains("404"),
        "{missing}"
    );
    assert!(
        azure_blob::read(client, "docs/missing.md", None)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn account_key_lists_reads_and_filters() -> Result<()> {
    let (_fake, client) = start_with_shared_key(SAMPLE_OBJECTS).await;
    assert_eq!(client.account_name(), ACCOUNT);
    assert_eq!(client.container_name(), CONTAINER);
    check_lists_reads_and_filters(&client).await?;

    let a = azure_blob::get_blob(&client, "docs/a.md").await?;
    assert_eq!(a.modified_secs, Some(LAST_MODIFIED_SECS));
    Ok(())
}

#[tokio::test]
async fn empty_container_lists_nothing() -> Result<()> {
    let (_fake, client) = start_with_shared_key(&[]).await;
    let files = azure_blob::list_blobs(&client, ListOptions::default())
        .list()
        .await?;
    assert!(files.is_empty());
    Ok(())
}

#[tokio::test]
async fn sas_token_and_bearer_token_authenticate() -> Result<()> {
    const SAS: &str = "sv=2021-08-06&sp=rl&sr=c&sig=abc%2Bdef%3D";
    let cases = [
        (
            Accepts::Sas { query: SAS },
            // The leading `?` of a copied SAS is accepted.
            Credential::SasToken(format!("?{SAS}")),
        ),
        (
            Accepts::Bearer { token: "token-1" },
            Credential::BearerToken("token-1".to_string()),
        ),
    ];
    for (accepts, credential) in cases {
        let fake = Fake::start(accepts, SAMPLE_OBJECTS).await;
        let client = fake.client(credential);
        check_lists_reads_and_filters(&client).await?;
    }
    Ok(())
}

#[tokio::test]
async fn rejected_credential_is_an_error_with_the_service_code() -> Result<()> {
    let (fake, _) = start_with_shared_key(SAMPLE_OBJECTS).await;
    let wrong = fake.client(Credential::AccountKey(BASE64.encode(b"another key")));

    let list_error = azure_blob::list_blobs(&wrong, ListOptions::default())
        .list()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        list_error.contains("403") && list_error.contains("AuthenticationFailed"),
        "{list_error}"
    );
    assert!(azure_blob::get_blob(&wrong, "docs/a.md").await.is_err());
    assert!(azure_blob::read(&wrong, "docs/a.md", None).await.is_err());
    Ok(())
}

/// End-to-end: list -> `mount_each` -> memoized per-file read. A later run
/// reprocesses only the blob that changed, and drops the one that was deleted.
#[tokio::test]
async fn mount_each_pipeline_reprocesses_only_changed_blobs() -> Result<()> {
    static PROCESSED: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(memo)]
    async fn process(_ctx: &cocoindex::Ctx, file: &AzureBlobFile) -> Result<usize> {
        PROCESSED.fetch_add(1, Ordering::SeqCst);
        Ok(file.read().await?.len())
    }

    cocoindex::context_key!(
        static CLIENT: ContainerClient, key = "azure_blob_test_client"
    );

    let (fake, client) = start_with_shared_key(&[
        ("in/a.md", b"hello"),
        ("in/sub/b.md", b"worldwide"),
        ("in/c.md", b"!"),
    ])
    .await;

    let tempdir = tempfile::tempdir().unwrap();
    let app = Environment::builder()
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&CLIENT, client)
        .build()
        .await?
        .app("AzureBlobPipelineTest")
        .await?;

    let run = || {
        app.update(|ctx| async move {
            let files = azure_blob::list_blobs(ctx.get_key(&CLIENT)?, under("in"))
                .list()
                .await?;
            let sizes = ctx
                .use_mount_each(
                    files,
                    |f| f.key(),
                    |child, file| async move { process(&child, &file).await },
                )
                .await?;
            Ok::<usize, cocoindex::Error>(sizes.into_iter().sum())
        })
    };

    assert_eq!(run().await?, "hello".len() + "worldwide".len() + 1);
    assert_eq!(PROCESSED.load(Ordering::SeqCst), 3);

    // Nothing changed: every file is a memo hit.
    assert_eq!(run().await?, "hello".len() + "worldwide".len() + 1);
    assert_eq!(PROCESSED.load(Ordering::SeqCst), 3);

    // One blob rewritten with content of the same length, one deleted.
    fake.put("in/a.md", b"HELLO");
    fake.delete("in/c.md");
    assert_eq!(run().await?, "HELLO".len() + "worldwide".len());
    assert_eq!(
        PROCESSED.load(Ordering::SeqCst),
        4,
        "only the rewritten blob is read again"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Azurite
// ---------------------------------------------------------------------------

/// Send a Shared Key-signed `PUT` to Azurite (Create Container or Put Blob).
async fn azurite_put(url: &str, extra_headers: &[(&str, &str)], body: &[u8]) {
    let url = reqwest::Url::parse(url).unwrap();
    let mut headers = reqwest::header::HeaderMap::new();
    let mut set = |name: &'static str, value: &str| {
        headers.insert(name, value.parse().unwrap());
    };
    set("x-ms-date", &httpdate::fmt_http_date(SystemTime::now()));
    set("x-ms-version", "2021-08-06");
    set("content-length", &body.len().to_string());
    for (name, value) in extra_headers {
        headers.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    let authorization =
        shared_key_authorization(AZURITE_ACCOUNT, AZURITE_KEY, "PUT", &url, &headers);
    let response = reqwest::Client::new()
        .put(url.clone())
        .headers(headers)
        .header("authorization", authorization)
        .body(body.to_vec())
        .send()
        .await
        .expect("send to Azurite");
    let status = response.status();
    assert!(
        status.is_success(),
        "PUT {url}: {status}: {}",
        response.text().await.unwrap_or_default()
    );
}

#[tokio::test]
async fn account_key_lists_reads_and_filters_on_azurite_when_available() -> Result<()> {
    let Some(endpoint) = std::env::var("AZURITE_BLOB_ENDPOINT")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        eprintln!("skipping Azurite test; AZURITE_BLOB_ENDPOINT is not set");
        return Ok(());
    };
    let endpoint = endpoint.trim_end_matches('/');
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let container = format!("coco-test-{nonce}");
    azurite_put(
        &format!("{endpoint}/{container}?restype=container"),
        &[],
        b"",
    )
    .await;
    for (name, content) in SAMPLE_OBJECTS {
        let encoded: String = name
            .bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect();
        azurite_put(
            &format!("{endpoint}/{container}/{encoded}"),
            &[("x-ms-blob-type", "BlockBlob")],
            content,
        )
        .await;
    }

    let client = ContainerClient::new(
        endpoint,
        container.as_str(),
        Credential::AccountKey(AZURITE_KEY.to_string()),
    )?;
    assert_eq!(client.account_name(), AZURITE_ACCOUNT);
    check_lists_reads_and_filters(&client).await
}
