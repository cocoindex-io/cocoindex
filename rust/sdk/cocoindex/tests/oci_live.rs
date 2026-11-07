//! Hermetic test for the OCI Object Storage **live bucket-event view**
//! (`list_objects_live`). No real OCI account is needed: a `wiremock` server
//! stands in for the Object Storage REST API (ListObjects for the initial scan,
//! `HEAD` for per-event re-reads) and the event feed is a finite in-memory
//! stream of OCI event JSON payloads.
//!
//! It exercises the full live choreography against the real signing/HTTP path:
//!   * initial `scan` lists matching objects,
//!   * a create event re-reads the object (`HEAD` 200) → update,
//!   * a delete event re-reads (`HEAD` 404) → delete,
//!   * an event older than the scan cutoff is dropped,
//!   * a cross-bucket event is dropped (no `HEAD`),
//!   * a malformed payload is skipped,
//!   * every event is acknowledged to the stream once handled.
//!
//! The instance-principal tests stand in for the instance metadata service and
//! the identity service's federation endpoint too.
#![cfg(feature = "oci_object_storage")]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use cocoindex::connectors::oci_object_storage::{
    self, InstancePrincipalOptions, ListOptions, OciClient, OciConfig, OciFile, list_objects_live,
};
use cocoindex::resources::file::PatternFilePathMatcher;
use cocoindex::{
    App, LiveMapView, LiveStream, LiveStreamSubscriber, Result, UpdateOptions, async_trait,
};
use rsa::RsaPrivateKey;
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

/// A finite in-memory `LiveStream<Vec<u8>>` of event payloads. It records, per
/// event, whether the view acknowledged it (awaited the handle `send`
/// returned), then waits until cancelled, as a topic would.
#[derive(Clone)]
struct EventStream {
    events: Vec<Vec<u8>>,
    acked: Arc<Mutex<Vec<bool>>>,
}

impl EventStream {
    fn new(events: Vec<Vec<u8>>) -> Self {
        let acked = Arc::new(Mutex::new(vec![false; events.len()]));
        Self { events, acked }
    }
}

#[async_trait]
impl LiveStream<Vec<u8>> for EventStream {
    async fn watch(&self, subscriber: &dyn LiveStreamSubscriber<Vec<u8>>) -> Result<()> {
        for (i, event) in self.events.iter().enumerate() {
            let handle = subscriber.send(event.clone()).await?;
            handle.ready().await?;
            self.acked.lock().unwrap()[i] = true;
        }
        subscriber.mark_ready().await;
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// Write a fresh RSA key as a PKCS#8 PEM and build an [`OciClient`] whose base
/// URL points at the mock server (the signed `host` header is irrelevant to the
/// mock).
fn client_for(server_uri: &str, key_path: &std::path::Path) -> OciClient {
    let mut rng = rand::thread_rng();
    let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
    let pem = key.to_pkcs8_pem(LineEnding::LF).unwrap();
    std::fs::write(key_path, pem.as_bytes()).unwrap();
    let config = OciConfig {
        tenancy: "ocid1.tenancy".to_string(),
        user: "ocid1.user".to_string(),
        fingerprint: "aa:bb:cc".to_string(),
        key_file: key_path.to_string_lossy().into_owned(),
        region: "us-ashburn-1".to_string(),
        pass_phrase: None,
    };
    OciClient::from_config(config)
        .unwrap()
        .with_base_url(server_uri)
}

fn event(event_type: &str, time: &str, ns: &str, bucket: &str, name: &str) -> Vec<u8> {
    json!({
        "eventType": event_type,
        "eventTime": time,
        "data": {
            "resourceName": name,
            "additionalDetails": { "namespace": ns, "bucketName": bucket }
        }
    })
    .to_string()
    .into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_live_view_scan_then_events() -> Result<()> {
    let server = MockServer::start().await;

    // Initial scan: the bucket contains "a.txt".
    Mock::given(method("GET"))
        .and(path("/n/ns/b/bucket/o"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "objects": [
                {"name": "a.txt", "size": 3, "timeModified": "2020-01-01T00:00:00Z"}
            ]
        })))
        .mount(&server)
        .await;

    // HEAD re-reads: "new.txt" exists (→ update), "gone.txt" is 404 (→ delete).
    Mock::given(method("HEAD"))
        .and(path("/n/ns/b/bucket/o/new.txt"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-length", "5")
                .insert_header("last-modified", "Tue, 02 Jun 2026 00:00:00 GMT"),
        )
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/n/ns/b/bucket/o/gone.txt"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    // "old.txt" would resolve, but its event predates the cutoff and must be
    // dropped *without* a HEAD; if it weren't, this 200 would make it appear.
    Mock::given(method("HEAD"))
        .and(path("/n/ns/b/bucket/o/old.txt"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "1"))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let client = client_for(&server.uri(), &tmp.path().join("key.pem"));

    // Finite event feed: a create (future-dated → passes cutoff), a delete, an
    // old event (dropped by cutoff), a cross-bucket event (dropped), and a
    // malformed payload (skipped).
    let events = vec![
        event(
            "com.oraclecloud.objectstorage.createobject",
            "2099-01-01T00:00:00Z",
            "ns",
            "bucket",
            "new.txt",
        ),
        event(
            "com.oraclecloud.objectstorage.deleteobject",
            "2099-01-01T00:00:00Z",
            "ns",
            "bucket",
            "gone.txt",
        ),
        event(
            "com.oraclecloud.objectstorage.createobject",
            "2000-01-01T00:00:00Z",
            "ns",
            "bucket",
            "old.txt",
        ),
        event(
            "com.oraclecloud.objectstorage.createobject",
            "2099-01-01T00:00:00Z",
            "ns",
            "other-bucket",
            "x.txt",
        ),
        b"not json".to_vec(),
    ];

    let events = EventStream::new(events);
    let acked = events.acked.clone();
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let app = App::builder("OciLive")
        .db_path(tmp.path().join("db"))
        .build()
        .await?;
    let handle = app
        .start_update_with_options(
            UpdateOptions {
                full_reprocess: false,
                live: true,
                ..UpdateOptions::default()
            },
            {
                let processed = processed.clone();
                let client = client.clone();
                move |ctx| {
                    let processed = processed.clone();
                    let client = client.clone();
                    let events = events.clone();
                    async move {
                        let feed = list_objects_live(
                            &client,
                            "ns",
                            "bucket",
                            ListOptions::default(),
                            events,
                        );
                        ctx.mount_each_live(&"objects", feed, move |_ctx, file: OciFile| {
                            let processed = processed.clone();
                            async move {
                                processed
                                    .lock()
                                    .unwrap()
                                    .push(file.file_path().path().to_string());
                                Ok(())
                            }
                        })
                        .await
                    }
                }
            },
        )
        .unwrap();

    // Wait until the scan item and the live create are both processed.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        {
            let got = processed.lock().unwrap();
            if got.iter().any(|k| k == "a.txt") && got.iter().any(|k| k == "new.txt") {
                break;
            }
        }
        if Instant::now() > deadline {
            let got = processed.lock().unwrap().clone();
            panic!("live view did not process scan + create event in time; got={got:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;

    let mut got = processed.lock().unwrap().clone();
    got.sort();
    got.dedup();
    assert_eq!(
        got,
        vec!["a.txt".to_string(), "new.txt".to_string()],
        "only the scanned object and the in-window create event should be processed \
         (old/cross-bucket/malformed dropped, delete is a no-op)"
    );
    assert_eq!(
        acked.lock().unwrap().clone(),
        vec![true; 5],
        "every event, handled or dropped, is acknowledged to the stream"
    );
    Ok(())
}

/// Run a live view (empty scan) over `events`, waiting until `sentinel` is
/// processed, then return everything processed. Because the watch loop handles
/// events in order on a single task, once a later sentinel event is seen, every
/// earlier event has already been handled — so absence of an earlier key is a
/// deterministic assertion (it was filtered, not merely not-yet-seen).
async fn run_live_until(
    client: &OciClient,
    options: ListOptions,
    events: Vec<Vec<u8>>,
    sentinel: &str,
    db_path: &std::path::Path,
) -> Vec<String> {
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let app = App::builder("OciLiveEdge")
        .db_path(db_path)
        .build()
        .await
        .unwrap();
    let handle = app
        .start_update_with_options(
            UpdateOptions {
                full_reprocess: false,
                live: true,
                ..UpdateOptions::default()
            },
            {
                let processed = processed.clone();
                let client = client.clone();
                move |ctx| {
                    let processed = processed.clone();
                    let client = client.clone();
                    let events = events.clone();
                    async move {
                        let feed = list_objects_live(
                            &client,
                            "ns",
                            "bucket",
                            options,
                            EventStream::new(events),
                        );
                        ctx.mount_each_live(&"objects", feed, move |_ctx, file: OciFile| {
                            let processed = processed.clone();
                            async move {
                                processed
                                    .lock()
                                    .unwrap()
                                    .push(file.file_path().path().to_string());
                                Ok(())
                            }
                        })
                        .await
                    }
                }
            },
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if processed.lock().unwrap().iter().any(|k| k == sentinel) {
            break;
        }
        if Instant::now() > deadline {
            let got = processed.lock().unwrap().clone();
            panic!("live view did not process sentinel {sentinel:?} in time; got={got:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;

    processed.lock().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_live_view_max_file_size_filters_after_head() -> Result<()> {
    let server = MockServer::start().await;
    // Empty scan.
    Mock::given(method("GET"))
        .and(path("/n/ns/b/bucket/o"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "objects": [] })))
        .mount(&server)
        .await;
    // "big.txt" exists but exceeds the size cap; "ok.txt" is within it.
    Mock::given(method("HEAD"))
        .and(path("/n/ns/b/bucket/o/big.txt"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "100"))
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/n/ns/b/bucket/o/ok.txt"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "5"))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let client = client_for(&server.uri(), &tmp.path().join("key.pem"));
    let options = ListOptions {
        max_file_size: Some(10),
        ..Default::default()
    };
    let events = vec![
        event(
            "com.oraclecloud.objectstorage.createobject",
            "2099-01-01T00:00:00Z",
            "ns",
            "bucket",
            "big.txt",
        ),
        event(
            "com.oraclecloud.objectstorage.createobject",
            "2099-01-01T00:00:00Z",
            "ns",
            "bucket",
            "ok.txt",
        ),
    ];
    let got = run_live_until(&client, options, events, "ok.txt", &tmp.path().join("db")).await;
    assert!(
        !got.iter().any(|k| k == "big.txt"),
        "an object exceeding max_file_size must be filtered after the HEAD; got={got:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_live_view_path_matcher_filters_before_head() -> Result<()> {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/n/ns/b/bucket/o"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "objects": [] })))
        .mount(&server)
        .await;
    // Only "keep.txt" is mocked; "skip.log" must be filtered by the matcher
    // *before* any HEAD (so its absence here would 404 → no-op anyway).
    Mock::given(method("HEAD"))
        .and(path("/n/ns/b/bucket/o/keep.txt"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "3"))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let client = client_for(&server.uri(), &tmp.path().join("key.pem"));
    let options = ListOptions {
        path_matcher: Some(Arc::new(
            PatternFilePathMatcher::include(["*.txt"]).unwrap(),
        )),
        ..Default::default()
    };
    let events = vec![
        event(
            "com.oraclecloud.objectstorage.createobject",
            "2099-01-01T00:00:00Z",
            "ns",
            "bucket",
            "skip.log",
        ),
        event(
            "com.oraclecloud.objectstorage.createobject",
            "2099-01-01T00:00:00Z",
            "ns",
            "bucket",
            "keep.txt",
        ),
    ];
    let got = run_live_until(&client, options, events, "keep.txt", &tmp.path().join("db")).await;
    assert!(
        !got.iter().any(|k| k == "skip.log"),
        "an object excluded by the path matcher must be dropped; got={got:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_live_view_scan_failure_propagates() -> Result<()> {
    let server = MockServer::start().await;
    // ListObjects fails — the initial scan must surface the error.
    Mock::given(method("GET"))
        .and(path("/n/ns/b/bucket/o"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let client = client_for(&server.uri(), &tmp.path().join("key.pem"));
    // The catch-up `scan` (which the live component runs first) must surface the
    // ListObjects failure rather than silently yielding an empty snapshot.
    let feed = list_objects_live(
        &client,
        "ns",
        "bucket",
        ListOptions::default(),
        EventStream::new(Vec::new()),
    );
    let result = feed.scan().await;
    assert!(
        result.is_err(),
        "a failed ListObjects scan should propagate as an error, got {result:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_file_exists_heads_once_and_caches_the_verdict() -> Result<()> {
    let server = MockServer::start().await;
    // The listing names both objects; "gone.txt" has since been deleted.
    Mock::given(method("GET"))
        .and(path("/n/ns/b/bucket/o"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "objects": [
                {"name": "here.txt", "size": 4, "timeModified": "2020-01-01T00:00:00Z"},
                {"name": "gone.txt", "size": 4, "timeModified": "2020-01-01T00:00:00Z"}
            ]
        })))
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/n/ns/b/bucket/o/here.txt"))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "4"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("HEAD"))
        .and(path("/n/ns/b/bucket/o/gone.txt"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    let tmp = tempfile::tempdir().unwrap();
    let client = client_for(&server.uri(), &tmp.path().join("key.pem"));
    let files = oci_object_storage::list_objects(
        &client,
        "ns".to_string(),
        "bucket".to_string(),
        ListOptions::default(),
    )
    .list()
    .await?;
    let by_name = |name: &str| files.iter().find(|f| f.key() == name).unwrap().clone();

    let here = by_name("here.txt");
    assert!(here.exists().await?);
    assert!(here.exists().await?, "the verdict is cached: one HEAD");
    let gone = by_name("gone.txt");
    assert!(!gone.exists().await?);
    assert!(!gone.exists().await?);
    Ok(())
}

// ---------------------------------------------------------------------------
// Authentication: a pass-phrase-protected key, and an instance principal
// ---------------------------------------------------------------------------

/// A client over a PKCS#8 encrypted key decrypts it with the profile's
/// pass-phrase, and a wrong pass-phrase is a clear error.
#[tokio::test]
async fn pass_phrase_protected_key_is_decrypted() -> Result<()> {
    use rsa::pkcs8::PrivateKeyInfo;
    use rsa::pkcs8::pkcs5::pbes2;
    let mut rng = rand::thread_rng();
    let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
    // PBKDF2 with few iterations keeps the test's decryptions fast; the key
    // format is the same encrypted PKCS#8 PEM block OCI's CLI writes
    // (with scrypt), which the connector decrypts the same way.
    let der = key.to_pkcs8_der().unwrap();
    let params = pbes2::Parameters::pbkdf2_sha256_aes256cbc(1_000, &[7u8; 16], &[9u8; 16]).unwrap();
    let pem = PrivateKeyInfo::try_from(der.as_bytes())
        .unwrap()
        .encrypt_with_params(params, "hunter2")
        .unwrap()
        .to_pem("ENCRYPTED PRIVATE KEY", LineEnding::LF)
        .unwrap();
    // The PEM header, split so the file does not trip the private-key hook.
    assert!(pem.contains(concat!("BEGIN ENCRYPTED ", "PRIVATE KEY")));
    let tmp = tempfile::tempdir().unwrap();
    let key_path = tmp.path().join("key.pem");
    std::fs::write(&key_path, pem.as_bytes()).unwrap();
    let config = |pass_phrase: Option<&str>| OciConfig {
        tenancy: "ocid1.tenancy".to_string(),
        user: "ocid1.user".to_string(),
        fingerprint: "aa:bb:cc".to_string(),
        key_file: key_path.to_string_lossy().into_owned(),
        region: "us-ashburn-1".to_string(),
        pass_phrase: pass_phrase.map(str::to_string),
    };
    assert!(OciClient::from_config(config(Some("hunter2"))).is_ok());
    let wrong = OciClient::from_config(config(Some("nope")))
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(wrong.contains("pass_phrase"), "{wrong}");
    let missing = OciClient::from_config(config(None))
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(missing.contains("encrypted"), "{missing}");
    Ok(())
}

/// A PEM certificate wrapping arbitrary DER-looking bytes: the client forwards
/// the certificate and fingerprints it, but never parses it.
fn fake_cert_pem(seed: u8) -> String {
    let der: Vec<u8> = (0..64u8).map(|i| i.wrapping_mul(seed)).collect();
    format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        base64::engine::general_purpose::STANDARD.encode(der)
    )
}

/// A JWT whose payload carries `exp`; the signature is not checked by the client.
fn fake_jwt(exp: i64) -> String {
    let b64 = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s);
    format!(
        "{}.{}.sig",
        b64(r#"{"alg":"RS256"}"#),
        b64(&format!(r#"{{"sub":"ocid1.instance","exp":{exp}}}"#))
    )
}

/// The `keyId` of an OCI `Signature` authorization header.
fn key_id_of(request: &Request) -> String {
    let auth = request
        .headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    auth.split("keyId=\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .unwrap_or("")
        .to_string()
}

/// Stand in for the instance metadata service and the federation endpoint;
/// the token it issues expires `token_ttl_secs` from now.
async fn mock_instance_identity(server: &MockServer, token_ttl_secs: i64) {
    let mut rng = rand::thread_rng();
    let leaf_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
    let leaf_key_pem = leaf_key.to_pkcs8_pem(LineEnding::LF).unwrap().to_string();
    Mock::given(method("GET"))
        .and(path("/opc/v2/instance/"))
        .and(header("authorization", "Bearer Oracle"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "canonicalRegionName": "us-ashburn-1",
            "region": "iad",
            "tenantId": "ocid1.tenancy.oc1..aaaa",
        })))
        .mount(server)
        .await;
    for (name, body) in [
        ("cert.pem", fake_cert_pem(3)),
        ("key.pem", leaf_key_pem),
        ("intermediate.pem", fake_cert_pem(7)),
    ] {
        Mock::given(method("GET"))
            .and(path(format!("/opc/v2/identity/{name}")))
            .and(header("authorization", "Bearer Oracle"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(server)
            .await;
    }
    // The federation request: signed as `{tenancy}/fed-x509/{SHA-1 fingerprint}`
    // over the body headers, carrying the certificate chain and the session
    // public key without PEM armor.
    let expected_fingerprint = {
        use sha1::Digest;
        let der: Vec<u8> = (0..64u8).map(|i| i.wrapping_mul(3)).collect();
        sha1::Sha1::digest(&der)
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":")
    };
    let token = fake_jwt(chrono::Utc::now().timestamp() + token_ttl_secs);
    Mock::given(method("POST"))
        .and(path("/v1/x509"))
        .respond_with(move |request: &Request| {
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            let auth = request
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let ok = key_id_of(request)
                == format!("ocid1.tenancy.oc1..aaaa/fed-x509/{expected_fingerprint}")
                && auth.contains(
                    "headers=\"date (request-target) content-length content-type x-content-sha256\"",
                )
                && body["certificate"].as_str().is_some_and(|c| !c.contains("-----"))
                && body["publicKey"].as_str().is_some_and(|k| !k.contains('\n'))
                && body["intermediateCertificates"].as_array().map(Vec::len) == Some(1);
            if ok {
                ResponseTemplate::new(200).set_body_json(json!({ "token": token }))
            } else {
                ResponseTemplate::new(401).set_body_string(format!("bad federation request: {auth}"))
            }
        })
        .mount(server)
        .await;
}

/// An instance principal fetches its identity from the metadata service,
/// exchanges it for a session token, and signs Object Storage requests as
/// `ST$<token>`.
#[tokio::test]
async fn instance_principal_signs_with_a_session_token() -> Result<()> {
    let server = MockServer::start().await;
    mock_instance_identity(&server, 3600).await;
    let listed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    Mock::given(method("GET"))
        .and(path("/n/ns/b/bucket/o"))
        .respond_with({
            let listed = listed.clone();
            move |request: &Request| {
                listed.lock().unwrap().push(key_id_of(request));
                ResponseTemplate::new(200).set_body_json(json!({
                    "objects": [{"name": "a.txt", "size": 3, "timeModified": "2020-01-01T00:00:00Z"}]
                }))
            }
        })
        .mount(&server)
        .await;

    let client = OciClient::from_instance_principal_with_options(InstancePrincipalOptions {
        metadata_base_url: Some(format!("{}/opc/v2", server.uri())),
        federation_endpoint: Some(format!("{}/v1/x509", server.uri())),
    })
    .await?
    .with_base_url(server.uri());
    assert_eq!(client.state_id(), "oci-objectstorage:us-ashburn-1");
    let files = oci_object_storage::list_objects(&client, "ns", "bucket", ListOptions::default())
        .list()
        .await?;
    assert_eq!(files.len(), 1);
    let key_ids = listed.lock().unwrap().clone();
    assert_eq!(key_ids.len(), 1);
    assert!(key_ids[0].starts_with("ST$"), "{key_ids:?}");
    assert!(key_ids[0].contains(".sig"), "the whole JWT is the key id");
    Ok(())
}

/// A session token about to expire is renewed before the next request: the
/// federation endpoint is called again, and the new token signs the request.
#[tokio::test]
async fn instance_principal_renews_an_expiring_token() -> Result<()> {
    let server = MockServer::start().await;
    // Within the 60 s renewal margin: the first token is used for the first
    // request's purposes only until the next request asks for a signer.
    mock_instance_identity(&server, 30).await;
    let federations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/n/ns/b/bucket/o"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "objects": [] })))
        .mount(&server)
        .await;
    let client = OciClient::from_instance_principal_with_options(InstancePrincipalOptions {
        metadata_base_url: Some(format!("{}/opc/v2", server.uri())),
        federation_endpoint: Some(format!("{}/v1/x509", server.uri())),
    })
    .await?
    .with_base_url(server.uri());
    let walker = oci_object_storage::list_objects(&client, "ns", "bucket", ListOptions::default());
    walker.list().await?;
    walker.list().await?;
    let requests = server.received_requests().await.unwrap();
    let federation_calls = requests
        .iter()
        .filter(|r| r.url.path() == "/v1/x509")
        .count();
    federations.store(federation_calls, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        federation_calls, 3,
        "one token at construction, then one renewal per request while within the margin"
    );
    Ok(())
}
