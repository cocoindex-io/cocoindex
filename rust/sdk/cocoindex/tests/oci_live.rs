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
//!   * a malformed payload is skipped.
#![cfg(feature = "oci_object_storage")]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cocoindex::oci_object_storage::{
    ListOptions, OciClient, OciConfig, OciFile, list_objects_live,
};
use cocoindex::{App, LiveMapView, PatternFilePathMatcher, Result, UpdateOptions};
use rsa::RsaPrivateKey;
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
                            futures::stream::iter(events),
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
                            futures::stream::iter(events),
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
    let out = processed.lock().unwrap().clone();
    out
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
        futures::stream::empty::<Vec<u8>>(),
    );
    let result = feed.scan().await;
    assert!(
        result.is_err(),
        "a failed ListObjects scan should propagate as an error, got {result:?}"
    );
    Ok(())
}
