#![cfg(feature = "google_drive")]

use cocoindex::connectors::gdrive::{
    DriveFile, DriveFileInfo, GoogleDriveClient, GoogleDriveSource,
};
use cocoindex::resources::file::FileSourceItem;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const FOLDER_MIME: &str = "application/vnd.google-apps.folder";
const DOC_MIME: &str = "application/vnd.google-apps.document";

fn drive_file(file_id: &str, name: &str, mime_type: &str) -> DriveFile {
    DriveFile::new(DriveFileInfo {
        file_id: file_id.to_string(),
        name: name.to_string(),
        path: name.to_string(),
        mime_type: mime_type.to_string(),
        size: 12,
        modified_time: "2026-01-02T03:04:05Z".to_string(),
        md5_checksum: None,
    })
}

#[tokio::test]
async fn gdrive_source_lists_recursively_and_filters_mime_types() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/files"))
        .and(header("authorization", "Bearer test-token"))
        .and(query_param("q", "'root' in parents and trashed = false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "files": [
                {
                    "id": "folder-1",
                    "name": "nested",
                    "mimeType": FOLDER_MIME,
                    "modifiedTime": "2026-01-01T00:00:00Z"
                },
                {
                    "id": "txt-1",
                    "name": "root.txt",
                    "mimeType": "text/plain",
                    "size": "7",
                    "modifiedTime": "2026-01-02T00:00:00Z",
                    "md5Checksum": "9e107d9d372bb6826bd81d3542a419d6"
                }
            ]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/files"))
        .and(header("authorization", "Bearer test-token"))
        .and(query_param(
            "q",
            "'folder-1' in parents and trashed = false",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "files": [
                {
                    "id": "doc-1",
                    "name": "nested-doc",
                    "mimeType": DOC_MIME,
                    "modifiedTime": "2026-01-03T00:00:00Z"
                },
                {
                    "id": "pdf-1",
                    "name": "ignored.pdf",
                    "mimeType": "application/pdf",
                    "size": "20",
                    "modifiedTime": "2026-01-04T00:00:00Z"
                },
                {
                    "id": "txt-1",
                    "name": "root.txt",
                    "mimeType": "text/plain",
                    "size": "7",
                    "modifiedTime": "2026-01-02T00:00:00Z"
                }
            ]
        })))
        .mount(&server)
        .await;

    let client = GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri());
    let files = GoogleDriveSource::new(client, vec!["root".to_string()])
        .mime_types(vec!["text/plain".to_string(), DOC_MIME.to_string()])
        .list_files()
        .await
        .unwrap();

    let ids: Vec<&str> = files.iter().map(|f| f.file_id.as_str()).collect();
    assert_eq!(ids, vec!["txt-1", "doc-1"]);
    assert_eq!(files[0].key(), "txt-1");
    assert_eq!(files[0].path(), "root.txt");
    assert_eq!(files[0].size, 7);
    assert_eq!(
        files[0].md5_checksum.as_deref(),
        Some("9e107d9d372bb6826bd81d3542a419d6")
    );
    assert!(
        files[1].md5_checksum.is_none(),
        "a Google-native document has no md5Checksum"
    );
    assert_eq!(files[1].path(), "nested/nested-doc");
    assert_eq!(files[1].size, 0);
    assert_eq!(files[1].file_path().resolve(), "doc-1");
    assert_eq!(
        files[1].file_path().path().to_string_lossy(),
        "nested/nested-doc"
    );
    assert_eq!(FileSourceItem::key(&files[1]), "doc-1");

    let items = GoogleDriveSource::new(
        GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri()),
        vec!["root".to_string(), "folder-1".to_string()],
    )
    .mime_types(vec!["text/plain".to_string(), DOC_MIME.to_string()])
    .items()
    .await
    .unwrap();
    assert_eq!(items[0].0, "txt-1");
    assert_eq!(items.len(), 2);
    assert_eq!(items[1].1.path(), "nested-doc");
}

#[tokio::test]
async fn gdrive_memo_distinguishes_ids_and_moves() -> cocoindex::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[cocoindex::function(memo)]
    async fn process(_ctx: &cocoindex::Ctx, file: &DriveFile) -> cocoindex::Result<String> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(format!("{}:{}", file.file_id, file.path()))
    }

    let server = MockServer::start().await;
    for id in ["id-1", "id-2"] {
        Mock::given(method("GET"))
            .and(path(format!("/files/{id}")))
            .and(query_param("alt", "media"))
            .respond_with(ResponseTemplate::new(200).set_body_string(id))
            .mount(&server)
            .await;
    }
    let client = GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri());
    let dir = tempfile::tempdir().unwrap();
    let app = cocoindex::Environment::builder()
        .db_path(dir.path().join("db"))
        .build()
        .await?
        .app("drive_identity")
        .await?;
    for (id, name) in [
        ("id-1", "a/report.txt"),
        ("id-2", "a/report.txt"),
        ("id-2", "b/report.txt"),
        ("id-2", "b/report.txt"),
    ] {
        let file = drive_file(id, name, "text/plain").with_client(client.clone());
        let result = app
            .update(move |ctx| async move { process(&ctx, &file).await })
            .await?;
        assert_eq!(result, format!("{id}:{name}"));
    }
    assert_eq!(CALLS.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn gdrive_client_reads_binary_and_exports_google_docs() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/files/txt-1"))
        .and(header("authorization", "Bearer test-token"))
        .and(query_param("alt", "media"))
        .respond_with(ResponseTemplate::new(200).set_body_string("plain text"))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/files/doc-1/export"))
        .and(header("authorization", "Bearer test-token"))
        .and(query_param("mimeType", "text/markdown"))
        .respond_with(ResponseTemplate::new(200).set_body_string("# exported"))
        .mount(&server)
        .await;

    let client = GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri());

    assert_eq!(
        client
            .read_text(&drive_file("txt-1", "root.txt", "text/plain"))
            .await
            .unwrap(),
        "plain text"
    );
    assert_eq!(
        client
            .read_text(&drive_file("doc-1", "doc", DOC_MIME))
            .await
            .unwrap(),
        "# exported"
    );
}

#[tokio::test]
async fn gdrive_drive_file_uses_shared_cached_filelike_reads() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/files/txt-1"))
        .and(header("authorization", "Bearer test-token"))
        .and(query_param("alt", "media"))
        .respond_with(ResponseTemplate::new(200).set_body_string("cached text"))
        .expect(1)
        .mount(&server)
        .await;

    let client = GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri());
    let file = drive_file("txt-1", "root.txt", "text/plain").with_client(client);

    assert_eq!(file.read_text().await.unwrap(), "cached text");
    assert_eq!(file.read_text().await.unwrap(), "cached text");
}

/// A listing that fails with a 503, then a 429, then a rate-limit 403, is
/// retried until it succeeds; another 403 is not retried.
#[tokio::test]
async fn gdrive_retries_server_errors_and_rate_limits() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::{Request, Respond};

    struct Flaky(Arc<AtomicUsize>);
    impl Respond for Flaky {
        fn respond(&self, _: &Request) -> ResponseTemplate {
            match self.0.fetch_add(1, Ordering::SeqCst) {
                0 => ResponseTemplate::new(503),
                1 => ResponseTemplate::new(429),
                2 => ResponseTemplate::new(403).set_body_json(serde_json::json!({
                    "error": {"errors": [{"reason": "userRateLimitExceeded"}], "code": 403}
                })),
                _ => ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "files": [{
                        "id": "txt-1", "name": "root.txt", "mimeType": "text/plain",
                        "size": "7", "modifiedTime": "2026-01-02T00:00:00Z"
                    }]
                })),
            }
        }
    }

    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/files"))
        .respond_with(Flaky(attempts.clone()))
        .mount(&server)
        .await;
    let client = GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri());
    let files = GoogleDriveSource::new(client, vec!["root".to_string()])
        .list_files()
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        4,
        "three retried failures, then success"
    );

    // A 403 that is not a rate limit is the caller's problem: no retry.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error": {"errors": [{"reason": "insufficientFilePermissions"}], "code": 403}
        })))
        .expect(1)
        .mount(&server)
        .await;
    let client = GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri());
    let err = match GoogleDriveSource::new(client, vec!["root".to_string()])
        .list_files()
        .await
    {
        Ok(_) => panic!("a permission error is not retried into a success"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("403"), "{err}");
}

/// Drive lists children in no set order; the source sorts them by name and
/// id, so a file reachable through two folders keeps the same path every run.
#[tokio::test]
async fn gdrive_sorts_children_so_paths_are_stable() {
    let server = MockServer::start().await;
    // Two folders under the root, listed in reverse order; the same file is in
    // both. Sorted, "alpha" comes first, so the file's path is under it.
    Mock::given(method("GET"))
        .and(path("/files"))
        .and(query_param("q", "'root' in parents and trashed = false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "files": [
                {"id": "f-zulu", "name": "zulu", "mimeType": FOLDER_MIME, "modifiedTime": "2026-01-01T00:00:00Z"},
                {"id": "f-alpha", "name": "alpha", "mimeType": FOLDER_MIME, "modifiedTime": "2026-01-01T00:00:00Z"}
            ]
        })))
        .mount(&server)
        .await;
    for folder in ["f-zulu", "f-alpha"] {
        Mock::given(method("GET"))
            .and(path("/files"))
            .and(query_param("q", format!("'{folder}' in parents and trashed = false")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "files": [
                    {"id": "shared", "name": "shared.txt", "mimeType": "text/plain", "size": "1", "modifiedTime": "2026-01-01T00:00:00Z"},
                    {"id": "b", "name": "same-name.txt", "mimeType": "text/plain", "size": "1", "modifiedTime": "2026-01-01T00:00:00Z"},
                    {"id": "a", "name": "same-name.txt", "mimeType": "text/plain", "size": "1", "modifiedTime": "2026-01-01T00:00:00Z"}
                ]
            })))
            .mount(&server)
            .await;
    }
    let client = GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri());
    let files = GoogleDriveSource::new(client, vec!["root".to_string()])
        .list_files()
        .await
        .unwrap();
    let listed: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.file_id.clone(), f.path().to_string()))
        .collect();
    assert_eq!(
        listed,
        vec![
            ("a".to_string(), "alpha/same-name.txt".to_string()),
            ("b".to_string(), "alpha/same-name.txt".to_string()),
            ("shared".to_string(), "alpha/shared.txt".to_string()),
        ],
        "sorted by (name, id) within a folder; the shared file keeps the path of the first folder by name"
    );
}
