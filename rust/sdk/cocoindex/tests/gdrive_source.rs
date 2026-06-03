#![cfg(feature = "google_drive")]

use cocoindex::FileSourceItem;
use cocoindex::gdrive::{DriveFile, DriveFileInfo, GoogleDriveClient, GoogleDriveSource};
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
                    "modifiedTime": "2026-01-02T00:00:00Z"
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
    assert_eq!(files[0].key(), "root.txt");
    assert_eq!(files[0].path(), "root.txt");
    assert_eq!(files[0].size, 7);
    assert_eq!(files[1].path(), "nested/nested-doc");
    assert_eq!(files[1].size, 0);
    assert_eq!(files[1].file_path().resolve(), "doc-1");
    assert_eq!(
        files[1].file_path().path().to_string_lossy(),
        "nested/nested-doc"
    );
    assert_eq!(FileSourceItem::key(&files[1]), "nested/nested-doc");

    let items = GoogleDriveSource::new(
        GoogleDriveClient::from_static_token("test-token").with_base_url(server.uri()),
        vec!["root".to_string()],
    )
    .mime_types(vec!["text/plain".to_string(), DOC_MIME.to_string()])
    .items()
    .await
    .unwrap();
    assert_eq!(items[0].0, "root.txt");
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
