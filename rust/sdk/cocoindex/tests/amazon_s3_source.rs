//! Live-S3 integration test for the `amazon_s3` source, run against a local
//! MinIO (S3-compatible) server.
//!
//! Skips gracefully unless `AWS_ENDPOINT_URL` is set. Run with MinIO on
//! localhost (and the standard AWS env for creds/region):
//!   AWS_ENDPOINT_URL=http://localhost:9000 AWS_REGION=us-east-1 \
//!   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//!     cargo test -p cocoindex --features amazon_s3 --test amazon_s3_source
#![cfg(feature = "amazon_s3")]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cocoindex::amazon_s3::aws_sdk_s3::primitives::ByteStream;
use cocoindex::amazon_s3::{self, ListOptions, S3Client, S3File};
use cocoindex::file::PatternFilePathMatcher;
use cocoindex::{App, FileLike, Result};

/// Build a client against MinIO, or `None` to skip when `AWS_ENDPOINT_URL` is unset.
async fn try_client() -> Option<S3Client> {
    if std::env::var("AWS_ENDPOINT_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .is_none()
    {
        eprintln!("skipping live S3 test; AWS_ENDPOINT_URL is not set");
        return None;
    }
    Some(S3Client::connect().await.expect("connect to S3/MinIO"))
}

fn unique_bucket(tag: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("coco-test-{tag}-{nonce}")
}

/// Create a fresh bucket and upload `objects` (key, bytes).
async fn setup_bucket(client: &S3Client, bucket: &str, objects: &[(&str, &[u8])]) {
    let raw = client.client();
    raw.create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create_bucket");
    for (key, body) in objects {
        raw.put_object()
            .bucket(bucket)
            .key(*key)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .expect("put_object");
    }
}

/// Sorted relative-path keys of a list of files.
fn keys(files: &[S3File]) -> Vec<String> {
    let mut ks: Vec<String> = files.iter().map(S3File::key).collect();
    ks.sort();
    ks
}

#[tokio::test]
async fn s3_source_lists_reads_and_filters_when_available() -> Result<()> {
    let Some(client) = try_client().await else {
        return Ok(());
    };
    let bucket = unique_bucket("list");
    setup_bucket(
        &client,
        &bucket,
        &[
            ("docs/a.md", b"# A\nalpha"),
            ("docs/b.md", b"# B\nbeta beta"),
            ("docs/notes.txt", b"plain text"),
            ("docs/skip/c.md", b"# C skipped"),
            ("other/d.md", b"# D outside prefix"),
            ("docs/large.md", &[b'x'; 5000]),
            ("docs/", b""), // directory marker — must be skipped
        ],
    )
    .await;

    // --- basic listing (whole bucket; dir marker skipped) ---
    let all = amazon_s3::list_objects(&client, &bucket, ListOptions::default())
        .list()
        .await?;
    assert_eq!(
        keys(&all),
        vec![
            "docs/a.md",
            "docs/b.md",
            "docs/large.md",
            "docs/notes.txt",
            "docs/skip/c.md",
            "other/d.md",
        ],
        "lists all objects, skipping the directory marker"
    );

    // --- prefix: relative paths are stripped of the prefix ---
    let under_docs = amazon_s3::list_objects(
        &client,
        &bucket,
        ListOptions {
            prefix: "docs/".to_string(),
            ..Default::default()
        },
    )
    .list()
    .await?;
    assert_eq!(
        keys(&under_docs),
        vec!["a.md", "b.md", "large.md", "notes.txt", "skip/c.md"],
        "prefix is stripped from relative keys; 'other/' excluded"
    );

    // --- include + exclude pattern matcher (on the relative path) ---
    let matcher = Arc::new(PatternFilePathMatcher::new(["**/*.md"], ["**/skip/**"]).unwrap());
    let md_only = amazon_s3::list_objects(
        &client,
        &bucket,
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
        vec!["a.md", "b.md", "large.md"],
        "*.md included, .txt excluded, skip/ excluded"
    );

    // --- max_file_size filter ---
    let small = amazon_s3::list_objects(
        &client,
        &bucket,
        ListOptions {
            prefix: "docs/".to_string(),
            max_file_size: Some(1000),
            ..Default::default()
        },
    )
    .list()
    .await?;
    assert!(
        !keys(&small).contains(&"large.md".to_string()),
        "objects larger than max_file_size are skipped"
    );

    // --- items() yields (relative_key, file) ---
    let items = amazon_s3::list_objects(
        &client,
        &bucket,
        ListOptions {
            prefix: "docs/".to_string(),
            ..Default::default()
        },
    )
    .items()
    .await?;
    assert!(items.iter().any(|(k, f)| k == "a.md" && f.key() == "a.md"));

    // --- read / read_text / size / metadata via get_object ---
    let a = client.get_object(&bucket, "docs/a.md").await?;
    assert_eq!(a.size, 9); // "# A\nalpha" is 9 bytes
    assert_eq!(a.file_path().resolve(), "docs/a.md"); // full key
    assert!(a.etag.is_some());
    assert_eq!(client.read(&a).await?, b"# A\nalpha");
    assert_eq!(client.read_text(&a).await?, "# A\nalpha");
    assert_eq!(client.read_range(&a, 3).await?, b"# A"); // ranged read
    assert_eq!(a.read_text().await?, "# A\nalpha");
    assert_eq!(a.read_size(3).await?, b"# A");
    assert_eq!(
        FileLike::content_fingerprint(&a).await?,
        FileLike::content_fingerprint(&a).await?
    );

    // --- get_object via s3:// URI ---
    let b = client
        .get_object_uri(&format!("s3://{bucket}/docs/b.md"))
        .await?;
    assert_eq!(client.read_text(&b).await?, "# B\nbeta beta");

    // --- nonexistent object errors ---
    assert!(
        client.get_object(&bucket, "docs/missing.md").await.is_err(),
        "head_object on a missing key must error"
    );

    Ok(())
}

#[tokio::test]
async fn s3_source_empty_bucket_when_available() -> Result<()> {
    let Some(client) = try_client().await else {
        return Ok(());
    };
    let bucket = unique_bucket("empty");
    setup_bucket(&client, &bucket, &[]).await;
    let files = amazon_s3::list_objects(&client, &bucket, ListOptions::default())
        .list()
        .await?;
    assert!(files.is_empty(), "empty bucket lists no files");
    Ok(())
}

/// End-to-end: list S3 -> `mount_each` -> memoized per-file read. Confirms the
/// source integrates with the engine (keys, memoization) without a DB target.
#[tokio::test]
async fn s3_source_mount_each_pipeline_when_available() -> Result<()> {
    let Some(client) = try_client().await else {
        return Ok(());
    };
    let bucket = unique_bucket("pipeline");
    setup_bucket(
        &client,
        &bucket,
        &[("a.md", b"hello"), ("sub/b.md", b"worldwide")],
    )
    .await;

    static S3: std::sync::LazyLock<cocoindex::ContextKey<S3Client>> =
        std::sync::LazyLock::new(|| {
            cocoindex::ContextKey::new_with_state("s3_test_client", |c: &S3Client| {
                c.state_id().to_string()
            })
        });

    #[cocoindex::function(memo)]
    async fn process(ctx: &cocoindex::Ctx, file: &S3File) -> Result<usize> {
        let bytes = ctx.get_key(&S3)?.read(&file).await?;
        Ok(bytes.len())
    }

    let tempdir = tempfile::tempdir().unwrap();
    let app = App::builder("S3PipelineTest")
        .db_path(tempdir.path().join(".cocoindex_db"))
        .provide_key(&S3, client.clone())
        .build()
        .await?;

    let bucket_for_run = bucket.clone();
    let total = app
        .update(move |ctx| {
            let bucket = bucket_for_run.clone();
            async move {
                let files =
                    amazon_s3::list_objects(ctx.get_key(&S3)?, &bucket, ListOptions::default())
                        .list()
                        .await?;
                let sizes = ctx
                    .mount_each(
                        files,
                        |f| f.key(),
                        |child, file| async move { process(&child, &file).await },
                    )
                    .await?;
                Ok::<usize, cocoindex::Error>(sizes.into_iter().sum())
            }
        })
        .await?;

    assert_eq!(
        total,
        "hello".len() + "worldwide".len(),
        "read all object bytes via mount_each"
    );
    Ok(())
}
