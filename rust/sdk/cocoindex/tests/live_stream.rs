//! Hermetic test of the `LiveStream` plumbing through the engine: an in-memory
//! stream drives a live map through a `LiveStreamSubscriber`, each message is
//! mounted with `start_update` and acknowledged (its handle awaited) once its
//! child has synced, and readiness reaches the live component. No broker.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cocoindex::live_component::{
    LiveMapFeed, LiveMapSubscriber, LiveMapView, LiveStream, LiveStreamSubscriber, ReadyHandle,
    SingleWatcherGuard,
};
use cocoindex::{App, Result, UpdateOptions, async_trait};

/// What the test observed, in order: deliveries, acknowledgements, readiness.
type Log = Arc<Mutex<Vec<String>>>;

/// A finite in-memory `LiveStream<String>`: delivers its messages, awaits each
/// handle on a task of its own (so a slow child does not block the next
/// delivery) and logs the acknowledgement, then marks the subscriber ready and
/// waits until cancelled.
struct VecStream {
    messages: Vec<String>,
    log: Log,
    guard: SingleWatcherGuard,
}

#[async_trait]
impl LiveStream<String> for VecStream {
    async fn watch(&self, subscriber: &dyn LiveStreamSubscriber<String>) -> Result<()> {
        let _token = self.guard.enter()?;
        let mut handles = Vec::new();
        for message in &self.messages {
            self.log.lock().unwrap().push(format!("send {message}"));
            let handle = subscriber.send(message.clone()).await?;
            let immediate = handle.is_immediate();
            let log = self.log.clone();
            let message = message.clone();
            handles.push(async move {
                handle.ready().await?;
                log.lock()
                    .unwrap()
                    .push(format!("acked {message} immediate={immediate}"));
                Ok::<(), cocoindex::Error>(())
            });
        }
        futures::future::try_join_all(handles).await?;
        subscriber.mark_ready().await;
        self.log.lock().unwrap().push("ready".to_string());
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// A live map over the stream: an empty scan, then the stream's messages as
/// children keyed by the message itself (an `x` message is skipped: it needs
/// no work, so its handle is immediate).
struct StreamMap {
    stream: VecStream,
}

struct ToMap<'a>(&'a LiveMapSubscriber<String, String>);

#[async_trait]
impl LiveStreamSubscriber<String> for ToMap<'_> {
    async fn send(&self, message: String) -> Result<ReadyHandle> {
        if message == "x" {
            return Ok(ReadyHandle::immediate());
        }
        self.0.start_update(message.clone(), message).await
    }

    async fn mark_ready(&self) {
        self.0.mark_ready().await;
    }
}

#[async_trait]
impl LiveMapFeed<String, String> for StreamMap {
    async fn watch(&self, subscriber: LiveMapSubscriber<String, String>) -> Result<()> {
        self.stream.watch(&ToMap(&subscriber)).await
    }
}

#[async_trait]
impl LiveMapView<String, String> for StreamMap {
    async fn scan(&self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_messages_are_mounted_and_acknowledged_in_a_live_run() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let processed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let app = App::builder("LiveStream")
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
                let log = log.clone();
                let processed = processed.clone();
                move |ctx| {
                    let log = log.clone();
                    let processed = processed.clone();
                    async move {
                        let feed = StreamMap {
                            stream: VecStream {
                                messages: vec!["a".into(), "x".into(), "b".into()],
                                log,
                                guard: SingleWatcherGuard::new("VecStream"),
                            },
                        };
                        ctx.mount_each_live(&"messages", feed, move |_ctx, value: String| {
                            let processed = processed.clone();
                            async move {
                                // Slow children: an acknowledgement must wait.
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                processed.lock().unwrap().push(value);
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
    while !log.lock().unwrap().iter().any(|l| l == "ready") {
        assert!(Instant::now() < deadline, "the stream never became ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = app.drop_state().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), handle.result()).await;

    let log = log.lock().unwrap().clone();
    let position = |entry: &str| log.iter().position(|l| l == entry).unwrap();
    // Every message was delivered before any was acknowledged: the stream did
    // not wait for a child before sending the next message.
    assert!(
        position("send b") < position("acked a immediate=false"),
        "{log:?}"
    );
    // A skipped message is acknowledged at once; a mounted one only after its
    // child ran.
    assert!(
        log.contains(&"acked x immediate=true".to_string()),
        "{log:?}"
    );
    assert!(
        position("acked b immediate=false") < position("ready"),
        "{log:?}"
    );
    let mut processed = processed.lock().unwrap().clone();
    processed.sort();
    assert_eq!(processed, vec!["a".to_string(), "b".to_string()]);
    Ok(())
}
