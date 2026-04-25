//! Anonymous usage telemetry.
//!
//! See `specs/telemetry/design.md` for rationale. Events are POSTed to the
//! Scarf gateway as fire-and-forget HTTP requests. Tracking is opt-out via
//! `COCOINDEX_DISABLE_USAGE_TRACKING`, only active in release builds, and never
//! blocks or fails user operations.

use std::sync::OnceLock;
use std::time::Duration;

use serde::Serialize;
use tracing::info;

use cocoindex_utils::reqwest;

use crate::engine::runtime::{get_runtime, is_runtime_shutdown};

const SCARF_BASE: &str = "https://cocoindex.gateway.scarf.sh";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DISABLE_ENV: &str = "COCOINDEX_DISABLE_USAGE_TRACKING";

static TELEMETRY: OnceLock<TelemetryContext> = OnceLock::new();

struct TelemetryContext {
    client: reqwest::Client,
    base_url: String,
    platform: String,
    lang: String,
}

#[derive(Serialize)]
struct EventPayload<'a> {
    event: &'a str,
    platform: &'a str,
    lang: &'a str,
}

/// Initialize telemetry. No-op in debug builds, no-op if
/// `COCOINDEX_DISABLE_USAGE_TRACKING` is set (to any value other than empty or
/// `"0"`). Safe to call multiple times; only the first successful call
/// installs the global context. On installation, fires the `init` event.
///
/// `package_id` takes the form `"{name}-{version}"`, e.g. `"python-1.0.0a1"`.
pub fn init(package_id: String, lang: String) {
    if cfg!(debug_assertions) {
        return;
    }
    if is_disabled_by_env() {
        return;
    }
    let Some(ctx) = build_context(package_id, lang) else {
        return;
    };
    if TELEMETRY.set(ctx).is_err() {
        return;
    }
    track("init");
}

/// Fire a telemetry event. No-op if `init` never installed a global context,
/// or if the global Tokio runtime has been shut down. Non-blocking: spawns a
/// background task and returns immediately.
pub fn track(event: &'static str) {
    if is_runtime_shutdown() {
        return;
    }
    let Some(ctx) = TELEMETRY.get() else {
        return;
    };
    get_runtime().spawn(async move { send_event(ctx, event).await });
}

fn build_context(package_id: String, lang: String) -> Option<TelemetryContext> {
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()
        .ok()?;
    Some(TelemetryContext {
        client,
        base_url: format!("{SCARF_BASE}/{package_id}"),
        platform: current_platform(),
        lang,
    })
}

fn current_platform() -> String {
    format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS)
}

fn is_disabled_by_env() -> bool {
    match std::env::var(DISABLE_ENV) {
        Ok(v) => parse_disable_value(&v),
        Err(_) => false,
    }
}

fn parse_disable_value(v: &str) -> bool {
    !v.is_empty() && v != "0"
}

async fn send_event(ctx: &TelemetryContext, event: &'static str) {
    let payload = EventPayload {
        event,
        platform: &ctx.platform,
        lang: &ctx.lang,
    };
    let result = ctx
        .client
        .post(&ctx.base_url)
        .json(&payload)
        .send()
        .await
        .and_then(|resp| resp.error_for_status());
    if let Err(err) = result {
        info!("usage tracking event {event} failed: {err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use serde_json::Value;
    use tokio::net::TcpListener;

    #[derive(Clone, Debug)]
    struct Recorded {
        path: String,
        content_type: Option<String>,
        body: Value,
    }

    #[derive(Clone)]
    struct MockState {
        recorded: Arc<Mutex<Vec<Recorded>>>,
        response_status: StatusCode,
    }

    async fn handle_any(
        State(state): State<MockState>,
        headers: HeaderMap,
        axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
        body: Bytes,
    ) -> StatusCode {
        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        state.recorded.lock().unwrap().push(Recorded {
            path: uri.path().to_string(),
            content_type,
            body: parsed,
        });
        state.response_status
    }

    async fn spawn_mock_server(
        response_status: StatusCode,
    ) -> (SocketAddr, Arc<Mutex<Vec<Recorded>>>) {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let state = MockState {
            recorded: recorded.clone(),
            response_status,
        };
        let app = Router::new()
            .route("/{*any}", post(handle_any))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, recorded)
    }

    fn make_test_ctx(base_url: String, lang: String, timeout: Duration) -> TelemetryContext {
        TelemetryContext {
            client: reqwest::Client::builder().timeout(timeout).build().unwrap(),
            base_url,
            platform: current_platform(),
            lang,
        }
    }

    #[tokio::test]
    async fn send_event_posts_expected_payload() {
        let (addr, recorded) = spawn_mock_server(StatusCode::OK).await;
        let ctx = make_test_ctx(
            format!("http://{addr}/python-1.0.0a1"),
            "python3.11".to_string(),
            Duration::from_secs(5),
        );

        send_event(&ctx, "app_create").await;

        let recs = recorded.lock().unwrap().clone();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].path, "/python-1.0.0a1");
        assert_eq!(recs[0].content_type.as_deref(), Some("application/json"));
        let body = &recs[0].body;
        assert_eq!(body["event"], "app_create");
        assert_eq!(body["lang"], "python3.11");
        assert_eq!(body["platform"], current_platform());
    }

    #[tokio::test]
    async fn send_event_handles_non_2xx_without_panic() {
        let (addr, recorded) = spawn_mock_server(StatusCode::INTERNAL_SERVER_ERROR).await;
        let ctx = make_test_ctx(
            format!("http://{addr}/python-1.0.0a1"),
            "python3.11".to_string(),
            Duration::from_secs(5),
        );
        send_event(&ctx, "app_update").await;
        assert_eq!(recorded.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn send_event_handles_transport_error() {
        // Port 1 is almost never bound; connection should fail promptly.
        let ctx = make_test_ctx(
            "http://127.0.0.1:1/python-1.0.0a1".to_string(),
            "python3.11".to_string(),
            Duration::from_millis(500),
        );
        send_event(&ctx, "init").await;
    }

    #[tokio::test]
    async fn send_event_respects_timeout() {
        // Mock server that accepts connections but never routes a response for GETs;
        // Axum with our router returns 405 quickly for unmatched methods, so we need
        // a raw listener that accepts and holds the socket open.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
                // Hold the accepted socket by not dropping it; sleep forever.
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });

        let ctx = make_test_ctx(
            format!("http://{addr}/python-1.0.0a1"),
            "python3.11".to_string(),
            Duration::from_millis(300),
        );
        let start = std::time::Instant::now();
        send_event(&ctx, "app_update").await;
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(2), "elapsed = {elapsed:?}");
    }

    #[test]
    fn parse_disable_value_matrix() {
        assert!(!parse_disable_value(""));
        assert!(!parse_disable_value("0"));
        assert!(parse_disable_value("1"));
        assert!(parse_disable_value("true"));
        assert!(parse_disable_value("yes"));
        assert!(parse_disable_value("anything"));
    }

    #[test]
    fn track_noops_when_telemetry_not_initialized() {
        // In debug builds (cargo test), TELEMETRY is never installed.
        // This must not panic regardless of whether a mock server has been spawned.
        track("app_create");
    }

    #[test]
    fn init_short_circuits_in_debug_build() {
        // Under `cargo test` we are always in debug mode.
        init("python-1.0.0a1".to_string(), "python3.11".to_string());
        assert!(TELEMETRY.get().is_none());
    }
}
