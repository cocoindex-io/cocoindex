use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use std::process::Output;
use tempfile::TempDir;

fn write_text(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, text).unwrap();
}

fn extract_line<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.strip_prefix(prefix).map(str::trim))
}

fn mock_response_for(prompt: &str) -> String {
    if prompt.starts_with("Aggregate the following Python files into a project-level summary.") {
        let project_name = extract_line(prompt, "Project name: ").unwrap_or("unknown_project");
        serde_json::json!({
            "name": project_name,
            "summary": format!("aggregated summary for {project_name}"),
            "public_classes": [],
            "public_functions": [
                {
                    "name": format!("{project_name}_entry"),
                    "signature": format!("def {project_name}_entry()"),
                    "is_coco_function": true,
                    "summary": format!("entrypoint for {project_name}")
                }
            ],
            "mermaid_graphs": ["graph TD\n    app_main ==> worker"]
        })
        .to_string()
    } else {
        let file_path = extract_line(prompt, "File path: ").unwrap_or("unknown.py");
        let variant_suffix = if prompt.contains("helper v2") {
            " v2"
        } else {
            ""
        };
        let stem = Path::new(file_path)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("unknown");
        serde_json::json!({
            "name": file_path,
            "summary": format!("summary{variant_suffix} for {file_path}"),
            "public_classes": [],
            "public_functions": [
                {
                    "name": stem,
                    "signature": format!("def {stem}()"),
                    "is_coco_function": true,
                    "summary": format!("function{variant_suffix} for {file_path}")
                }
            ],
            "mermaid_graphs": []
        })
        .to_string()
    }
}

enum MockReply {
    OpenAiContent(String),
    Json {
        status_code: u16,
        body: String,
    },
    Raw {
        status_code: u16,
        content_type: &'static str,
        body: String,
    },
}

impl MockReply {
    fn into_http_response(self) -> String {
        match self {
            MockReply::OpenAiContent(content) => {
                let response_body = serde_json::json!({
                    "choices": [{
                        "message": {
                            "content": content
                        }
                    }]
                })
                .to_string();
                format_http_response(200, "application/json", response_body)
            }
            MockReply::Json { status_code, body } => {
                format_http_response(status_code, "application/json", body)
            }
            MockReply::Raw {
                status_code,
                content_type,
                body,
            } => format_http_response(status_code, content_type, body),
        }
    }
}

fn format_http_response(status_code: u16, content_type: &str, body: String) -> String {
    let reason = match status_code {
        200 => "OK",
        500 => "Internal Server Error",
        _ => "Response",
    };
    format!(
        "HTTP/1.1 {status_code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn read_http_request(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;

    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    let mut header_end = None;
    let mut content_length = 0usize;

    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                if header_end.is_none()
                    && let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    let end = end + 4;
                    header_end = Some(end);
                    let headers = String::from_utf8_lossy(&buffer[..end]);
                    for line in headers.lines() {
                        if let Some(value) = line.strip_prefix("Content-Length:") {
                            content_length = value.trim().parse().map_err(|err| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("invalid Content-Length header: {err}"),
                                )
                            })?;
                        }
                    }
                }
                if let Some(end) = header_end
                    && buffer.len() >= end + content_length
                {
                    return Ok(buffer);
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::Interrupted
                        | io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "mock LLM request closed before the full body was received",
    ))
}

fn extract_prompt_from_request(request: &[u8]) -> io::Result<String> {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing HTTP header terminator")
        })?;
    let body = &request[header_end..];
    let payload: serde_json::Value = serde_json::from_slice(body).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse request JSON: {err}"),
        )
    })?;
    payload["messages"][0]["content"]
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing prompt content"))
}

type MockHandler = dyn Fn(&str) -> MockReply + Send + Sync + 'static;

fn handle_mock_connection(
    mut stream: TcpStream,
    request_count: &AtomicUsize,
    handler: &MockHandler,
) {
    let result = (|| -> io::Result<()> {
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let request = read_http_request(&mut stream)?;
        let prompt = extract_prompt_from_request(&request)?;
        request_count.fetch_add(1, Ordering::Relaxed);

        let response = handler(&prompt).into_http_response();
        stream.write_all(response.as_bytes())?;
        stream.flush()?;
        Ok(())
    })();

    if let Err(err) = result {
        let response = MockReply::Json {
            status_code: 500,
            body: serde_json::json!({
                "error": format!("mock LLM server error: {err}"),
            })
            .to_string(),
        }
        .into_http_response();
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
        eprintln!("mock LLM connection handling failed: {err}");
    }

    let _ = stream.shutdown(Shutdown::Both);
}

fn spawn_mock_llm_with_handler(
    handler: impl Fn(&str) -> MockReply + Send + Sync + 'static,
) -> (
    String,
    Arc<AtomicUsize>,
    Arc<AtomicBool>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let request_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let handler = Arc::new(handler);
    let request_count_for_thread = request_count.clone();
    let stop_for_thread = stop.clone();
    let handler_for_thread = handler.clone();

    let handle = thread::spawn(move || {
        while !stop_for_thread.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let request_count_for_connection = request_count_for_thread.clone();
                    let handler_for_connection = handler_for_thread.clone();
                    thread::spawn(move || {
                        handle_mock_connection(
                            stream,
                            &request_count_for_connection,
                            handler_for_connection.as_ref(),
                        );
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(err) => panic!("mock LLM listener failed: {err}"),
            }
        }
    });

    (address, request_count, stop, handle)
}

fn spawn_mock_llm() -> (
    String,
    Arc<AtomicUsize>,
    Arc<AtomicBool>,
    thread::JoinHandle<()>,
) {
    spawn_mock_llm_with_handler(|prompt| MockReply::OpenAiContent(mock_response_for(prompt)))
}

fn sample_projects(root: &Path) {
    write_text(
        &root.join("alpha/main.py"),
        "def alpha_main():\n    return 'alpha'\n",
    );
    write_text(
        &root.join("alpha/pkg/helper.py"),
        "def alpha_helper():\n    return 'helper'\n",
    );
    write_text(
        &root.join("alpha/.venv/ignored.py"),
        "def ignored():\n    return 'ignored'\n",
    );
    write_text(
        &root.join("alpha/pkg/.hidden/ignored.py"),
        "def hidden():\n    return 'hidden'\n",
    );
    write_text(
        &root.join("alpha/__pycache__/ignored.py"),
        "def cache():\n    return 'cache'\n",
    );
    write_text(
        &root.join("beta/tool.py"),
        "def beta_tool():\n    return 'beta'\n",
    );
}

fn run_example(
    binary: &str,
    projects_dir: &Path,
    output_dir: &Path,
    run_dir: &Path,
    base_url: &str,
) -> Output {
    Command::new(binary)
        .arg(projects_dir)
        .arg(output_dir)
        .current_dir(run_dir)
        .env("LLM_API_KEY", "test-key")
        .env("LLM_BASE_URL", base_url)
        .env("LLM_MODEL", "mock-model")
        .output()
        .unwrap()
}

fn assert_success(output: &Output, label: &str) {
    assert!(
        output.status.success(),
        "{label} failed: stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_failure_contains(output: &Output, label: &str, needle: &str) {
    assert!(
        !output.status.success(),
        "{label} unexpectedly succeeded: stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(needle),
        "{label} stderr did not contain {needle:?}: {stderr}"
    );
}

#[test]
fn multi_codebase_example_runs_end_to_end_with_mock_llm() {
    let projects = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    let run_dir = TempDir::new().unwrap();
    sample_projects(projects.path());

    let (base_url, request_count, stop, handle) = spawn_mock_llm();
    let binary = env!("CARGO_BIN_EXE_multi-codebase-summarization");

    let run_once = || {
        run_example(
            binary,
            projects.path(),
            output.path(),
            run_dir.path(),
            &base_url,
        )
    };

    let first = run_once();
    assert_success(&first, "first run");
    assert_eq!(request_count.load(Ordering::Relaxed), 4);

    let alpha = std::fs::read_to_string(output.path().join("alpha.md")).unwrap();
    let beta = std::fs::read_to_string(output.path().join("beta.md")).unwrap();

    assert!(alpha.contains("# alpha"));
    assert!(alpha.contains("## Overview"));
    assert!(alpha.contains("## Components"));
    assert!(alpha.contains("## CocoIndex Pipeline"));
    assert!(alpha.contains("## File Details"));
    assert!(alpha.contains("```mermaid"));
    assert!(alpha.contains("aggregated summary for alpha"));
    assert!(alpha.contains("`def alpha_entry()` ★: entrypoint for alpha"));
    assert!(alpha.contains("summary for main.py"));
    assert!(alpha.contains("summary for pkg/helper.py"));
    assert!(!alpha.contains("ignored.py"));
    assert!(beta.contains("# beta"));
    assert!(beta.contains("## Overview"));
    assert!(beta.contains("## Components"));
    assert!(!beta.contains("## File Details"));
    assert!(beta.contains("summary for tool.py"));

    let second = run_once();
    assert_success(&second, "second run");
    assert_eq!(request_count.load(Ordering::Relaxed), 4);

    write_text(
        &projects.path().join("alpha/pkg/helper.py"),
        "def alpha_helper():\n    return 'helper v2'\n",
    );

    let third = run_once();
    assert_success(&third, "third run");
    assert_eq!(request_count.load(Ordering::Relaxed), 6);

    let alpha_after_edit = std::fs::read_to_string(output.path().join("alpha.md")).unwrap();
    assert!(alpha_after_edit.contains("summary v2 for pkg/helper.py"));

    write_text(
        &projects.path().join("alpha/new_module.py"),
        "def alpha_extra():\n    return 'extra'\n",
    );

    let fourth = run_once();
    assert_success(&fourth, "fourth run");
    assert_eq!(request_count.load(Ordering::Relaxed), 8);

    let alpha_after_add = std::fs::read_to_string(output.path().join("alpha.md")).unwrap();
    assert!(alpha_after_add.contains("summary for new_module.py"));

    std::fs::remove_dir_all(projects.path().join("beta")).unwrap();

    let fifth = run_once();
    assert_success(&fifth, "fifth run");
    assert_eq!(request_count.load(Ordering::Relaxed), 8);
    assert!(!output.path().join("beta.md").exists());

    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();
}

#[test]
fn multi_codebase_example_fails_on_missing_llm_content() {
    let projects = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    let run_dir = TempDir::new().unwrap();
    sample_projects(projects.path());

    let (base_url, _request_count, stop, handle) =
        spawn_mock_llm_with_handler(|_prompt| MockReply::Json {
            status_code: 500,
            body: "{}".to_string(),
        });
    let binary = env!("CARGO_BIN_EXE_multi-codebase-summarization");

    let output = run_example(
        binary,
        projects.path(),
        output.path(),
        run_dir.path(),
        &base_url,
    );
    assert_failure_contains(&output, "missing-content run", "no content in LLM response");

    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();
}

#[test]
fn multi_codebase_example_fails_on_malformed_llm_json() {
    let projects = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    let run_dir = TempDir::new().unwrap();
    sample_projects(projects.path());

    let (base_url, _request_count, stop, handle) =
        spawn_mock_llm_with_handler(|_prompt| MockReply::Raw {
            status_code: 200,
            content_type: "application/json",
            body: "{not valid json".to_string(),
        });
    let binary = env!("CARGO_BIN_EXE_multi-codebase-summarization");

    let output = run_example(
        binary,
        projects.path(),
        output.path(),
        run_dir.path(),
        &base_url,
    );
    assert_failure_contains(&output, "malformed-json run", "LLM response parse");

    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();
}
