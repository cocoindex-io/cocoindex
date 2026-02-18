# CocoIndex Agent Guide
## Build/Lint/Test
- Python lint: `ruff check .`; format: `ruff format .`
- Python types: `mypy python examples`
- Python tests: `pytest python` (single: `pytest python/tests/test_file.py::TestClass::test_name`)
- Rust format: `cargo fmt`
- Rust lint: `cargo clippy --workspace --all-targets --all-features`
- Rust tests: `cargo test` (single: `cargo test path::test_name`)
- Python-integration Rust: `dev/run_cargo_test.sh`
- Build Python extension: `maturin develop -E all,dev`
- Full checks: `pre-commit run --all-files`
## Code Style
- Python: Ruff formatter (double quotes, spaces), grouped imports stdlib/third-party/local.
- Python: Type hints required; mypy strict; prefer dataclasses/Pydantic where present.
- Python: snake_case for funcs/vars, PascalCase for classes; avoid wildcard/bare except; raise specific errors.
- Rust: use rustfmt defaults; prefer `?` with `Result`; avoid `unwrap` in library code; log via `tracing` if available.
- Rust: snake_case functions/vars, PascalCase types/modules, SCREAMING_SNAKE consts; keep modules small with clear ownership.
- Error handling: validate inputs early; propagate context rather than silent failures.
- Dataflow: keep transforms pure/immutable; avoid hidden mutation.
- Cursor rules: follow `.cursor/rules` (surrealdb-python, surrealdb-rust-sdk [alwaysApply], surrealdb-vector, surrealql) when working with SurrealDB examples.
