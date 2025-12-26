---
title: Setup Development Environment
description: Learn how to setup your development environment to develop CocoIndex
---

Follow the steps below to get CocoIndex built on the latest codebase locally - if you are making changes to CocoIndex functionality and want to test it out.

- 🦀 [Install Rust](https://rust-lang.org/tools/install)

    If you don't have Rust installed, run

    ```sh
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```

    Already have Rust? Make sure it's up to date

    ```sh
    rustup update
    ```

- Install [uv](https://docs.astral.sh/uv/) for Python project management:

    ```sh
    # macOS / Linux
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Windows
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

- Setup your local development environment:

  - Install and enable pre-commit hooks. This ensures all checks run automatically before each commit:

    ```sh
    uv run pre-commit install
    ```

  - (Optionally) Install all optional dependencies:

    ```sh
    uv sync --all-extras
    ```

- Build the library. Run at the root of cocoindex directory:

    ```sh
    uv run maturin develop
    ```

    This step needs to be repeated whenever you make changes to the Rust code.

## Running Examples

Before running a specific example, set extra environment variables, for exposing extra traces, allowing dev UI, etc.

```sh
. ./.env.lib_debug
```

To run examples during development, you need to use the local editable version of cocoindex.
`.env.lib_debug` provides a `coco-dev-run` function to make it more convenient.

```sh
# Navigate to an example directory
cd examples/text_embedding

# Run with your local cocoindex changes
coco-dev-run cocoindex update main
```

The `coco-dev-run` function runs `uv run --with-editable $COCOINDEX_DEV_ROOT`, which ensures the example uses your locally built cocoindex package instead of the published version. The `COCOINDEX_DEV_ROOT` variable is automatically set to the repo root when you source `.env.lib_debug`.

## Troubleshooting

### `cargo test` fails with `ModuleNotFoundError: encodings` (embedded Python can't find stdlib)

On some setups (notably when using `uv venv` with an `uv`-managed Python), `cargo test` may crash with:

`ModuleNotFoundError: No module named 'encodings'`

This can happen when the embedded Python interpreter (used by Rust tests) cannot locate the Python stdlib (you may see `sys.prefix=/install` in the crash output).

Workaround:

- Run cargo tests via:

  `./dev/run_cargo_test.sh -p cocoindex --lib`

This wrapper sets `PYTHONHOME`/`PYTHONPATH` for that command only, so embedded Python can locate the stdlib and site-packages.

Note: the cargo-test pre-commit hook uses this wrapper.
