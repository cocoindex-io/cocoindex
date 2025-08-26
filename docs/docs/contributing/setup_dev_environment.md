---
title: Setup Development Environment
description: Learn how to setup your development environment to develop CocoIndex
---

Follow the steps below to get CocoIndex built on the latest codebase locally - if you are making changes to CocoIndex functionality and want to test it out.

-   🦀 [Install Rust](https://rust-lang.org/tools/install)

    If you don't have Rust installed, run
    ```sh
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
    Already have Rust? Make sure it's up to date
    ```sh
    rustup update
    ```

-   Setup Python virtual environment:
    ```sh
    python3 -m venv .venv
    ```
    Activate the virtual environment, before any installing / building / running:

    ```sh
    . .venv/bin/activate
    ```

-   Install required tools:
    ```sh
    pip install maturin
    ```

-   Build the library. Run at the root of cocoindex directory:
    ```sh
    maturin develop -E all,dev
    ```

-   Install and enable pre-commit hooks. This ensures all checks run automatically before each commit:
    ```sh
    pre-commit install
    ```

-   Before running a specific example, set extra environment variables, for exposing extra traces, allowing dev UI, etc.
    ```sh
    . ./.env.lib_debug
    ```
