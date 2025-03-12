---
title: Contributing
description: Learn how to contribute to CocoIndex
---

# Contributing

We love contributions from our community! This guide explains how to get involved and contribute to CocoIndex.

## Setting Up Development Environment

-   Install Rust toolchain: [docs](https://rust-lang.org/tools/install)

-   (Optional) Setup and activate python virtual environment
    ```bash
    virtualenv --python=$(which python3.12) .venv
    . .venv/bin/activate
    ```

-   Install maturin
    ```bash
    pip install maturin
    ```

-   Build the library
    ```bash
    maturin develop
    ```

-   (Optional) Before running a specific example, set extra environment variables, for exposing extra traces, allowing dev UI, etc.
    ```bash
    . ./.env.lib_debug
    ```

## Submit Your Code

To submit your code:

1. Fork the [CocoIndex repository](https://github.com/cocoIndex/cocoindex)
2. [Create a new branch](https://docs.github.com/en/desktop/making-changes-in-a-branch/managing-branches-in-github-desktop) on your fork
3. Make your changes
4. [Open a Pull Request (PR)](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) when your work is ready for review

In your PR description, please include:
- Description of the changes
- Motivation and context
- Test coverage details
- Note if it's a breaking change
- Reference any related GitHub issues

A core team member will review your PR within one business day and provide feedback on any required changes. Once approved and all tests pass, the reviewer will squash and merge your PR into the main branch.

Your contribution will then be part of CocoIndex! We'll highlight your contribution in our release notes 🌴.
