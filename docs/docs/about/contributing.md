---
title: Contributing
description: Learn how to contribute to CocoIndex
---

# Contributing

[CocoIndex](https://github.com/cocoindex-io/cocoindex) is an open source project. We love contributions from our community! We are respectful, open and friendly. This guide explains how to get involved and contribute to [CocoIndex](https://github.com/cocoindex-io/cocoindex).

## Issues:

We use [GitHub Issues](https://github.com/cocoindex-io/cocoindex/issues) to track bugs and feature requests.

## Good First Issues

We tag issues with the ["good first issue"](https://github.com/cocoindex-io/cocoindex/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) label for beginner contributors.

## How to Contribute 
- If you decide to work on an issue, please comment on the issue **`I'm working on it`** to helping other contributors avoid duplicating work.
- Our [discord server](https://discord.com/invite/zpA9S2DR7s) are constantly open. If you are unsure about anything, either leaving a message under a issue or asking in the discord server will work!

## Start hacking! Setting Up Development Environment 
Followinng the steps below to get cocoindex build on latest codebase locally - if you are making changes to cocoindex funcionality and want to test it out.

-   Install Rust toolchain: [docs](https://rust-lang.org/tools/install)

-   (Optional) Setup Python virtual environment:
    ```bash
    virtualenv --python=$(which python3.12) .venv
    ```
    Activate the virtual environment, before any installings / buildings / runnings:

    ```bash
    . .venv/bin/activate
    ```

-   Install maturin:
    ```bash
    pip install maturin
    ```

-   Build the library. Run at the root of cocoindex directory:
    ```bash
    maturin develop
    ```

-   (Optional) Before running a specific example, set extra environment variables, for exposing extra traces, allowing dev UI, etc.
    ```bash
    . ./.env.lib_debug
    ```

## Submit Your Code

To submit your code:

1. Fork the [CocoIndex repository](https://github.com/cocoindex-io/cocoindex)
2. [Create a new branch](https://docs.github.com/en/desktop/making-changes-in-a-branch/managing-branches-in-github-desktop) on your fork
3. Make your changes
4. [Open a Pull Request (PR)](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) when your work is ready for review

In your PR description, please include:
- Description of the changes
- Motivation and context
- Note if it's a breaking change
- Reference any related GitHub issues

A core team member will review your PR within one business day and provide feedback on any required changes. Once approved and all tests pass, the reviewer will squash and merge your PR into the main branch.

Your contribution will then be part of CocoIndex! We'll highlight your contribution in our release notes 🌴.
