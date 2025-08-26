---
title: Contributing Guide
description: How to contribute to CocoIndex
---

[CocoIndex](https://github.com/cocoindex-io/cocoindex) is an open source project. We are respectful, open and friendly. This guide explains how to get involved and contribute to [CocoIndex](https://github.com/cocoindex-io/cocoindex).

Our [Discord server](https://discord.com/invite/zpA9S2DR7s) is constantly open.
If you are unsure about anything, it is a good place to discuss! We'd love to collaborate and will always be friendly.

## Good First Issues

We tag issues with the ["good first issue"](https://github.com/cocoindex-io/cocoindex/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) label for beginner contributors.

## How to Contribute
- If you decide to take an issue, we recommend you to leave a comment on the issue like  **`Can I work on this issue?`** so we could assign it to you. This helps you and others avoid duplicating work.
- For larger features, we recommend you to discuss with us first in our [Discord server](https://discord.com/invite/zpA9S2DR7s) to coordinate the design and work.

## Submit Your Code
CocoIndex is committed to the highest standards of code quality. Please ensure your code is thoroughly tested before submitting a PR.

To submit your code:

1. Fork the [CocoIndex repository](https://github.com/cocoindex-io/cocoindex)
2. [Create a new branch](https://docs.github.com/en/desktop/making-changes-in-a-branch/managing-branches-in-github-desktop) on your fork
3. Make your changes
4. Run the pre-commit checks (automatically triggered on `git commit`)

    :::tip
    To run them manually (same as CI):
        ```sh
        pre-commit run --all-files
        ```
    :::

5. [Open a Pull Request (PR)](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) when your work is ready for review

In your PR description, please include:
- Description of the changes
- Motivation and context
- Note if it's a breaking change
- Reference any related GitHub issues


A core team member will review your PR within one business day and provide feedback on any required changes. Once approved and all tests pass, the reviewer will squash and merge your PR into the main branch.

Your contribution will then be part of CocoIndex! We'll highlight your contribution in our release notes 🌴.
