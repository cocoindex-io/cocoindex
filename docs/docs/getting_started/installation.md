---
title: Installation
description: Setup the CocoIndex environment in 0-3 min
---

## 🖥️ System Requirements

CocoIndex is supported on the following operating systems:

- **macOS**: 10.12+ on x86_64, 11.0+ on arm64
- **Linux**: x86_64 or arm64, glibc 2.28+ (e.g., Debian 10+, Ubuntu 18.10+, Fedora 29+, CentOS/RHEL 8+)
- **Windows**: 10+ on x86_64

## 🐍 Install Python and Pip

To follow the steps in this guide, you'll need:

1. Install [Python](https://wiki.python.org/moin/BeginnersGuide/Download/). We support Python 3.11 to 3.13.
2. Install [pip](https://pip.pypa.io/en/stable/installation/) - a Python package installer

## 🌴 Install CocoIndex

```sh
pip install -U cocoindex
```

## 🤖 Install Claude Code Skill (Optional)

If you're using [Claude Code](https://claude.com/claude-code), you can install the CocoIndex skill for enhanced development support. Run these commands in Claude Code:

```
/plugin marketplace add cocoindex-io/cocoindex-claude
/plugin install cocoindex-skills@cocoindex
```

This provides specialized CocoIndex knowledge and workflow support within Claude Code.

## 🎉 All set

You can now start using CocoIndex.
