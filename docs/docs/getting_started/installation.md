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
```bash
pip install -U cocoindex
```

## 📦 Install Postgres

You can skip this step if you already have a Postgres database with pgvector extension installed.

If you don't have a Postgres database:

1. Install [Docker Compose](https://docs.docker.com/compose/install/) 🐳.
2. Start a Postgres SQL database for cocoindex using our docker compose config:

```bash
docker compose -f <(curl -L https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/postgres.yaml) up -d
```

## 🎉 All set!

You can now start using CocoIndex.
