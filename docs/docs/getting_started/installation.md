---
title: Installation 
description: Setup the CocoIndex environment in 0-3 min
---

## 🐍 Python and Pip
To follow the steps in this guide, you'll need:

1. Install [Python](https://www.python.org/downloads/). We support Python 3.11 to 3.13 
[![Python](https://img.shields.io/badge/python-3.11%20to%203.13-5B5BD6?logo=python&logoColor=white)](https://www.python.org/).
2. Install [pip](https://pip.pypa.io/en/stable/installation/) - a Python package installer


## 🌴 Install CocoIndex
```bash
pip install cocoindex
```

## 📦 Install Postgres

You can skip this step if you already have a Postgres database with pgvector extension installed. 

If you don't have a Postgres database:

1. Make sure Docker Compose 🐳 is installed: [docs](https://docs.docker.com/compose/install/)
2. Start a Postgres SQL database for cocoindex using our docker compose config:

```bash
docker compose -f <(curl -L https://raw.githubusercontent.com/cocoindex-io/cocoindex/refs/heads/main/dev/postgres.yaml) up -d
```

## 🎉 All set!

You can now start using CocoIndex.

