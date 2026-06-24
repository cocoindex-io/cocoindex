<p align="center">
  <a href="https://cocoindex.io/docs/examples/csv-to-kafka/" title="Watch a folder of CSV files and publish each row as a JSON message to a Kafka topic with CocoIndex — only-changed-rows, live, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/csv-to-kafka/cover.svg" alt="Watch a folder of CSV files and turn it into a live Kafka stream with CocoIndex — each row published as a JSON message keyed by its primary key, with a message produced only for rows that actually changed" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Stream a folder of CSVs to <em>Kafka</em>, row by row.</h1>

<p align="center">
  <b>Each row published as a JSON message keyed by its primary key — edit one cell and exactly <em>one</em> message lands on the topic, within a second — in plain async Python.</b><br/>
  Declare the topic as a target state; CocoIndex produces the upserts and deletes, never the no-ops.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/csv-to-kafka/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

CSV is the format that shows up everywhere and gets respect nowhere — BI exports, vendor dumps, spreadsheets parked in a shared drive, dropped into a folder and edited at random with no schema contract. This pipeline turns a directory of them into a clean, row-keyed, diff-only [Kafka](https://kafka.apache.org/) stream. You declare the transformation in native Python — `target_state = transformation(source_state)` — and the Rust engine does the [incremental processing](https://cocoindex.io/docs/programming_guide/core_concepts/) underneath: it tracks what each row last looked like and produces a message only for rows that *actually changed* — no producer loop, no dedup bookkeeping.

## How it works

The Kafka topic is just a [target](https://cocoindex.io/docs/connectors/kafka/) you declare on, the same way you'd declare a Postgres table. `process_csv` runs once per file: parse rows with `csv.DictReader`, then declare each row as a target state — key from the first column, value the JSON-encoded row. Read it in [`main.py`](main.py):

```python
@coco.fn(memo=True)
async def process_csv(file: FileLike, topic_target: kafka.KafkaTopicTarget) -> None:
    reader = csv.DictReader(io.StringIO(await file.read_text()))
    headers = reader.fieldnames
    if not headers:
        return
    first_col = headers[0]
    for row in reader:
        key_value = row.get(first_col)
        if key_value is not None:
            topic_target.declare_target_state(key=key_value, value=json.dumps(row))

@coco.fn
async def app_main() -> None:
    topic_target = await kafka.mount_kafka_topic_target(KAFKA_PRODUCER, KAFKA_TOPIC)
    files = localfs.walk_dir(
        localfs.FilePath(path="./data"),
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.csv"]),
        live=True,  # watch for changes; pass -L to `cocoindex update` to run live
    )
    await coco.mount_each(process_csv, files.items(), topic_target)
```

The one line worth pausing on is `declare_target_state` — deliberately *not* `produce()`. You describe what the topic *should be* as a function of the source; CocoIndex turns the state transitions into wire messages. A new or changed key produces an upsert `(k, v)`; a key that's no longer declared produces a delete `(k, None)`; a key declared with the same value sends **nothing**.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/csv-to-kafka/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the producer lifespan, the state-vs-messages model, live mode, and exactly which messages each change produces.
</p>

## Why it's worth a star ⭐

- **Declare states, not messages.** A topic is a log of events; you only ever talk about row states. CocoIndex owns the gap — it produces the upserts and deletes a hand-rolled producer would, and skips the no-ops.
- **Live mode is one keyword + one flag.** `live=True` on `walk_dir` and `-L` on the CLI is the entire difference between a catch-up run and a streaming one — `process_csv` and the target don't change. No separate "streaming" code path.
- **Survives restarts.** An [internal state store](https://cocoindex.io/docs/advanced_topics/internal_storage/) remembers the last value sent for every key, so stopping and restarting never re-broadcasts unchanged rows.
- **User-managed topic.** CocoIndex never creates or deletes topics — it produces into one you already own, so it slots into existing Kafka ops.
- **Managed broker ready.** A SASL block in the lifespan covers managed brokers (StreamNative and similar); drop it for a local broker.

## Run it

> Needs a running Kafka broker. CocoIndex never creates topics — you create the one it produces into.

**1. Start a broker & create the topic** — a single-container [Redpanda](https://redpanda.com/) (Kafka-API compatible) is the quickest local broker:

```sh
docker run -d --name redpanda -p 9092:9092 redpandadata/redpanda:latest \
  redpanda start --mode dev-container --smp 1 \
  --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://localhost:9092

docker exec redpanda rpk topic create cocoindex-csv-rows
```

**2. Configure & install:**

```sh
cp .env.example .env     # set KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC (+ SASL creds for a managed broker)
pip install -e .
```

**3. Run the pipeline** — the example ships a `data/` folder of sample CSVs. Choose catch-up (scan, sync, exit) or live (catch up, then keep watching `./data`):

```sh
# Catch-up run: reconcile the topic up to now, then exit
cocoindex update main.py

# Live run: catch up, then produce on every change
cocoindex update -L main.py
```

**4. Look at the topic** — keys are each row's first column, values the JSON-encoded rows:

```sh
docker exec redpanda rpk topic consume cocoindex-csv-rows --num 10
```

Edit a cell in `data/products.csv` while live mode runs, and a new message with the *same key* appears within a second. The consumer side — [kafka_to_lancedb](https://github.com/cocoindex-io/cocoindex/tree/main/examples/kafka_to_lancedb) — reads these messages back off the topic and dispatches them into LanceDB tables.

---

<p align="center">
  If this got your CSVs onto a topic, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/csv-to-kafka/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/csv_to_kafka" alt="" width="1" height="1" />
