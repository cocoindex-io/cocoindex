<p align="center">
  <a href="https://cocoindex.io/docs/examples/kafka-to-lancedb/" title="Consume a Kafka topic of JSON messages and dispatch each one by shape into the right LanceDB table with CocoIndex — offsets committed after each durable write, in plain async Python">
    <img src="https://cocoindex.io/blobs/docs-v1/img/examples/kafka-to-lancedb/cover.svg" alt="Consume a live Kafka topic of JSON messages with CocoIndex and fan them into LanceDB tables — a message with a sku field becomes a products row, one with an emp_id field an employees row, with the Kafka offset committed only after each row is durably written" width="100%" draggable="false"/>
  </a>
</p>

<h1 align="center">Consume a Kafka topic into <em>LanceDB</em>, routed by shape.</h1>

<p align="center">
  <b>Each message parsed and dispatched to the table that matches it — a <em>sku</em> field becomes a <code>products</code> row, an <em>emp_id</em> field an <code>employees</code> row — in plain async Python.</b><br/>
  CocoIndex commits each Kafka offset only after the row is durably written, so a crash mid-flight replays cleanly.
</p>

<p align="center">
  <strong>Star us&nbsp;❤️&nbsp;→</strong>&nbsp;<a href="https://github.com/cocoindex-io/cocoindex" title="Star CocoIndex on GitHub"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/star-btn-small-light.svg" alt="Star CocoIndex on GitHub" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://cocoindex.io/docs/examples/kafka-to-lancedb/" title="Read the full walkthrough"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/docs-inline-light.svg" alt="CocoIndex documentation" height="36" align="absmiddle"/></picture></a> &nbsp;·&nbsp;
  <a href="https://discord.com/invite/zpA9S2DR7s" title="Join the CocoIndex Discord"><picture><source media="(prefers-color-scheme: dark)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-dark.svg"><source media="(prefers-color-scheme: light)" srcset="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg"><img src="https://cocoindex.io/blobs/github/homepage/discord-inline-light.svg" alt="Join the CocoIndex Discord" height="36" align="absmiddle"/></picture></a>
</p>

<div align="center">

[![stars](https://img.shields.io/github/stars/cocoindex-io/cocoindex?style=flat-square&label=stars&color=FB6A76)](https://github.com/cocoindex-io/cocoindex)
[![pypi](https://img.shields.io/pypi/v/cocoindex?style=flat-square&label=pypi&color=E59A63)](https://pypi.org/project/cocoindex/)
[![discord](https://img.shields.io/discord/1314801574169673738?style=flat-square&logo=discord&logoColor=white&label=discord&color=5865F2)](https://discord.com/invite/zpA9S2DR7s)
[![license](https://img.shields.io/badge/license-Apache--2.0-5B5BD6?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

</div>

<br/>

A topic is often a firehose of heterogeneous events — orders, users, inventory — sharing a transport but not a schema. The consumer's job is to *sort the mail*: read each envelope, decide what it is, and put it where it belongs. This is the consumer side of [csv-to-kafka](https://github.com/cocoindex-io/cocoindex/tree/main/examples/csv_to_kafka): the same declarative flow that *produced* the topic now *consumes* it. You declare the transformation in native Python — `target_state = transformation(source_state)` — and the Rust engine consumes one message per processing component, writes the row, and only then commits the offset, so a crash replays from the last durably-written message.

## How it works

Kafka is a [source](https://cocoindex.io/docs/connectors/kafka/) you treat as a keyed map; each LanceDB table is a [target](https://cocoindex.io/docs/connectors/lancedb/) you declare rows on. `process_message` runs once per message: decode the value, `json.loads` it, and dispatch on shape. Read it in [`main.py`](main.py):

```python
@coco.fn
async def process_message(
    msg: Message,
    products_table: lancedb.TableTarget[Product],
    employees_table: lancedb.TableTarget[Employee],
) -> None:
    value = msg.value()
    if value is None:
        return
    text = value.decode() if isinstance(value, bytes) else value
    row = json.loads(text)
    if "sku" in row:
        products_table.declare_row(row=Product(**{**row, "price": float(row["price"])}))
    elif "emp_id" in row:
        employees_table.declare_row(row=Employee(**row))

@coco.fn
async def app_main() -> None:
    products_table = await lancedb.mount_table_target(
        LANCE_DB, table_name="products",
        table_schema=await lancedb.TableSchema.from_class(Product, primary_key=["sku"]))
    employees_table = await lancedb.mount_table_target(
        LANCE_DB, table_name="employees",
        table_schema=await lancedb.TableSchema.from_class(Employee, primary_key=["emp_id"]))
    config = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "group.id": KAFKA_GROUP_ID,
              "enable.auto.commit": "false", "auto.offset.reset": "earliest"}
    consumer = AIOConsumer(config)
    items = kafka.topic_as_map(consumer, [KAFKA_TOPIC])
    await coco.mount_each(process_message, items, products_table, employees_table)
```

The line worth pausing on is `declare_row` — deliberately *not* `upsert()`. A new or changed primary key is upserted; a tombstone (null value) removes that key's row; the same row declared again writes nothing. `enable.auto.commit` is **off** on purpose: CocoIndex commits each offset *after* the row is durably written, so the consumer group resumes from the last message it actually persisted.

<p align="center">
  📘 <b><a href="https://cocoindex.io/docs/examples/kafka-to-lancedb/">Full Tutorial →</a></b><br/>
  Step-by-step walkthrough with the row schemas, shape dispatch, offset-after-write delivery, and exactly what each message does.
</p>

## Why it's worth a star ⭐

- **Sort the mail.** Branch on a discriminator field (`sku` vs `emp_id`) and declare a typed row — the same pattern whether the destination is LanceDB, Postgres, or a vector index. A message matching neither shape is quietly skipped.
- **At-least-once, your offsets won't drift.** Auto-commit off + offset-after-write means `__consumer_offsets` never runs ahead of the data; a crash mid-flight replays from the last committed offset.
- **The dataclass is the schema.** [`TableSchema.from_class`](https://cocoindex.io/docs/connectors/lancedb/) maps your dataclass to LanceDB/PyArrow column types — `Product` keyed by `sku`, `Employee` by `emp_id`, the same keys the messages carried in.
- **Embedded target, no server.** LanceDB tables are just files under `./lancedb_data` — there's nothing to run alongside the consumer.
- **Live mode is one flag.** `-L` is the entire difference between draining the backlog and exiting versus consuming forever — `process_message` doesn't change.

## Run it

> Needs a running Kafka broker with a topic to consume. The easy way to populate it: run [csv-to-kafka](https://github.com/cocoindex-io/cocoindex/tree/main/examples/csv_to_kafka) first.

**1. Configure & install** — point `KAFKA_TOPIC` at the topic csv-to-kafka produced (its default is `cocoindex-csv-rows`):

```sh
cp .env.example .env     # set KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC / LANCEDB_URI (+ SASL for a managed broker)
pip install -e .
```

**2. Run the pipeline** — choose catch-up (drain what's there, then exit) or live (catch up, then keep consuming):

```sh
# Catch-up run: consume everything up to now, write the rows, then exit
cocoindex update main.py

# Live run: catch up, then keep consuming new messages
cocoindex update -L main.py
```

**3. Look at the tables** — they're just files under `./lancedb_data`:

```python
import lancedb

db = lancedb.connect("./lancedb_data")
for row in db.open_table("products").to_arrow().to_pylist():
    print(row)
for row in db.open_table("employees").to_arrow().to_pylist():
    print(row)
```

Every `sku` message is a row in `products`, every `emp_id` message a row in `employees` — keyed exactly as it was on the topic, so re-consuming the same key updates the row in place rather than duplicating.

---

<p align="center">
  If this sorted your topic into typed tables, <a href="https://github.com/cocoindex-io/cocoindex"><b>give CocoIndex a star ⭐</b></a> — it helps a lot.<br/>
  <a href="https://cocoindex.io/docs">Docs</a> · <a href="https://cocoindex.io/docs/examples/kafka-to-lancedb/">Walkthrough</a> · <a href="https://discord.com/invite/zpA9S2DR7s">Discord</a> · <a href="https://github.com/cocoindex-io/cocoindex/tree/main/examples"><b>See all examples →</b></a>
</p>

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=7f27e85b-be3a-411a-b612-0b9d53711814&page=examples/kafka_to_lancedb" alt="" width="1" height="1" />
