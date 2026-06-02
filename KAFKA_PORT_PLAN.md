# Rust Kafka target connector + csv-to-kafka example — plan

> Scratch plan; delete when done & tested.

## Thought process (from cocoindex docs + target-connector skill)

CocoIndex is **declarative**: you declare target states; the engine reconciles
(create / update / skip-unchanged / delete-orphan). A target connector implements
`TargetHandler::reconcile` (pure, no I/O — decide an action by comparing the
desired state's fingerprint against previous tracking records) + a
`TargetActionSink` (batched async I/O against the external system).

Python Kafka target (`connectors/kafka/_target.py`) is two-level:
topic (user-managed container, generation tracking) → message (fingerprint
tracking; upsert→produce, delete→tombstone or deletion_value_fn).

**Rust mapping.** Mirror the established Rust idiom (`fs::DirTarget`, `postgres`):
a **single root provider** (the topic is user-managed, no create/drop needed —
matches Python "CocoIndex does not create/drop topics"). The producer connection
is captured directly in the handler/sink closures (Rust has no context-resolution
indirection), keyed for *stable identity* by `state_id` (bootstrap servers), per
the skill's "ContextKey for resource identity" rule.

- **Client:** `rskafka` — pure-Rust, async, **no librdkafka/cmake** (none on this
  box). CI-tested against Redpanda (which is running on :9092 here).
- **Reconcile (fingerprint, like `fs`/`postgres` row):** upsert new/changed →
  produce; unchanged → `None` (skip); orphan → tombstone (value=None) or
  `deletion_value_fn(key)`; `prev_may_be_missing` forces produce.
- **Sink:** lazily open a `PartitionClient` (partition 0) via `OnceCell`, produce
  `Record { key, value, ... }`. tombstone = `value: None`.
- **API:** `KafkaProducer::connect(brokers)`, `.ensure_topic(topic, partitions)`
  (explicit, idempotent — not part of reconcile), `mount_kafka_topic_target(ctx,
  &producer, topic, options)` → `KafkaTopicTarget::declare_message(ctx, key, value)`.

Scope/parity notes (documented, not hidden): partition 0 only (no key-hash fan-out
yet); SASL/TLS not ported (example/tests use plaintext localhost). fs::walk is
one-shot, so the example runs `index` once (Python uses `live=True`).

## Test strategy (write first)

**Unit (no broker), in `kafka.rs` `#[cfg(test)]` — mirror Python target tests:**
- U1 upsert new (prev empty, may_be_missing) → action(key,value), tracking Some
- U2 unchanged (prev=[fp], !missing) → None
- U3 changed value → action with new value
- U4 prev_may_be_missing + same fp → still produces
- U5 delete, no callback, prev=[fp] → tombstone (value None), tracking None
- U6 delete, with deletion_value_fn → value Some(fn(key))
- U7 delete, no prev, !missing → None (skip)

**Integration (live, skip if no `KAFKA_BOOTSTRAP_SERVERS`), `tests/kafka_target.rs`:**
Run a real source→topic pipeline, consume back, assert by **high-watermark count**
(append-only log makes "did we re-produce?" observable):
- T1 declare 2 msgs → run → consume: 2 records, correct key/value, hw=2
- T2 re-run unchanged → hw still 2 (memo/skip — no re-produce)
- T3 change 1 value → run → hw=3, latest record for that key = new value
- T4 stop declaring 1 (delete) → run → hw=4, latest record for that key = tombstone(None)
- cleanup: delete topic

## Steps
- [ ] Cargo.toml: `kafka` feature + `rskafka` (default-features=false) dep
- [ ] `src/kafka.rs` + `lib.rs`/`prelude` wiring (feature-gated)
- [ ] Unit tests (U1–U7), build default + `--features kafka`
- [ ] `tests/kafka_target.rs` (T1–T4), run live vs Redpanda :9092
- [ ] Example `examples/rust/csv-to-kafka` (index + consume), validate live
- [ ] fmt + clippy (no new warnings in my files); delete this plan
