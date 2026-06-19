# Sources and Targets

A CocoIndex pipeline reads from a source and writes to a target. The source is
the system of record; the target is derived data that CocoIndex keeps in sync.

A source provides items to process. CocoIndex supports sources such as the local
file system, Amazon S3, Google Drive, and Kafka. The local file system source
walks a directory and yields one file per item.

A target receives declared target states. CocoIndex supports targets such as
Postgres, Qdrant, LanceDB, and Neo4j. A target state describes what should
exist — a database row, a file, or a graph node — and the engine creates,
updates, or deletes it to match.

A processing component connects a source item to its target states. CocoIndex
mounts one processing component per item, and each component owns the target
states it declares.
