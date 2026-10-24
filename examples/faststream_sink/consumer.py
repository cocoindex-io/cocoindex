"""Consume the example topic into a durable SQLite projection."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import dotenv
from aiokafka import AIOKafkaConsumer, TopicPartition

from .connection import Config, drain_cleanup, verify_cluster
from .projection import Projection
from .protocol import decode_event


async def consume(path: Path) -> None:
    config = Config.from_env()
    await verify_cluster(config)
    projection = Projection(path, config.route)
    try:
        consumer = AIOKafkaConsumer(
            config.topic,
            bootstrap_servers=config.servers,
            group_id=projection.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            isolation_level="read_committed",
        )
        try:
            await consumer.start()
            async for message in consumer:
                if message.value is None:
                    raise ValueError(
                        "This protocol uses revisioned deletes, not Kafka null tombstones"
                    )
                event = decode_event(message.value)
                await asyncio.to_thread(projection.apply, event)
                # A crash before this commit replays an already-committed SQLite row.
                await consumer.commit(
                    {
                        TopicPartition(message.topic, message.partition): message.offset
                        + 1
                    }
                )
        finally:
            await drain_cleanup(consumer.stop())
    finally:
        projection.close()


def main() -> None:
    dotenv.load_dotenv(Path(__file__).with_name(".env"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database", type=Path, default=Path("faststream-projection.sqlite3")
    )
    args = parser.parse_args()
    asyncio.run(consume(args.database))


if __name__ == "__main__":
    main()
