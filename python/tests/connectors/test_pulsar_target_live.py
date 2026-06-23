"""End-to-end test for the Pulsar target connector against a live broker.

Unlike ``test_pulsar_target.py`` (which mocks the client), this drives the real
produce path through ``_MessageHandler._apply_actions`` and consumes the topic
back to assert the message payload, partition key, and tombstone behaviour.

It is **skipped unless** ``PULSAR_SERVICE_URL`` is set, so it never runs in the
default (broker-less) suite.

Run it against a local Pulsar standalone::

    docker run -d --name pulsar -p 6650:6650 -p 8080:8080 \
        apachepulsar/pulsar:3.3.1 bin/pulsar standalone

    PULSAR_SERVICE_URL=pulsar://localhost:6650 \
        pytest python/tests/connectors/test_pulsar_target_live.py -v
"""

from __future__ import annotations

import os
import sys
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("PULSAR_SERVICE_URL"),
    reason="set PULSAR_SERVICE_URL to run the Pulsar target connector against a live broker",
)


def _real_pulsar() -> Any:
    """Return the real ``pulsar-client`` module.

    ``test_pulsar_target.py`` installs a mock into ``sys.modules['pulsar']`` at
    import time; drop it so a live run uses the genuine client.
    """
    sys.modules.pop("pulsar", None)
    import pulsar  # type: ignore[import-not-found]

    return pulsar


@pytest.mark.asyncio
async def test_pulsar_target_produces_and_deletes_against_live_broker() -> None:
    pulsar = _real_pulsar()
    from cocoindex.connectors.pulsar._target import _MessageAction, _MessageHandler

    service_url = os.environ["PULSAR_SERVICE_URL"]
    topic = f"persistent://public/default/coco-e2e-{uuid.uuid4().hex}"
    key = "order-123"
    value = b'{"status":"ready"}'

    client = pulsar.Client(service_url)
    handler = _MessageHandler(client=client, topic=topic, deletion_value_fn=None)
    context_provider = MagicMock()

    try:
        # 1. Upsert: a message is produced with the target-state key as the
        #    Pulsar partition key and the value as the payload.
        await handler._apply_actions(context_provider, [_MessageAction(key=key, value=value)])

        reader = client.create_reader(topic, pulsar.MessageId.earliest)
        try:
            upsert = reader.read_next(timeout_millis=15000)
            assert upsert.data() == value
            assert upsert.partition_key() == key

            # 2. Delete: a tombstone is produced as an empty payload (no
            #    deletion_value_fn was configured) under the same key.
            await handler._apply_actions(
                context_provider, [_MessageAction(key=key, value=None)]
            )
            tombstone = reader.read_next(timeout_millis=15000)
            assert tombstone.data() == b""
            assert tombstone.partition_key() == key
        finally:
            reader.close()
    finally:
        client.close()
