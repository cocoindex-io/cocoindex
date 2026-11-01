"""App-level tests for the Iggy target connector's container-deletion semantics.

These tests stub the Python Iggy SDK to verify the connector without a real
Iggy server. The stream/topic is user-managed, so CocoIndex reconciles its
messages one by one only while the topic itself stays declared. Removing the
topic declaration, or dropping the app, abandons the topic: no deletion
messages are sent for the messages it held.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, NamedTuple

import pytest
import pytest_asyncio

from tests.common.iggy_stub import MockIggyClient

# ``tests.common.iggy_stub`` must be imported before the connector so that the
# connector binds the stub SDK.
import cocoindex as coco
from cocoindex.connectors import iggy
from tests import common

_CLIENT_KEY: coco.ContextKey[Any] = coco.ContextKey("test_iggy_target_client")


def _deletion_value(key: bytes | str) -> bytes:
    return b"DEL:" + (key if isinstance(key, bytes) else key.encode())


@dataclass
class _TopicScenario:
    """What ``_app_main`` declares; tests mutate it between runs."""

    declare_topic: bool = True
    messages: dict[str, bytes] = field(default_factory=dict)


@coco.fn
async def _app_main(scenario: _TopicScenario) -> None:
    if not scenario.declare_topic:
        return
    target = await iggy.mount_iggy_topic_target(
        _CLIENT_KEY, "s1", "t1", deletion_value_fn=_deletion_value
    )
    for key, value in scenario.messages.items():
        target.declare_target_state(key=key, value=value)


class _TopicApp(NamedTuple):
    app: coco.App[Any, Any]
    client: MockIggyClient
    scenario: _TopicScenario


@pytest_asyncio.fixture
async def topic_app(request: pytest.FixtureRequest) -> _TopicApp:
    client = MockIggyClient()
    # Created inside an async fixture so the Environment binds to the test's
    # running event loop.
    env = common.create_test_env(__file__, suffix=request.node.name)
    env.context_provider.provide(_CLIENT_KEY, client)
    scenario = _TopicScenario()
    app = coco.App(
        coco.AppConfig(name=request.node.name, environment=env), _app_main, scenario
    )
    return _TopicApp(app=app, client=client, scenario=scenario)


@pytest.mark.asyncio
async def test_app_child_removal_sends_deletion_value(topic_app: _TopicApp) -> None:
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()
    assert topic_app.client.sent_payloads == [b"v1"]

    topic_app.client.clear()
    del topic_app.scenario.messages["k1"]
    await topic_app.app.update()
    assert topic_app.client.sent_payloads == [b"DEL:k1"]


@pytest.mark.asyncio
async def test_app_parent_removal_abandons_messages(topic_app: _TopicApp) -> None:
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()

    topic_app.client.clear()
    topic_app.scenario.declare_topic = False
    await topic_app.app.update()
    # Intended: un-declaring the user-managed topic abandons it, messages
    # included, so no deletion value is sent for k1.
    assert topic_app.client.sent_payloads == []


@pytest.mark.asyncio
async def test_app_drop_abandons_messages(topic_app: _TopicApp) -> None:
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()

    topic_app.client.clear()
    await topic_app.app.drop()
    # Intended: dropping the app abandons the user-managed topic, messages
    # included, so no deletion value is sent for k1.
    assert topic_app.client.sent_payloads == []


@pytest.mark.asyncio
async def test_app_redeclared_topic_starts_from_scratch(
    topic_app: _TopicApp,
) -> None:
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()
    topic_app.scenario.declare_topic = False
    await topic_app.app.update()

    # Abandoning the topic pruned k1's tracking entry, so bringing the topic
    # back without k1 has nothing to delete...
    topic_app.client.clear()
    topic_app.scenario.declare_topic = True
    del topic_app.scenario.messages["k1"]
    await topic_app.app.update()
    assert topic_app.client.sent_payloads == []

    # ...and re-declaring k1 with its old value resends it as new.
    topic_app.scenario.messages["k1"] = b"v1"
    await topic_app.app.update()
    assert topic_app.client.sent_payloads == [b"v1"]
