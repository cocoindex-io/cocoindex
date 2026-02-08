import uuid
import pytest
from typing import Any, Collection, NamedTuple
import cocoindex as coco
from tests import common
from tests.common.target_states import DictDataWithPrev, Metrics
import threading


coco_env = common.create_test_env(__file__)


class AnyKeyDictTargetStateStore:
    data: dict[Any, DictDataWithPrev]
    metrics: Metrics
    _lock: threading.Lock
    _use_async: bool
    sink_exception: bool = False

    def __init__(self, use_async: bool = False) -> None:
        self.data = {}
        self.metrics = Metrics()
        self._lock = threading.Lock()
        self._use_async = use_async

    def _sink(
        self,
        actions: Collection[tuple[Any, DictDataWithPrev | coco.NonExistenceType]],
    ) -> None:
        if self.sink_exception:
            raise ValueError("injected sink exception")
        with self._lock:
            for key, value in actions:
                if coco.is_non_existence(value):
                    self.data.pop(key, None)
                    self.metrics.increment("delete")
                else:
                    self.data[key] = value
                    self.metrics.increment("upsert")
            self.metrics.increment("sink")

    async def _async_sink(
        self,
        actions: Collection[tuple[Any, DictDataWithPrev | coco.NonExistenceType]],
    ) -> None:
        self._sink(actions)

    def reconcile(
        self,
        key: Any,
        desired_state: Any | coco.NonExistenceType,
        prev_possible_states: Collection[Any],
        prev_may_be_missing: bool,
    ) -> (
        coco.TargetReconcileOutput[
            tuple[Any, DictDataWithPrev | coco.NonExistenceType], Any
        ]
        | None
    ):
        # Short-circuit no-change case
        if coco.is_non_existence(desired_state):
            if len(prev_possible_states) == 0:
                return None
        else:
            if not prev_may_be_missing and all(
                prev == desired_state for prev in prev_possible_states
            ):
                return None

        new_value = (
            coco.NON_EXISTENCE
            if coco.is_non_existence(desired_state)
            else DictDataWithPrev(
                data=desired_state,
                prev=prev_possible_states,
                prev_may_be_missing=prev_may_be_missing,
            )
        )
        return coco.TargetReconcileOutput(
            action=(key, new_value),
            sink=(
                coco.TargetActionSink.from_async_fn(self._async_sink)
                if self._use_async
                else coco.TargetActionSink.from_fn(self._sink)
            ),
            tracking_record=desired_state,
        )

    def clear(self) -> None:
        self.data.clear()
        self.metrics.clear()


class AnyKeyTarget:
    store = AnyKeyDictTargetStateStore()
    _provider = coco.register_root_target_states_provider(
        "test_target_key_types/any_key", store
    )
    target_state = _provider.target_state


def _declare_any_key_data(data: dict[Any, Any]) -> None:
    for key, value in data.items():
        coco.declare_target_state(AnyKeyTarget.target_state(key, value))


def test_valid_stable_keys() -> None:
    coco_env = common.create_test_env("test_valid_stable_keys")
    AnyKeyTarget.store.clear()

    app = coco.App(
        coco.AppConfig(name="test_valid_stable_keys", environment=coco_env),
        lambda: None,
    )

    test_data = {
        "str_key": "value1",
        123: "value2",
        True: "value3",
        b"bytes": "value4",
        (1, "tuple"): "value5",
    }

    uid = uuid.uuid4()
    test_data[uid] = "value6"

    def declare_keys() -> None:
        _declare_any_key_data(test_data)

    app = coco.App(
        coco.AppConfig(name="test_valid_stable_keys_run", environment=coco_env),
        declare_keys,
    )
    app.update()

    stored_data = AnyKeyTarget.store.data

    assert stored_data["str_key"].data == "value1"
    assert stored_data[123].data == "value2"
    assert stored_data[True].data == "value3"
    assert stored_data[b"bytes"].data == "value4"
    assert stored_data[(1, "tuple")].data == "value5"
    assert stored_data[uid].data == "value6"

    AnyKeyTarget.store.clear()

    def declare_none_key() -> None:
        coco.declare_target_state(AnyKeyTarget.target_state(None, "none_val"))

    app = coco.App(
        coco.AppConfig(name="test_none_key", environment=coco_env), declare_none_key
    )
    app.update()
    assert AnyKeyTarget.store.data[None].data == "none_val"


def test_invalid_keys() -> None:
    coco_env = common.create_test_env("test_invalid_keys")
    AnyKeyTarget.store.clear()

    class Foo:
        pass

    def declare_invalid_key() -> None:
        coco.declare_target_state(AnyKeyTarget.target_state(Foo(), "val"))

    app = coco.App(
        coco.AppConfig(name="test_invalid_keys", environment=coco_env),
        declare_invalid_key,
    )
    with pytest.raises(TypeError, match="Unsupported StableKey Python type"):
        app.update()


def test_nested_container_keys() -> None:
    coco_env = common.create_test_env("test_nested_container_keys")
    AnyKeyTarget.store.clear()
    key_input = [1, 2]
    key_expected = (1, 2)

    def declare_list_key() -> None:
        coco.declare_target_state(AnyKeyTarget.target_state(key_input, "val"))

    app = coco.App(
        coco.AppConfig(name="test_nested_keys", environment=coco_env), declare_list_key
    )
    app.update()

    assert key_expected in AnyKeyTarget.store.data
    assert AnyKeyTarget.store.data[key_expected].data == "val"
