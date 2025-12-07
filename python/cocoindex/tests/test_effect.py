from typing import Any, Collection, Literal, NamedTuple

import pytest

import cocoindex as coco
from cocoindex.tests.environment import create_test_env

coco_env = create_test_env("effect")

# === Define effects on a stored dict ===


class _DictDataWithPrev(NamedTuple):
    data: Any
    prev: Collection[Any]
    prev_may_be_missing: bool


MetricsName = Literal["sink", "upsert", "delete"]


class Metrics:
    _data: dict[MetricsName, int]

    def __init__(self) -> None:
        self._data = {}

    def increment(self, metric: MetricsName) -> None:
        self._data[metric] = self._data.get(metric, 0) + 1

    def collect(self) -> dict[MetricsName, int]:
        m = self._data
        self._data = {}
        return m


class _DictEffectStore:
    data: dict[str, _DictDataWithPrev]
    metrics: Metrics

    def __init__(self) -> None:
        self.data = {}
        self.metrics = Metrics()

    def _sink(
        self,
        actions: Collection[tuple[str, _DictDataWithPrev | coco.NonExistenceType]],
    ) -> None:
        for key, value in actions:
            if coco.is_non_existence(value):
                del self.data[key]
                self.metrics.increment("delete")
            else:
                self.data[key] = value
                self.metrics.increment("upsert")
        self.metrics.increment("sink")

    def reconcile(
        self,
        key: str,
        desired_effect: Any | coco.NonExistenceType,
        prev_possible_states: Collection[Any],
        prev_may_be_missing: bool,
    ) -> (
        coco.EffectReconcileOutput[
            tuple[str, _DictDataWithPrev | coco.NonExistenceType], Any
        ]
        | None
    ):
        # Short-circuit no-change case
        if coco.is_non_existence(desired_effect):
            if len(prev_possible_states) == 0:
                return None
        else:
            if not prev_may_be_missing and all(
                prev == desired_effect for prev in prev_possible_states
            ):
                return None

        new_value = (
            coco.NON_EXISTENCE
            if coco.is_non_existence(desired_effect)
            else _DictDataWithPrev(
                data=desired_effect,
                prev=prev_possible_states,
                prev_may_be_missing=prev_may_be_missing,
            )
        )
        return coco.EffectReconcileOutput(
            action=(key, new_value),
            sink=coco.EffectSink.from_fn(self._sink),
            state=desired_effect,
        )

    def clear(self) -> None:
        self.data.clear()


_global_dict_effect_store = _DictEffectStore()
_global_dict_effect_provider = coco.register_root_effect_provider(
    "test_effect/global_dict",
    coco.EffectReconciler.from_fn(_global_dict_effect_store.reconcile),
)


# === Tests on the global dict effect ===

_source_data: dict[str, Any] = {}


@coco.function
def declare_global_dict_entries(csp: coco.StatePath) -> None:
    for key, value in _source_data.items():
        coco.declare_effect(_global_dict_effect_provider.effect(key, value))


def test_global_dict_effect_insert() -> None:
    _global_dict_effect_store.clear()
    _source_data.clear()

    app = coco.App(
        declare_global_dict_entries,
        coco.AppConfig(name="test_global_dict_effect_insert", environment=coco_env),
    )

    _source_data["a"] = 1
    app.update()
    assert _global_dict_effect_store.data == {
        "a": _DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {"sink": 1, "upsert": 1}

    _source_data["b"] = 2
    app.update()
    assert _global_dict_effect_store.data == {
        "a": _DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_global_dict_effect_upsert() -> None:
    _global_dict_effect_store.clear()
    _source_data.clear()

    app = coco.App(
        declare_global_dict_entries,
        coco.AppConfig(name="test_global_dict_effect_upsert", environment=coco_env),
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.update()
    assert _global_dict_effect_store.data == {
        "a": _DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {"sink": 1, "upsert": 2}

    _source_data["a"] = 3
    app.update()
    assert _global_dict_effect_store.data == {
        "a": _DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": _DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {"sink": 1, "upsert": 1}


def test_global_dict_effect_delete() -> None:
    _global_dict_effect_store.clear()
    _source_data.clear()

    app = coco.App(
        declare_global_dict_entries,
        coco.AppConfig(name="test_global_dict_effect_delete", environment=coco_env),
    )

    _source_data["a"] = 1
    _source_data["b"] = 2
    app.update()
    assert _global_dict_effect_store.metrics.collect() == {"sink": 1, "upsert": 2}

    del _source_data["a"]
    app.update()
    assert _global_dict_effect_store.data == {
        "b": _DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {"sink": 1, "delete": 1}


def test_global_dict_effect_no_change() -> None:
    _global_dict_effect_store.clear()
    _source_data.clear()

    app = coco.App(
        declare_global_dict_entries,
        coco.AppConfig(name="test_global_dict_effect_no_change", environment=coco_env),
    )

    _source_data["a"] = 1
    _source_data["b"] = 2

    app.update()
    assert _global_dict_effect_store.data == {
        "a": _DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {"sink": 1, "upsert": 2}

    app.update()
    assert _global_dict_effect_store.data == {
        "a": _DictDataWithPrev(data=1, prev=[], prev_may_be_missing=True),
        "b": _DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {}

    _source_data["a"] = 3

    app.update()
    assert _global_dict_effect_store.data == {
        "a": _DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": _DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {"sink": 1, "upsert": 1}

    app.update()
    assert _global_dict_effect_store.data == {
        "a": _DictDataWithPrev(data=3, prev=[1], prev_may_be_missing=False),
        "b": _DictDataWithPrev(data=2, prev=[], prev_may_be_missing=True),
    }
    assert _global_dict_effect_store.metrics.collect() == {}


# --- Define two-level effects on a multiple dicts ---


class _DictsEffectStore:
    _stores: dict[str, _DictEffectStore]
    metrics: Metrics

    def __init__(self) -> None:
        self._stores = {}
        self.metrics = Metrics()

    def _sink(
        self, actions: Collection[tuple[str, bool]]
    ) -> list[coco.EffectReconciler[str]]:
        child_recons = []
        for name, exists in actions:
            if exists:
                if name not in self._stores:
                    self._stores[name] = _DictEffectStore()
                    self.metrics.increment("upsert")
            else:
                del self._stores[name]
                self.metrics.increment("delete")
            child_recons.append(
                coco.EffectReconciler.from_fn(self._stores[name].reconcile)
            )
        self.metrics.increment("sink")
        return child_recons

    def reconcile(
        self,
        key: str,
        desired_effect: None | coco.NonExistenceType,
        prev_possible_states: Collection[Any],
        prev_may_be_missing: bool,
    ) -> (
        coco.EffectReconcileOutput[tuple[str, bool], None, coco.EffectReconciler[str]]
        | None
    ):
        if desired_effect is coco.NON_EXISTENCE:
            if len(prev_possible_states) == 0:
                return None
        else:
            return coco.EffectReconcileOutput(
                action=(key, False),
                sink=coco.EffectSink.from_fn(self._sink),
                state=coco.NON_EXISTENCE,
            )

        if not prev_may_be_missing:
            return None
        else:
            return coco.EffectReconcileOutput(
                action=(key, True),
                sink=coco.EffectSink.from_fn(self._sink),
                state=None,
            )

    def clear(self) -> None:
        self._stores.clear()

    @property
    def data(self) -> dict[str, dict[str, _DictDataWithPrev]]:
        return {name: store.data for name, store in self._stores.items()}


_dicts_effect_store = _DictsEffectStore()
_dicts_effect_provider = coco.register_root_effect_provider(
    "test_effect/dicts",
    coco.EffectReconciler.from_fn(_dicts_effect_store.reconcile),
)

_multi_source_data: dict[str, dict[str, Any]] = {}
